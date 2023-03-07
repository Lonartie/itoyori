#pragma once

#include "ityr/common/util.hpp"
#include "ityr/common/mpi_util.hpp"
#include "ityr/common/mpi_rma.hpp"
#include "ityr/common/topology.hpp"
#include "ityr/common/allocator.hpp"
#include "ityr/common/profiler.hpp"
#include "ityr/ito/context.hpp"
#include "ityr/ito/wsqueue.hpp"
#include "ityr/ito/callstack.hpp"

namespace ityr::ito {

class scheduler {
public:
  struct no_retval_t {};

  struct suspended_state {
    void*       evacuation_ptr;
    void*       frame_base;
    std::size_t frame_size;
  };

  template <typename T>
  struct thread_state {
    T               retval;
    int             resume_flag = 0;
    suspended_state suspended;
  };

  template <typename T>
  struct thread_handler {
    thread_state<T>* state      = nullptr;
    bool             serialized = false;
    T                retval_ser; // return the result by value if the thread is serialized
  };

  scheduler(const callstack& stack)
    : stack_(stack),
      wsq_(common::getenv_coll("ITYR_ITO_WSQUEUE_CAPACITY", 1024, common::topology::mpicomm())) {}

  template <typename T, typename Fn, typename... Args>
  T root_exec(Fn&& fn, Args&&... args) {
    thread_state<T>* ts = new (thread_state_allocator_.allocate(sizeof(thread_state<T>))) thread_state<T>;

    suspend([&, ts](context_frame* cf) {
      sched_cf_ = cf;
      cf_top_ = reinterpret_cast<context_frame*>(stack_.bottom());
      root_on_stack([&, ts, fn, args...]() {
        common::verbose("Starting root thread %p", ts);

        T retval = invoke_fn<T>(fn, args...);

        common::verbose("Root thread %p is completed", ts);

        on_root_die(ts, retval);
      });
    });

    sched_loop([=]() { return ts->resume_flag >= 1; });

    T retval = ts->retval;
    std::destroy_at(ts);
    thread_state_allocator_.deallocate(ts, sizeof(thread_state<T>));
    return retval;
  }

  template <typename T, typename Fn, typename... Args>
  thread_handler<T> fork(Fn&& fn, Args&&... args) {
    thread_state<T>* ts = new (thread_state_allocator_.allocate(sizeof(thread_state<T>))) thread_state<T>;
    thread_handler<T> th;
    th.state = ts;

    suspend([&, ts, fn, args...](context_frame* cf) {
      common::verbose("push context frame [%p, %p) into task queue", cf, cf->parent_frame);

      std::size_t cf_size = reinterpret_cast<uintptr_t>(cf->parent_frame) - reinterpret_cast<uintptr_t>(cf);
      wsq_.push(wsqueue_entry{cf, cf_size});

      common::verbose("Starting new thread %p", ts);

      T retval = invoke_fn<T>(fn, args...);

      common::verbose("Thread %p is completed", ts);

      on_die(ts, retval);

      common::verbose("Thread %p is serialized (fast path)", ts);

      // The following is executed only when the thread is serialized
      std::destroy_at(ts);
      thread_state_allocator_.deallocate(ts, sizeof(thread_state<T>));
      th.state      = nullptr;
      th.serialized = true;
      th.retval_ser = retval;

      common::verbose("Resume parent context frame [%p, %p) (fast path)", cf, cf->parent_frame);
    });

    return th;
  }

  template <typename T>
  T join(thread_handler<T>& th) {
    if (th.serialized) {
      common::verbose("Skip join for serialized thread (fast path)");
      // We can skip deallocaton for its thread state because it has been already deallocated
      // when the thread is serialized (i.e., at a fork)
      return th.retval_ser;
    }

    ITYR_CHECK(th.state != nullptr);
    thread_state<T>* ts = th.state;

    T retval;
    if (remote_get_value(thread_state_allocator_, &ts->resume_flag) >= 1) {
      common::verbose("Thread %p is already joined", ts);
      if constexpr (!std::is_same_v<T, no_retval_t>) {
        retval = remote_get_value(thread_state_allocator_, &ts->retval);
      }

    } else {
      suspend([&, ts](context_frame* cf) {
        std::size_t cf_size = reinterpret_cast<uintptr_t>(cf->parent_frame) - reinterpret_cast<uintptr_t>(cf);
        void* evacuation_ptr = suspended_thread_allocator_.allocate(cf_size);
        std::memcpy(evacuation_ptr, cf, cf_size);

        common::verbose("Evacuate suspended thread context [%p, %p) to %p",
                        cf, cf->parent_frame, evacuation_ptr);

        suspended_state ss {evacuation_ptr, cf, cf_size};
        remote_put_value(thread_state_allocator_, ss, &ts->suspended);

        // race
        if (remote_faa_value(thread_state_allocator_, 1, &ts->resume_flag) == 0) {
          common::verbose("Win the join race for thread %p (joining thread)", ts);
          resume_sched();
        } else {
          common::verbose("Lose the join race for thread %p (joining thread)", ts);
          suspended_thread_allocator_.deallocate(ss.evacuation_ptr, ss.frame_size);
          resume(cf);
        }
      });

      common::verbose("Resume continuation of join for thread %p", ts);

      if constexpr (!std::is_same_v<T, no_retval_t>) {
        retval = remote_get_value(thread_state_allocator_, &ts->retval);
      }
    }

    std::destroy_at(ts);
    thread_state_allocator_.deallocate(ts, sizeof(thread_state<T>));
    th.state = nullptr;
    return retval;
  }

  template <typename CondFn>
  void sched_loop(CondFn&& cond_fn) {
    common::verbose("Enter scheduling loop");

    while (!should_exit_sched_loop(std::forward<CondFn>(cond_fn))) {
      steal();
    }

    common::verbose("Exit scheduling loop");
  }

private:
  template <typename T, typename Fn, typename... Args>
  T invoke_fn(Fn&& fn, Args&&... args) {
    T retval;
    if constexpr (!std::is_same_v<T, no_retval_t>) {
      retval = fn(args...);
    } else {
      fn(args...);
    }
    return retval;
  }

  template <typename T>
  void on_die(thread_state<T>* ts, const T& retval) {
    auto qe = wsq_.pop();
    if (!qe.has_value()) {
      if constexpr (!std::is_same_v<T, no_retval_t>) {
        remote_put_value(thread_state_allocator_, retval, &ts->retval);
      }
      // race
      if (remote_faa_value(thread_state_allocator_, 1, &ts->resume_flag) == 0) {
        common::verbose("Win the join race for thread %p (joined thread)", ts);
        resume_sched();
      } else {
        common::verbose("Lose the join race for thread %p (joined thread)", ts);
        suspended_state ss = remote_get_value(thread_state_allocator_, &ts->suspended);
        resume(ss);
      }
    }
  }

  template <typename T>
  void on_root_die(thread_state<T>* ts, const T& retval) {
    if constexpr (!std::is_same_v<T, no_retval_t>) {
      remote_put_value(thread_state_allocator_, retval, &ts->retval);
    }
    remote_put_value(thread_state_allocator_, 1, &ts->resume_flag);
    resume_sched();
  }

  common::topology::rank_t get_random_rank() {
    ITYR_CHECK(common::topology::n_ranks() > 1);
    static std::mt19937 engine(std::random_device{}());
    static std::uniform_int_distribution<common::topology::rank_t> dist(0, common::topology::n_ranks() - 2);

    auto rank = dist(engine);
    if (rank >= common::topology::my_rank()) rank++;

    ITYR_CHECK(0 <= rank);
    ITYR_CHECK(rank != common::topology::my_rank());
    ITYR_CHECK(rank < common::topology::n_ranks());
    return rank;
  }

  void steal() {
    auto target_rank = get_random_rank();

    auto ibd = common::profiler::interval_begin<prof_event_steal>(target_rank);

    if (wsq_.empty(target_rank)) {
      common::profiler::interval_end<prof_event_steal>(ibd, false);
      return;
    }

    if (!wsq_.lock().trylock(target_rank)) {
      common::profiler::interval_end<prof_event_steal>(ibd, false);
      return;
    }

    auto we = wsq_.steal_nolock(target_rank);
    if (!we.has_value()) {
      wsq_.lock().unlock(target_rank);
      common::profiler::interval_end<prof_event_steal>(ibd, false);
      return;
    }

    common::verbose("Steal context frame [%p, %p) from rank %d",
                    we->frame_base, reinterpret_cast<std::byte*>(we->frame_base) + we->frame_size, target_rank);

    stack_.direct_copy_from(we->frame_base, we->frame_size, target_rank);

    wsq_.lock().unlock(target_rank);

    common::profiler::interval_end<prof_event_steal>(ibd, true);

    context_frame* next_cf = reinterpret_cast<context_frame*>(we->frame_base);
    suspend([&](context_frame* cf) {
      sched_cf_ = cf;
      resume(next_cf);
    });
  }

  template <typename Fn>
  void suspend(Fn&& fn) {
    context_frame* prev_cf_top = cf_top_;
    context::save_context_with_call(prev_cf_top,
        [](context_frame* cf, void* cf_top_p, void* fn_p) {
      context_frame*& cf_top = *reinterpret_cast<context_frame**>(cf_top_p);
      Fn              fn     = *reinterpret_cast<Fn*>(fn_p); // copy closure to the new stack frame
      cf_top = cf;
      fn(cf);
    }, &cf_top_, &fn);
    cf_top_ = prev_cf_top;
  }

  void resume(context_frame* cf) {
    common::verbose("Resume context frame [%p, %p) in the stack", cf, cf->parent_frame);
    context::resume(cf);
  }

  void resume(suspended_state ss) {
    common::verbose("Resume context frame [%p, %p) evacuated at %p",
                    ss.frame_base, ss.frame_size, ss.evacuation_ptr);

    // We pass the suspended thread states *by value* because the current local variables can be overwritten by the
    // new stack we will bring from remote nodes.
    context::jump_to_stack(ss.frame_base, [](void* allocator_, void* evacuation_ptr, void* frame_base, void* frame_size_) {
      common::remotable_resource& allocator  = *reinterpret_cast<common::remotable_resource*>(allocator_);
      std::size_t                 frame_size = reinterpret_cast<std::size_t>(frame_size_);
      common::remote_get(allocator,
                         reinterpret_cast<std::byte*>(frame_base),
                         reinterpret_cast<std::byte*>(evacuation_ptr),
                         frame_size);
      allocator.deallocate(evacuation_ptr, frame_size);
      context_frame* cf = reinterpret_cast<context_frame*>(frame_base);
      context::resume(cf);
    }, &suspended_thread_allocator_, ss.evacuation_ptr, ss.frame_base, reinterpret_cast<void*>(ss.frame_size));
  }

  void resume_sched() {
    cf_top_ = nullptr;
    common::verbose("Resume scheduler context");
    context::resume(sched_cf_);
  }

  template <typename Fn>
  void root_on_stack(Fn&& fn) {
    context::call_on_stack(stack_.top(), stack_.size(), [](void* fn_, void*, void*, void*) {
      Fn fn = *reinterpret_cast<Fn*>(fn_); // copy closure to the new stack frame
      fn();
    }, &fn, nullptr, nullptr, nullptr);
  }

  template <typename CondFn>
  bool should_exit_sched_loop(CondFn&& cond_fn) {
    if (sched_loop_exit_req_ == MPI_REQUEST_NULL &&
        std::forward<CondFn>(cond_fn)()) {
      // If a given condition is met, enters a barrier
      sched_loop_exit_req_ = common::mpi_ibarrier(common::topology::mpicomm());
    }
    if (sched_loop_exit_req_ != MPI_REQUEST_NULL) {
      // If the barrier is resolved, the scheduler loop should terminate
      return common::mpi_test(sched_loop_exit_req_);
    }
    return false;
  }

  struct wsqueue_entry {
    void*       frame_base;
    std::size_t frame_size;
  };

  class prof_event_steal : public common::profiler::event {
  public:
    using common::profiler::event::event;
    auto interval_begin(common::topology::rank_t target_rank [[maybe_unused]]) {
      return common::profiler::event::interval_begin();
    }
    void interval_end(interval_begin_data ibd, bool success) {
      if (state_.enabled) {
        auto t = common::wallclock::gettime_ns();
        auto t0 = ibd;
        if (success) {
          acc_time_success_ += t - t0;
          count_success_++;
        } else {
          acc_time_fail_ += t - t0;
          count_fail_++;
        }
      }
    }
    std::string str() const override {
      return success_mode ? "steal_success" : "steal_fail";
    }
    void flush() override {
      success_mode = true;
      acc_time_ = acc_time_success_;
      count_ = count_success_;
      common::profiler::event::flush();
      success_mode = false;
      acc_time_ = acc_time_fail_;
      count_ = count_fail_;
      common::profiler::event::flush();
    }
    void clear() override {
      acc_time_success_ = 0;
      acc_time_fail_    = 0;
      count_success_    = 0;
      count_fail_       = 0;
    }
  private:
    common::wallclock::wallclock_t acc_time_success_ = 0;
    common::wallclock::wallclock_t acc_time_fail_    = 0;
    counter_t                      count_success_    = 0;
    counter_t                      count_fail_       = 0;
    bool                           success_mode;
  };
  common::profiler::event_initializer<prof_event_steal> prof_event_steal_;

  const callstack&           stack_;
  wsqueue<wsqueue_entry>     wsq_;
  common::remotable_resource thread_state_allocator_;
  common::remotable_resource suspended_thread_allocator_;
  context_frame*             cf_top_               = nullptr;
  context_frame*             sched_cf_             = nullptr;
  MPI_Request                sched_loop_exit_req_  = MPI_REQUEST_NULL;
};

}
