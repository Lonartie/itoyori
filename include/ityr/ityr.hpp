#pragma once

#include "ityr/common/util.hpp"
#include "ityr/common/options.hpp"
#include "ityr/common/topology.hpp"
#include "ityr/common/wallclock.hpp"
#include "ityr/common/profiler.hpp"
#include "ityr/ito/ito.hpp"
#include "ityr/ori/ori.hpp"
#include "ityr/pattern/count_iterator.hpp"
#include "ityr/pattern/root_exec.hpp"
#include "ityr/pattern/parallel_invoke.hpp"
#include "ityr/pattern/parallel_loop.hpp"
#include "ityr/pattern/parallel_reduce.hpp"
#include "ityr/pattern/parallel_filter.hpp"
#include "ityr/pattern/parallel_merge.hpp"
#include "ityr/pattern/parallel_sort.hpp"
#include "ityr/pattern/parallel_search.hpp"
#include "ityr/pattern/parallel_shuffle.hpp"
#include "ityr/pattern/random.hpp"
#include "ityr/pattern/reducer_extra.hpp"
#include "ityr/container/global_span.hpp"
#include "ityr/container/global_vector.hpp"
#include "ityr/container/checkout_span.hpp"
#include "ityr/container/workhint.hpp"
#include "ityr/container/unique_file_ptr.hpp"

namespace ityr {

namespace internal {

class ityr {
public:
  ityr(MPI_Comm comm)
    : mi_(comm),
      topo_(comm),
      ito_(comm),
      ori_(comm) {}

private:
  common::mpi_initializer                                    mi_;
  common::runtime_options                                    opts_;
  common::singleton_initializer<common::topology::instance>  topo_;
  common::singleton_initializer<common::wallclock::instance> clock_;
  common::singleton_initializer<common::profiler::instance>  prof_;
  common::singleton_initializer<ito::instance>               ito_;
  common::singleton_initializer<ori::instance>               ori_;
};

using instance = common::singleton<ityr>;

}

/**
 * @brief Initialize Itoyori (collective).
 *
 * @param comm MPI communicator to be used in Itoyori (default: `MPI_COMM_WORLD`).
 *
 * This function initializes the Itoyori runtime system.
 * Any itoyori APIs (except for runtime option settings) cannot be called prior to this call.
 * `ityr::fini()` must be called to properly release allocated resources.
 *
 * If MPI is not initialized at this point, Itoyori calls `MPI_Init()` and finalizes MPI by calling
 * `MPI_Finalize()` when `ityr::fini()` is called. If MPI is already initialized at this point,
 * Itoyori does not have the responsibility to finalize MPI.
 *
 * @see `ityr::fini()`
 */
inline void init(MPI_Comm comm = MPI_COMM_WORLD) {
  internal::instance::init(comm);
}

/**
 * @brief Finalize Itoyori (collective).
 *
 * This function finalizes the Itoyori runtime system.
 * Any itoyori APIs cannot be called after this call unless `ityr::init()` is called again.
 *
 * `MPI_Finalize()` may be called in this function if MPI is not initialized by the user when
 * `ityr::init()` is called.
 *
 * @see `ityr::init()`
 */
inline void fini() {
  internal::instance::fini();
}

/**
 * @brief Process rank (ID) starting from 0 (corresponding to an MPI rank).
 * @see `ityr::my_rank()`
 * @see `ityr::n_ranks()`
 */
using rank_t = common::topology::rank_t;

/**
 * @brief Return the rank of the process running the current thread.
 * @see `ityr::n_ranks()`
 */
inline rank_t my_rank() {
  return common::topology::my_rank();
}

/**
 * @brief Return the total number of processes.
 * @see `ityr::n_ranks()`
 */
inline rank_t n_ranks() {
  return common::topology::n_ranks();
}

/**
 * @brief Return true if `ityr::my_rank() == 0`.
 * @see `ityr::my_rank()`
 */
inline bool is_master() {
  return my_rank() == 0;
}

/**
 * @brief Return true if the current thread is the root thread.
 */
inline bool is_root() {
  return ito::is_root();
}

/**
 * @brief Migrate the current thread to `target_rank`. For the root thread only.
 */
inline void migrate_to(rank_t target_rank) {
  ito::migrate_to(target_rank, [] { ori::release(); }, [] { ori::acquire(); });
}

/**
 * @brief Migrate the current thread to the master worker (of rank 0).
 */
inline void migrate_to_master() {
  ito::migrate_to(0, [] { ori::release(); }, [] { ori::acquire(); });
}

/**
 * @brief Return true if the current execution context is within the SPMD region.
 */
inline bool is_spmd() {
  return ito::is_spmd();
}

/**
 * @brief Barrier for all processes (collective).
 */
inline void barrier() {
  ITYR_CHECK(is_spmd());
  ori::release();
  common::mpi_barrier(common::topology::mpicomm());
  ori::acquire();
}

/**
 * @brief Wallclock time in nanoseconds.
 * @see `ityr::gettime_ns()`.
 */
using wallclock_t = common::wallclock::wallclock_t;

/**
 * @brief Return the current wallclock time in nanoseconds.
 *
 * The wallclock time is calibrated across different processes (that may reside in different machines)
 * at the program startup in a simple way, but the clock may be skewed due to various reasons.
 * To get an accurate execution time, it is recommended to call this function in the same process and
 * calculate the difference.
 */
inline wallclock_t gettime_ns() {
  return common::wallclock::gettime_ns();
}

/**
 * @brief Start the profiler (collective).
 * @see `ityr::profiler_end()`.
 * @see `ityr::profiler_flush()`.
 */
inline void profiler_begin() {
  ITYR_CHECK(is_spmd());
  ori::cache_prof_begin();
  ito::dag_prof_begin();
  common::profiler::begin();
#if ITYR_DEBUG_UCX
  common::ityr_ucx_log_enable(1);
  ucs_info("Itoyori profiler begin");
#endif
}

/**
 * @brief Stop the profiler (collective).
 * @see `ityr::profiler_begin()`.
 * @see `ityr::profiler_flush()`.
 */
inline void profiler_end() {
  ITYR_CHECK(is_spmd());
#if ITYR_DEBUG_UCX
  ucs_info("Itoyori profiler end");
  common::ityr_ucx_log_enable(0);
#endif
  common::profiler::end();
  ito::dag_prof_end();
  ori::cache_prof_end();
}

/**
 * @brief Print the profiled results to stdout (collective).
 * @see `ityr::profiler_begin()`.
 * @see `ityr::profiler_end()`.
 */
inline void profiler_flush() {
  ITYR_CHECK(is_spmd());
#if ITYR_DEBUG_UCX
  common::ityr_ucx_log_flush();
#endif
  common::profiler::flush();
  ito::dag_prof_print();
  ori::cache_prof_print();

  // print byte size measurements
  ityr::root_exec([=]() -> void {
      ityr::global_vector<std::size_t> get(ityr::n_ranks()), put(ityr::n_ranks()), cas(ityr::n_ranks()),
            faa(ityr::n_ranks()), faog(ityr::n_ranks()), faop(ityr::n_ranks()), send(ityr::n_ranks()),
            recv(ityr::n_ranks()), brdc(ityr::n_ranks()), rcvc(ityr::n_ranks()), sendc(ityr::n_ranks());
      ityr::global_span<std::size_t> sget(get), sput(put), scas(cas), sfaa(faa), sfaog(faog), sfaop(faop), ssend(send),
            srecv(recv), sbrdc(brdc), srcvc(rcvc), ssendc(sendc);

      ityr::global_vector<std::size_t> stolen_count(ityr::n_ranks()), stolen_size(ityr::n_ranks());
      ityr::global_span<std::size_t> s_stolen_count(stolen_count), s_stolen_size(stolen_size);

      ityr::coll_exec([=]() {
         // printf("%d stole %llu tasks", ityr::my_rank(), ityr::ito::STOLEN_FRAMES_COUNT);
         auto c_stolen_size = ityr::make_checkout(s_stolen_size.data() + ityr::my_rank(), 1, ityr::checkout_mode::write);
         c_stolen_size[0] = ityr::ito::STOLEN_FRAMES_SIZE;

         auto c_stolen_count = ityr::make_checkout(s_stolen_count.data() + ityr::my_rank(), 1, ityr::checkout_mode::write);
         c_stolen_count[0] = ityr::ito::STOLEN_FRAMES_COUNT;

         auto cget = ityr::make_checkout(sget.data() + ityr::my_rank(), 1, ityr::checkout_mode::write);
         cget[0] = ityr::common::RMA_GET_DATA_SIZE;

         auto cput = ityr::make_checkout(sput.data() + ityr::my_rank(), 1, ityr::checkout_mode::write);
         cput[0] = ityr::common::RMA_PUT_DATA_SIZE;

         auto ccas = ityr::make_checkout(scas.data() + ityr::my_rank(), 1, ityr::checkout_mode::write);
         ccas[0] = ityr::common::RMA_CAS_DATA_SIZE;

         auto cfaa = ityr::make_checkout(sfaa.data() + ityr::my_rank(), 1, ityr::checkout_mode::write);
         cfaa[0] = ityr::common::RMA_FAA_DATA_SIZE;

         auto cfaog = ityr::make_checkout(sfaog.data() + ityr::my_rank(), 1, ityr::checkout_mode::write);
         cfaog[0] = ityr::common::RMA_FAO_GET_DATA_SIZE;

         auto cfaop = ityr::make_checkout(sfaop.data() + ityr::my_rank(), 1, ityr::checkout_mode::write);
         cfaop[0] = ityr::common::RMA_FAO_PUT_DATA_SIZE;

         auto csend = ityr::make_checkout(ssend.data() + ityr::my_rank(), 1, ityr::checkout_mode::write);
         csend[0] = ityr::common::MPI_SEND_SIZE;

         auto crecv = ityr::make_checkout(srecv.data() + ityr::my_rank(), 1, ityr::checkout_mode::write);
         crecv[0] = ityr::common::MPI_RECV_SIZE;

         auto cbrdc = ityr::make_checkout(sbrdc.data() + ityr::my_rank(), 1, ityr::checkout_mode::write);
         cbrdc[0] = ityr::common::MPI_BROADCAST_SIZE;

         auto crecvc = ityr::make_checkout(srcvc.data() + ityr::my_rank(), 1, ityr::checkout_mode::write);
         crecvc[0] = ityr::common::MPI_RECV_SIZE;

         auto csendc = ityr::make_checkout(ssendc.data() + ityr::my_rank(), 1, ityr::checkout_mode::write);
         csendc[0] = ityr::common::MPI_BROADCAST_SIZE;
      });

      {
         auto size = ityr::make_checkout(s_stolen_size, ityr::checkout_mode::read);
         auto count = ityr::make_checkout(s_stolen_count, ityr::checkout_mode::read);
         std::size_t gsize = 0, gcount = 0;
         for (int i = 0; i < ityr::n_ranks(); i++) {
            gsize += size[i];
            gcount += count[i];
         }
         printf("stolen %llu (%llu bytes total, %f bytes avg)\n", gcount, gsize, (float)gsize / (float)gcount);
      }

      {
         auto cget = ityr::make_checkout(sget, ityr::checkout_mode::read);
         uint64_t sum = 0;
         for (auto& v: cget) {
            sum += v;
         }
         printf("get: %llu\n", sum);
      }

      {
         auto cput = ityr::make_checkout(sput, ityr::checkout_mode::read);
         uint64_t sum = 0;
         for (auto& v: cput) {
            sum += v;
         }
         printf("put: %llu\n", sum);
      }

      {
         auto ccas = ityr::make_checkout(scas, ityr::checkout_mode::read);
         uint64_t sum = 0;
         for (auto& v: ccas) {
            sum += v;
         }
         printf("cas: %llu\n", sum);
      }

      {
         auto cfaa = ityr::make_checkout(sfaa, ityr::checkout_mode::read);
         uint64_t sum = 0;
         for (auto& v: cfaa) {
            sum += v;
         }
         printf("faa: %llu\n", sum);
      }

      {
         auto cfaog = ityr::make_checkout(sfaog, ityr::checkout_mode::read);
         uint64_t sum = 0;
         for (auto& v: cfaog) {
            sum += v;
         }
         printf("faog: %llu\n", sum);
      }

      {
         auto cfaop = ityr::make_checkout(sfaop, ityr::checkout_mode::read);
         uint64_t sum = 0;
         for (auto& v: cfaop) {
            sum += v;
         }
         printf("faop: %llu\n", sum);
      }

      {
         auto csend = ityr::make_checkout(ssend, ityr::checkout_mode::read);
         uint64_t sum = 0;
         for (auto& v: csend) {
            sum += v;
         }
         printf("send: %llu\n", sum);
      }

      {
         auto crecv = ityr::make_checkout(srecv, ityr::checkout_mode::read);
         uint64_t sum = 0;
         for (auto& v: crecv) {
            sum += v;
         }
         printf("recv: %llu\n", sum);
      }

      {
         auto cbrdc = ityr::make_checkout(sbrdc, ityr::checkout_mode::read);
         uint64_t sum = 0;
         for (auto& v: cbrdc) {
            sum += v;
         }
         printf("brdc: %llu\n", sum);
      }

      {
         auto csendc = ityr::make_checkout(ssendc, ityr::checkout_mode::read);
         uint64_t sum = 0;
         for (auto& v: csendc) {
            sum += v;
         }
         printf("send#: %llu\n", sum);
      }

      {
         auto crecvc = ityr::make_checkout(srcvc, ityr::checkout_mode::read);
         uint64_t sum = 0;
         for (auto& v: crecvc) {
            sum += v;
         }
         printf("recv#: %llu\n", sum);
      }
   });
}

/**
 * @brief Print the compile-time options to stdout.
 * @see `ityr::print_runtime_options()`.
 */
inline void print_compile_options() {
  common::print_compile_options();
  ito::print_compile_options();
  ori::print_compile_options();
}

/**
 * @brief Print the runtime options to stdout.
 * @see `ityr::print_compile_options()`.
 */
inline void print_runtime_options() {
  common::print_runtime_options();
}

}
