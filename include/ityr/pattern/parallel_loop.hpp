#pragma once

#include "ityr/common/util.hpp"
#include "ityr/common/topology.hpp"
#include "ityr/ito/ito.hpp"
#include "ityr/ori/ori.hpp"
#include "ityr/pattern/count_iterator.hpp"
#include "ityr/pattern/global_iterator.hpp"
#include "ityr/pattern/serial_loop.hpp"
#include "ityr/pattern/reducer.hpp"

namespace ityr {

namespace internal {

template <typename Op, typename ReleaseHandler,
          typename ForwardIterator, typename... ForwardIterators>
inline void parallel_loop_generic(execution::parallel_policy policy,
                                  Op                         op,
                                  ReleaseHandler             rh,
                                  ForwardIterator            first,
                                  ForwardIterator            last,
                                  ForwardIterators...        firsts) {
  ori::poll();

  // for immediately executing cross-worker tasks in ADWS
  ito::poll([] { return ori::release_lazy(); },
            [&](ori::release_handler rh_) { ori::acquire(rh); ori::acquire(rh_); });

  auto d = std::distance(first, last);
  if (static_cast<std::size_t>(d) <= policy.cutoff_count) {
    auto seq_policy = execution::internal::to_sequenced_policy(policy);
    for_each_aux(seq_policy, [&](auto&&... its) {
      op(std::forward<decltype(its)>(its)...);
    }, first, last, firsts...);
    return;
  }

  auto mid = std::next(first, d / 2);

  auto tgdata = ito::task_group_begin();

  ito::thread<void> th(
      ito::with_callback, [=] { ori::acquire(rh); }, [] { ori::release(); },
      ito::with_workhint, 1, 1,
      [=] {
        parallel_loop_generic(policy, op, rh,
                              first, mid, firsts...);
      });

  parallel_loop_generic(policy, op, rh,
                        mid, last, std::next(firsts, d / 2)...);

  if (!th.serialized()) {
    ori::release();
  }

  th.join();

  ito::task_group_end(tgdata, [] { ori::release(); }, [] { ori::acquire(); });

  // TODO: needed?
  if (!th.serialized()) {
    ori::acquire();
  }
}

template <typename AccumulateOp, typename CombineOp, typename Reducer, typename AccView,
          typename ReleaseHandler, typename ForwardIterator, typename... ForwardIterators>
inline AccView parallel_reduce_generic(execution::parallel_policy policy,
                                       AccumulateOp               accumulate_op,
                                       CombineOp                  combine_op,
                                       Reducer                    reducer,
                                       AccView                    acc,
                                       ReleaseHandler             rh,
                                       ForwardIterator            first,
                                       ForwardIterator            last,
                                       ForwardIterators...        firsts) {
  ori::poll();

  // for immediately executing cross-worker tasks in ADWS
  ito::poll([] { return ori::release_lazy(); },
            [&](ori::release_handler rh_) { ori::acquire(rh); ori::acquire(rh_); });

  auto d = std::distance(first, last);
  if (static_cast<std::size_t>(d) <= policy.cutoff_count) {
    auto seq_policy = execution::internal::to_sequenced_policy(policy);
    for_each_aux(seq_policy, [&](auto&&... its) {
      accumulate_op(acc, std::forward<decltype(its)>(its)...);
    }, first, last, firsts...);
    return acc;
  }

  auto mid = std::next(first, d / 2);

  auto tgdata = ito::task_group_begin();

  ito::thread<AccView> th(
      ito::with_callback, [=] { ori::acquire(rh); }, [] { ori::release(); },
      ito::with_workhint, 1, 1,
      [=] {
        return parallel_reduce_generic(policy, accumulate_op, combine_op, reducer,
                                       acc, rh, first, mid, firsts...);
      });

  if (th.serialized()) {
    AccView acc1 = th.join();
    acc1 = parallel_reduce_generic(policy, accumulate_op, combine_op, reducer,
                                   acc1, rh, mid, last, std::next(firsts, d / 2)...);

    ito::task_group_end(tgdata, [] { ori::release(); }, [] { ori::acquire(); });

    return acc1;

  } else {
    auto new_acc = reducer.identity();

    AccView acc2 = parallel_reduce_generic(policy, accumulate_op, combine_op, reducer,
                                           reducer.view(new_acc), rh, mid, last, std::next(firsts, d / 2)...);

    ori::release();

    AccView acc1 = th.join();

    ito::task_group_end(tgdata, [] { ori::release(); }, [] { ori::acquire(); });

    ori::acquire();

    combine_op(acc1, acc2, first, mid, last, firsts...);

    return acc1;
  }
}

template <typename Op, typename ForwardIterator, typename... ForwardIterators>
inline void loop_generic(const execution::sequenced_policy& policy,
                         Op                                 op,
                         ForwardIterator                    first,
                         ForwardIterator                    last,
                         ForwardIterators...                firsts) {
  execution::internal::assert_policy(policy);
  auto seq_policy = execution::internal::to_sequenced_policy(policy);
  for_each_aux(seq_policy, [&](auto&&... its) {
    op(std::forward<decltype(its)>(its)...);
  }, first, last, firsts...);
}

template <typename Op, typename ForwardIterator, typename... ForwardIterators>
inline void loop_generic(const execution::parallel_policy& policy,
                         Op                                op,
                         ForwardIterator                   first,
                         ForwardIterator                   last,
                         ForwardIterators...               firsts) {
  execution::internal::assert_policy(policy);
  auto rh = ori::release_lazy();
  parallel_loop_generic(policy, op, rh, first, last, firsts...);
}

template <typename AccumulateOp, typename CombineOp, typename Reducer, typename AccView,
          typename ForwardIterator, typename... ForwardIterators>
inline AccView reduce_generic(const execution::sequenced_policy& policy,
                              AccumulateOp                       accumulate_op,
                              CombineOp                          combine_op [[maybe_unused]],
                              Reducer                            reducer [[maybe_unused]],
                              AccView                            acc,
                              ForwardIterator                    first,
                              ForwardIterator                    last,
                              ForwardIterators...                firsts) {
  execution::internal::assert_policy(policy);
  auto seq_policy = execution::internal::to_sequenced_policy(policy);
  for_each_aux(seq_policy, [&](auto&&... its) {
    accumulate_op(acc, std::forward<decltype(its)>(its)...);
  }, first, last, firsts...);
  return acc;
}

template <typename AccumulateOp, typename CombineOp, typename Reducer, typename AccView,
          typename ForwardIterator, typename... ForwardIterators>
inline AccView reduce_generic(const execution::parallel_policy& policy,
                              AccumulateOp                      accumulate_op,
                              CombineOp                         combine_op,
                              Reducer                           reducer,
                              AccView                           acc,
                              ForwardIterator                   first,
                              ForwardIterator                   last,
                              ForwardIterators...               firsts) {
  execution::internal::assert_policy(policy);
  auto rh = ori::release_lazy();
  return parallel_reduce_generic(policy, accumulate_op, combine_op, reducer, acc,
                                 rh, first, last, firsts...);
}

}

/**
 * @brief Apply an operator to each element in a range.
 *
 * @param policy Execution policy (`ityr::execution`).
 * @param first  Begin iterator.
 * @param last   End iterator.
 * @param op     Operator for the i-th element in the range.
 *
 * This function iterates over the given range and applies the operator `op` to the i-th element.
 * The operator `op` should accept an argument of type `T`, which is the reference type of the
 * given iterator type.
 * This function resembles the standard `std::for_each()`, but it is extended to accept multiple
 * streams of iterators.
 *
 * Global pointers are not automatically checked out. If global iterators are explicitly given
 * (by `ityr::make_global_iterator`), the regions are automatically checked out with the specified
 * mode in the specified granularity (`ityr::execution::sequenced_policy::checkout_count` if serial,
 * or `ityr::execution::parallel_policy::checkout_count` if parallel).
 *
 * Example:
 * ```
 * ityr::global_vector<int> v1 = {1, 2, 3, 4, 5};
 * ityr::for_each(ityr::execution::par,
 *                ityr::make_global_iterator(v1.begin(), ityr::checkout_mode::read_write),
 *                ityr::make_global_iterator(v1.end()  , ityr::checkout_mode::read_write),
 *                [](int& x) { x++; });
 * // v1 = {2, 3, 4, 5, 6}
 * ```
 *
 * @see [std::for_each -- cppreference.com](https://en.cppreference.com/w/cpp/algorithm/for_each)
 * @see `ityr::reduce()`
 * @see `ityr::transform()`
 * @see `ityr::transform_reduce()`
 * @see `ityr::execution::sequenced_policy`, `ityr::execution::seq`,
 *      `ityr::execution::parallel_policy`, `ityr::execution::par`
 */
template <typename ExecutionPolicy, typename ForwardIterator, typename Op>
inline void for_each(const ExecutionPolicy& policy,
                     ForwardIterator        first,
                     ForwardIterator        last,
                     Op                     op) {
  internal::loop_generic(policy, op, first, last);
}

/**
 * @brief Apply an operator to each element in a range.
 *
 * @param policy Execution policy (`ityr::execution`).
 * @param first1 1st begin iterator.
 * @param last1  1st end iterator.
 * @param first2 2nd begin iterator.
 * @param op     Operator for the i-th element in the 1st and 2nd iterators.
 *
 * This function iterates over the given ranges and applies the operator `op` to the i-th elements.
 * The operator `op` should accept three arguments of type `T1` and `T2`, which are the reference
 * types of the given iterators `first1` and `first2`.
 * This function resembles the standard `std::for_each()`, but it is extended to accept multiple
 * streams of iterators.
 *
 * Global pointers are not automatically checked out. If global iterators are explicitly given
 * (by `ityr::make_global_iterator`), the regions are automatically checked out with the specified
 * mode in the specified granularity (`ityr::execution::sequenced_policy::checkout_count` if serial,
 * or `ityr::execution::parallel_policy::checkout_count` if parallel).
 *
 * Example:
 * ```
 * ityr::global_vector<int> v1 = {1, 2, 3, 4, 5};
 * ityr::global_vector<int> v2 = {1, 2, 3, 4, 5};
 * ityr::for_each(ityr::execution::par,
 *                ityr::make_global_iterator(v1.begin(), ityr::checkout_mode::read),
 *                ityr::make_global_iterator(v1.end()  , ityr::checkout_mode::read),
 *                ityr::make_global_iterator(v2.begin(), ityr::checkout_mode::read_write),
 *                [](int x, int& y) { y += x; });
 * // v2 = {2, 4, 6, 8, 10}
 * ```
 *
 * @see [std::for_each -- cppreference.com](https://en.cppreference.com/w/cpp/algorithm/for_each)
 * @see `ityr::reduce()`
 * @see `ityr::transform()`
 * @see `ityr::transform_reduce()`
 * @see `ityr::execution::sequenced_policy`, `ityr::execution::seq`,
 *      `ityr::execution::parallel_policy`, `ityr::execution::par`
 */
template <typename ExecutionPolicy, typename ForwardIterator1, typename ForwardIterator2, typename Op>
inline void for_each(const ExecutionPolicy& policy,
                     ForwardIterator1       first1,
                     ForwardIterator1       last1,
                     ForwardIterator2       first2,
                     Op                     op) {
  internal::loop_generic(policy, op, first1, last1, first2);
}

/**
 * @brief Apply an operator to each element in a range.
 *
 * @param policy Execution policy (`ityr::execution`).
 * @param first1 1st begin iterator.
 * @param last1  1st end iterator.
 * @param first2 2nd begin iterator.
 * @param first3 3rd begin iterator.
 * @param op     Operator for the i-th element in the 1st, 2nd, and 3rd iterators.
 *
 * This function iterates over the given ranges and applies the operator `op` to the i-th elements.
 * The operator `op` should accept three arguments of type `T1`, `T2`, and `T3`, which are the
 * reference types of the given iterators `first1`, `first2`, and `first3`.
 * This function resembles the standard `std::for_each()`, but it is extended to accept multiple
 * streams of iterators.
 *
 * Global pointers are not automatically checked out. If global iterators are explicitly given
 * (by `ityr::make_global_iterator`), the regions are automatically checked out with the specified
 * mode in the specified granularity (`ityr::execution::sequenced_policy::checkout_count` if serial,
 * or `ityr::execution::parallel_policy::checkout_count` if parallel).
 *
 * Example:
 * ```
 * ityr::global_vector<int> v1 = {1, 1, 1, 1, 1};
 * ityr::global_vector<int> v2(v1.size());
 * ityr::for_each(ityr::execution::par,
 *                ityr::count_iterator<int>(0),
 *                ityr::count_iterator<int>(5),
 *                ityr::make_global_iterator(v1.begin(), ityr::checkout_mode::read),
 *                ityr::make_global_iterator(v2.begin(), ityr::checkout_mode::write),
 *                [](int i, int x, int& y) { y = x << i; });
 * // v2 = {1, 2, 4, 8, 16}
 * ```
 *
 * @see [std::for_each -- cppreference.com](https://en.cppreference.com/w/cpp/algorithm/for_each)
 * @see `ityr::reduce()`
 * @see `ityr::transform()`
 * @see `ityr::transform_reduce()`
 * @see `ityr::execution::sequenced_policy`, `ityr::execution::seq`,
 *      `ityr::execution::parallel_policy`, `ityr::execution::par`
 */
template <typename ExecutionPolicy, typename ForwardIterator1, typename ForwardIterator2,
          typename ForwardIterator3, typename Op>
inline void for_each(const ExecutionPolicy& policy,
                     ForwardIterator1       first1,
                     ForwardIterator1       last1,
                     ForwardIterator2       first2,
                     ForwardIterator3       first3,
                     Op                     op) {
  internal::loop_generic(policy, op, first1, last1, first2, first3);
}

ITYR_TEST_CASE("[ityr::pattern::serial_loop] serial for_each") {
  class move_only_t {
  public:
    move_only_t() {}
    move_only_t(const long v) : value_(v) {}

    long value() const { return value_; }

    move_only_t(const move_only_t&) = delete;
    move_only_t& operator=(const move_only_t&) = delete;

    move_only_t(move_only_t&& mo) : value_(mo.value_) {
      mo.value_ = -1;
    }
    move_only_t& operator=(move_only_t&& mo) {
      value_ = mo.value_;
      mo.value_ = -1;
      return *this;
    }

  private:
    long value_ = -1;
  };

  ori::init();

  long n = 100000;

  ITYR_SUBCASE("without global_ptr") {
    ITYR_SUBCASE("count iterator") {
      long count = 0;
      for_each(execution::seq,
               count_iterator<long>(0),
               count_iterator<long>(n),
               [&](long i) { count += i; });
      ITYR_CHECK(count == n * (n - 1) / 2);

      count = 0;
      for_each(
          execution::seq,
          count_iterator<long>(0),
          count_iterator<long>(n),
          count_iterator<long>(n),
          [&](long i, long j) { count += i + j; });
      ITYR_CHECK(count == 2 * n * (2 * n - 1) / 2);

      count = 0;
      for_each(
          execution::seq,
          count_iterator<long>(0),
          count_iterator<long>(n),
          count_iterator<long>(n),
          count_iterator<long>(2 * n),
          [&](long i, long j, long k) { count += i + j + k; });
      ITYR_CHECK(count == 3 * n * (3 * n - 1) / 2);
    }

    ITYR_SUBCASE("vector copy") {
      std::vector<long> mos1(count_iterator<long>(0),
                                    count_iterator<long>(n));

      std::vector<long> mos2;
      for_each(
          execution::seq,
          mos1.begin(), mos1.end(),
          std::back_inserter(mos2),
          [&](long i, auto&& out) { out = i; });

      long count = 0;
      for_each(
          execution::seq,
          mos2.begin(), mos2.end(),
          [&](long i) { count += i; });
      ITYR_CHECK(count == n * (n - 1) / 2);
    }

    ITYR_SUBCASE("move iterator with vector") {
      std::vector<move_only_t> mos1(count_iterator<long>(0),
                                    count_iterator<long>(n));

      std::vector<move_only_t> mos2;
      for_each(
          execution::seq,
          std::make_move_iterator(mos1.begin()),
          std::make_move_iterator(mos1.end()),
          std::back_inserter(mos2),
          [&](move_only_t&& in, auto&& out) { out = std::move(in); });

      long count = 0;
      for_each(
          execution::seq,
          mos2.begin(), mos2.end(),
          [&](move_only_t& mo) { count += mo.value(); });
      ITYR_CHECK(count == n * (n - 1) / 2);

      for_each(
          execution::seq,
          mos1.begin(), mos1.end(),
          [&](move_only_t& mo) { ITYR_CHECK(mo.value() == -1); });
    }
  }

  ITYR_SUBCASE("with global_ptr") {
    ori::global_ptr<long> gp = ori::malloc<long>(n);

    for_each(
        execution::seq,
        count_iterator<long>(0),
        count_iterator<long>(n),
        make_global_iterator(gp, checkout_mode::write),
        [&](long i, long& out) { new (&out) long(i); });

    ITYR_SUBCASE("read array without global_iterator") {
      long count = 0;
      for_each(
          execution::seq,
          gp,
          gp + n,
          [&](ori::global_ref<long> gr) { count += gr; });
      ITYR_CHECK(count == n * (n - 1) / 2);
    }

    ITYR_SUBCASE("read array with global_iterator") {
      long count = 0;
      for_each(
          execution::seq,
          make_global_iterator(gp    , checkout_mode::read),
          make_global_iterator(gp + n, checkout_mode::read),
          [&](long i) { count += i; });
      ITYR_CHECK(count == n * (n - 1) / 2);
    }

    ITYR_SUBCASE("move iterator") {
      ori::global_ptr<move_only_t> mos1 = ori::malloc<move_only_t>(n);
      ori::global_ptr<move_only_t> mos2 = ori::malloc<move_only_t>(n);

      for_each(
          execution::seq,
          make_global_iterator(gp    , checkout_mode::read),
          make_global_iterator(gp + n, checkout_mode::read),
          make_global_iterator(mos1  , checkout_mode::write),
          [&](long i, move_only_t& out) { new (&out) move_only_t(i); });

      for_each(
          execution::seq,
          make_move_iterator(mos1),
          make_move_iterator(mos1 + n),
          make_global_iterator(mos2, checkout_mode::write),
          [&](move_only_t&& in, move_only_t& out) { new (&out) move_only_t(std::move(in)); });

      long count = 0;
      for_each(
          execution::seq,
          make_global_iterator(mos2    , checkout_mode::read),
          make_global_iterator(mos2 + n, checkout_mode::read),
          [&](const move_only_t& mo) { count += mo.value(); });
      ITYR_CHECK(count == n * (n - 1) / 2);

      for_each(
          execution::seq,
          make_global_iterator(mos1    , checkout_mode::read),
          make_global_iterator(mos1 + n, checkout_mode::read),
          [&](const move_only_t& mo) { ITYR_CHECK(mo.value() == -1); });

      ori::free(mos1, n);
      ori::free(mos2, n);
    }

    ori::free(gp, n);
  }

  ori::fini();
}

ITYR_TEST_CASE("[ityr::pattern::parallel_loop] parallel for_each") {
  ito::init();
  ori::init();

  int n = 100000;
  ori::global_ptr<int> p1 = ori::malloc_coll<int>(n);
  ori::global_ptr<int> p2 = ori::malloc_coll<int>(n);

  ito::root_exec([=] {
    int count = 0;
    for_each(
        execution::sequenced_policy{.checkout_count = 100},
        make_global_iterator(p1    , checkout_mode::write),
        make_global_iterator(p1 + n, checkout_mode::write),
        [&](int& v) { v = count++; });

    for_each(
        execution::par,
        make_global_iterator(p1    , checkout_mode::read),
        make_global_iterator(p1 + n, checkout_mode::read),
        count_iterator<int>(0),
        [=](int x, int i) { ITYR_CHECK(x == i); });

    for_each(
        execution::par,
        count_iterator<int>(0),
        count_iterator<int>(n),
        make_global_iterator(p1, checkout_mode::read),
        [=](int i, int x) { ITYR_CHECK(x == i); });

    for_each(
        execution::par,
        make_global_iterator(p1    , checkout_mode::read),
        make_global_iterator(p1 + n, checkout_mode::read),
        make_global_iterator(p2    , checkout_mode::write),
        [=](int x, int& y) { y = x * 2; });

    for_each(
        execution::par,
        make_global_iterator(p2    , checkout_mode::read_write),
        make_global_iterator(p2 + n, checkout_mode::read_write),
        [=](int& y) { y *= 2; });

    for_each(
        execution::par,
        count_iterator<int>(0),
        count_iterator<int>(n),
        make_global_iterator(p2, checkout_mode::read),
        [=](int i, int y) { ITYR_CHECK(y == i * 4); });
  });

  ori::free_coll(p1);
  ori::free_coll(p2);

  ori::fini();
  ito::fini();
}

/**
 * @brief Calculate reduction while transforming each element.
 *
 * @param policy             Execution policy (`ityr::execution`).
 * @param first              Begin iterator.
 * @param last               End iterator.
 * @param reducer            Reducer object (`ityr::reducer`).
 * @param unary_transform_op Unary operator to transform each element.
 *
 * @return The reduced result.
 *
 * This function applies `unary_transform_op` to each element in the range `[first, last)` and
 * performs reduction over them.
 *
 * If global pointers are provided as iterators, they are automatically checked out with the read-only
 * mode in the specified granularity (`ityr::execution::sequenced_policy::checkout_count` if serial,
 * or `ityr::execution::parallel_policy::checkout_count` if parallel) without explicitly passing them
 * as global iterators.
 *
 * Example:
 * ```
 * ityr::global_vector<int> v1 = {1, 2, 3, 4, 5};
 * int r = ityr::transform_reduce(ityr::execution::par, v1.begin(), v1.end(), ityr::reducer::plus<int>{},
 *                                [](int x) { return x * x; });
 * // r = 55
 * ```
 *
 * @see [std::transform_reduce -- cppreference.com](https://en.cppreference.com/w/cpp/algorithm/transform_reduce)
 * @see `ityr::reduce()`
 * @see `ityr::transform()`
 * @see `ityr::execution::sequenced_policy`, `ityr::execution::seq`,
 *      `ityr::execution::parallel_policy`, `ityr::execution::par`
 */
template <typename ExecutionPolicy, typename ForwardIterator,
          typename Reducer, typename UnaryTransformOp>
inline typename Reducer::accumulator_type
transform_reduce(const ExecutionPolicy& policy,
                 ForwardIterator        first,
                 ForwardIterator        last,
                 Reducer                reducer,
                 UnaryTransformOp       unary_transform_op) {
  if constexpr (is_global_iterator_v<ForwardIterator>) {
    static_assert(std::is_same_v<typename ForwardIterator::mode, checkout_mode::read_t> ||
                  std::is_same_v<typename ForwardIterator::mode, checkout_mode::no_access_t>);

  } else if constexpr (ori::is_global_ptr_v<ForwardIterator>) {
    // automatically convert global pointers to global iterators with read-only access
    auto first_ = make_global_iterator(first, checkout_mode::read);
    auto last_  = make_global_iterator(last , checkout_mode::read);
    return transform_reduce(policy, first_, last_, reducer, unary_transform_op);
  }

  if constexpr (!ori::is_global_ptr_v<ForwardIterator>) {
    auto accumulate_op = [=](auto& acc, const auto& v) {
      reducer.foldl(acc, unary_transform_op(v));
    };

    auto combine_op = [=](auto& acc1, const auto& acc2,
                          ForwardIterator, ForwardIterator, ForwardIterator) {
      reducer.foldl(acc1, acc2);
    };

    if constexpr (Reducer::direct_accumulation) {
      return internal::reduce_generic(policy, accumulate_op, combine_op, reducer,
                                      reducer.identity(), first, last);
    } else {
      auto acc = reducer.identity();
      internal::reduce_generic(policy, accumulate_op, combine_op, reducer,
                               reducer.view(acc), first, last);
      return acc;
    }

  } else {
    ITYR_CHECK(false);
  }
}

/**
 * @brief Calculate reduction by transforming each element.
 *
 * @param policy              Execution policy (`ityr::execution`).
 * @param first1              1st begin iterator.
 * @param last1               1st end iterator.
 * @param first2              2nd begin iterator.
 * @param reducer             Reducer object (`ityr::reducer`).
 * @param binary_transform_op Binary operator to transform a pair of each element.
 *
 * @return The reduced result.
 *
 * This function applies `binary_transform_op` to a pair of each element in the range `[first1, last1)`
 * and `[first2, first2 + (last1 - first1)]` and performs reduction over them.
 *
 * If global pointers are provided as iterators, they are automatically checked out with the read-only
 * mode in the specified granularity (`ityr::execution::sequenced_policy::checkout_count` if serial,
 * or `ityr::execution::parallel_policy::checkout_count` if parallel) without explicitly passing them
 * as global iterators.
 * The specified regions can be overlapped.
 *
 * Example:
 * ```
 * ityr::global_vector<int> v1 = {1, 2, 3, 4, 5};
 * bool r = ityr::transform_reduce(ityr::execution::par, v1.begin(), v1.end() - 1, v1.begin() + 1,
 *                                 ityr::reducer::logical_and<>{}, [](int x, int y) { return x <= y; });
 * // r = true (v1 is sorted)
 * ```
 *
 * @see [std::transform_reduce -- cppreference.com](https://en.cppreference.com/w/cpp/algorithm/transform_reduce)
 * @see `ityr::reduce()`
 * @see `ityr::transform()`
 * @see `ityr::execution::sequenced_policy`, `ityr::execution::seq`,
 *      `ityr::execution::parallel_policy`, `ityr::execution::par`
 */
template <typename ExecutionPolicy, typename ForwardIterator1, typename ForwardIterator2,
          typename Reducer, typename BinaryTransformOp>
inline typename Reducer::accumulator_type
transform_reduce(const ExecutionPolicy& policy,
                 ForwardIterator1       first1,
                 ForwardIterator1       last1,
                 ForwardIterator2       first2,
                 Reducer                reducer,
                 BinaryTransformOp      binary_transform_op) {
  if constexpr (is_global_iterator_v<ForwardIterator1>) {
    static_assert(std::is_same_v<typename ForwardIterator1::mode, checkout_mode::read_t> ||
                  std::is_same_v<typename ForwardIterator1::mode, checkout_mode::no_access_t>);

  } else if constexpr (ori::is_global_ptr_v<ForwardIterator1>) {
    // automatically convert global pointers to global iterators with read-only access
    auto first1_ = make_global_iterator(first1, checkout_mode::read);
    auto last1_  = make_global_iterator(last1 , checkout_mode::read);
    return transform_reduce(policy, first1_, last1_, first2, reducer, binary_transform_op);
  }

  if constexpr (is_global_iterator_v<ForwardIterator2>) {
    static_assert(std::is_same_v<typename ForwardIterator2::mode, checkout_mode::read_t> ||
                  std::is_same_v<typename ForwardIterator2::mode, checkout_mode::no_access_t>);

  } else if constexpr (ori::is_global_ptr_v<ForwardIterator2>) {
    // automatically convert global pointers to global iterators with read-only access
    auto first2_ = make_global_iterator(first2, checkout_mode::read);
    return transform_reduce(policy, first1, last1, first2_, reducer, binary_transform_op);
  }

  if constexpr (!ori::is_global_ptr_v<ForwardIterator1> &&
                !ori::is_global_ptr_v<ForwardIterator2>) {
    auto accumulate_op = [=](auto& acc, const auto& v1, const auto& v2) {
      reducer.foldl(acc, binary_transform_op(v1, v2));
    };

    auto combine_op = [=](auto& acc1, const auto& acc2,
                          ForwardIterator1, ForwardIterator1, ForwardIterator1, ForwardIterator2) {
      reducer.foldl(acc1, acc2);
    };

    if constexpr (Reducer::direct_accumulation) {
      return internal::reduce_generic(policy, accumulate_op, combine_op, reducer,
                                      reducer.identity(), first1, last1, first2);
    } else {
      auto acc = reducer.identity();
      internal::reduce_generic(policy, accumulate_op, combine_op, reducer,
                               reducer.view(acc), first1, last1, first2);
      return acc;
    }

  } else {
    ITYR_CHECK(false);
  }
}

/**
 * @brief Calculate a dot product.
 *
 * @param policy Execution policy (`ityr::execution`).
 * @param first1 1st begin iterator.
 * @param last1  1st end iterator.
 * @param first2 2nd begin iterator.
 *
 * @return The reduced result.
 *
 * Equivalent to `ityr::transform_reduce(policy, first1, last1, first2, ityr::reducer::plus<T>{}, std::multiplies<>{})`,
 * where `T` is the type of the expression `(*first1) * (*first2)`.
 * This corresponds to calculating a dot product of two vectors.
 *
 * Example:
 * ```
 * ityr::global_vector<int> v1 = {1, 2, 3, 4, 5};
 * ityr::global_vector<int> v2 = {2, 3, 4, 5, 6};
 * int dot = ityr::transform_reduce(ityr::execution::par, v1.begin(), v1.end(), v2.begin());
 * // dot = 70
 * ```
 *
 * @see [std::transform_reduce -- cppreference.com](https://en.cppreference.com/w/cpp/algorithm/transform_reduce)
 * @see `ityr::reduce()`
 * @see `ityr::transform()`
 * @see `ityr::execution::sequenced_policy`, `ityr::execution::seq`,
 *      `ityr::execution::parallel_policy`, `ityr::execution::par`
 */
template <typename ExecutionPolicy, typename ForwardIterator1, typename ForwardIterator2>
inline auto transform_reduce(const ExecutionPolicy& policy,
                             ForwardIterator1       first1,
                             ForwardIterator1       last1,
                             ForwardIterator2       first2) {
  using T = decltype((*first1) * (*first2));
  return transform_reduce(policy, first1, last1, first2, reducer::plus<T>{}, std::multiplies<>{});
}

/**
 * @brief Calculate reduction.
 *
 * @param policy  Execution policy (`ityr::execution`).
 * @param first   Begin iterator.
 * @param last    End iterator.
 * @param reducer Reducer object (`ityr::reducer`).
 *
 * @return The reduced result.
 *
 * This function performs reduction over the elements in the given range `[first, last)`.
 *
 * If global pointers are provided as iterators, they are automatically checked out with the read-only
 * mode in the specified granularity (`ityr::execution::sequenced_policy::checkout_count` if serial,
 * or `ityr::execution::parallel_policy::checkout_count` if parallel) without explicitly passing them
 * as global iterators.
 *
 * Itoyori's reduce operation resembles the standard `std::reduce()`, but it differs from the standard
 * one in that `ityr::reduce()` receives a `reducer`. A reducer offers an *associative* binary operator
 * that satisfies `op(x, op(y, z)) == op(op(x, y), z)`, and an *identity* element that satisfies
 * `op(identity, x) = x` and `op(x, identity) = x`. Note that *commucativity* is not required unlike
 * the standard `std::reduce()`.
 *
 * TODO: How to define a reducer is to be documented.
 *
 * Example:
 * ```
 * ityr::global_vector<int> v = {1, 2, 3, 4, 5};
 * int product = ityr::reduce(ityr::execution::par, v.begin(), v.end(), ityr::reducer::multiplies<int>{});
 * // product = 120
 * ```
 *
 * @see [std::reduce -- cppreference.com](https://en.cppreference.com/w/cpp/algorithm/reduce)
 * @see `ityr::transform_reduce()`
 * @see `ityr::execution::sequenced_policy`, `ityr::execution::seq`,
 *      `ityr::execution::parallel_policy`, `ityr::execution::par`
 */
template <typename ExecutionPolicy, typename ForwardIterator, typename Reducer>
inline typename Reducer::accumulator_type
reduce(const ExecutionPolicy& policy,
       ForwardIterator        first,
       ForwardIterator        last,
       Reducer                reducer) {
  return transform_reduce(policy, first, last, reducer,
                          [](auto&& v) { return std::forward<decltype(v)>(v); });
}

/**
 * @brief Calculate reduction.
 *
 * @param policy Execution policy (`ityr::execution`).
 * @param first  Begin iterator.
 * @param last   End iterator.
 *
 * @return The reduced result.
 *
 * Equivalent to `ityr::reduce(policy, first, last, ityr::reducer::plus<T>{})`, where type `T` is
 * the value type of given iterators (`ForwardIterator`).
 */
template <typename ExecutionPolicy, typename ForwardIterator>
inline typename std::iterator_traits<ForwardIterator>::value_type
reduce(const ExecutionPolicy& policy,
       ForwardIterator        first,
       ForwardIterator        last) {
  using T = typename std::iterator_traits<ForwardIterator>::value_type;
  return reduce(policy, first, last, reducer::plus<T>{});
}

/**
 * @brief Transform elements in a given range and store them in another range.
 *
 * @param policy   Execution policy (`ityr::execution`).
 * @param first1   Input begin iterator.
 * @param last1    Input end iterator.
 * @param first_d  Output begin iterator.
 * @param unary_op Unary operator to transform each element.
 *
 * @return The end iterator of the output range (`first_d + (last1 - first1)`).
 *
 * This function applies `unary_op` to each element in the range `[first1, last1)` and stores the
 * result to the output range `[first_d, first_d + (last1 - first1))`. The element order is preserved,
 * and this operation corresponds to the higher-order *map* function.
 * This function resembles the standard `std::transform`.
 *
 * If the input iterators (`first1` and `last1`) are global pointers, they are automatically checked
 * out with the read-only mode in the specified granularity
 * (`ityr::execution::sequenced_policy::checkout_count` if serial,
 * or `ityr::execution::parallel_policy::checkout_count` if parallel) without explicitly passing them
 * as global iterators.
 * Similarly, if the output iterator (`first_d`) is a global pointer, the output region is checked
 * out with the write-only mode if the output type is *trivially copyable*; otherwise, it is checked
 * out with the read-write mode.
 * Overlapping regions can be specified for `first1` and `first_d`, as long as no data race occurs.
 *
 * Example:
 * ```
 * ityr::global_vector<int> v1 = {1, 2, 3, 4, 5};
 * ityr::global_vector<int> v2(v1.size());
 * ityr::transform(ityr::execution::par, v1.begin(), v1.end(), v2.begin(),
 *                 [](int x) { return x * x; });
 * // v2 = {1, 4, 9, 16, 25}
 * ```
 *
 * @see [std::transform -- cppreference.com](https://en.cppreference.com/w/cpp/algorithm/transform)
 * @see `ityr::transform_reduce()`
 * @see `ityr::execution::sequenced_policy`, `ityr::execution::seq`,
 *      `ityr::execution::parallel_policy`, `ityr::execution::par`
 */
template <typename ExecutionPolicy, typename ForwardIterator1,
          typename ForwardIteratorD, typename UnaryOp>
inline ForwardIteratorD transform(const ExecutionPolicy& policy,
                                  ForwardIterator1       first1,
                                  ForwardIterator1       last1,
                                  ForwardIteratorD       first_d,
                                  UnaryOp                unary_op) {
  if constexpr (is_global_iterator_v<ForwardIterator1>) {
    static_assert(std::is_same_v<typename ForwardIterator1::mode, checkout_mode::read_t> ||
                  std::is_same_v<typename ForwardIterator1::mode, checkout_mode::no_access_t>);

  } else if constexpr (ori::is_global_ptr_v<ForwardIterator1>) {
    // automatically convert global pointers to global iterators with read-only access
    auto first1_ = make_global_iterator(first1, checkout_mode::read);
    auto last1_  = make_global_iterator(last1 , checkout_mode::read);
    return transform(policy, first1_, last1_, first_d, unary_op);
  }

  // If the destination value type is trivially copyable, write-only access is possible
  using value_type_d = typename std::iterator_traits<ForwardIteratorD>::value_type;
  using checkout_mode_d = std::conditional_t<std::is_trivially_copyable_v<value_type_d>,
                                             checkout_mode::write_t,
                                             checkout_mode::read_write_t>;
  if constexpr (is_global_iterator_v<ForwardIteratorD>) {
    static_assert(std::is_same_v<typename ForwardIteratorD::mode, checkout_mode_d> ||
                  std::is_same_v<typename ForwardIteratorD::mode, checkout_mode::no_access_t>);

  } else if constexpr (ori::is_global_ptr_v<ForwardIteratorD>) {
    // automatically convert global pointers to global iterators
    auto first_d_ = make_global_iterator(first_d, checkout_mode_d{});
    return transform(policy, first1, last1, first_d_, unary_op);
  }

  if constexpr (!ori::is_global_ptr_v<ForwardIterator1> &&
                !ori::is_global_ptr_v<ForwardIteratorD>) {
    auto op = [=](const auto& v1, auto&& d) {
      d = unary_op(v1);
    };

    internal::loop_generic(policy, op, first1, last1, first_d);

  } else {
    ITYR_CHECK(false);
  }

  return std::next(first_d, std::distance(first1, last1));
}

/**
 * @brief Transform elements in given ranges and store them in another range.
 *
 * @param policy    Execution policy (`ityr::execution`).
 * @param first1    1st input begin iterator.
 * @param last1     1st input end iterator.
 * @param first2    2nd input begin iterator.
 * @param first_d   Output begin iterator (output).
 * @param binary_op Binary operator to transform a pair of each element.
 *
 * @return The end iterator of the output range (`first_d + (last1 - first1)`).
 *
 * This function applies `binary_op` to a pair of each element in the range `[first1, last1)` and
 * `[first2, (last1 - first1))`, and the result is stored to the output range
 * `[first_d, first_d + (last1 - first1))`. The element order is preserved,
 * and this operation corresponds to the higher-order *map* function.
 * This function resembles the standard `std::transform`.
 *
 * If the input iterators (`first1`, `last1`, and `first2`) are global pointers, they are
 * automatically checked out with the read-only mode in the specified granularity
 * (`ityr::execution::sequenced_policy::checkout_count` if serial,
 * or `ityr::execution::parallel_policy::checkout_count` if parallel) without explicitly passing them
 * as global iterators.
 * Similarly, if the output iterator (`first_d`) is a global pointer, the output region is checked
 * out with the write-only mode if the output type is *trivially copyable*; otherwise, it is checked
 * out with the read-write mode.
 * Overlapping regions can be specified for `first1`, `first2`, and `first_d`, as long as no data race occurs.
 *
 * Example:
 * ```
 * ityr::global_vector<int> v1 = {1, 2, 3, 4, 5};
 * ityr::global_vector<int> v2 = {2, 3, 4, 5, 6};
 * ityr::global_vector<int> v3(v1.size());
 * ityr::transform(ityr::execution::par, v1.begin(), v1.end(), v2.begin(), v3.begin(),
 *                 [](int x, int y) { return x * y; });
 * // v3 = {2, 6, 12, 20, 30}
 * ```
 *
 * @see [std::transform -- cppreference.com](https://en.cppreference.com/w/cpp/algorithm/transform)
 * @see `ityr::transform_reduce()`
 * @see `ityr::execution::sequenced_policy`, `ityr::execution::seq`,
 *      `ityr::execution::parallel_policy`, `ityr::execution::par`
 */
template <typename ExecutionPolicy, typename ForwardIterator1, typename ForwardIterator2,
          typename ForwardIteratorD, typename BinaryOp>
inline ForwardIteratorD transform(const ExecutionPolicy& policy,
                                  ForwardIterator1       first1,
                                  ForwardIterator1       last1,
                                  ForwardIterator2       first2,
                                  ForwardIteratorD       first_d,
                                  BinaryOp               binary_op) {
  if constexpr (is_global_iterator_v<ForwardIterator1>) {
    static_assert(std::is_same_v<typename ForwardIterator1::mode, checkout_mode::read_t> ||
                  std::is_same_v<typename ForwardIterator1::mode, checkout_mode::no_access_t>);

  } else if constexpr (ori::is_global_ptr_v<ForwardIterator1>) {
    // automatically convert global pointers to global iterators with read-only access
    auto first1_ = make_global_iterator(first1, checkout_mode::read);
    auto last1_  = make_global_iterator(last1 , checkout_mode::read);
    return transform(policy, first1_, last1_, first2, first_d, binary_op);
  }

  if constexpr (is_global_iterator_v<ForwardIterator2>) {
    static_assert(std::is_same_v<typename ForwardIterator2::mode, checkout_mode::read_t> ||
                  std::is_same_v<typename ForwardIterator2::mode, checkout_mode::no_access_t>);

  } else if constexpr (ori::is_global_ptr_v<ForwardIterator2>) {
    // automatically convert global pointers to global iterators with read-only access
    auto first2_ = make_global_iterator(first2, checkout_mode::read);
    return transform(policy, first1, last1, first2_, first_d, binary_op);
  }

  // If the destination value type is trivially copyable, write-only access is possible
  using value_type_d = typename std::iterator_traits<ForwardIteratorD>::value_type;
  using checkout_mode_d = std::conditional_t<std::is_trivially_copyable_v<value_type_d>,
                                             checkout_mode::write_t,
                                             checkout_mode::read_write_t>;
  if constexpr (is_global_iterator_v<ForwardIteratorD>) {
    static_assert(std::is_same_v<typename ForwardIteratorD::mode, checkout_mode_d> ||
                  std::is_same_v<typename ForwardIteratorD::mode, checkout_mode::no_access_t>);

  } else if constexpr (ori::is_global_ptr_v<ForwardIteratorD>) {
    // automatically convert global pointers to global iterators
    auto first_d_ = make_global_iterator(first_d, checkout_mode_d{});
    return transform(policy, first1, last1, first2, first_d_, binary_op);
  }

  if constexpr (!ori::is_global_ptr_v<ForwardIterator1> &&
                !ori::is_global_ptr_v<ForwardIterator2> &&
                !ori::is_global_ptr_v<ForwardIteratorD>) {
    auto op = [=](const auto& v1, const auto& v2, auto&& d) {
      d = binary_op(v1, v2);
    };

    internal::loop_generic(policy, op, first1, last1, first2, first_d);

  } else {
    ITYR_CHECK(false);
  }

  return std::next(first_d, std::distance(first1, last1));
}

/**
 * @brief Fill a range with a given value.
 *
 * @param policy Execution policy (`ityr::execution`).
 * @param first  Begin iterator.
 * @param last   End iterator.
 * @param value  Value to be filled with.
 *
 * This function assigns `value` to every element in the given range `[first, last)`.
 *
 * If global pointers are provided as iterators, they are automatically checked out with the read-only
 * mode in the specified granularity (`ityr::execution::sequenced_policy::checkout_count` if serial,
 * or `ityr::execution::parallel_policy::checkout_count` if parallel) without explicitly passing them
 * as global iterators.
 *
 * Example:
 * ```
 * ityr::global_vector<int> v(5);
 * ityr::fill(ityr::execution::par, v.begin(), v.end(), 100);
 * // v = {100, 100, 100, 100, 100}
 * ```
 *
 * @see [std::reduce -- cppreference.com](https://en.cppreference.com/w/cpp/algorithm/reduce)
 * @see `ityr::transform()`
 * @see `ityr::execution::sequenced_policy`, `ityr::execution::seq`,
 *      `ityr::execution::parallel_policy`, `ityr::execution::par`
 */
template <typename ExecutionPolicy, typename ForwardIterator, typename T>
inline void fill(const ExecutionPolicy& policy,
                 ForwardIterator        first,
                 ForwardIterator        last,
                 const T&               value) {
  // If the value type is trivially copyable, write-only access is possible
  using checkout_mode_t = std::conditional_t<std::is_trivially_copyable_v<T>,
                                             checkout_mode::write_t,
                                             checkout_mode::read_write_t>;
  if constexpr (is_global_iterator_v<ForwardIterator>) {
    static_assert(std::is_same_v<typename ForwardIterator::mode, checkout_mode_t> ||
                  std::is_same_v<typename ForwardIterator::mode, checkout_mode::no_access_t>);

  } else if constexpr (ori::is_global_ptr_v<ForwardIterator>) {
    // automatically convert global pointers to global iterators
    auto first_ = make_global_iterator(first, checkout_mode_t{});
    auto last_  = make_global_iterator(last , checkout_mode_t{});
    fill(policy, first_, last_, value);
    return;
  }

  if constexpr (!ori::is_global_ptr_v<ForwardIterator>) {
    auto op = [=](auto&& d) {
      d = value;
    };

    internal::loop_generic(policy, op, first, last);

  } else {
    ITYR_CHECK(false);
  }
}

/**
 * @brief Calculate a prefix sum (inclusive scan) while transforming each element.
 *
 * @param policy             Execution policy (`ityr::execution`).
 * @param first1             Input begin iterator.
 * @param last1              Input end iterator.
 * @param first_d            Output begin iterator.
 * @param reducer            Reducer object (`ityr::reducer`).
 * @param unary_transform_op Unary operator to transform each element.
 * @param init               Initial value for the prefix sum.
 *
 * @return The end iterator of the output range (`first_d + (last1 - first1)`).
 *
 * This function applies `unary_transform_op` to each element in the range `[first1, last1)` and
 * calculates a prefix sum over them. The prefix sum is inclusive, which means that the i-th element
 * of the prefix sum includes the i-th element in the input range. That is, the i-th element of the
 * prefix sum is: `init + f(*first1) + ... + f(*(first1 + i))`, where `+` is the associative binary
 * operator (provided by `reducer`) and `f()` is the transform operator (`unary_transform_op`).
 * The calculated prefix sum is stored in the output range `[first_d, first_d + (last1 - first1))`.
 *
 * If the input iterators (`first1` and `last1`) are global pointers, they are automatically checked
 * out with the read-only mode in the specified granularity
 * (`ityr::execution::sequenced_policy::checkout_count` if serial,
 * or `ityr::execution::parallel_policy::checkout_count` if parallel) without explicitly passing them
 * as global iterators.
 * Similarly, if the output iterator (`first_d`) is a global pointer, the output region is checked
 * out with the write-only mode if the output type is *trivially copyable*; otherwise, it is checked
 * out with the read-write mode.
 * Overlapping regions can be specified for `first1` and `first_d`, as long as no data race occurs.
 *
 * Unlike the standard `std::transform_inclusive_scan()`, Itoyori's `ityr::transform_inclusive_scan()`
 * requires a `reducer` as `ityr::reduce()` does.
 *
 * Example:
 * ```
 * ityr::global_vector<int> v1 = {1, 2, 3, 4, 5};
 * ityr::global_vector<double> v2(v1.size());
 * ityr::transform_inclusive_scan(ityr::execution::par, v1.begin(), v1.end(), v2.begin(),
 *                                ityr::reducer::multiplies<double>{},
 *                                [](int x) { return static_cast<double>(x); }, 0.01);
 * // v2 = {0.01, 0.02, 0.06, 0.24, 1.2}
 * ```
 *
 * @see [std::transform_inclusive_scan -- cppreference.com](https://en.cppreference.com/w/cpp/algorithm/transform_inclusive_scan)
 * @see `ityr::inclusive_scan()`
 * @see `ityr::reduce()`
 * @see `ityr::transform()`
 * @see `ityr::transform_reduce()`
 * @see `ityr::execution::sequenced_policy`, `ityr::execution::seq`,
 *      `ityr::execution::parallel_policy`, `ityr::execution::par`
 */
template <typename ExecutionPolicy, typename ForwardIterator1, typename ForwardIteratorD,
          typename Reducer, typename UnaryTransformOp>
inline ForwardIteratorD
transform_inclusive_scan(const ExecutionPolicy&                    policy,
                         ForwardIterator1                          first1,
                         ForwardIterator1                          last1,
                         ForwardIteratorD                          first_d,
                         Reducer                                   reducer,
                         UnaryTransformOp                          unary_transform_op,
                         const typename Reducer::accumulator_type& init) {
  if constexpr (is_global_iterator_v<ForwardIterator1>) {
    static_assert(std::is_same_v<typename ForwardIterator1::mode, checkout_mode::read_t> ||
                  std::is_same_v<typename ForwardIterator1::mode, checkout_mode::no_access_t>);

  } else if constexpr (ori::is_global_ptr_v<ForwardIterator1>) {
    // automatically convert global pointers to global iterators with read-only access
    auto first1_ = make_global_iterator(first1, checkout_mode::read);
    auto last1_  = make_global_iterator(last1 , checkout_mode::read);
    return transform_inclusive_scan(policy, first1_, last1_, first_d, reducer, unary_transform_op, init);
  }

  // If the destination value type is trivially copyable, write-only access is possible
  using value_type_d = typename std::iterator_traits<ForwardIteratorD>::value_type;
  using checkout_mode_d = std::conditional_t<std::is_trivially_copyable_v<value_type_d>,
                                             checkout_mode::write_t,
                                             checkout_mode::read_write_t>;
  if constexpr (is_global_iterator_v<ForwardIteratorD>) {
    static_assert(std::is_same_v<typename ForwardIteratorD::mode, checkout_mode_d> ||
                  std::is_same_v<typename ForwardIteratorD::mode, checkout_mode::no_access_t>);

  } else if constexpr (ori::is_global_ptr_v<ForwardIteratorD>) {
    // automatically convert global pointers to global iterators
    auto first_d_ = make_global_iterator(first_d, checkout_mode_d{});
    return transform_inclusive_scan(policy, first1, last1, first_d_, reducer, unary_transform_op, init);
  }

  if constexpr (!ori::is_global_ptr_v<ForwardIterator1> &&
                !ori::is_global_ptr_v<ForwardIteratorD>) {
    auto accumulate_op = [=](auto& acc, const auto& v1, auto&& d) {
      reducer.foldl(acc, unary_transform_op(v1));
      d = reducer.clone(acc);
    };

    // TODO: more efficient scan implementation
    auto combine_op = [=](auto&            acc1,
                          const auto&      acc2,
                          ForwardIterator1 first_,
                          ForwardIterator1 mid_,
                          ForwardIterator1 last_,
                          ForwardIteratorD first_d_) {
      // Add the left accumulator `acc1` to the right half of the region
      auto dm = std::distance(first_, mid_);
      auto dl = std::distance(first_, last_);
      if constexpr (!is_global_iterator_v<ForwardIteratorD>) {
        for_each(policy, std::next(first_d_, dm), std::next(first_d_, dl),
                 [=](auto&& acc_r) { reducer.foldr(acc1, acc_r); });
      } else if constexpr (std::is_same_v<typename ForwardIteratorD::mode, checkout_mode::no_access_t>) {
        for_each(policy, std::next(first_d_, dm), std::next(first_d_, dl),
                 [=](auto&& acc_r) { reducer.foldr(acc1, acc_r); });
      } else {
        // &*: convert global_iterator -> global_ref -> global_ptr
        auto fd = make_global_iterator(&*first_d_, checkout_mode::read_write);
        for_each(policy, std::next(fd, dm), std::next(fd, dl),
                 [=](auto&& acc_r) { reducer.foldr(acc1, acc_r); });
      }
      reducer.foldl(acc1, acc2);
    };

    internal::reduce_generic(policy, accumulate_op, combine_op, reducer,
                             reducer.view(init), first1, last1, first_d);

  } else {
    ITYR_CHECK(false);
  }

  return std::next(first_d, std::distance(first1, last1));
}

/**
 * @brief Calculate a prefix sum (inclusive scan) while transforming each element.
 *
 * @param policy             Execution policy (`ityr::execution`).
 * @param first1             Input begin iterator.
 * @param last1              Input end iterator.
 * @param first_d            Output begin iterator.
 * @param reducer            Reducer object (`ityr::reducer`).
 * @param unary_transform_op Unary operator to transform each element.
 *
 * @return The end iterator of the output range (`first_d + (last1 - first1)`).
 *
 * Equivalent to `ityr::transform_inclusive_reduce(policy, first1, last1, first_d, reducer, unary_transform_op, reducer.identity())`.
 *
 * Example:
 * ```
 * ityr::global_vector<int> v1 = {1, 2, 3, 4, 5};
 * ityr::global_vector<double> v2(v1.size());
 * ityr::transform_inclusive_scan(ityr::execution::par, v1.begin(), v1.end(), v2.begin(),
 *                                ityr::reducer::multiplies<double>{}, [](int x) { return 0.1 * x; });
 * // v2 = {0.1, 0.02, 0.006, 0.0024, 0.0012}
 * ```
 *
 * @see [std::transform_inclusive_scan -- cppreference.com](https://en.cppreference.com/w/cpp/algorithm/transform_inclusive_scan)
 * @see `ityr::inclusive_scan()`
 * @see `ityr::reduce()`
 * @see `ityr::transform()`
 * @see `ityr::transform_reduce()`
 * @see `ityr::execution::sequenced_policy`, `ityr::execution::seq`,
 *      `ityr::execution::parallel_policy`, `ityr::execution::par`
 */
template <typename ExecutionPolicy, typename ForwardIterator1, typename ForwardIteratorD,
          typename Reducer, typename UnaryTransformOp>
inline ForwardIteratorD transform_inclusive_scan(const ExecutionPolicy& policy,
                                                 ForwardIterator1       first1,
                                                 ForwardIterator1       last1,
                                                 ForwardIteratorD       first_d,
                                                 Reducer                reducer,
                                                 UnaryTransformOp       unary_transform_op) {
  return transform_inclusive_scan(policy, first1, last1, first_d, reducer,
                                  unary_transform_op, reducer.identity());
}

/**
 * @brief Calculate a prefix sum (inclusive scan).
 *
 * @param policy  Execution policy (`ityr::execution`).
 * @param first1  Input begin iterator.
 * @param last1   Input end iterator.
 * @param first_d Output begin iterator.
 * @param reducer Reducer object (`ityr::reducer`).
 * @param init    Initial value for the prefix sum.
 *
 * @return The end iterator of the output range (`first_d + (last1 - first1)`).
 *
 * This function calculates a prefix sum over the elements in the input range `[first1, last1)`.
 * The prefix sum is inclusive, which means that the i-th element of the prefix sum includes the
 * i-th element in the input range. That is, the i-th element of the prefix sum is:
 * `init + *first1 + ... + *(first1 + i)`, where `+` is the associative binary operator (provided
 * by `reducer`).
 * The calculated prefix sum is stored in the output range `[first_d, first_d + (last1 - first1))`.
 *
 * If the input iterators (`first1` and `last1`) are global pointers, they are automatically checked
 * out with the read-only mode in the specified granularity
 * (`ityr::execution::sequenced_policy::checkout_count` if serial,
 * or `ityr::execution::parallel_policy::checkout_count` if parallel) without explicitly passing them
 * as global iterators.
 * Similarly, if the output iterator (`first_d`) is a global pointer, the output region is checked
 * out with the write-only mode if the output type is *trivially copyable*; otherwise, it is checked
 * out with the read-write mode.
 * Overlapping regions can be specified for `first1` and `first_d`, as long as no data race occurs.
 *
 * Unlike the standard `std::inclusive_scan()`, Itoyori's `ityr::inclusive_scan()`
 * requires a `reducer` as `ityr::reduce()` does.
 *
 * Example:
 * ```
 * ityr::global_vector<int> v1 = {1, 2, 3, 4, 5};
 * ityr::global_vector<int> v2(v1.size());
 * ityr::inclusive_scan(ityr::execution::par, v1.begin(), v1.end(), v2.begin(),
 *                      ityr::reducer::multiplies<int>{}, 10);
 * // v2 = {10, 20, 60, 240, 1200}
 * ```
 *
 * @see [std::inclusive_scan -- cppreference.com](https://en.cppreference.com/w/cpp/algorithm/inclusive_scan)
 * @see `ityr::transform_inclusive_scan()`
 * @see `ityr::reduce()`
 * @see `ityr::execution::sequenced_policy`, `ityr::execution::seq`,
 *      `ityr::execution::parallel_policy`, `ityr::execution::par`
 */
template <typename ExecutionPolicy, typename ForwardIterator1, typename ForwardIteratorD,
          typename Reducer>
inline ForwardIteratorD
inclusive_scan(const ExecutionPolicy&                    policy,
               ForwardIterator1                          first1,
               ForwardIterator1                          last1,
               ForwardIteratorD                          first_d,
               Reducer                                   reducer,
               const typename Reducer::accumulator_type& init) {
  return transform_inclusive_scan(policy, first1, last1, first_d, reducer,
                                  [](auto&& v) { return std::forward<decltype(v)>(v); }, init);
}

/**
 * @brief Calculate a prefix sum (inclusive scan).
 *
 * @param policy  Execution policy (`ityr::execution`).
 * @param first1  Input begin iterator.
 * @param last1   Input end iterator.
 * @param first_d Output begin iterator.
 * @param reducer Reducer object (`ityr::reducer`).
 *
 * @return The end iterator of the output range (`first_d + (last1 - first1)`).
 *
 * Equivalent to `ityr::inclusive_scan(policy, first1, last1, first_d, reducer, reducer.identity())`.
 *
 * Example:
 * ```
 * ityr::global_vector<int> v1 = {1, 2, 3, 4, 5};
 * ityr::global_vector<int> v2(v1.size());
 * ityr::inclusive_scan(ityr::execution::par, v1.begin(), v1.end(), v2.begin(),
 *                      ityr::reducer::multiplies<int>{});
 * // v2 = {1, 2, 6, 24, 120}
 * ```
 *
 * @see [std::inclusive_scan -- cppreference.com](https://en.cppreference.com/w/cpp/algorithm/inclusive_scan)
 * @see `ityr::transform_inclusive_scan()`
 * @see `ityr::reduce()`
 * @see `ityr::execution::sequenced_policy`, `ityr::execution::seq`,
 *      `ityr::execution::parallel_policy`, `ityr::execution::par`
 */
template <typename ExecutionPolicy, typename ForwardIterator1, typename ForwardIteratorD,
          typename Reducer>
inline ForwardIteratorD inclusive_scan(const ExecutionPolicy& policy,
                                       ForwardIterator1       first1,
                                       ForwardIterator1       last1,
                                       ForwardIteratorD       first_d,
                                       Reducer                reducer) {
  return inclusive_scan(policy, first1, last1, first_d, reducer, reducer.identity());
}

/**
 * @brief Calculate a prefix sum (inclusive scan).
 *
 * @param policy  Execution policy (`ityr::execution`).
 * @param first1  Input begin iterator.
 * @param last1   Input end iterator.
 * @param first_d Output begin iterator.
 *
 * @return The end iterator of the output range (`first_d + (last1 - first1)`).
 *
 * Equivalent to `ityr::inclusive_scan(policy, first1, last1, first_d, ityr::reducer::plus<T>{})`, where
 * `T` is the value type of the input iterator.
 *
 * Example:
 * ```
 * ityr::global_vector<int> v1 = {1, 2, 3, 4, 5};
 * ityr::global_vector<int> v2(v1.size());
 * ityr::inclusive_scan(ityr::execution::par, v1.begin(), v1.end(), v2.begin());
 * // v2 = {1, 3, 6, 10, 15}
 * ```
 *
 * @see [std::inclusive_scan -- cppreference.com](https://en.cppreference.com/w/cpp/algorithm/inclusive_scan)
 * @see `ityr::transform_inclusive_scan()`
 * @see `ityr::reduce()`
 * @see `ityr::execution::sequenced_policy`, `ityr::execution::seq`,
 *      `ityr::execution::parallel_policy`, `ityr::execution::par`
 */
template <typename ExecutionPolicy, typename ForwardIterator1, typename ForwardIteratorD>
inline ForwardIteratorD inclusive_scan(const ExecutionPolicy& policy,
                                       ForwardIterator1       first1,
                                       ForwardIterator1       last1,
                                       ForwardIteratorD       first_d) {
  using T = typename std::iterator_traits<ForwardIterator1>::value_type;
  return inclusive_scan(policy, first1, last1, first_d, reducer::plus<T>{});
}

ITYR_TEST_CASE("[ityr::pattern::parallel_loop] reduce and transform_reduce") {
  ito::init();
  ori::init();

  ITYR_SUBCASE("default cutoff") {
    long n = 10000;
    long r = ito::root_exec([=] {
      return reduce(
          execution::par,
          count_iterator<long>(0),
          count_iterator<long>(n));
    });
    ITYR_CHECK(r == n * (n - 1) / 2);
  }

  ITYR_SUBCASE("custom cutoff") {
    long n = 100000;
    long r = ito::root_exec([=] {
      return reduce(
          execution::parallel_policy{.cutoff_count = 100},
          count_iterator<long>(0),
          count_iterator<long>(n));
    });
    ITYR_CHECK(r == n * (n - 1) / 2);
  }

  ITYR_SUBCASE("transform unary") {
    long n = 100000;
    long r = ito::root_exec([=] {
      return transform_reduce(
          execution::parallel_policy{.cutoff_count = 100},
          count_iterator<long>(0),
          count_iterator<long>(n),
          reducer::plus<long>{},
          [](long x) { return x * x; });
    });
    ITYR_CHECK(r == n * (n - 1) * (2 * n - 1) / 6);
  }

  ITYR_SUBCASE("transform binary") {
    long n = 100000;
    long r = ito::root_exec([=] {
      return transform_reduce(
          execution::parallel_policy{.cutoff_count = 100},
          count_iterator<long>(0),
          count_iterator<long>(n),
          count_iterator<long>(0),
          reducer::plus<long>{},
          [](long x, long y) { return x * y; });
    });
    ITYR_CHECK(r == n * (n - 1) * (2 * n - 1) / 6);
  }

  ITYR_SUBCASE("zero elements") {
    long r = ito::root_exec([=] {
      return reduce(
          execution::parallel_policy{.cutoff_count = 100},
          count_iterator<long>(0),
          count_iterator<long>(0));
    });
    ITYR_CHECK(r == 0);
  }

  ori::fini();
  ito::fini();
}

ITYR_TEST_CASE("[ityr::pattern::parallel_loop] parallel reduce with global_ptr") {
  ito::init();
  ori::init();

  long n = 100000;
  ori::global_ptr<long> p = ori::malloc_coll<long>(n);

  ito::root_exec([=] {
    long count = 0;
    for_each(
        execution::sequenced_policy{.checkout_count = 100},
        make_global_iterator(p    , checkout_mode::write),
        make_global_iterator(p + n, checkout_mode::write),
        [&](long& v) { v = count++; });
  });

  ITYR_SUBCASE("default cutoff") {
    long r = ito::root_exec([=] {
      return reduce(
          execution::par,
          p, p + n);
    });
    ITYR_CHECK(r == n * (n - 1) / 2);
  }

  ITYR_SUBCASE("custom cutoff and checkout count") {
    long r = ito::root_exec([=] {
      return reduce(
          execution::parallel_policy{.cutoff_count = 100, .checkout_count = 50},
          p, p + n);
    });
    ITYR_CHECK(r == n * (n - 1) / 2);
  }

  ITYR_SUBCASE("without auto checkout") {
    long r = ito::root_exec([=] {
      return transform_reduce(
          execution::par,
          make_global_iterator(p    , checkout_mode::no_access),
          make_global_iterator(p + n, checkout_mode::no_access),
          reducer::plus<long>{},
          [](ori::global_ref<long> gref) {
            return gref.get();
          });
    });
    ITYR_CHECK(r == n * (n - 1) / 2);
  }

  ITYR_SUBCASE("serial") {
    long r = ito::root_exec([=] {
      return reduce(
          execution::sequenced_policy{.checkout_count = 100},
          p, p + n);
    });
    ITYR_CHECK(r == n * (n - 1) / 2);
  }

  ori::free_coll(p);

  ori::fini();
  ito::fini();
}

ITYR_TEST_CASE("[ityr::pattern::parallel_loop] parallel transform with global_ptr") {
  ito::init();
  ori::init();

  long n = 100000;
  ori::global_ptr<long> p1 = ori::malloc_coll<long>(n);
  ori::global_ptr<long> p2 = ori::malloc_coll<long>(n);

  ITYR_SUBCASE("parallel") {
    ito::root_exec([=] {
      auto r = transform(
          execution::parallel_policy{.cutoff_count = 100, .checkout_count = 100},
          count_iterator<long>(0), count_iterator<long>(n), p1,
          [](long i) { return i * 2; });
      ITYR_CHECK(r == p1 + n);

      transform(
          execution::parallel_policy{.cutoff_count = 100, .checkout_count = 100},
          count_iterator<long>(0), count_iterator<long>(n), p1, p2,
          [](long i, long j) { return i * j; });

      auto sum = reduce(
          execution::parallel_policy{.cutoff_count = 100, .checkout_count = 100},
          p2, p2 + n);

      ITYR_CHECK(sum == n * (n - 1) * (2 * n - 1) / 3);
    });
  }

  ITYR_SUBCASE("serial") {
    ito::root_exec([=] {
      auto r = transform(
          execution::sequenced_policy{.checkout_count = 100},
          count_iterator<long>(0), count_iterator<long>(n), p1,
          [](long i) { return i * 2; });
      ITYR_CHECK(r == p1 + n);

      transform(
          execution::sequenced_policy{.checkout_count = 100},
          count_iterator<long>(0), count_iterator<long>(n), p1, p2,
          [](long i, long j) { return i * j; });

      auto sum = reduce(
          execution::sequenced_policy{.checkout_count = 100},
          p2, p2 + n);

      ITYR_CHECK(sum == n * (n - 1) * (2 * n - 1) / 3);
    });
  }

  ori::free_coll(p1);
  ori::free_coll(p2);

  ori::fini();
  ito::fini();
}

ITYR_TEST_CASE("[ityr::pattern::parallel_loop] parallel fill") {
  ito::init();
  ori::init();

  long n = 100000;
  ori::global_ptr<long> p = ori::malloc_coll<long>(n);

  ito::root_exec([=] {
    long val = 33;
    fill(execution::parallel_policy{.cutoff_count = 100, .checkout_count = 100},
         p, p + n, val);

    auto sum = reduce(
        execution::parallel_policy{.cutoff_count = 100, .checkout_count = 100},
        p, p + n);

    ITYR_CHECK(sum == n * val);
  });

  ori::free_coll(p);

  ori::fini();
  ito::fini();
}

ITYR_TEST_CASE("[ityr::pattern::parallel_loop] inclusive scan") {
  ito::init();
  ori::init();

  long n = 100000;
  ori::global_ptr<long> p1 = ori::malloc_coll<long>(n);
  ori::global_ptr<long> p2 = ori::malloc_coll<long>(n);

  ito::root_exec([=] {
    fill(execution::parallel_policy{.cutoff_count = 100, .checkout_count = 100},
         p1, p1 + n, 1);

    inclusive_scan(
        execution::parallel_policy{.cutoff_count = 100, .checkout_count = 100},
        p1, p1 + n, p2);

    ITYR_CHECK(p2[0].get() == 1);
    ITYR_CHECK(p2[n - 1].get() == n);

    auto sum = reduce(
        execution::parallel_policy{.cutoff_count = 100, .checkout_count = 100},
        p2, p2 + n);

    ITYR_CHECK(sum == n * (n + 1) / 2);

    inclusive_scan(
        execution::parallel_policy{.cutoff_count = 100, .checkout_count = 100},
        p1, p1 + n, p2, reducer::multiplies<long>{}, 10);

    ITYR_CHECK(p2[0].get() == 10);
    ITYR_CHECK(p2[n - 1].get() == 10);

    transform_inclusive_scan(
        execution::parallel_policy{.cutoff_count = 100, .checkout_count = 100},
        p1, p1 + n, p2, reducer::plus<long>{}, [](long x) { return x + 1; }, 10);

    ITYR_CHECK(p2[0].get() == 12);
    ITYR_CHECK(p2[n - 1].get() == 10 + n * 2);
  });

  ori::free_coll(p1);
  ori::free_coll(p2);

  ori::fini();
  ito::fini();
}

}
