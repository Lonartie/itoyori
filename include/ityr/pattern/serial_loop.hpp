#pragma once

#include "ityr/common/util.hpp"
#include "ityr/ori/ori.hpp"
#include "ityr/pattern/iterator.hpp"
#include "ityr/pattern/global_iterator.hpp"
#include "ityr/container/checkout_span.hpp"

namespace ityr {

namespace execution {

struct sequenced_policy {
  std::size_t checkout_count = 1;
};

struct parallel_policy {
  std::size_t cutoff_count   = 1;
  std::size_t checkout_count = 1;
};

inline sequenced_policy to_sequenced_policy(const sequenced_policy& opts) {
  return opts;
}

inline sequenced_policy to_sequenced_policy(const parallel_policy& opts) {
  return {.checkout_count = opts.checkout_count};
}

inline void assert_policy(const sequenced_policy& opts) {
  ITYR_CHECK(0 < opts.checkout_count);
}

inline void assert_policy(const parallel_policy& opts) {
  ITYR_CHECK(0 < opts.checkout_count);
  ITYR_CHECK(opts.checkout_count <= opts.cutoff_count);
}

inline constexpr sequenced_policy seq;
inline constexpr parallel_policy  par;

}

template <typename T, typename Mode>
inline auto make_checkout_iter(global_iterator<T, Mode> it,
                               std::size_t              count) {
  auto cs = make_checkout(&*it, count, Mode{});
  return std::make_tuple(std::move(cs), cs.data());
}

template <typename T>
inline auto make_checkout_iter(global_move_iterator<T> it,
                               std::size_t             count) {
  auto cs = make_checkout(&*it, count, checkout_mode::read_write);
  return std::make_tuple(std::move(cs), std::make_move_iterator(cs.data()));
}

template <typename T>
inline auto make_checkout_iter(global_construct_iterator<T> it,
                               std::size_t                  count) {
  auto cs = make_checkout(&*it, count, checkout_mode::write);
  return std::make_tuple(std::move(cs), make_count_iterator(cs.data()));
}

template <typename T>
inline auto make_checkout_iter(global_destruct_iterator<T> it,
                               std::size_t                 count) {
  auto cs = make_checkout(&*it, count, checkout_mode::read_write);
  return std::make_tuple(std::move(cs), make_count_iterator(cs.data()));
}

inline auto checkout_global_iterators(std::size_t) {
  return std::make_tuple(std::make_tuple(), std::make_tuple());
}

template <typename ForwardIterator, typename... ForwardIterators>
inline auto checkout_global_iterators(std::size_t n, ForwardIterator it, ForwardIterators... rest) {
  if constexpr (is_global_iterator_v<ForwardIterator>) {
    if constexpr (ForwardIterator::auto_checkout) {
      auto [cs, it_] = make_checkout_iter(it, n);
      auto [css, its] = checkout_global_iterators(n, rest...);
      return std::make_tuple(std::tuple_cat(std::make_tuple(std::move(cs)), std::move(css)),
                             std::tuple_cat(std::make_tuple(it_), its));
    } else {
      auto [css, its] = checkout_global_iterators(n, rest...);
      // &*: convert global_iterator -> global_ref -> global_ptr
      return std::make_tuple(std::move(css),
                             std::tuple_cat(std::make_tuple(&*it), its));
    }
  } else {
    auto [css, its] = checkout_global_iterators(n, rest...);
    return std::make_tuple(std::move(css),
                           std::tuple_cat(std::make_tuple(it), its));
  }
}

template <typename Op, typename... ForwardIterators>
inline void apply_iterators(Op                  op,
                            std::size_t         n,
                            ForwardIterators... its) {
  for (std::size_t i = 0; i < n; (++i, ..., ++its)) {
    op(*its...);
  }
}

template <typename Op, typename ForwardIterator, typename... ForwardIterators>
inline void for_each_aux(const execution::sequenced_policy& policy,
                         Op                                 op,
                         ForwardIterator                    first,
                         ForwardIterator                    last,
                         ForwardIterators...                firsts) {
  if constexpr ((is_global_iterator_v<ForwardIterator> || ... ||
                 is_global_iterator_v<ForwardIterators>)) {
    // perform automatic checkout for global iterators
    std::size_t n = std::distance(first, last);
    std::size_t c = policy.checkout_count;

    for (std::size_t d = 0; d < n; d += c) {
      auto n_ = std::min(n - d, c);

      auto [css, its] = checkout_global_iterators(n_, first, firsts...);
      std::apply([&](auto&&... args) {
        apply_iterators(op, n_, std::forward<decltype(args)>(args)...);
      }, its);

      ((first = std::next(first, n_)), ..., (firsts = std::next(firsts, n_)));
    }

  } else {
    for (; first != last; (++first, ..., ++firsts)) {
      op(*first, *firsts...);
    }
  }
}

template <typename ForwardIterator, typename Op>
inline void for_each(const execution::sequenced_policy& opts,
                     ForwardIterator                    first,
                     ForwardIterator                    last,
                     Op                                 op) {
  for_each_aux(opts, op, first, last);
}

template <typename ForwardIterator1, typename ForwardIterator2, typename Op>
inline void for_each(const execution::sequenced_policy& opts,
                     ForwardIterator1                   first1,
                     ForwardIterator1                   last1,
                     ForwardIterator2                   first2,
                     Op                                 op) {
  for_each_aux(opts, op, first1, last1, first2);
}

template <typename ForwardIterator1, typename ForwardIterator2, typename ForwardIterator3, typename Op>
inline void for_each(const execution::sequenced_policy& opts,
                     ForwardIterator1                   first1,
                     ForwardIterator1                   last1,
                     ForwardIterator2                   first2,
                     ForwardIterator3                   first3,
                     Op                                 op) {
  for_each_aux(opts, op, first1, last1, first2, first3);
}

ITYR_TEST_CASE("[ityr::pattern::serial_loop] for_each seq") {
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

}
