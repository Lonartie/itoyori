#include <profiling/prof.hpp>
#include <ityr/ityr.hpp>

static volatile uint64_t result;

uint64_t fib(uint64_t n);

int main(int argc, char** argv) {
   ityr::init();
   ityr::profiler_begin();

   const int iterations = argc > 1 ? std::stoi(argv[1]) : 10;
   const int input = argc > 2 ? std::stoi(argv[2]) : 20;

   ityr::root_exec([=]() -> void {
      for (int i = 0; i < iterations; i++) {
         result = fib(input);
         std::cout << "iteration #" << i+1 << " done\n";
      }
   });

   ityr::profiler_end();
   ityr::profiler_flush();
   ityr::fini();
}

uint64_t fib(const uint64_t n) {
   if (n <= 2) return 1;

   const auto [a, b] = ityr::parallel_invoke(
      [=] { return fib(n-1); },
      [=] { return fib(n-2); }
   );

   return a + b;
}
