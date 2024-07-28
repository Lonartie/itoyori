#include "ityr/ityr.hpp"
#include <filesystem>
#include <fstream>

std::string path() {
  return "/media/psf/git/itoyori/logs/" + std::to_string(ityr::my_rank()) + ".txt";
}

void log_call(uint64_t n) {
  std::ofstream stream(path(), std::ios::app);
  stream << n << "\n";
  stream.flush();
  stream.close();
}

uint64_t fib(uint64_t n) {
  log_call(n);

  if (n <= 2) return 1;

  auto [a, b] = ityr::parallel_invoke(
    [n] { return fib(n - 1); },
    [n] { return fib(n - 2); }
  );

  return a + b;
}

int main() {
  ityr::init();
  if (std::filesystem::exists(path()))
    std::filesystem::remove(path());
  ityr::ito::adws_enable_steal_option::set(false);

  ityr::root_exec([]() {
    std::cout << "RESULT=" << fib(10) << "\n";
  });

  ityr::fini();
}