#include "ityr/ityr.hpp"
#include "log.h"
#include <array>
#include <cstdio>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <array>

std::string exec(const char* cmd) {
  std::array<char, 128> buffer;
  std::string result;
  std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd, "r"), pclose);
  if (!pipe) {
    throw std::runtime_error("popen() failed!");
  }
  while (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe.get()) != nullptr) {
    result += buffer.data();
  }
  return result;
}

int main() {
  ityr::init();

  ityr::root_exec([]() {
    ityr::coll_exec([]() {
      ityr::common::global_lock lock;
      lock.lock(0);
      std::cout << "Hello World from #" << ityr::my_rank() << std::endl;
      lock.unlock(0);
    });
  });

  ityr::fini();
  return 0;
}