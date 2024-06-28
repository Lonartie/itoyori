#pragma once

#include "ityr/ityr.hpp"
#include <fstream>
#include <string>
#include <thread>

std::string exec_name() {
  std::string sp;
  std::ifstream("/proc/self/comm") >> sp;
  return sp;
}

inline void ityr_log(const std::string& txt) {
  // return;
  std::string path = "/media/psf/git/itoyori/logs/" + exec_name() + "_";
  path += std::to_string(ityr::my_rank()) + ".txt";
  std::ofstream stream(path.c_str(), std::ios::app);
  if (!stream.is_open()) ityr::common::die("couldn't open file");
  stream << txt << std::endl;
  stream.flush();
  stream.close();
}
