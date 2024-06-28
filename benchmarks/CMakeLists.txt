cmake_minimum_required(VERSION 3.1)

project(benchmarks)

add_executable(benchmarks main.cpp)
target_link_libraries(benchmarks itoyori)