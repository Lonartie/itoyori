cmake_minimum_required(VERSION 3.1)

set(examples fib nqueens)

foreach(example IN LISTS examples)

  add_compile_options(-fno-omit-frame-pointer)
  add_executable(${example}.out ${example}.cpp)
  target_link_libraries(${example}.out itoyori)
  target_compile_options(${example}.out PRIVATE -DITYR_ITO_SCHEDULER=randws)

endforeach()
