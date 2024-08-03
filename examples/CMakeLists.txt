cmake_minimum_required(VERSION 3.1)

set(examples fib nqueens mandelbrot)

foreach(example IN LISTS examples)

  add_compile_options(-fno-omit-frame-pointer)
  add_executable(ityr_${example}.out ${example}.cpp)
  target_link_libraries(ityr_${example}.out itoyori)
  target_compile_options(ityr_${example}.out PRIVATE -DITYR_ITO_SCHEDULER=randws -DITYR_PROFILER_MODE=stats)

  install(TARGETS ityr_${example}.out
          DESTINATION "${CMAKE_INSTALL_LIBEXECDIR}/itoyori/examples")

endforeach()
