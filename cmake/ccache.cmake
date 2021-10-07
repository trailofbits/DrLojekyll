# Copyright 2020, Trail of Bits, Inc. All rights reserved.

find_program(ccache_executable_path "ccache")
if(NOT ccache_executable_path)
  message(WARNING "drlojekyll: The ccache program was not found")

else()
  message(STATUS "drlojekyll: Enabling ccache support")

  set(CMAKE_C_COMPILER_LAUNCHER "${ccache_executable_path}" CACHE STRING "C compiler launcher (ccache)" FORCE)
  set(CMAKE_CXX_COMPILER_LAUNCHER "${ccache_executable_path}" CACHE STRING "C++ compiler launcher (ccache)" FORCE)
endif()
