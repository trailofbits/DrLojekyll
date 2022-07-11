# Copyright 2021, Trail of Bits, Inc. All rights reserved.

include(CTest)
enable_testing()

include("${CMAKE_SOURCE_DIR}/cmake/Compiler.cmake")

add_subdirectory(MiniDisassembler)
add_subdirectory(PointsTo)

find_package(gap CONFIG)
find_package(LLVM CONFIG)
if(NOT "${gap}" STREQUAL "gap-NOTFOUND")
  if(NOT "${LLVM}" STREQUAL "LLVM-NOTFOUND")
    add_subdirectory(LLVM)
  endif()
endif()