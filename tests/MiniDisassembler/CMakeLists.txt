# Copyright 2021, Trail of Bits, Inc. All rights reserved.

find_package(GTest REQUIRED)
include(GoogleTest)

compile_datalog(
  DATABASE_NAME database
  LIBRARY_NAME mini_disassembler
  SERVICE_NAME MiniDisassembler
  CXX_OUTPUT_DIR "${CMAKE_CURRENT_BINARY_DIR}"
  PY_OUTPUT_DIR "${CMAKE_CURRENT_BINARY_DIR}"
  SOURCES database.dr
)

add_executable(test_mini_disassembler
  Main.cpp)
  
target_link_libraries(test_mini_disassembler PUBLIC gtest gtest_main PRIVATE mini_disassembler)

