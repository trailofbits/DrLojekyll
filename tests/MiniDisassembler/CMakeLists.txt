# Copyright 2021, Trail of Bits, Inc. All rights reserved.

find_package(GTest CONFIG REQUIRED)
include(GoogleTest)

compile_datalog(
  DATABASE_NAME mini_disassembler
  LIBRARY_NAME mini_disassembler
  SERVICE_NAME mini_disassembler_server
  CXX_OUTPUT_DIR "${CMAKE_CURRENT_BINARY_DIR}"
  DOT_OUTPUT_FILE "${CMAKE_CURRENT_BINARY_DIR}/database.dot"
  IR_OUTPUT_FILE "${CMAKE_CURRENT_BINARY_DIR}/database.ir"
  FB_OUTPUT_FILE "${CMAKE_CURRENT_BINARY_DIR}/database.fbs"
  SOURCES database.dr
)

add_executable(mini_disassembler_standalone
  Standalone.cpp)
  
target_link_libraries(mini_disassembler_standalone PUBLIC GTest::gtest GTest::gtest_main PRIVATE mini_disassembler)


add_executable(mini_disassembler_client
  Client.cpp)
  
target_link_libraries(mini_disassembler_client PUBLIC GTest::gtest GTest::gtest_main PRIVATE mini_disassembler)

