# Copyright 2021, Trail of Bits, Inc. All rights reserved.

find_package(GTest CONFIG REQUIRED)
include(GoogleTest)

compile_datalog(
  DATABASE_NAME points_to
  LIBRARY_NAME points_to
  CXX_OUTPUT_DIR "${CMAKE_CURRENT_BINARY_DIR}"
  DOT_OUTPUT_FILE "${CMAKE_CURRENT_BINARY_DIR}/database.dot"
  IR_OUTPUT_FILE "${CMAKE_CURRENT_BINARY_DIR}/database.ir"
  FB_OUTPUT_FILE "${CMAKE_CURRENT_BINARY_DIR}/database.fbs"
  SOURCES database.dr
)

set(PT_ASSIGN_ALLOC_PATH "${CMAKE_CURRENT_LIST_DIR}/facts/AssignAlloc.facts")
set(PT_LOAD_PATH "${CMAKE_CURRENT_LIST_DIR}/facts/Load.facts")
set(PT_PRIMITIVE_ASSIGN_PATH "${CMAKE_CURRENT_LIST_DIR}/facts/PrimitiveAssign.facts")
set(PT_STORE_PATH "${CMAKE_CURRENT_LIST_DIR}/facts/Store.facts")

set(PT_ALIAS_PATH "${CMAKE_CURRENT_BINARY_DIR}/Alias.tsv")
set(PT_ASSIGN_PATH "${CMAKE_CURRENT_BINARY_DIR}/Assign.tsv")
set(PT_VAR_POINTS_TO_PATH "${CMAKE_CURRENT_BINARY_DIR}/VarPointsTo.tsv")

configure_file(
  "${CMAKE_CURRENT_LIST_DIR}/FactPaths.h.in"
  "${CMAKE_CURRENT_BINARY_DIR}/FactPaths.h"
  @ONLY)

add_executable(points_to_standalone
  Standalone.cpp)

target_link_libraries(points_to_standalone PUBLIC GTest::gtest GTest::gtest_main PRIVATE points_to)

