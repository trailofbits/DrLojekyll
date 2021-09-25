# Copyright 2021, Trail of Bits, Inc. All rights reserved.

compile_datalog(
  DATABASE_NAME datalog
  SERVICE_NAME MiniDisassembler
  CXX_OUTPUT_DIR "${CMAKE_CURRENT_BINARY_DIR}"
  SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/database.dr"
)
