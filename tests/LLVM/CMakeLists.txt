# Copyright 2022, Trail of Bits, Inc. All rights reserved.

find_package(Filesystem REQUIRED)
find_package(LLVM CONFIG REQUIRED)
find_package(gap CONFIG REQUIRED)

compile_datalog(
  DATABASE_NAME llvm
  LIBRARY_NAME llvm_datalog
  CXX_OUTPUT_DIR "${CMAKE_CURRENT_BINARY_DIR}"
  DOT_OUTPUT_FILE "${CMAKE_CURRENT_BINARY_DIR}/llvm.dot"
  IR_OUTPUT_FILE "${CMAKE_CURRENT_BINARY_DIR}/llvm.ir"
  SOURCES llvm.dr analyze.dr
)

# These are out-of-order in `LLVM_AVAILABLE_LIBS` and should always be last.
list(REMOVE_ITEM LLVM_LIBRARIES LLVMMC LLVMCore LLVMSupport)
list(APPEND LLVM_LIBRARIES LLVMMC LLVMCore LLVMSupport)

add_executable(analyze-llvm
  Interface.cpp
  Interface.h
  Main.cpp
  Report.cpp
  Report.h)

target_link_libraries(analyze-llvm
  PRIVATE
    std::filesystem
    gap::gap-core
    LLVMCore
    LLVMSupport
    LLVMIRReader
    llvm_datalog
)

