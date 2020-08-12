# Copyright 2020, Trail of Bits, Inc. All rights reserved.

add_library(codegen STATIC
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/CodeGen/BAM.h"

    "BAM.cpp"
)

target_link_libraries(codegen
    PUBLIC
        settings
        dataflow
        display
        lex
        parse
        util
)

install(
  TARGETS codegen
  RUNTIME
    DESTINATION "bin"
  LIBRARY
    DESTINATION "lib"
  ARCHIVE
    DESTINATION "lib"
)
