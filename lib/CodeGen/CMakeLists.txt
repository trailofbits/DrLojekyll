# Copyright 2020, Trail of Bits, Inc. All rights reserved.

set(CodeGen_PUBLIC_H
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/CodeGen/BAM.h"
)

set(CodeGen_SRCS
    "BAM.cpp"
)

set(CodeGen_DEPS
    settings
    DataFlow
    Display
    Lex
    Parse
    Util
)

DefineLibrary(CodeGen
  CURDIR ${CMAKE_CURRENT_SOURCE_DIR}
  SOURCES ${CodeGen_SRCS}
  PUBLIC_HEADERS ${CodeGen_PUBLIC_H}
  DEPENDENCIES ${CodeGen_DEPS}
)
