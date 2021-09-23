# Copyright 2020, Trail of Bits, Inc. All rights reserved.

set(CodeGen_PUBLIC_H
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/CodeGen/CodeGen.h"
)

set(CodeGen_SRCS
    "CPlusPlus/Database.cpp"
    "CPlusPlus/Interface.cpp"
    "CPlusPlus/Service.cpp"
    "CPlusPlus/Util.cpp"
    "CPlusPlus/Util.h"
    "FlatBuffer/Interface.cpp"
    "Python/Database.cpp"
    "Python/Interface.cpp"
    "Python/Util.cpp"
    "Python/Util.h"
    "Util.cpp"
)

add_library(codegen_includes INTERFACE)
target_include_directories(codegen_includes INTERFACE
    "${CMAKE_CURRENT_SOURCE_DIR}/CPlusPlus"
)

set(CodeGen_PRIV_DEPS
    codegen_includes
)

set(CodeGen_DEPS
    ControlFlow
    DataFlow
    Display
    Lex
    Parse
    Util
)

dr_define_static_library(CodeGen
  CURDIR ${CMAKE_CURRENT_SOURCE_DIR}
  SOURCES ${CodeGen_SRCS}
  PUBLIC_HEADERS ${CodeGen_PUBLIC_H}
  DEPENDENCIES ${CodeGen_DEPS}
  PRIVATE_DEPS ${CodeGen_PRIV_DEPS}
)
