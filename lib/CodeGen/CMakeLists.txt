# Copyright 2020, Trail of Bits, Inc. All rights reserved.

set(CodeGen_PUBLIC_H
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/CodeGen/CodeGen.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/CodeGen/MessageSerialization.h"
)

set(CodeGen_SRCS
    "MessageSerialization.cpp"
    "CPlusPlus/Database.cpp"
    "CPlusPlus/Interface.cpp"
    "CPlusPlus/Util.cpp"
    "CPlusPlus/Util.h"
    "Python/Database.cpp"
    "Python/Interface.cpp"
    "Python/Util.cpp"
    "Python/Util.h"
)

set(CodeGen_PRIV_DEPS
)

set(CodeGen_DEPS
    nlohmann_json::nlohmann_json
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
