# Copyright 2020, Trail of Bits, Inc. All rights reserved.

set(CodeGen_PUBLIC_H
    #"${PROJECT_SOURCE_DIR}/include/drlojekyll/CodeGen/BAM.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/CodeGen/MessageSerialization.h"
)

set(CodeGen_SRCS
    # "BAM.cpp" # Does not compile...
    "MessageSerialization.cpp"
)

set(CodeGen_PRIV_DEPS
)

set(CodeGen_DEPS
    nlohmann_json::nlohmann_json
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