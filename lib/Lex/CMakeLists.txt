# Copyright 2020, Trail of Bits, Inc. All rights reserved.

set(Lex_PUBLIC_H
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Lex/Format.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Lex/Lexer.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Lex/StringPool.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Lex/Token.h"
)

set(Lex_SRCS
    "Token.h"

    "Format.cpp"
    "Lexer.cpp"
    "StringPool.cpp"
    "Token.cpp"
)

set(Lex_PRIV_DEPS
)

set(Lex_DEPS
    Display
)

dr_define_static_library(Lex
  CURDIR ${CMAKE_CURRENT_SOURCE_DIR}
  SOURCES ${Lex_SRCS}
  PUBLIC_HEADERS ${Lex_PUBLIC_H}
  DEPENDENCIES ${Lex_DEPS}
  PRIVATE_DEPS ${Lex_PRIV_DEPS}
)
