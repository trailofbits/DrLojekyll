# Copyright 2020, Trail of Bits, Inc. All rights reserved.

add_library(lex STATIC
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Lex/Format.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Lex/Lexer.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Lex/StringPool.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Lex/Token.h"

    "Token.h"

    "Format.cpp"
    "Lexer.cpp"
    "StringPool.cpp"
    "Token.cpp"
)

target_link_libraries(lex
    PUBLIC
        settings
        display
)