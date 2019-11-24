# Copyright 2019, Trail of Bits, Inc. All rights reserved.

add_library(lexer STATIC
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Lex/Lexer.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Lex/StringPool.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Lex/Token.h"
    Token.cpp Token.h
    Lexer.cpp
    StringPool.cpp)

target_link_libraries(lexer PUBLIC
    display
    util)
