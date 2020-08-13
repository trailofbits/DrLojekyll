# Copyright 2020, Trail of Bits, Inc. All rights reserved.

set(Lex_PUBLIC_H
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Lex/Format.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Lex/Lexer.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Lex/StringPool.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Lex/Token.h"
)

set(Lex_SRCS
    ${Lex_PUBLIC_H}

    "Token.h"

    "Format.cpp"
    "Lexer.cpp"
    "StringPool.cpp"
    "Token.cpp"
)

add_library(Lex STATIC
    ${Lex_SRCS}
)

target_link_libraries(Lex
    PUBLIC
        settings
        Display
)

set_target_properties (Lex PROPERTIES PUBLIC_HEADER "${Lex_PUBLIC_H}")

target_include_directories(Lex PUBLIC
  $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include/drlojekyll/Lex>
  $<INSTALL_INTERFACE:include/drlojekyll/Lex>
  PRIVATE ${CMAKE_CURRENT_BINARY_DIR}
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
)

install(
  TARGETS Lex
  RUNTIME
    DESTINATION "bin"
  LIBRARY
    DESTINATION "lib/drlojekyll"
  ARCHIVE
    DESTINATION "lib/drlojekyll"
  PUBLIC_HEADER
    DESTINATION "include/drlojekyll/Lex"
)
