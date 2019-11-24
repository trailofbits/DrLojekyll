# Copyright 2019, Trail of Bits, Inc. All rights reserved.

add_library(parser STATIC
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Parse/Error.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Parse/ErrorLog.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Parse/Parse.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Parse/Parser.h"
    Parse.cpp Parse.h
    Error.cpp ErrorLog.cpp
    Parser.cpp ErrorLog.cpp Error.h)

target_link_libraries(parser PUBLIC
    lexer)
