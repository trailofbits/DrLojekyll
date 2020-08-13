# Copyright 2020, Trail of Bits, Inc. All rights reserved.

set(Parse_PUBLIC_H
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Parse/Error.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Parse/ErrorLog.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Parse/Format.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Parse/ModuleIterator.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Parse/Parse.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Parse/Parser.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Parse/Type.h"
)

set(Parse_SRCS
    "Error.h"
    "Parser.h"
    "Parse.h"

    "Aggregate.cpp"
    "Clause.cpp"
    "Error.cpp"
    "ErrorLog.cpp"
    "Format.cpp"
    "Functor.cpp"
    "Import.cpp"
    "Include.cpp"
    "Inline.cpp"
    "Message.cpp"
    "ModuleIterator.cpp"
    "Parse.cpp"
    "Parser.cpp"
    "Resolve.cpp"
    "Query.cpp"
    "Type.cpp"
    "Variable.cpp"
)

set(Parse_PRIV_DEPS
    settings
)

set(Parse_DEPS
    Display
    Lex
    Util
)

DefineLibrary(Parse
  CURDIR ${CMAKE_CURRENT_SOURCE_DIR}
  SOURCES ${Parse_SRCS}
  PUBLIC_HEADERS ${Parse_PUBLIC_H}
  DEPENDENCIES ${Parse_DEPS}
  PRIVATE_DEPS ${Parse_PRIV_DEPS}
)
