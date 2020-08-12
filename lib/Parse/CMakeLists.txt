# Copyright 2020, Trail of Bits, Inc. All rights reserved.

add_library(parse STATIC
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Parse/Error.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Parse/ErrorLog.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Parse/Format.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Parse/ModuleIterator.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Parse/Parse.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Parse/Parser.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Parse/Type.h"

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

find_package(Filesystem REQUIRED)
target_link_libraries(parse
    PUBLIC
        settings
        display
        lex
        util
        std::filesystem
)
