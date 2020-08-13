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
    ${Parse_PUBLIC_H}

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

add_library(Parse STATIC
    ${Parse_SRCS}
)

target_link_libraries(Parse
    PUBLIC
        settings
        Display
        Lex
        Util
)

set_target_properties (Parse PROPERTIES PUBLIC_HEADER "${Parse_PUBLIC_H}")

target_include_directories(Parse PUBLIC
  $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include/drlojekyll/Parse>
  $<INSTALL_INTERFACE:include/drlojekyll/Parse>
  PRIVATE ${CMAKE_CURRENT_BINARY_DIR}
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
)

install(
  TARGETS Parse
  RUNTIME
    DESTINATION "bin"
  LIBRARY
    DESTINATION "lib/drlojekyll"
  ARCHIVE
    DESTINATION "lib/drlojekyll"
  PUBLIC_HEADER
    DESTINATION "include/drlojekyll/Parse"
)
