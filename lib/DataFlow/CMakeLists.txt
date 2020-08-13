# Copyright 2020, Trail of Bits, Inc. All rights reserved.

set(DataFlow_PUBLIC_H
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/DataFlow/Format.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/DataFlow/Query.h"
)

set(DataFlow_SRCS
    ${DataFlow_PUBLIC_H}

    "Optimize.h"
    "Query.h"

    "Aggregate.cpp"
    "Build.cpp"
    "Column.cpp"
    "Columns.cpp"
    "Condition.cpp"
    "Connect.cpp"
    "Constant.cpp"
    "Constraint.cpp"
    "DeadFlowElimination.cpp"
    "Differential.cpp"
    "Format.cpp"
    "IO.cpp"
    "Insert.cpp"
    "Join.cpp"
    "KVIndex.cpp"
    "Map.cpp"
    "Merge.cpp"
    "Optimize.cpp"
    "Query.cpp"
    "Select.cpp"
    "Stream.cpp"
    "Tuple.cpp"
    "View.cpp"
)

add_library(DataFlow STATIC
    ${DataFlow_SRCS}
)

target_link_libraries(DataFlow
    PUBLIC
        settings
        Display
        Lex
        Parse
        Util
)

set_target_properties (DataFlow PROPERTIES PUBLIC_HEADER "${DataFlow_PUBLIC_H}")

target_include_directories(DataFlow PUBLIC
  $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include/drlojekyll/DataFlow>
  $<INSTALL_INTERFACE:include/drlojekyll/DataFlow>
  PRIVATE ${CMAKE_CURRENT_BINARY_DIR}
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
)

install(
  TARGETS DataFlow
  RUNTIME
    DESTINATION "bin"
  LIBRARY
    DESTINATION "lib/drlojekyll"
  ARCHIVE
    DESTINATION "lib/drlojekyll"
  PUBLIC_HEADER
    DESTINATION "include/drlojekyll/DataFlow"
)
