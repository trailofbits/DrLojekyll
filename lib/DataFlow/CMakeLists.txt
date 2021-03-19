# Copyright 2020, Trail of Bits, Inc. All rights reserved.

set(DataFlow_PUBLIC_H
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/DataFlow/Format.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/DataFlow/Query.h"
)

set(DataFlow_SRCS
    "Optimize.h"
    "Query.h"
    "Taint.h"

    "Aggregate.cpp"
    "Build.cpp"
    "Column.cpp"
    "Columns.cpp"
    "Compare.cpp"
    "Condition.cpp"
    "Connect.cpp"
    "Compare.cpp"
    "Constant.cpp"
    "Delete.cpp"
    "DeadFlowElimination.cpp"
    "Differential.cpp"
    "Format.cpp"
    "IO.cpp"
    "Insert.cpp"
    "Join.cpp"
    "KVIndex.cpp"
    "Link.cpp"
    "Map.cpp"
    "Merge.cpp"
    "Negate.cpp"
    "Optimize.cpp"
    "Query.cpp"
    "Select.cpp"
    "Stream.cpp"
    "Tuple.cpp"
    "View.cpp"
)

set(DataFlow_PRIV_DEPS
)

set(DataFlow_DEPS
    Display
    Lex
    Parse
    Util
)

dr_define_static_library(DataFlow
  CURDIR ${CMAKE_CURRENT_SOURCE_DIR}
  SOURCES ${DataFlow_SRCS}
  PUBLIC_HEADERS ${DataFlow_PUBLIC_H}
  DEPENDENCIES ${DataFlow_DEPS}
  PRIVATE_DEPS ${DataFlow_PRIV_DEPS}
)