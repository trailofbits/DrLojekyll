# Copyright 2020, Trail of Bits, Inc. All rights reserved.

add_library(dataflow STATIC
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/DataFlow/Format.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/DataFlow/Query.h"

    "Optimize.h"
    "Query.h"

    "Aggregate.cpp"
    "Build.cpp"
    "Column.cpp"
    "Columns.cpp"
    "Compare.cpp"
    "Condition.cpp"
    "Connect.cpp"
    "Constant.cpp"
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
    "Optimize.cpp"
    "Query.cpp"
    "Select.cpp"
    "Stream.cpp"
    "Tuple.cpp"
    "View.cpp"
)

target_link_libraries(dataflow
    PUBLIC
        settings
        display
        lex
        parse
        util
)