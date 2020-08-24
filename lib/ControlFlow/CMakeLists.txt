# Copyright 2020, Trail of Bits, Inc. All rights reserved.

add_library(controlflow STATIC
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/ControlFlow/Format.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/ControlFlow/Program.h"

    "Program.h"
    
    "Build/Build.h"
    "Build/Build.cpp"
    "Build/Induction.cpp"
    "Build/Insert.cpp"
    "Build/Join.cpp"
    "Build/Merge.cpp"
    "Build/Product.cpp"
    "Build/Union.cpp"
    
    "Format.cpp"
    "Induction.cpp"
    "Operation.cpp"
    "Parallel.cpp"
    "Procedure.cpp"
    "Program.cpp"
    "Region.cpp"
    "Series.cpp"
    "Table.cpp"
)

target_link_libraries(controlflow
    PUBLIC
        settings
        dataflow
        display
        lex
        parse
        util
)