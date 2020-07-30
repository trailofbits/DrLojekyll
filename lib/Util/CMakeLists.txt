# Copyright 2020, Trail of Bits, Inc. All rights reserved.

add_library(util STATIC
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Util/BitManipulation.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Util/Compiler.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Util/DefUse.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Util/DisjointSet.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Util/EqualitySet.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Util/Node.h"

    "DefUse.cpp"
    "DisjointSet.cpp"
    "EqualitySet.cpp"
    "Node.cpp"
)

target_link_libraries(util PUBLIC settings)
