# Copyright 2020, Trail of Bits, Inc. All rights reserved.

set(Util_PUBLIC_H
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Util.h"
)

set(Util_SRCS
    ${Util_PUBLIC_H}

    "DefUse.cpp"
    "DisjointSet.cpp"
    "EqualitySet.cpp"
    "Node.cpp"
)

set(Util_PRIV_DEPS
)

set(Util_DEPS
)

dr_define_static_library(Util
  CURDIR ${CMAKE_CURRENT_SOURCE_DIR}
  SOURCES ${Util_SRCS}
  PUBLIC_HEADERS ${Util_PUBLIC_H}
  DEPENDENCIES ${Util_DEPS}
  PRIVATE_DEPS ${Util_PRIV_DEPS}
)
