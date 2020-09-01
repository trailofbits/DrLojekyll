# Copyright 2020, Trail of Bits, Inc. All rights reserved.

set(Runtime_PUBLIC_H
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Runtime.h"
)

set(Runtime_SRCS
    "Runtime.cpp"
)

set(Runtime_PRIV_DEPS
)

set(Runtime_DEPS
    settings
)

define_static_library(Runtime
  CURDIR ${CMAKE_CURRENT_SOURCE_DIR}
  SOURCES ${Runtime_SRCS}
  PUBLIC_HEADERS ${Runtime_PUBLIC_H}
  DEPENDENCIES ${Runtime_DEPS}
  PRIVATE_DEPS ${Runtime_PRIV_DEPS}
)
