# Copyright 2020, Trail of Bits, Inc. All rights reserved.

set(Display_PUBLIC_H
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Display.h"
)

set(Display_SRCS
    "DataStream.h"
    "Display.h"
    "DisplayPosition.h"

    "DataStream.cpp"
    "Display.cpp"
    "DisplayManager.cpp"
    "DisplayPosition.cpp"
    "DisplayReader.cpp"
    "Format.cpp"
)

set(Display_PRIV_DEPS
)

set(Display_DEPS
)

dr_define_static_library(Display
    CURDIR ${CMAKE_CURRENT_SOURCE_DIR}
    SOURCES ${Display_SRCS}
    PUBLIC_HEADERS ${Display_PUBLIC_H}
    DEPENDENCIES ${Display_DEPS}
    PRIVATE_DEPS ${Display_PRIV_DEPS}
)
