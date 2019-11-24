# Copyright 2019, Trail of Bits, Inc. All rights reserved.

add_library(display STATIC
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Display/Display.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Display/DisplayConfiguration.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Display/DisplayManager.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Display/DisplayPosition.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Display/DisplayReader.h"
    DisplayPosition.cpp DisplayPosition.h
    Display.cpp Display.h
    DisplayManager.cpp
    DisplayReader.cpp
    DataStream.cpp DataStream.h)

target_link_libraries(display PUBLIC
    cxx_settings)
