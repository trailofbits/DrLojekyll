# Copyright 2019, Trail of Bits, Inc. All rights reserved.

add_library(util STATIC
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Util/FileManager.h"
    FileManager.cpp)

target_link_libraries(util PUBLIC
    cxx_settings)
