# Copyright 2019, Trail of Bits, Inc. All rights reserved.

function(display source_dir config)
    add_library(display-${config} STATIC
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Display/Display.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Display/DisplayConfiguration.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Display/DisplayManager.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Display/DisplayPosition.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Display/DisplayReader.h"
        "${source_dir}/DisplayPosition.cpp"
        "${source_dir}/DisplayPosition.h"
        "${source_dir}/Display.cpp"
        "${source_dir}/Display.h"
        "${source_dir}/DisplayManager.cpp"
        "${source_dir}/DisplayReader.cpp"
        "${source_dir}/DataStream.cpp"
        "${source_dir}/DataStream.h")

    target_link_libraries(display-${config} PUBLIC
        ${config})
endfunction(display)