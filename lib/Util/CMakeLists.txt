# Copyright 2019, Trail of Bits, Inc. All rights reserved.

function(util source_dir config)
    add_library(util-${config} STATIC
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Util/FileManager.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Util/DisjointSet.h"
        "${source_dir}/FileManager.cpp"
        "${source_dir}/DisjointSet.cpp")

    target_link_libraries(util-${config} PUBLIC
        ${config})
endfunction(util)
