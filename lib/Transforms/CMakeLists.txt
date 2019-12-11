# Copyright 2019, Trail of Bits, Inc. All rights reserved.

function(transforms source_dir config)
    add_library(transforms-${config} STATIC
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Transforms/CombineModules.h"
        "${source_dir}/CombineModules.cpp")

    target_link_libraries(transforms-${config} PUBLIC
        sema-${config})
endfunction(transforms)
