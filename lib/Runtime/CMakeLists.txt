# Copyright 2020, Trail of Bits, Inc. All rights reserved.

function(runtime source_dir config)
    add_library(runtime-${config} STATIC
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Runtime.h"
        "${source_dir}/Runtime.cpp")

    target_link_libraries(runtime-${config} PUBLIC
        cxx_settings)
endfunction(runtime)
