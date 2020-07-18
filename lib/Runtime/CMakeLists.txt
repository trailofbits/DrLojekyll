# Copyright 2020, Trail of Bits, Inc. All rights reserved.

function(add_runtime_library source_dir config)
    add_library(runtime-${config} STATIC
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Runtime.h"
        "${source_dir}/Runtime.cpp")

    target_link_libraries(runtime-${config}
        PUBLIC
            settings-${config})
endfunction(add_runtime_library)
