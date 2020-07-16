# Copyright 2019, Trail of Bits, Inc. All rights reserved.

function(transforms source_dir config)
    add_library(transforms-${config} STATIC
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Transforms/CombineModules.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Transforms/ConvertQueriesToMessages.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Transforms/ProxyExternalsWithExports.h"
        "${source_dir}/CombineModules.cpp"
        "${source_dir}/ConvertQueriesToMessages.cpp"
        "${source_dir}/ProxyExternalsWithExports.cpp")

    target_link_libraries(transforms-${config} PUBLIC
        parser-${config})
endfunction(transforms)
