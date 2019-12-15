# Copyright 2019, Trail of Bits, Inc. All rights reserved.

function(rel source_dir config)
    add_library(rel-${config} STATIC
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Rel/Builder.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Rel/Composer.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Rel/Format.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Rel/Query.h"
        "${source_dir}/Builder.cpp"
        "${source_dir}/Format.cpp"
        "${source_dir}/Query.h"
        "${source_dir}/Query.cpp")

    target_link_libraries(rel-${config} PUBLIC
        sema-${config})
endfunction(rel)
