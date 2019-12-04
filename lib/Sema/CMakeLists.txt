# Copyright 2019, Trail of Bits, Inc. All rights reserved.

function(sema source_dir config)
    add_library(sema-${config} STATIC
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Sema/SIPSAnalysis.h"
        "${source_dir}/SIPSAnalysis.cpp")

    target_link_libraries(sema-${config} PUBLIC
        parser-${config})
endfunction(sema)
