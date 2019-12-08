# Copyright 2019, Trail of Bits, Inc. All rights reserved.

function(sema source_dir config)
    add_library(sema-${config} STATIC
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Sema/SIPSAnalysis.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Sema/ModuleIterator.h"
        "${source_dir}/SIPSAnalysis.cpp"
        "${source_dir}/ModuleIterator.cpp")

    target_link_libraries(sema-${config} PUBLIC
        parser-${config})
endfunction(sema)
