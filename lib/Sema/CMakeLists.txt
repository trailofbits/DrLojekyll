# Copyright 2019, Trail of Bits, Inc. All rights reserved.

function(sema source_dir config)
    add_library(sema-${config} STATIC
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Sema/BottomUpAnalysis.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Sema/ModuleIterator.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Sema/SIPSChecker.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Sema/SIPSScore.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Sema/SIPSAnalysis.h"
        "${source_dir}/BottomUpAnalysis.cpp"
        "${source_dir}/ModuleIterator.cpp"
        "${source_dir}/SIPSChecker.cpp"
        "${source_dir}/SIPSScore.cpp"
        "${source_dir}/SIPSAnalysis.cpp")

    target_link_libraries(sema-${config} PUBLIC
        parser-${config})
endfunction(sema)
