# Copyright 2020, Trail of Bits, Inc. All rights reserved.

function(add_codegen_library source_dir config)
    add_library(codegen-${config} STATIC
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/CodeGen/BAM.h"
        "${source_dir}/BAM.cpp")

    target_link_libraries(codegen-${config}
        PUBLIC
            settings-${config}
        INTERFACE
            dataflow-${config}-headers)
endfunction(add_codegen_library)
