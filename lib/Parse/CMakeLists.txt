# Copyright 2019, Trail of Bits, Inc. All rights reserved.

function(parser source_dir config)
    add_library(parser-${config} STATIC
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Parse/Error.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Parse/ErrorLog.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Parse/Parse.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Parse/Parser.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Parse/Type.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Parse/Format.h"
        "${source_dir}/Parse.cpp"
        "${source_dir}/Parse.h"
        "${source_dir}/Error.cpp"
        "${source_dir}/ErrorLog.cpp"
        "${source_dir}/Parser.cpp"
        "${source_dir}/ErrorLog.cpp"
        "${source_dir}/Error.h"
        "${source_dir}/Format.cpp"
        )
    target_link_libraries(parser-${config} PUBLIC
        lexer-${config})
endfunction(parser)
