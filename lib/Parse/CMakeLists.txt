# Copyright 2019, Trail of Bits, Inc. All rights reserved.

function(parser source_dir config)
    add_library(parser-${config} STATIC
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Parse/Error.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Parse/ErrorLog.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Parse/Format.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Parse/ModuleIterator.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Parse/Parse.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Parse/Parser.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Parse/Type.h"
        
        "${source_dir}/Error.h"
        "${source_dir}/Parse.h"
        "${source_dir}/Parser.h"
        
        "${source_dir}/Aggregate.cpp"
        "${source_dir}/Clause.cpp"
        "${source_dir}/Error.cpp"
        "${source_dir}/ErrorLog.cpp"
        "${source_dir}/Format.cpp"
        "${source_dir}/Functor.cpp"
        "${source_dir}/Import.cpp"
        "${source_dir}/Include.cpp"
        "${source_dir}/Inline.cpp"
        "${source_dir}/Message.cpp"
        "${source_dir}/ModuleIterator.cpp"
        "${source_dir}/Parse.cpp"
        "${source_dir}/Parser.cpp"
        "${source_dir}/Query.cpp"
        "${source_dir}/Type.cpp"
        "${source_dir}/Variable.cpp"
        )
    target_link_libraries(parser-${config} PUBLIC
        lexer-${config})
endfunction(parser)
