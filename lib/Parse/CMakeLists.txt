# Copyright 2020, Trail of Bits, Inc. All rights reserved.

function(add_parse_library source_dir config)
    add_library(parse-${config}-headers INTERFACE)
    target_sources(parse-${config}-headers INTERFACE
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Parse/Error.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Parse/ErrorLog.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Parse/Format.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Parse/ModuleIterator.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Parse/Parse.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Parse/Parser.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Parse/Type.h")
    target_link_libraries(parse-${config}-headers INTERFACE
        lex-${config}-headers
        util-${config}-headers)

    add_library(parse-${config} STATIC
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
        "${source_dir}/Variable.cpp")

    target_link_libraries(parse-${config}
        PUBLIC
            settings-${config}
        INTERFACE
            parse-${config}-headers)
endfunction(add_parse_library)
