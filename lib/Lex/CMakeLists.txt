# Copyright 2019, Trail of Bits, Inc. All rights reserved.

function(lexer source_dir config)
    add_library(lexer-${config} STATIC
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Lex/Lexer.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Lex/StringPool.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Lex/Token.h"
        "${source_dir}/Token.cpp"
        "${source_dir}/Token.h"
        "${source_dir}/Lexer.cpp"
        "${source_dir}/StringPool.cpp")

    target_link_libraries(lexer-${config} PUBLIC
        display-${config}
        util-${config})
endfunction(lexer)
