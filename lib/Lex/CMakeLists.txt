# Copyright 2020, Trail of Bits, Inc. All rights reserved.

function(add_lex_library source_dir config)
    add_library(lex-${config} STATIC
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Lex/Format.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Lex/Lexer.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Lex/StringPool.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Lex/Token.h"

        "${source_dir}/Token.h"

        "${source_dir}/Format.cpp"
        "${source_dir}/Lexer.cpp"
        "${source_dir}/StringPool.cpp"
        "${source_dir}/Token.cpp"
    )

    target_link_libraries(lex-${config}
        PUBLIC
            settings-${config}
            display-${config}
    )
endfunction()
