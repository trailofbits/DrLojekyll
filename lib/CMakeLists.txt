# Copyright 2019, Trail of Bits, Inc. All rights reserved.

add_subdirectory(Util)
add_subdirectory(Display)
add_subdirectory(Lex)
add_subdirectory(Parse)

function(lib source_dir config)
    util("${source_dir}/Util" ${config})
    display("${source_dir}/Display" ${config})
    lexer("${source_dir}/Lex" ${config})
    parser("${source_dir}/Parse" ${config})
endfunction(lib)