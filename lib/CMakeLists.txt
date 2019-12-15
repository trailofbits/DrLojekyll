# Copyright 2019, Trail of Bits, Inc. All rights reserved.

add_subdirectory(Util)
add_subdirectory(Display)
add_subdirectory(Lex)
add_subdirectory(Parse)
add_subdirectory(Sema)
add_subdirectory(Rel)
add_subdirectory(Transforms)

function(lib source_dir config)
    util("${source_dir}/Util" ${config})
    display("${source_dir}/Display" ${config})
    lexer("${source_dir}/Lex" ${config})
    parser("${source_dir}/Parse" ${config})
    sema("${source_dir}/Sema" ${config})
    rel("${source_dir}/Rel" ${config})
    transforms("${source_dir}/Transforms" ${config})
endfunction(lib)