# Copyright 2020, Trail of Bits, Inc. All rights reserved.

add_subdirectory(Util)
add_subdirectory(Display)
add_subdirectory(Lex)
add_subdirectory(Parse)
add_subdirectory(DataFlow)
add_subdirectory(CodeGen)
add_subdirectory(Runtime)

function(add_libraries source_dir config)
    add_util_library("${source_dir}/Util" ${config})
    add_display_library("${source_dir}/Display" ${config})
    add_lex_library("${source_dir}/Lex" ${config})
    add_parse_library("${source_dir}/Parse" ${config})
    add_dataflow_library("${source_dir}/DataFlow" ${config})
    add_codegen_library("${source_dir}/CodeGen" ${config})
    add_runtime_library("${source_dir}/Runtime" ${config})
endfunction(add_libraries)