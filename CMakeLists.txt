# Copyright 2020, Trail of Bits, Inc. All rights reserved.

cmake_minimum_required(VERSION 3.14)
project(DrLojekyll)

include(cmake/utils.cmake)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(ENABLE_FUZZING "Enable generation of libFuzzer targets" OFF)

add_library(settings INTERFACE)
target_include_directories(settings INTERFACE
    "${CMAKE_CURRENT_LIST_DIR}/include")

target_compile_options(settings INTERFACE
   -Werror -Wall -pedantic -Wconversion)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    target_compile_options(settings INTERFACE
       -Wno-error=unknown-pragmas)
endif()

# release ------------------------------------
add_library(settings-release INTERFACE)
target_compile_options(settings-release INTERFACE
    -O3 -gline-tables-only)

target_link_libraries(settings-release INTERFACE
    settings)

# debug --------------------------------------
add_library(settings-debug INTERFACE)
target_compile_options(settings-debug INTERFACE
    -O0 -g3)

target_link_libraries(settings-debug INTERFACE
    settings)

# sanitizer ----------------------------------

add_sanitizer(settings-sanitizer
    SANITIZERS
        address
        undefined
    OPTIONS
        -fsanitize-address-use-after-scope)

target_link_libraries(settings-sanitizer INTERFACE
    settings)

# libfuzzer ----------------------------------

add_sanitizer(settings-libfuzzer
    SANITIZERS
        fuzzer
        address
        undefined
    OPTIONS
        -fsanitize-address-use-after-scope
        -g)

target_link_libraries(settings-libfuzzer INTERFACE
    settings)

# targets ------------------------------------

add_subdirectory(lib)

function(main config)
    add_libraries("${CMAKE_CURRENT_LIST_DIR}/lib" ${config})
    add_executable(test-${config}
        Main.cpp)
    target_link_libraries(test-${config}
        settings-${config}
        util-${config}
        display-${config}
        lex-${config}
        parse-${config}
        dataflow-${config}
        codegen-${config})
endfunction(main)

function(fuzzer)
    add_libraries("${CMAKE_CURRENT_LIST_DIR}/lib" libfuzzer)
    add_executable(fuzzer
        Fuzzer.cpp)
    target_link_libraries(fuzzer
        settings-libfuzzer
        util-libfuzzer
        display-libfuzzer
        lex-libfuzzer
        parse-libfuzzer)
endfunction(fuzzer)

# build what we need -------------------------
main(debug)
main(sanitizer)

if(ENABLE_FUZZING)
    fuzzer()
endif(ENABLE_FUZZING)
