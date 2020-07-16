# Copyright 2019, Trail of Bits, Inc. All rights reserved.

cmake_minimum_required(VERSION 3.14)
project(DrLojekyll)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(ENABLE_FUZZING "Enable generation of libFuzzer targets" OFF)

add_library(cxx_settings INTERFACE)
target_include_directories(cxx_settings INTERFACE
    "${CMAKE_CURRENT_LIST_DIR}/include")

target_compile_options(cxx_settings INTERFACE
   -Werror -Wall -pedantic -Wconversion)

# release ------------------------------------
add_library(rel INTERFACE)
target_compile_options(rel INTERFACE
    -O3 -gline-tables-only)

target_link_libraries(rel INTERFACE
    cxx_settings)

# debug --------------------------------------
add_library(dbg INTERFACE)
target_compile_options(dbg INTERFACE
    -O0 -g3)

target_link_libraries(dbg INTERFACE
    cxx_settings)

# sanitizer ----------------------------------
add_library(sanitizer INTERFACE)
target_compile_options(sanitizer INTERFACE
    -fsanitize=address,undefined
    -fsanitize-address-use-after-scope
    -fno-omit-frame-pointer
    -fno-optimize-sibling-calls)

target_link_options(sanitizer INTERFACE
    -fsanitize=address,undefined
    -Wl,-undefined,dynamic_lookup)

target_link_libraries(sanitizer INTERFACE
    cxx_settings)

# libfuzzer ---------------------------------
add_library(libfuzzer INTERFACE)

target_compile_options(libfuzzer INTERFACE
    -Werror -Wall -pedantic -Wconversion -g
    -fsanitize=fuzzer,address,undefined
    -fsanitize-address-use-after-scope
    -fno-omit-frame-pointer
    -fno-optimize-sibling-calls)

target_link_options(libfuzzer INTERFACE
    -fsanitize=address,fuzzer,undefined
    -Wl,-undefined,dynamic_lookup)

target_link_libraries(libfuzzer INTERFACE
    cxx_settings)

add_subdirectory(lib)

function(main config)
    add_executable(test-${config}
        Main.cpp)
    target_link_libraries(test-${config}
        rel-${config}
        codegen-${config})
endfunction(main)

function(fuzzer config)
    add_executable(fuzzer-${config}
        Fuzzer.cpp)
    target_link_libraries(fuzzer-${config}
        parser-${config})
endfunction(fuzzer)

# build what we need ------------------------
lib("${CMAKE_CURRENT_LIST_DIR}/lib" dbg)
main(dbg)

lib("${CMAKE_CURRENT_LIST_DIR}/lib" sanitizer)
main(sanitizer)

if (ENABLE_FUZZING)
    lib("${CMAKE_CURRENT_LIST_DIR}/lib" libfuzzer)
    fuzzer(libfuzzer)
endif(ENABLE_FUZZING)
