# Copyright 2019, Trail of Bits, Inc. All rights reserved.

cmake_minimum_required(VERSION 3.14)
project(DrLojekyll)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

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

# asan ---------------------------------------
add_library(asan INTERFACE)
target_compile_options(asan INTERFACE
    -fsanitize=address,undefined
    -fsanitize-address-use-after-scope
    -fno-omit-frame-pointer
    -fno-optimize-sibling-calls)

target_link_options(asan INTERFACE
    -fsanitize=address,undefined
    -Wl,-undefined,dynamic_lookup)

target_link_libraries(asan INTERFACE
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
        main.cpp)

    target_link_libraries(test-${config}
        transforms-${config})
endfunction(main)

function(fuzz_harness config)
    add_executable(fuzz_harness-${config}
        fuzz_harness.cpp
    )
    target_link_libraries(fuzz_harness-${config}
		parser-${config}
    )
endfunction(fuzz_harness)

# Build everything in the `lib` dir using the `dbg`, `asan`, and `libfuzzer`
# configurations.
lib("${CMAKE_CURRENT_LIST_DIR}/lib" dbg)
lib("${CMAKE_CURRENT_LIST_DIR}/lib" asan)
lib("${CMAKE_CURRENT_LIST_DIR}/lib" libfuzzer)

# Build the main program against the `dbg` and `asan` configurations.
main(dbg)
main(asan)
fuzz_harness(libfuzzer)
