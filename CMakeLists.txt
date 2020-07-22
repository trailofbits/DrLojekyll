# Copyright 2020, Trail of Bits, Inc. All rights reserved.

cmake_minimum_required(VERSION 3.14)
project(DrLojekyll)

# --------------------------------------------
# build-time options -------------------------
# --------------------------------------------
option(ENABLE_LIBFUZZER "Build libFuzzer fuzz targets" OFF)
option(ENABLE_SANITIZERS "Build with ASAN and UBSAN" OFF)

include(cmake/utils.cmake)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# settings -----------------------------------
add_library(settings INTERFACE)
target_include_directories(settings INTERFACE
    "${CMAKE_CURRENT_LIST_DIR}/include")

target_compile_options(settings INTERFACE
   -Werror -Wall -pedantic -Wconversion)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    target_compile_options(settings INTERFACE
       -Wno-unknown-pragmas)
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
if(ENABLE_SANITIZERS)
    add_sanitizer(settings-sanitizer
        SANITIZERS
            address
            undefined
        OPTIONS
            -fsanitize-address-use-after-scope)

    target_link_libraries(settings-sanitizer INTERFACE
        settings)
endif()

# libfuzzer ----------------------------------
if(ENABLE_LIBFUZZER)
    if(NOT CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
        message(FATAL_ERROR "You must use Clang to use ENABLE_LIBFUZZER")
    endif()

    add_sanitizer(settings-libfuzzer
        SANITIZERS
            fuzzer
        OPTIONS
            -fsanitize-address-use-after-scope
            -g)

    target_link_libraries(settings-libfuzzer INTERFACE
        settings)
endif()

# targets ------------------------------------

add_subdirectory(lib)  # note: defines `add_libraries` function

function(main config)
    add_libraries("${CMAKE_CURRENT_LIST_DIR}/lib" ${config})
    add_executable(test-${config}
        Main.cpp)
    target_link_libraries(test-${config} PRIVATE
        settings-${config}
        util-${config}
        display-${config}
        lex-${config}
        parse-${config}
        dataflow-${config}
        codegen-${config})
endfunction()

if(ENABLE_LIBFUZZER)
    function(fuzzer)
        add_libraries("${CMAKE_CURRENT_LIST_DIR}/lib" libfuzzer)
        add_executable(fuzzer
            fuzz/Fuzzer.cpp)
        target_link_libraries(fuzzer PRIVATE
            settings-libfuzzer
            util-libfuzzer
            display-libfuzzer
            lex-libfuzzer
            parse-libfuzzer)
    endfunction()
endif()

# --------------------------------------------
# build what we need -------------------------
# --------------------------------------------
main(debug)

if(ENABLE_SANITIZERS)
    main(sanitizer)
endif()

if(ENABLE_LIBFUZZER)
    fuzzer()
endif()
