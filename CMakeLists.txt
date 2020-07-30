# Copyright 2020, Trail of Bits, Inc. All rights reserved.

cmake_minimum_required(VERSION 3.14)
project(DrLojekyll LANGUAGES CXX)

include(cmake/utils.cmake)

# --------------------------------------------
# build-time options -------------------------
# --------------------------------------------
option(ENABLE_LIBFUZZER "Build libFuzzer fuzz targets" OFF)
option(ENABLE_SANITIZERS "Build with ASAN and UBSAN" OFF)
option(WARNINGS_AS_ERRORS "Build treating warnings as errors" OFF)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# --------------------------------------------
# toolchain-specific warning options ---------
# --------------------------------------------
set(GNULIKE_COMPILER_LIST "Clang" "AppleClang" "GNU")

if(CMAKE_CXX_COMPILER_ID IN_LIST GNULIKE_COMPILER_LIST)
    set(CXX_WARNING_OPTIONS -Wall -pedantic -Wconversion -Wno-unknown-pragmas)
    set(CXX_WARNINGS_AS_ERRORS_OPTION -Werror)
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
    set(CXX_WARNING_OPTIONS /W4)
    set(CXX_WARNINGS_AS_ERRORS_OPTION /WX)
else()
    set(CXX_WARNING_OPTIONS)
    set(CXX_WARNINGS_AS_ERRORS_OPTION)
    message(WARNING "Unsupported C++ compiler '${CMAKE_CXX_COMPILER_ID}'; build may not work right!")
endif()

# --------------------------------------------
# `settings` for global build options --------
# --------------------------------------------
add_library(settings INTERFACE)
target_include_directories(settings INTERFACE
    "${CMAKE_CURRENT_LIST_DIR}/include")

target_compile_options(settings INTERFACE ${CXX_WARNING_OPTIONS})

if(WARNINGS_AS_ERRORS)
    target_compile_options(settings INTERFACE ${CXX_WARNINGS_AS_ERRORS_OPTION})
endif()

# sanitizer settings -------------------------
if(ENABLE_SANITIZERS)
    add_sanitizer_settings(settings
        SANITIZERS
            address
            undefined
        OPTIONS
            -fsanitize-address-use-after-scope
    )
endif()

# fuzzer settings ----------------------------
if(ENABLE_LIBFUZZER)
    if(NOT CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
        message(FATAL_ERROR "ENABLE_LIBFUZZER requires using Clang (currently using '${CMAKE_CXX_COMPILER_ID}')")
    endif()

    add_sanitizer_settings(settings
        SANITIZERS
            fuzzer-no-link
        OPTIONS
            -g
    )

    add_sanitizer_settings(settings-fuzz-target SANITIZERS fuzzer)
endif()

# targets ------------------------------------
add_subdirectory(lib)

add_executable(test
    Main.cpp)
target_link_libraries(test PRIVATE
    settings
    util
    display
    lex
    parse
    dataflow
    codegen)

if(ENABLE_LIBFUZZER)
    add_executable(fuzzer
        fuzz/Fuzzer.cpp)
    target_link_libraries(fuzzer PRIVATE
        settings
        settings-fuzz-target
        util
        display
        lex
        parse
    )
endif()

