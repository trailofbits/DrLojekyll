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
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# --------------------------------------------
# toolchain-specific warning options ---------
# --------------------------------------------
set(GNULIKE_COMPILER_LIST "Clang" "AppleClang" "GNU")

if(CMAKE_CXX_COMPILER_ID IN_LIST GNULIKE_COMPILER_LIST)
    set(CXX_WARNING_OPTIONS "-Wall -pedantic -Wconversion -Wno-unknown-pragmas")
    set(CXX_WARNINGS_AS_ERRORS_OPTION "-Werror")
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
    set(CXX_WARNING_OPTIONS "/Wall")
    set(CXX_WARNINGS_AS_ERRORS_OPTION "/WX")
else()
    set(CXX_WARNING_OPTIONS "")
    set(CXX_WARNINGS_AS_ERRORS_OPTION "")
    message(WARNING "Unsupported C++ compiler '${CMAKE_CXX_COMPILER_ID}'; build may not work right!")
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_WARNING_OPTIONS}")

if(WARNINGS_AS_ERROR)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_WARNINGS_AS_ERRORS_OPTION}")
endif()

# settings -----------------------------------
add_library(settings INTERFACE)
target_include_directories(settings INTERFACE
    "${CMAKE_CURRENT_LIST_DIR}/include")

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
    add_sanitizer_settings(settings-sanitizer
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
        message(FATAL_ERROR "You must use Clang to use ENABLE_LIBFUZZER (currently using '${CMAKE_CXX_COMPILER_ID}')")
    endif()

    add_sanitizer_settings(settings-libfuzzer
        SANITIZERS
            fuzzer
        OPTIONS
            -g)

    target_link_libraries(settings-libfuzzer INTERFACE
        settings)
endif()

# targets ------------------------------------
add_subdirectory(lib)

function(add_test_target config)
    add_doctor_lojekyll_libraries("${CMAKE_CURRENT_LIST_DIR}/lib" ${config})
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
    function(add_fuzzers)
        add_doctor_lojekyll_libraries("${CMAKE_CURRENT_LIST_DIR}/lib" libfuzzer)
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
add_test_target(debug)

if(ENABLE_SANITIZERS)
    add_test_target(sanitizer)
endif()

if(ENABLE_LIBFUZZER)
    add_fuzzers()
endif()
