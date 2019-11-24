# Copyright 2019, Trail of Bits, Inc. All rights reserved.

cmake_minimum_required(VERSION 3.15)
project(DrLojekyll)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_library(cxx_settings INTERFACE)
target_include_directories(cxx_settings INTERFACE
    "${CMAKE_CURRENT_LIST_DIR}/include")

target_compile_options(cxx_settings INTERFACE
    -Werror -Wall -pedantic -Wconversion
    -fsanitize=address,undefined -fno-optimize-sibling-calls
    -fsanitize-address-use-after-scope
    -fno-omit-frame-pointer)

target_link_options(cxx_settings INTERFACE
    -fsanitize=address,undefined
    -Wl,-undefined,dynamic_lookup)

add_subdirectory(lib)

add_executable(test
    main.cpp)

target_link_libraries(test
    parser)