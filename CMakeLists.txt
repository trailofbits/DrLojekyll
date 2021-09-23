# Copyright 2020, Trail of Bits, Inc. All rights reserved.

cmake_minimum_required(VERSION 3.17)
project(DrLojekyll LANGUAGES C CXX)

include("cmake/options.cmake")
include("cmake/add_sanitizer_settings.cmake")
include("cmake/dr_define_static_library.cmake")
include("cmake/utils.cmake")

add_subdirectory("libraries" EXCLUDE_FROM_ALL)

if(DRLOJEKYLL_ENABLE_VENDORED_LIBRARIES)
  add_subdirectory("vendor" EXCLUDE_FROM_ALL)
endif()

# TODO: Move this where the library is required
find_package(gRPC CONFIG REQUIRED)
if(NOT TARGET gRPC::grpc++)
    message(FATAL_ERROR "gRPC was not found")
endif()

# --------------------------------------------
# build-time options -------------------------
# --------------------------------------------
option(ENABLE_LIBFUZZER "Build libFuzzer fuzz targets" OFF)
option(ENABLE_SANITIZERS "Build with ASAN and UBSAN" OFF)
option(ENABLE_TESTS "Build test suite" ON)
option(WARNINGS_AS_ERRORS "Build treating warnings as errors" OFF)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_STANDARD_REQUIRED ON)
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
# dependencies -------------------------------
# --------------------------------------------

# Make sure cmake can find our custom Find* modules.
list(PREPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

#list(APPEND CMAKE_PREFIX_PATH "${CMAKE_CURRENT_BINARY_DIR}/vendor/install")
#list(APPEND CMAKE_PREFIX_PATH "${CMAKE_CURRENT_BINARY_DIR}/vendor/install/lib/cmake/xxHash")
#list(APPEND CMAKE_PREFIX_PATH "${CMAKE_CURRENT_BINARY_DIR}/vendor/install/lib/cmake/GTest")
#list(APPEND CMAKE_PREFIX_PATH "${CMAKE_CURRENT_BINARY_DIR}/vendor/install/lib/cmake/reproc")
#list(APPEND CMAKE_PREFIX_PATH "${CMAKE_CURRENT_BINARY_DIR}/vendor/install/lib/cmake/protobuf")
#list(APPEND CMAKE_PREFIX_PATH "${CMAKE_CURRENT_BINARY_DIR}/vendor/install/lib/cmake/c-ares")
#list(APPEND CMAKE_PREFIX_PATH "${CMAKE_CURRENT_BINARY_DIR}/vendor/install/lib/cmake/grpc")
#list(APPEND CMAKE_PREFIX_PATH "${CMAKE_CURRENT_BINARY_DIR}/vendor/install/lib/cmake/flatbuffers")

#message("WOO" ${CMAKE_PREFIX_PATH})
#find_package(Flatbuffers CONFIG REQUIRED)
#find_package(gRPC CONFIG REQUIRED)
#find_package(GTest CONFIG REQUIRED)

#get_target_property(flatc_SOURCES flatc SOURCES)
#get_target_property(flatc_COMPILE_OPTIONS flatc COMPILE_OPTIONS)
#get_target_property(flatc_COMPILE_DEFINITIONS flatc COMPILE_DEFINITIONS)
#get_target_property(flatc_INCLUDE_DIRECTORIES flatc INCLUDE_DIRECTORIES)
#get_target_property(flatc_LINK_DIRECTORIES flatc LINK_DIRECTORIES)
    
# Iterate over the source files of the `flatc` compiler target, remove the
# file with the `main` function, and add package it up as `flatclib`.
#foreach(fb_source_file IN LISTS flatc_SOURCES)
#  if(NOT fb_source_file MATCHES "^.*flatc_main.cpp$")
#    list(APPEND FLATBUFFERS_COMPILER_FILES "${FlatBuffers_SOURCE_DIR}/${fb_source_file}")
#  endif()
#endforeach()

#add_library(flatclib STATIC ${FLATBUFFERS_COMPILER_FILES})

#if(NOT flatc_COMPILE_OPTIONS STREQUAL "flatc_COMPILE_OPTIONS-NOTFOUND")
#  target_compile_options(flatclib PRIVATE ${flatc_COMPILE_OPTIONS})
#endif()

#if(NOT flatc_COMPILE_DEFINITIONS STREQUAL "flatc_COMPILE_DEFINITIONS-NOTFOUND")
#  target_compile_definitions(flatclib PRIVATE ${flatc_COMPILE_DEFINITIONS})
#endif()

#if(NOT flatc_INCLUDE_DIRECTORIES STREQUAL "flatc_INCLUDE_DIRECTORIES-NOTFOUND")
#  target_include_directories(flatclib PUBLIC ${flatc_INCLUDE_DIRECTORIES})
#endif()

#if(NOT flatc_COMPILE_DEFINITIONS STREQUAL "flatc_COMPILE_DEFINITIONS-NOTFOUND")
#  target_link_directories(flatclib PRIVATE ${flatc_LINK_DIRECTORIES})
#endif()

#target_link_libraries(flatclib PRIVATE
#  settings_public)

# --------------------------------------------
# CMake installation stuff -------------------
# --------------------------------------------
export(PACKAGE ${PROJECT_NAME})

set(DRLOJEKYLL_CMAKE_INSTALL_DIR "lib/cmake/${PROJECT_NAME}")

include(CMakePackageConfigHelpers)
configure_package_config_file("${PROJECT_NAME}Config.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Config.cmake"
    INSTALL_DESTINATION "${DRLOJEKYLL_CMAKE_INSTALL_DIR}"
)
# # Runtime header
# install(
#   FILES
#     "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Runtime.h"
#   DESTINATION
#     "include/drlojekyll/Runtime"
#   )
install(
    FILES
        "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Config.cmake"
        "${PROJECT_SOURCE_DIR}/cmake/FindFilesystem.cmake"
        "${PROJECT_SOURCE_DIR}/cmake/Compiler.cmake"
    DESTINATION "${DRLOJEKYLL_CMAKE_INSTALL_DIR}"
)
install(EXPORT "${PROJECT_NAME}Targets"
    DESTINATION "${DRLOJEKYLL_CMAKE_INSTALL_DIR}"
    NAMESPACE "${PROJECT_NAME}::"
)

# --------------------------------------------
# settings interface libraries
# --------------------------------------------

# `settings_private` is for _private_ build options of Dr. Lojekyll libraries,
# i.e., for build options we don't want to be exposed to downstream users.
#
# Example of settings we don't want to propagate: compiler warning settings
add_library(settings_private INTERFACE)

# `settings_private` is for _public build options of Dr. Lojekyll libraries,
# i.e., for build options that should be propagated to downstream users.
#
# Example of settings we need to propagate: sanitizer settings
add_library(settings_public INTERFACE)

target_include_directories(settings_public INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

target_compile_options(settings_private INTERFACE ${CXX_WARNING_OPTIONS})

if(WARNINGS_AS_ERRORS)
    target_compile_options(settings_private INTERFACE ${CXX_WARNINGS_AS_ERRORS_OPTION})
endif()

# sanitizer settings -------------------------
if(ENABLE_SANITIZERS)
    add_sanitizer_settings(settings_public
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

    add_sanitizer_settings(settings_public
        SANITIZERS
            fuzzer-no-link
        OPTIONS
            -g
    )

    # make a `settings_fuzz_target` library that enables `-fsanitize=fuzzer`
    add_sanitizer_settings(settings_fuzz_target SANITIZERS fuzzer)
endif()

install(
    TARGETS settings_public settings_private
    EXPORT "${PROJECT_NAME}Targets"
)

# --------------------------------------------
# targets ------------------------------------
# --------------------------------------------
add_subdirectory(lib)
add_subdirectory(bin)

if(DRLOJEKYLL_ENABLE_LIBFUZZER)
  add_subdirectory(fuzz)
endif()

if(DRLOJEKYLL_ENABLE_TESTS)
  add_subdirectory(tests)
endif()
