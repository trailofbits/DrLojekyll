# Copyright 2020, Trail of Bits, Inc. All rights reserved.

cmake_minimum_required(VERSION 3.14)

# Switch default of NO_PACKAGE_REGISTRY. Can be removed with CMake 3.15+
#
# Without this, installing Dr. Lojekyll using CMake ends up polluting the
# filesystem outside of the build, source, or install directories.
set(CMAKE_EXPORT_NO_PACKAGE_REGISTRY ON CACHE BOOL "")
set(CMAKE_EXPORT_PACKAGE_REGISTRY OFF CACHE BOOL "")

project(DrLojekyll LANGUAGES CXX)

enable_testing()

include(cmake/add_sanitizer_settings.cmake)
include(cmake/dr_define_static_library.cmake)

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
# Dependencies -------------------------------
# --------------------------------------------
list(PREPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

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
install(
    FILES
        "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Config.cmake"
        "${PROJECT_SOURCE_DIR}/cmake/FindFilesystem.cmake"
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
# vendored dependencies ----------------------
# --------------------------------------------
if(ENABLE_TESTS)
  # Google Test --------------------------------
  #
  # Cribbed from https://github.com/google/googletest/tree/master/googletest
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  add_subdirectory(vendor/googletest-release-1.10.0
      EXCLUDE_FROM_ALL)

  # rapidcheck ---------------------------------
  set(RC_ENABLE_GTEST ON CACHE BOOL "" FORCE)
  add_subdirectory(vendor/rapidcheck
      EXCLUDE_FROM_ALL)
endif()

# --------------------------------------------
# targets ------------------------------------
# --------------------------------------------
add_subdirectory(lib)
add_subdirectory(bin)
add_subdirectory(fuzz)

if(ENABLE_TESTS)
  add_subdirectory(tests)
endif()
