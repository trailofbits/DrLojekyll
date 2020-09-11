# Copyright 2020, Trail of Bits, Inc. All rights reserved.

cmake_minimum_required(VERSION 3.14)
project(DrLojekyll LANGUAGES CXX)

enable_testing()

include(cmake/add_sanitizer_settings.cmake)
include(cmake/define_static_library.cmake)

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
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/modules/")

# --------------------------------------------
# CMake installation stuff -------------------
# --------------------------------------------
# Export the package for use from the build-tree
# (this registers the build-tree with a global CMake-registry)
export(PACKAGE ${PROJECT_NAME})

include(CMakePackageConfigHelpers)
# Create the ${PROJECT_NAME}Config.cmake
configure_package_config_file(${PROJECT_NAME}Config.cmake.in
    "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Config.cmake"
    INSTALL_DESTINATION lib/cmake/${PROJECT_NAME}
)

# Install the ${PROJECT_NAME}Config.cmake
install(FILES
  "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Config.cmake"
  DESTINATION lib/cmake/${PROJECT_NAME}
)
install(DIRECTORY
  "${PROJECT_SOURCE_DIR}/cmake/modules"
  DESTINATION lib/cmake/${PROJECT_NAME}
)

# Install the export set for use with the install-tree
install(EXPORT ${PROJECT_NAME}Targets
        DESTINATION lib/cmake/${PROJECT_NAME}
        NAMESPACE "${PROJECT_NAME}::"
)

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

    # make a `settings-fuzz-target` library that enables `-fsanitize=fuzzer`
    add_sanitizer_settings(settings-fuzz-target SANITIZERS fuzzer)
endif()

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
# Dr. Lojekyll Libraries
add_subdirectory(lib)

# `drlojekyll`
add_executable(drlojekyll
    bin/drlojekyll/Main.cpp)
target_link_libraries(drlojekyll PRIVATE
    settings
    Util
    Display
    Lex
    Parse
    DataFlow
    CodeGen
    ControlFlow
    Version
)
install(TARGETS drlojekyll RUNTIME DESTINATION "bin")

# `fuzzer`
if(ENABLE_LIBFUZZER)
    add_executable(fuzzer
        fuzz/Fuzzer.cpp)
    target_link_libraries(fuzzer PRIVATE
        settings
        settings-fuzz-target
        Util
        Display
        Lex
        Parse
        Version
    )
endif()

# tests
if(ENABLE_TESTS)
  add_subdirectory(tests)
endif()