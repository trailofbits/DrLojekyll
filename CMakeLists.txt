# Copyright 2020, Trail of Bits, Inc. All rights reserved.

if(NOT DEFINED VCPKG_ROOT)
  message(FATAL_ERROR "Must build with VCPKG")
endif()

include(cmake/vcpkg_helper.cmake)

cmake_minimum_required(VERSION 3.17)
project(DrLojekyll LANGUAGES C CXX)

include("cmake/options.cmake")
include("cmake/add_sanitizer_settings.cmake")
include("cmake/dr_define_static_library.cmake")
include("cmake/utils.cmake")
include("cmake/ccache.cmake")

if(DRLOJEKYLL_ENABLE_SANITIZERS)
  defineSanitizersTarget()
endif()

defineWarningSilencerTarget()

if(DRLOJEKYLL_ENABLE_VENDORED_LIBRARIES)
  add_subdirectory("vendor" EXCLUDE_FROM_ALL)
endif()

# --------------------------------------------
# build-time options -------------------------
# --------------------------------------------

set(CMAKE_CXX_STANDARD 20)
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

if(DRLOJEKYLL_ENABLE_INSTALL)
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
endif()

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
endif(WARNINGS_AS_ERRORS)

# sanitizer settings -------------------------
if(DRLOJEKYLL_ENABLE_SANITIZERS)
    add_sanitizer_settings(settings_public
        SANITIZERS
            address
            undefined
        OPTIONS
            -fsanitize-address-use-after-scope
    )
endif(DRLOJEKYLL_ENABLE_SANITIZERS)

# fuzzer settings ----------------------------
if(DRLOJEKYLL_ENABLE_LIBFUZZER)
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
endif(DRLOJEKYLL_ENABLE_LIBFUZZER)

if(DRLOJEKYLL_ENABLE_INSTALL)
  if(DRLOJEKYLL_ENABLE_SANITIZERS)
    install(
      TARGETS drlojekyll_sanitizers
      EXPORT "${PROJECT_NAME}Targets"
  )
  endif(DRLOJEKYLL_ENABLE_SANITIZERS)
  
  install(
      TARGETS settings_public settings_private
      EXPORT "${PROJECT_NAME}Targets"
  )
endif(DRLOJEKYLL_ENABLE_INSTALL)

# --------------------------------------------
# targets ------------------------------------
# --------------------------------------------

# Test: remove me!
find_package(absl REQUIRED CONFIG)

add_subdirectory(lib)
add_subdirectory(bin)

if(DRLOJEKYLL_ENABLE_LIBFUZZER)
  add_subdirectory(fuzz)
endif(DRLOJEKYLL_ENABLE_LIBFUZZER)

if(DRLOJEKYLL_ENABLE_TESTS)
  add_subdirectory(tests)
endif(DRLOJEKYLL_ENABLE_TESTS)
