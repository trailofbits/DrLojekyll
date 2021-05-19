# Cribbed from https://github.com/google/googletest/tree/master/googletest.

cmake_minimum_required(VERSION 3.17)

include(GoogleTest)

set(DrLojekyll_RUNTIME_LIB_PATH $<TARGET_FILE:Runtime>)

configure_file("${CMAKE_CURRENT_SOURCE_DIR}/drlojekyll_paths.h.in"
               "${CMAKE_CURRENT_BINARY_DIR}/include/${CMAKE_BUILD_TYPE}/drlojekyll_paths.h.gen" @ONLY)
file(GENERATE OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/include/$<CONFIG>/drlojekyll_paths.h"
              INPUT "${CMAKE_CURRENT_BINARY_DIR}/include/${CMAKE_BUILD_TYPE}/drlojekyll_paths.h.gen")


find_package(Filesystem REQUIRED)

add_executable(unittests
    UnitTests.h
    BitManipulation.cpp
    Parsing.cpp
    SlabRuntime.cpp
    )

option(ENABLE_TEST_MYPY_CHECKS "Test and verify generated Python code using Mypy" ON)
if (ENABLE_TEST_MYPY_CHECKS)
    message(STATUS "Enabled testing and verification of generated Python code using Mypy")
    find_package(Python3 3.8 REQUIRED COMPONENTS Interpreter)
    # Setup Python virtualenv and install requirements.txt
    include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/setup_venv.cmake")

    add_custom_target(
        Mypy
        DEPENDS requirements.txt
        SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/requirements.txt"
        COMMENT "Python Mypy")

    # Tell our tests that we can use mypy on PATH
    target_compile_definitions(unittests PUBLIC
        MYPY_PATH="${VIRTUALENV_EXE_DIRECTORY}/mypy${CMAKE_EXECUTABLE_SUFFIX}")
    add_dependencies(unittests Mypy)
else()
    message(STATUS "Disabled testing and verification of generated Python code using Mypy")
endif()

target_include_directories(unittests PRIVATE
  "${CMAKE_CURRENT_BINARY_DIR}/include/$<CONFIG>"
)

target_link_libraries(
  unittests
  PRIVATE gtest_main
          rapidcheck_gtest
          settings_private
          settings_public
          Util
          CodeGen
          Display
          Lex
          Parse
          DataFlow
          Runtime
          Version
          std::filesystem)

# Dynamically add all gtest tests from the `unittests` binary as CTest test
# cases:
gtest_discover_tests(unittests)

# If we want some other test cases aside from gtest, we can explicitly add them:
# add_test(NAME test_foo COMMAND true)
