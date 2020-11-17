# Cribbed from https://github.com/google/googletest/tree/master/googletest.

cmake_minimum_required(VERSION 3.17)

include(GoogleTest)

configure_file(drlojekyll_paths.h.in drlojekyll_paths.h @ONLY)

find_package(Filesystem REQUIRED)

add_executable(unittests
    UnitTests.h
    BitManipulation.cpp
    Parsing.cpp
    )

option(DISABLE_MYPY_TEST_CHECKS "Disables using mypy to check generated Python code" OFF)
if (NOT DISABLE_MYPY_TEST_CHECKS)
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
endif()

target_include_directories(unittests PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
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
          Version
          std::filesystem)

# Dynamically add all gtest tests from the `unittests` binary as CTest test
# cases:
gtest_discover_tests(unittests)

# If we want some other test cases aside from gtest, we can explicitly add them:
# add_test(NAME test_foo COMMAND true)
