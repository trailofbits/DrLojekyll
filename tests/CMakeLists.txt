# Cribbed from https://github.com/google/googletest/tree/master/googletest.

cmake_minimum_required(VERSION 3.14)

include(GoogleTest)

configure_file(drlojekyll_paths.h.in drlojekyll_paths.h @ONLY)

find_package(Filesystem REQUIRED)

add_executable(unittests
    unittests.h

    BitManipulation.cpp
    Parsing.cpp
)
target_include_directories(unittests PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR}
)
target_link_libraries(unittests PRIVATE
    gtest_main
    rapidcheck_gtest

    settings_private
    settings_public

    Util
    Display
    Lex
    Parse
    DataFlow
    CodeGen
    Version

    std::filesystem
)

# Dynamically add all gtest tests from the `unittests` binary as CTest test
# cases:
gtest_discover_tests(unittests)

# If we want some other test cases aside from gtest, we can explicitly add
# them:
# add_test(NAME test_foo COMMAND true)
