# Cribbed from https://github.com/google/googletest/tree/master/googletest.

cmake_minimum_required(VERSION 3.14)

include(GoogleTest)

add_executable(unittests unittests.cpp)
target_link_libraries(unittests PRIVATE
    gtest_main
    rapidcheck_gtest

    settings

    Util
    Display
    Lex
    Parse
    DataFlow
    CodeGen
    Version
)

# Dynamically add all gtest tests from the `unittests` binary as CTest test
# cases:
gtest_discover_tests(unittests)

# If we want some other test cases aside from gtest, we can explicitly add
# them:
# add_test(NAME test_foo COMMAND true)
