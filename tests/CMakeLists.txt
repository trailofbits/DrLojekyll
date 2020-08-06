# Cribbed from https://github.com/google/googletest/tree/master/googletest.

cmake_minimum_required(VERSION 3.14)

include(GoogleTest)

# Now simply link against gtest or gtest_main as needed. Eg
add_executable(example example.cpp)
target_link_libraries(example gtest_main)

gtest_discover_tests(example)

# add_test(NAME test_foo COMMAND true)
