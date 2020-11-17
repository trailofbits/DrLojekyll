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

set(VIRTUALENV_DIRECTORY "${CMAKE_BINARY_DIR}/python/virtualenv"
    CACHE INTERNAL "Path to virtualenv")

find_package (Python3 3.8 COMPONENTS Interpreter)
if (Python3_FOUND)
    message(INFO "Creating Python virtual environment")

    add_custom_command(
        OUTPUT "${VIRTUALENV_DIRECTORY}"
        COMMAND "${Python3_EXECUTABLE}" -m venv "${VIRTUALENV_DIRECTORY}"
    )

    add_custom_command(
        OUTPUT requirements.txt
        DEPENDS "${VIRTUALENV_DIRECTORY}" "${CMAKE_CURRENT_SOURCE_DIR}/requirements.txt"
        COMMAND ${CMAKE_COMMAND} -E copy "${CMAKE_CURRENT_SOURCE_DIR}/requirements.txt" requirements.txt
        COMMAND "${VIRTUALENV_DIRECTORY}/bin/pip" install --upgrade pip --requirement requirements.txt
    )

    add_custom_target(Mypy
        DEPENDS requirements.txt
        SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/requirements.txt"
    )
    # Tell our tests that we can use mypy on PATH
    target_compile_definitions(unittests PUBLIC MYPY_PATH="${VIRTUALENV_DIRECTORY}/bin/mypy")
    add_dependencies(unittests Mypy)
else()
    message(WARNING "Could not find Python 3.8+ interpreter for testing Python CodeGen")
endif()

target_include_directories(unittests PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR}
)
target_link_libraries(unittests PRIVATE
    gtest_main
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

    std::filesystem
)

# Dynamically add all gtest tests from the `unittests` binary as CTest test
# cases:
gtest_discover_tests(unittests)

# If we want some other test cases aside from gtest, we can explicitly add
# them:
# add_test(NAME test_foo COMMAND true)
