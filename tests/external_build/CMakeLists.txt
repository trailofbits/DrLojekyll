cmake_minimum_required(VERSION 3.15)

project(TestExternalBuild)

set(CMAKE_CXX_STANDARD 17)

find_package(DrLojekyll CONFIG REQUIRED)

# FIXME(bjl): swap out this program for a test file that lives here
#             this will be a problem some day
add_executable(ext_test ../../bin/drlojekyll/Main.cpp)

# add library
target_link_libraries(ext_test PRIVATE
    DrLojekyll::Util
    DrLojekyll::Display
    DrLojekyll::Lex
    DrLojekyll::Parse
    DrLojekyll::DataFlow
    DrLojekyll::CodeGen
    DrLojekyll::Version
)
