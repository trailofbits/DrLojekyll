# Copyright 2020, Trail of Bits, Inc. All rights reserved.

# `drlojekyll`
find_package(Filesystem REQUIRED)

add_executable(drlojekyll
    drlojekyll/Main.cpp)
target_link_libraries(drlojekyll PRIVATE
    settings_private
    settings_public
    std::filesystem
    Util
    CodeGen
    Display
    Lex
    Parse
    DataFlow
    ControlFlow
    Version
)
install(TARGETS drlojekyll)
