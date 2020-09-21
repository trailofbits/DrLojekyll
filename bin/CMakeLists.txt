# Copyright 2020, Trail of Bits, Inc. All rights reserved.

# `drlojekyll`
add_executable(drlojekyll
    drlojekyll/Main.cpp)
target_link_libraries(drlojekyll PRIVATE
    settings_private
    settings_public
    Util
    Display
    Lex
    Parse
    DataFlow
    ControlFlow
    Version
)
install(TARGETS drlojekyll)
