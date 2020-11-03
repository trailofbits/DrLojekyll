# Copyright 2020, Trail of Bits, Inc. All rights reserved.

# `drlojekyll`
add_executable(drlojekyll
    drlojekyll/Main.cpp)
target_link_libraries(drlojekyll PRIVATE
    settings_private
    settings_public

    ControlFlow
    CodeGen
    DataFlow
    Display
    Lex
    Parse
    Util
    Version
)
install(TARGETS drlojekyll)
