# Copyright 2020, Trail of Bits, Inc. All rights reserved.

# `drlojekyll`
find_package(Filesystem REQUIRED)

add_executable(drlojekyll
    drlojekyll/Main.cpp)
target_link_libraries(drlojekyll PRIVATE
    settings_private
    settings_public
    std::filesystem
    
    # Custom target that gives us access to the source code that is needed to
    # emit flatbuffer code.
    flatclib

    ControlFlow
    CodeGen
    DataFlow
    Display
    Lex
    Parse
    Util
    Version
)

add_executable(${PROJECT_NAME}::drlojekyll ALIAS drlojekyll)

install(
  TARGETS drlojekyll
  EXPORT "${PROJECT_NAME}Targets")
