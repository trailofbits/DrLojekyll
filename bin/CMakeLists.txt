# Copyright 2020, Trail of Bits, Inc. All rights reserved.

# `drlojekyll`
find_package(Filesystem REQUIRED)

add_executable(drlojekyll
    drlojekyll/Main.cpp
)

target_link_libraries(drlojekyll PRIVATE
    settings_private
    settings_public
    std::filesystem

    ControlFlow
    CodeGen
    DataFlow
    Display
    Lex
    Parse
    Util
    Version

    flatc_internals
)

if(DRLOJEKYLL_ENABLE_SANITIZERS)
    target_link_libraries(drlojekyll PRIVATE
        drlojekyll_sanitizers
    )
endif()

add_executable(${PROJECT_NAME}::drlojekyll ALIAS drlojekyll)

if(DRLOJEKYLL_ENABLE_INSTALL)
    install(
        TARGETS drlojekyll
        EXPORT "${PROJECT_NAME}Targets"
    )
endif()
