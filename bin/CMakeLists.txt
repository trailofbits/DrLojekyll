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
    CodeGen
    Version
)
install(TARGETS drlojekyll)
