# `drlojekyll`
add_executable(drlojekyll
    drlojekyll/Main.cpp)
target_link_libraries(drlojekyll PRIVATE
    settings
    Util
    Display
    Lex
    Parse
    DataFlow
    CodeGen
    Version
)
install(TARGETS drlojekyll)
