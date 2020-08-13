# Copyright 2020, Trail of Bits, Inc. All rights reserved.

set(CodeGen_PUBLIC_H
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/CodeGen/BAM.h"
)

set(CodeGen_SRCS
    ${CodeGen_PUBLIC_H}

    "BAM.cpp"
)

add_library(CodeGen STATIC
    ${CodeGen_SRCS}
)

target_link_libraries(CodeGen
    PUBLIC
        settings
        DataFlow
        Display
        Lex
        Parse
        Util
)

set_target_properties (CodeGen PROPERTIES PUBLIC_HEADER "${CodeGen_PUBLIC_H}")

target_include_directories(CodeGen PUBLIC
  $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include/drlojekyll/CodeGen>
  $<INSTALL_INTERFACE:include/drlojekyll/CodeGen>
  PRIVATE ${CMAKE_CURRENT_BINARY_DIR}
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
)

install(
  TARGETS CodeGen
  RUNTIME
    DESTINATION "bin"
  LIBRARY
    DESTINATION "lib/drlojekyll"
  ARCHIVE
    DESTINATION "lib/drlojekyll"
  PUBLIC_HEADER
    DESTINATION "include/drlojekyll/CodeGen"
)
