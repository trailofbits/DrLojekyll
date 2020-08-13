# Copyright 2020, Trail of Bits, Inc. All rights reserved.

set(Util_PUBLIC_H
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Util/BitManipulation.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Util/DefUse.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Util/DisjointSet.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Util/EqualitySet.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Util/Node.h"
)

set(Util_SRCS
    ${Util_PUBLIC_H}

    "DefUse.cpp"
    "DisjointSet.cpp"
    "EqualitySet.cpp"
    "Node.cpp"
)

add_library(Util STATIC
  ${Util_SRCS}
)

target_link_libraries(Util PUBLIC settings)

set_target_properties (Util PROPERTIES PUBLIC_HEADER "${Util_PUBLIC_H}")

target_include_directories(Util PUBLIC
  $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include/drlojekyll/Util>
  $<INSTALL_INTERFACE:include/drlojekyll/Util>
  PRIVATE ${CMAKE_CURRENT_BINARY_DIR}
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
)

install(
  TARGETS Util
  RUNTIME
    DESTINATION "bin"
  LIBRARY
    DESTINATION "lib/drlojekyll"
  ARCHIVE
    DESTINATION "lib/drlojekyll"
  PUBLIC_HEADER
    DESTINATION "include/drlojekyll/Util"
)
