# Copyright 2020, Trail of Bits, Inc. All rights reserved.

set(Runtime_PUBLIC_H
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Runtime.h"
)

set(Runtime_SRCS
    ${Runtime_PUBLIC_H}

    "Runtime.cpp"
)

add_library(Runtime STATIC
    ${Runtime_SRCS}
)

target_link_libraries(Runtime PUBLIC settings)

set_target_properties (Runtime PROPERTIES PUBLIC_HEADER "${Runtime_PUBLIC_H}")

target_include_directories(Runtime PUBLIC
  $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime>
  $<INSTALL_INTERFACE:include/drlojekyll/Runtime>
  PRIVATE ${CMAKE_CURRENT_BINARY_DIR}
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
)

install(
  TARGETS Runtime
  RUNTIME
    DESTINATION "bin"
  LIBRARY
    DESTINATION "lib/drlojekyll"
  ARCHIVE
    DESTINATION "lib/drlojekyll"
  PUBLIC_HEADER
    DESTINATION "include/drlojekyll/Runtime"
)
