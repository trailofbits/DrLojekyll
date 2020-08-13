# Copyright 2020, Trail of Bits, Inc. All rights reserved.

set(Display_PUBLIC_H
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Display/Display.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Display/DisplayConfiguration.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Display/DisplayManager.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Display/DisplayPosition.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Display/DisplayReader.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Display/Format.h"
)

set(Display_SRCS
    ${Display_PUBLIC_H}

    "DataStream.h"
    "Display.h"
    "DisplayPosition.h"

    "DataStream.cpp"
    "Display.cpp"
    "DisplayManager.cpp"
    "DisplayPosition.cpp"
    "DisplayReader.cpp"
    "Format.cpp"
)

add_library(Display STATIC
    ${Display_SRCS}
)

target_link_libraries(Display PUBLIC settings)

set_target_properties (Display PROPERTIES PUBLIC_HEADER "${Display_PUBLIC_H}")

target_include_directories(Display PUBLIC
  $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include/drlojekyll/Display>
  $<INSTALL_INTERFACE:include/drlojekyll/Display>
  PRIVATE ${CMAKE_CURRENT_BINARY_DIR}
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
)

install(
  TARGETS Display
  RUNTIME
    DESTINATION "bin"
  LIBRARY
    DESTINATION "lib/drlojekyll"
  ARCHIVE
    DESTINATION "lib/drlojekyll"
  PUBLIC_HEADER
    DESTINATION "include/drlojekyll/Display"
)
