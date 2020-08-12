# Copyright 2020, Trail of Bits, Inc. All rights reserved.

add_library(display STATIC
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Display/Display.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Display/DisplayConfiguration.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Display/DisplayManager.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Display/DisplayPosition.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Display/DisplayReader.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Display/Format.h"

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

target_link_libraries(display PUBLIC settings)

install(
  TARGETS display
  RUNTIME
    DESTINATION "bin"
  LIBRARY
    DESTINATION "lib/drlojekyll"
  ARCHIVE
    DESTINATION "lib/drlojekyll"
)
