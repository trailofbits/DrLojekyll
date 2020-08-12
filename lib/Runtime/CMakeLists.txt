# Copyright 2020, Trail of Bits, Inc. All rights reserved.

add_library(runtime STATIC
    "Runtime.cpp"
)

target_link_libraries(runtime PUBLIC settings)

install(
  TARGETS runtime
  RUNTIME
    DESTINATION "bin"
  LIBRARY
    DESTINATION "lib"
  ARCHIVE
    DESTINATION "lib"
)
