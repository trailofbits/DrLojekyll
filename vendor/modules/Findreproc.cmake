# Copyright 2020, Trail of Bits, Inc. All rights reserved.

if(NOT TARGET reproc AND NOT TARGET reproc++)
  add_subdirectory(
    "${CMAKE_CURRENT_LIST_DIR}/../reproc"
    "${CMAKE_BINARY_DIR}/vendor/reproc"
    EXCLUDE_FROM_ALL
  )
endif()
