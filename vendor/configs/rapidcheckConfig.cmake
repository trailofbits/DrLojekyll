# Copyright 2020, Trail of Bits, Inc. All rights reserved.

if(NOT TARGET rapidcheck)
  add_subdirectory(
    "${CMAKE_CURRENT_LIST_DIR}/../rapidcheck"
    "${CMAKE_BINARY_DIR}/vendor/rapidcheck"
    EXCLUDE_FROM_ALL
  )
endif()
