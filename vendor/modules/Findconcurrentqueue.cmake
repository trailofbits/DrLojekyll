# Copyright 2020, Trail of Bits, Inc. All rights reserved.

if(NOT TARGET concurrentqueue)
  add_subdirectory(
    "${CMAKE_CURRENT_LIST_DIR}/../concurrentqueue"
    "${CMAKE_BINARY_DIR}/vendor/concurrentqueue"
    EXCLUDE_FROM_ALL
  )
endif()
