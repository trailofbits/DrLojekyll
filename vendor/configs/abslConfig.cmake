# Copyright 2020, Trail of Bits, Inc. All rights reserved.

if(NOT TARGET absl::base)
  add_subdirectory(
    "${CMAKE_CURRENT_LIST_DIR}/../abseil"
    "${CMAKE_BINARY_DIR}/vendor/abseil"
    EXCLUDE_FROM_ALL
  )
endif()
