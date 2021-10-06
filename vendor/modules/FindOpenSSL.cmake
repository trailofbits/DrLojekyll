# Copyright 2020, Trail of Bits, Inc. All rights reserved.

if(NOT TARGET OpenSSL::SSL)
  add_subdirectory(
    "${CMAKE_CURRENT_LIST_DIR}/../openssl"
    "${CMAKE_BINARY_DIR}/vendor/openssl"
    EXCLUDE_FROM_ALL
  )
endif()
