# Copyright 2020, Trail of Bits, Inc. All rights reserved.

if(NOT TARGET gRPC::grpc++)
  add_subdirectory(
    "${CMAKE_CURRENT_LIST_DIR}/../grpc"
    "${CMAKE_BINARY_DIR}/vendor/grpc"
    EXCLUDE_FROM_ALL
  )
endif()