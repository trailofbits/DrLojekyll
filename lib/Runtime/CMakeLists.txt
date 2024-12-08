# Copyright 2021, Trail of Bits, Inc. All rights reserved.

find_package(gRPC CONFIG REQUIRED)
find_package(Flatbuffers CONFIG REQUIRED)
find_package(gflags CONFIG REQUIRED)
find_package(glog CONFIG REQUIRED)

set(Runtime_PUBLIC_H

  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Endian.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/FlatBuffers.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Int.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Reference.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Result.h"

  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Client.h"
  
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/xxhash.h"

  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Bytes.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Column.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Index.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Runtime.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Serializer.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Table.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Util.h"
    
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/StdRuntime.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/StdScan.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/StdStorage.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/StdTable.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/StdVector.h"
  
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Semaphore.h"
)

set(Runtime_SRCS
  "Client/Serialize.cpp"
  "Client/Serialize.h"
  "Client/Client.cpp"
  "Client/Client.h"
  "Server/Std/Storage.cpp"
  "Semaphore.cpp"
)

set(Runtime_PRIV_DEPS
  $<BUILD_INTERFACE:concurrentqueue>
)

set(Runtime_DEPS
  flatbuffers::flatbuffers
  gRPC::gpr gRPC::upb gRPC::grpc gRPC::grpc++
  glog::glog
  gflags::gflags
)

dr_define_static_library(Runtime
  CURDIR ${CMAKE_CURRENT_SOURCE_DIR}
  SOURCES ${Runtime_SRCS}
  PUBLIC_HEADERS ${Runtime_PUBLIC_H}
  DEPENDENCIES ${Runtime_DEPS}
  PRIVATE_DEPS ${Runtime_PRIV_DEPS}
)
