# Copyright 2021, Trail of Bits, Inc. All rights reserved.

set(Runtime_PUBLIC_H

  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Endian.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Int.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Reference.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Result.h"

  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Client/Connection.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Client/Result.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Client/Stream.h"
  
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Server/xxhash.h"

  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Server/Column.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Server/Index.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Server/Runtime.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Server/Serializer.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Server/Table.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Server/Util.h"
    
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Server/Std/Runtime.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Server/Std/Scan.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Server/Std/Storage.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Server/Std/Table.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Server/Std/Vector.h"
)

set(Runtime_SRCS
  "Client/Connection.cpp"
  "Client/Connection.h"
  "Client/Serialize.cpp"
  "Client/Serialize.h"
  "Client/Stream.cpp"
  "Client/Stream.h"
  "Server/Std/Storage.cpp"
)

set(Runtime_PRIV_DEPS
)

find_package(gRPC CONFIG REQUIRED)
find_package(concurrentqueue REQUIRED)

set(Runtime_DEPS
  flatbuffers
  concurrentqueue
  gRPC::grpc++
)

dr_define_static_library(Runtime
  CURDIR ${CMAKE_CURRENT_SOURCE_DIR}
  SOURCES ${Runtime_SRCS}
  PUBLIC_HEADERS ${Runtime_PUBLIC_H}
  DEPENDENCIES ${Runtime_DEPS}
  PRIVATE_DEPS ${Runtime_PRIV_DEPS}
)
