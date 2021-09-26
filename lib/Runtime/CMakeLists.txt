# Copyright 2021, Trail of Bits, Inc. All rights reserved.

set(Runtime_PUBLIC_H
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/xxhash.h"

  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Column.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Endian.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Index.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Int.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Result.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Runtime.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Serializer.h"
    
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/StdRuntime.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/StdScan.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/StdSet.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/StdStorage.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/StdTable.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/StdVector.h"
  
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Table.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Util.h"
)

set(Runtime_SRCS
  "StdStorage.cpp"
)

set(Runtime_PRIV_DEPS
)

find_package(concurrentqueue REQUIRED)

set(Runtime_DEPS
  flatbuffers
  concurrentqueue
)

dr_define_static_library(Runtime
  CURDIR ${CMAKE_CURRENT_SOURCE_DIR}
  SOURCES ${Runtime_SRCS}
  PUBLIC_HEADERS ${Runtime_PUBLIC_H}
  DEPENDENCIES ${Runtime_DEPS}
  PRIVATE_DEPS ${Runtime_PRIV_DEPS}
)
