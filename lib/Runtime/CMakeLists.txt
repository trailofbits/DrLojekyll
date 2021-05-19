# Copyright 2021, Trail of Bits, Inc. All rights reserved.

# Copyright 2021, Trail of Bits, Inc. All rights reserved.



if(WIN32)
  set(SlabRuntime_PUBLIC_H)
  set(SlabRuntime_SRCS)
else()
  set(SlabRuntime_PUBLIC_H
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Slab.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/SlabIndex.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/SlabList.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/SlabManager.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/SlabPointer.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/SlabReference.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/SlabStorage.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/SlabTable.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/SlabTuple.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/SlabVector.h"
  )
  set(SlabRuntime_SRCS
    "Slab/Error.cpp"
    "Slab/Error.h"
    "Slab/Slab.cpp"
    "Slab/Slab.h"
    "Slab/SlabList.cpp"
    "Slab/SlabManager.cpp"
    "Slab/SlabManager.h" 
    "Slab/SlabReference.cpp"
    "Slab/SlabStorage.cpp"
    "Slab/SlabTable.cpp"
    "Slab/SlabVector.cpp"
  )
endif()

set(Runtime_PUBLIC_H
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/xxhash.h"

  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Column.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Index.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Result.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Runtime.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Serializer.h"
  
  ${SlabRuntime_PUBLIC_H}
  
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/StdRuntime.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/StdScan.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/StdStorage.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/StdTable.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/StdVector.h"
  
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Table.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Util.h"
)



set(Runtime_SRCS
  ${SlabRuntime_SRCS}
  "Std/Storage.cpp"
)

set(Runtime_PRIV_DEPS
)

set(Runtime_DEPS
)

dr_define_static_library(Runtime
  CURDIR ${CMAKE_CURRENT_SOURCE_DIR}
  SOURCES ${Runtime_SRCS}
  PUBLIC_HEADERS ${Runtime_PUBLIC_H}
  DEPENDENCIES ${Runtime_DEPS}
  PRIVATE_DEPS ${Runtime_PRIV_DEPS}
)
