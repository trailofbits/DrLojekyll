# Copyright 2021, Trail of Bits, Inc. All rights reserved.


set(Runtime_PUBLIC_H
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Column.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Index.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Result.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Runtime.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Serializer.h"
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
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Table.h"
  "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/Util.h"
)

set(Runtime_SRCS
  "Error.cpp"
  "Error.h"

  "Slab.cpp"
  "Slab.h"
  
  "SlabList.cpp"
  
  "SlabManager.cpp"
  "SlabManager.h"
  
  "SlabReference.cpp"
  "SlabStorage.cpp"
  "SlabTable.cpp"
  "SlabVector.cpp"
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
