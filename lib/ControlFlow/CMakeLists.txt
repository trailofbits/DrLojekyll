# Copyright 2020, Trail of Bits, Inc. All rights reserved.

# Copyright 2020, Trail of Bits, Inc. All rights reserved.

set(ControlFlow_PUBLIC_H
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/ControlFlow/Format.h"
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/ControlFlow/Program.h"
)

set(ControlFlow_SRCS
    "Program.h"
    
    #"Build/Build.h"
    "Build/Build.cpp"
    #"Build/Induction.cpp"
    #"Build/Insert.cpp"
    #"Build/Join.cpp"
    #"Build/Merge.cpp"
    #"Build/Product.cpp"
    #"Build/Union.cpp"
    
    "Format.cpp"
    "Induction.cpp"
    "Operation.cpp"
    "Parallel.cpp"
    "Procedure.cpp"
    "Program.cpp"
    "Region.cpp"
    "Series.cpp"
    "Table.cpp"
)

set(ControlFlow_PRIV_DEPS
    settings
)

set(ControlFlow_DEPS
    DataFlow
    Display
    Lex
    Parse
    Util
)

dr_define_static_library(ControlFlow
  CURDIR ${CMAKE_CURRENT_SOURCE_DIR}
  SOURCES ${ControlFlow_SRCS}
  PUBLIC_HEADERS ${ControlFlow_PUBLIC_H}
  DEPENDENCIES ${ControlFlow_DEPS}
  PRIVATE_DEPS ${ControlFlow_PRIV_DEPS}
)
