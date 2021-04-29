# Copyright 2021, Trail of Bits, Inc. All rights reserved.

# Need a cpp file to support IDE include paths when parsing compile_commands.json...
add_library(StdRuntime STATIC
  "StdRuntime.cpp"
  )

add_library(${PROJECT_NAME}::StdRuntime ALIAS StdRuntime)
set_target_properties(StdRuntime PROPERTIES
  PUBLIC_HEADER "${PROJECT_SOURCE_DIR}/include/drlojekyll/Runtime/StdRuntime.h")
target_include_directories(StdRuntime PUBLIC
  $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>
  PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)

string(TOLOWER ${PROJECT_NAME} lower_project_name)
install(
  TARGETS StdRuntime
  EXPORT "${PROJECT_NAME}Targets"
  RUNTIME
    DESTINATION "bin"
  LIBRARY
    DESTINATION "lib/${lower_project_name}"
  ARCHIVE
    DESTINATION "lib/${lower_project_name}"
  PUBLIC_HEADER
    DESTINATION "include/${lower_project_name}/Runtime"
)
