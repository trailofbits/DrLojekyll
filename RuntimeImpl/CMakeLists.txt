# Need a cpp file to support IDE include paths when parsing compile_commands.json...
add_library(RuntimeStdImpl STATIC
  "runtime_std_impl.cpp"
  "runtime_std_impl.h"
  )
add_library(${PROJECT_NAME}::RuntimeStdImpl ALIAS RuntimeStdImpl)
set_target_properties(RuntimeStdImpl PROPERTIES PUBLIC_HEADER "runtime_std_impl.h")
target_include_directories(RuntimeStdImpl PUBLIC
  $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>
  PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)

string(TOLOWER ${PROJECT_NAME} lower_project_name)
install(
  TARGETS RuntimeStdImpl
  EXPORT "${PROJECT_NAME}Targets"
  RUNTIME
    DESTINATION "bin"
  LIBRARY
    DESTINATION "lib/${lower_project_name}"
  ARCHIVE
    DESTINATION "lib/${lower_project_name}"
  PUBLIC_HEADER
    DESTINATION "include/${lower_project_name}/RuntimeImpl"
)
