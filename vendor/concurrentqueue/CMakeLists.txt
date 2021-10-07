# Copyright 2021, Trail of Bits, Inc. All rights reserved.

# Make sure we match the standard path used by upstream:
# - ${CMAKE_INSTALL_INCLUDEDIR}/${PROJECT_NAME}/moodycamel
set(concurrentqueue_include_path "${CMAKE_CURRENT_BINARY_DIR}/concurrentqueue")

set(output_file_list
  "${concurrentqueue_include_path}/moodycamel/blockingconcurrentqueue.h"
  "${concurrentqueue_include_path}/moodycamel/concurrentqueue.h"
  "${concurrentqueue_include_path}/moodycamel/lightweightsemaphore.h"
)

add_custom_command(
  OUTPUT
    ${output_file_list}

  COMMAND "${CMAKE_COMMAND}" -E make_directory "${concurrentqueue_include_path}/moodycamel"
  COMMAND "${CMAKE_COMMAND}" -E create_symlink "${CMAKE_CURRENT_SOURCE_DIR}/src/blockingconcurrentqueue.h" "${concurrentqueue_include_path}/moodycamel/blockingconcurrentqueue.h"
  COMMAND "${CMAKE_COMMAND}" -E create_symlink "${CMAKE_CURRENT_SOURCE_DIR}/src/concurrentqueue.h" "${concurrentqueue_include_path}/moodycamel/concurrentqueue.h"
  COMMAND "${CMAKE_COMMAND}" -E create_symlink "${CMAKE_CURRENT_SOURCE_DIR}/src/lightweightsemaphore.h" "${concurrentqueue_include_path}/moodycamel/lightweightsemaphore.h"

  VERBATIM

  WORKING_DIRECTORY
    "${CMAKE_CURRENT_BINARY_DIR}"

  COMMENT
    "DrLojekyll: Generating the moodycamel/concurrentqueue headers"
)

add_custom_target(
  concurrentqueue_builder
  DEPENDS ${output_file_list}
)

add_library(concurrentqueue INTERFACE)
target_include_directories(concurrentqueue INTERFACE
  "${concurrentqueue_include_path}"
)

add_dependencies(concurrentqueue concurrentqueue_builder)
