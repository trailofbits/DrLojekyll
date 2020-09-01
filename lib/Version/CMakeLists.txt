# Based on: https://github.com/andrew-hardin/cmake-git-version-tracking/blob/master/better-example/CMakeLists.txt
# By Andrew Hardin
# Released under the MIT License.
# https://raw.githubusercontent.com/andrew-hardin/cmake-git-version-tracking/master/LICENSE
#
# Define the two required variables before including
# the source code for watching a git repository.
set(PRE_CONFIGURE_FILE "Version.cpp.in")
set(POST_CONFIGURE_FILE "${CMAKE_CURRENT_BINARY_DIR}/Version.cpp")
set(GIT_WORKING_DIR "${PROJECT_SOURCE_DIR}")
include("${PROJECT_SOURCE_DIR}/cmake/git_watcher.cmake")

# Create a library out of the compiled post-configure file.
set(Version_PUBLIC_H
    "${PROJECT_SOURCE_DIR}/include/drlojekyll/Version/Version.h"
)

set(Version_SRCS
    ${POST_CONFIGURE_FILE}
)

set(Version_PRIV_DEPS
)

set(Version_DEPS
    settings
)

define_static_library(Version
  CURDIR ${CMAKE_CURRENT_SOURCE_DIR}
  SOURCES ${Version_SRCS}
  PUBLIC_HEADERS ${Version_PUBLIC_H}
  DEPENDENCIES ${Version_DEPS}
  PRIVATE_DEPS ${Version_PRIV_DEPS}
)

add_dependencies(Version check_git_${PROJECT_NAME})
