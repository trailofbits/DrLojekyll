#
# Adapted from Google or-tools:
#
# https://github.com/google/or-tools/blob/master/cmake/utils.cmake
#

include(ProcessorCount)

function(get_patch_from_git VERSION_PATCH)
  find_package(Git QUIET)
  if(NOT GIT_FOUND)
    message(STATUS "Did not find git package.")
    set(PATCH 9999)
  else()
    # If no tags can be found, it is a git shallow clone
    execute_process(COMMAND
      ${GIT_EXECUTABLE} describe --tags
      RESULT_VARIABLE _OUTPUT_VAR
      OUTPUT_VARIABLE FULL
      ERROR_QUIET
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
    if(NOT _OUTPUT_VAR)
      execute_process(COMMAND
        ${GIT_EXECUTABLE} rev-list HEAD --count
        RESULT_VARIABLE _OUTPUT_VAR
        OUTPUT_VARIABLE PATCH
        ERROR_QUIET
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
      STRING(STRIP PATCH ${PATCH})
      STRING(REGEX REPLACE "\n$" "" PATCH ${PATCH})
      STRING(REGEX REPLACE " " "" PATCH ${PATCH})
    else()
      message(STATUS "Did not find any tag.")
      set(PATCH 9999)
    endif()
  endif()
  set(${VERSION_PATCH} ${PATCH} PARENT_SCOPE)
endfunction()

function(set_version VERSION)
  # Get Major and Minor from Version.txt
  file(STRINGS "Version.txt" VERSION_STR)
  foreach(STR ${VERSION_STR})
    if(${STR} MATCHES "OR_TOOLS_MAJOR=(.*)")
      set(MAJOR ${CMAKE_MATCH_1})
    endif()
    if(${STR} MATCHES "OR_TOOLS_MINOR=(.*)")
      set(MINOR ${CMAKE_MATCH_1})
    endif()
  endforeach()

  # Compute Patch if .git is present otherwise set it to 9999
  get_filename_component(GIT_DIR ".git" ABSOLUTE)
  if(EXISTS ${GIT_DIR})
    get_patch_from_git(PATCH)
  else()
    set(PATCH 9999)
  endif()
  set(${VERSION} "${MAJOR}.${MINOR}.${PATCH}" PARENT_SCOPE)
endfunction()


# fetch_git_dependency()
#
# CMake function to download, build and install (in staging area) a dependency at configure
# time.
#
# Parameters:
# NAME: name of the dependency
# REPOSITORY: git url of the dependency
# TAG: tag of the dependency
# PATCH_COMMAND: apply patch
# SOURCE_SUBDIR: Path to source CMakeLists.txt relative to root dir
# CMAKE_ARGS: List of specific CMake args to add
#
# e.g.:
# fetch_git_dependency(
#   NAME
#     abseil-cpp
#   URL
#     https://github.com/abseil/abseil-cpp.git
#   TAG
#     master
#   PATCH_COMMAND
#     "git apply ${CMAKE_SOURCE_DIR}/patches/abseil-cpp.patch"
# )
function(fetch_git_dependency)
  set(options "")
  set(oneValueArgs NAME REPOSITORY TAG APPLY_PATCH PATCH_COMMAND SOURCE_SUBDIR)
  set(multiValueArgs CMAKE_ARGS)
  cmake_parse_arguments(GIT_DEP
    "${options}"
    "${oneValueArgs}"
    "${multiValueArgs}"
    ${ARGN}
  )
  message(STATUS "Building ${GIT_DEP_NAME}: ...")
  string(TOLOWER ${GIT_DEP_NAME} NAME_LOWER)
  
  if(GIT_DEP_CMAKE_ARGS)
    list(JOIN GIT_DEP_CMAKE_ARGS "\n    " ADDITIONAL_ARGS)
  else()
    set(ADDITIONAL_ARGS "")
  endif()
  
  if(GIT_DEP_TAG)
    set(TAG_CMD "GIT_TAG \"${GIT_DEP_TAG}\"")
  else()
    set(TAG_CMD "")
  endif()

  if(GIT_DEP_APPLY_PATCH)
    set(GIT_DEP_PATCH_COMMAND "git apply \"${GIT_DEP_APPLY_PATCH}\"")
  elseif(NOT GIT_DEP_PATCH_COMMAND)
    set(GIT_DEP_PATCH_COMMAND "")
  endif()
  
  if(GIT_DEP_SOURCE_SUBDIR)
    set(SUBDIR_CMD "SOURCE_SUBDIR \"${GIT_DEP_SOURCE_SUBDIR}\"")
  else()
    set(SUBDIR_CMD "")
  endif()
  
  configure_file(
    ${CMAKE_CURRENT_LIST_DIR}/CMakeLists.txt.in
    ${CMAKE_BINARY_DIR}/_deps/${NAME_LOWER}-subbuild/CMakeLists.txt @ONLY)

  execute_process(
    COMMAND ${CMAKE_COMMAND} -S. -Bproject_build -G "${CMAKE_GENERATOR}"
    RESULT_VARIABLE result
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/_deps/${NAME_LOWER}-subbuild)
  if(result)
    message(FATAL_ERROR "CMake step for ${GIT_DEP_NAME} failed: ${result}")
  endif()
  
  ProcessorCount(job_count)
  math(EXPR job_count "${job_count} + 1")

  execute_process(
    COMMAND ${CMAKE_COMMAND} --build project_build --config ${CMAKE_BUILD_TYPE} -- -j "${job_count}"
    RESULT_VARIABLE result
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/_deps/${NAME_LOWER}-subbuild)
  if(result)
    message(FATAL_ERROR "Build step for ${GIT_DEP_NAME} failed: ${result}")
  endif()

  message(STATUS "Building ${GIT_DEP_NAME}: ...DONE")
endfunction()

function(defineWarningSilencerTarget)
  add_library(drlojekyll_disable_warnings INTERFACE)

  if(CMAKE_CXX_COMPILER_ID MATCHES "Clang" OR CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    target_compile_options(drlojekyll_disable_warnings INTERFACE
      -w
    )

  elseif(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
    target_compile_options(drlojekyll_disable_warnings INTERFACE
      /W0
    )
  endif()
endfunction()

function(defineSanitizersTarget)
  set(flag_list
    -fomit-frame-pointer
    -fsanitize=address,undefined
  )

  add_library(drlojekyll_sanitizers INTERFACE)
  target_compile_options(drlojekyll_sanitizers INTERFACE
    ${flag_list}
  )

  target_link_options(drlojekyll_sanitizers INTERFACE
    ${flag_list}
  )
endfunction()