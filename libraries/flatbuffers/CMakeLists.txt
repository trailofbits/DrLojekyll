# Copyright 2021, Trail of Bits, Inc. All rights reserved.

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(enabled_option_list
  FLATBUFFERS_BUILD_FLATLIB
  FLATBUFFERS_BUILD_CPP17
  FLATBUFFERS_BUILD_FLATC
  FLATBUFFERS_BUILD_FLATHASH
)

set(disabled_option_list
  FLATBUFFERS_BUILD_GRPCTEST
  FLATBUFFERS_BUILD_LEGACY
  FLATBUFFERS_BUILD_SHAREDLIB
  FLATBUFFERS_BUILD_TESTS
  FLATBUFFERS_CODE_COVERAGE
  FLATBUFFERS_CODE_SANITIZE
  FLATBUFFERS_ENABLE_PCH
  FLATBUFFERS_INSTALL
  FLATBUFFERS_LIBCXX_WITH_CLANG
  FLATBUFFERS_PACKAGE_DEBIAN
  FLATBUFFERS_PACKAGE_REDHAT
  FLATBUFFERS_STATIC_FLATC
)

foreach(enabled_option ${enabled_option_list})
  set("${enabled_option}" true CACHE BOOL "Forced flatbuffers setting" FORCE)
endforeach()

foreach(disabled_option ${disabled_option_list})
  set("${disabled_option}" false CACHE BOOL "Forced flatbuffers setting" FORCE)
endforeach()

add_subdirectory(src EXCLUDE_FROM_ALL)

set(property_name_list
  SOURCES
  LINK_LIBRARIES
  INTERFACE_LINK_LIBRARIES
  INCLUDE_DIRECTORIES
  INTERFACE_INCLUDE_DIRECTORIES
  LINK_OPTIONS
  INTERFACE_LINK_OPTIONS
  COMPILE_DEFINITIONS
  INTERFACE_COMPILE_DEFINITIONS
  COMPILE_OPTIONS
  INTERFACE_COMPILE_OPTIONS
  COMPILE_FEATURES
  INTERFACE_COMPILE_FEATURES
)

get_target_property(library_root flatc SOURCE_DIR)
if(NOT library_root)
  message(FATAL_ERROR "Failed to locate the flatbuffers library root")
endif()


set(removed_file_list
  # Removed, because there is no matching file under "${CMAKE_CURRENT_SOURCE_DIR}/patches"
  flatc_main.cpp
)

set(replacement_file_list

  # Replaced, because we have a matching file under "${CMAKE_CURRENT_SOURCE_DIR}/patches"
  idl_gen_python.cpp
  idl_gen_grpc.cpp
  idl_gen_cpp.cpp
  idl_parser.cpp
  cpp_generator.cc
  cpp_generator.h
)

add_library(flatc_internals
  ${patched_file_list}
  
  # Added, because it does not exist in the original target
  "${CMAKE_CURRENT_SOURCE_DIR}/patches/disable_logger.cpp"
)

target_link_libraries(flatc_internals PRIVATE
  drlojekyll_disable_warnings
)

target_compile_features(flatc_internals PRIVATE
  cxx_std_11
)

target_include_directories(flatc_internals INTERFACE
  "${CMAKE_CURRENT_SOURCE_DIR}/src/include"
)

# Inherit ASAN settings, if enabled.
target_link_libraries(flatc_internals PRIVATE
    settings_public)

foreach(property_name ${property_name_list})
  get_target_property(property_value flatc "${property_name}")
  if(NOT property_value)
    continue()
  endif()

  if(property_name MATCHES "INTERFACE")
    set(property_scope "INTERFACE")
  else()
    set(property_scope "PRIVATE")
  endif()

  if(property_name MATCHES "LINK_LIBRARIES")
    target_link_libraries(flatc_internals "${property_scope}"
      ${property_value}
    )

  elseif(property_name MATCHES "INCLUDE_DIRECTORIES")
    target_include_directories(flatc_internals "${property_scope}"
      ${property_value}
    )

  elseif(property_name MATCHES "LINK_OPTIONS")
    target_link_options(flatc_internals "${property_scope}"
      ${property_value}
    )

  elseif(property_name MATCHES "COMPILE_DEFINITIONS")
    target_compile_definitions(flatc_internals "${property_scope}"
      ${property_value}
    )

  elseif(property_name MATCHES "COMPILE_OPTIONS")
    target_compile_options(flatc_internals "${property_scope}"
      ${property_value}
    )

  elseif(property_name MATCHES "COMPILE_FEATURES")
    target_compile_features(flatc_internals "${property_scope}"
      ${property_value}
    )

  elseif(property_name STREQUAL "SOURCES")
    foreach(source_file_path ${property_value})
      get_filename_component(source_file_name "${source_file_path}" NAME)
      if("${source_file_name}" IN_LIST removed_file_list)
        continue()
      endif()
      if("${source_file_name}" IN_LIST replacement_file_list)
        target_sources(flatc_internals "${property_scope}"
          "${CMAKE_CURRENT_SOURCE_DIR}/patches/${source_file_name}")
      else()
        target_sources(flatc_internals "${property_scope}"
          "${library_root}/${source_file_path}"
        )
      endif()
    endforeach()
  endif()
endforeach()
