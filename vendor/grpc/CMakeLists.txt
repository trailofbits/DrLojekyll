# Copyright 2020, Trail of Bits, Inc. All rights reserved.

set(enabled_option_list
  gRPC_BACKWARDS_COMPATIBILITY_MODE
  gRPC_BUILD_GRPC_CPP_PLUGIN
  gRPC_BUILD_CODEGEN
)

set(disabled_option_list
  gRPC_BUILD_CSHARP_EXT
  gRPC_BUILD_GRPC_CSHARP_PLUGIN
  gRPC_BUILD_GRPC_NODE_PLUGIN
  gRPC_BUILD_GRPC_OBJECTIVE_C_PLUGIN
  gRPC_BUILD_GRPC_PHP_PLUGIN
  gRPC_BUILD_GRPC_PYTHON_PLUGIN
  gRPC_BUILD_GRPC_RUBY_PLUGIN
  gRPC_BUILD_TESTS
  gRPC_INSTALL
  gRPC_USE_PROTO_LITE
)

set(module_library_list
  gRPC_CARES_PROVIDER
  gRPC_PROTOBUF_PROVIDER
  gRPC_RE2_PROVIDER
  gRPC_ZLIB_PROVIDER
)

set(package_library_list
  gRPC_SSL_PROVIDER
  gRPC_ABSL_PROVIDER
)

set(library_target_list
  address_sorting
  gpr
  grpc
  grpc_unsecure
  grpc++
  grpc++_alts
  grpc++_error_details
  grpc++_reflection
  grpc++_unsecure
  grpc_plugin_support
  grpcpp_channelz
  upb
)

set(executable_target_list
  grpc_cpp_plugin
  grpc_csharp_plugin
  grpc_node_plugin
  grpc_objective_c_plugin
  grpc_php_plugin
  grpc_python_plugin
  grpc_ruby_plugin
)

foreach(module_library ${module_library_list})
  set("${module_library}" "module" CACHE STRING "Forced gRPC setting" FORCE)
endforeach()

foreach(package_library ${package_library_list})
  set("${package_library}" "package" CACHE STRING "Forced gRPC setting" FORCE)
endforeach()

foreach(enabled_option ${enabled_option_list})
  set("${enabled_option}" true CACHE BOOL "Forced gRPC setting" FORCE)
endforeach()

foreach(disabled_option ${disabled_option_list})
  set("${disabled_option}" false CACHE BOOL "Forced gRPC setting" FORCE)
endforeach()

# The abseil library, imported by gRPC, uses BUILD_TESTING to enable its
# tests, instead of a dedicated ABSEIL_BUILD_TESTING option. This causes
# their tests to get enabled when CMake/CTest is used. Force it to off
# while we include the external folder to work around this issue.
set(grpc_orig_build_testing "${BUILD_TESTING}")
set("BUILD_TESTING" false CACHE BOOL "Forced gRPC setting" FORCE)

add_subdirectory(src EXCLUDE_FROM_ALL)

set("BUILD_TESTING" "${grpc_orig_build_testing}" CACHE BOOL "Forced gRPC setting" FORCE)

foreach(library_target ${library_target_list})
  if(TARGET "${library_target}")
    add_library("gRPC::${library_target}" ALIAS "${library_target}")
  endif()
endforeach()

foreach(executable_target ${executable_target_list})
  if(TARGET "${executable_target}")
    add_executable("gRPC::${executable_target}" ALIAS "${executable_target}")
  endif()
endforeach()

foreach(target_name ${library_target_list} ${executable_target_list})
  if(NOT TARGET "${target_name}")
    continue()
  endif()

  # The plain signature of target_link_libraries has been used
  # already, so we have to do the same
  target_link_libraries("${target_name}"
    drlojekyll_disable_warnings
  )

  if(DRLOJEKYLL_ENABLE_SANITIZERS)
    target_link_libraries("${target_name}"
      drlojekyll_sanitizers
    )
  endif()
endforeach()
