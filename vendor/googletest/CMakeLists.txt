# Copyright 2020, Trail of Bits, Inc. All rights reserved.

set(enabled_option_list
  BUILD_GMOCK
)

set(disabled_option_list
  INSTALL_GTEST
  gmock_build_tests
  gtest_build_samples
  gtest_build_tests
  gtest_disable_pthreads
  gtest_force_shared_crt
  gtest_hide_internal_symbols
)

foreach(enabled_option ${enabled_option_list})
  set("${enabled_option}" true CACHE BOOL "Forced googletest setting" FORCE)
endforeach()

foreach(disabled_option ${disabled_option_list})
  set("${disabled_option}" false CACHE BOOL "Forced googletest setting" FORCE)
endforeach()

add_subdirectory(src EXCLUDE_FROM_ALL)
