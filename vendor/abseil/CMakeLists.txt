# Copyright 2021, Trail of Bits, Inc. All rights reserved.

set(enabled_option_list
  ABSL_PROPAGATE_CXX_STD
)

set(disabled_option_list
  ABSL_ENABLE_INSTALL
  ABSL_LOCAL_GOOGLETEST_DIR
  ABSL_USE_EXTERNAL_GOOGLETEST
  ABSL_USE_GOOGLETEST_HEAD
)

foreach(enabled_option ${enabled_option_list})
  set("${enabled_option}" true CACHE BOOL "Forced abseil-cpp setting" FORCE)
endforeach()

foreach(disabled_option ${disabled_option_list})
  set("${disabled_option}" false CACHE BOOL "Forced abseil-cpp setting" FORCE)
endforeach()

add_subdirectory(
  "src"
  EXCLUDE_FROM_ALL
)
