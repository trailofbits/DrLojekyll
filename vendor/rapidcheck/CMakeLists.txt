# Copyright 2020, Trail of Bits, Inc. All rights reserved.

set(enabled_option_list
  RC_ENABLE_RTTI
)

set(disabled_option_list
  RC_ENABLE_BOOST
  RC_ENABLE_BOOST_TEST
  RC_ENABLE_CATCH
  RC_ENABLE_EXAMPLES
  RC_ENABLE_GMOCK
  RC_ENABLE_GTEST
  RC_ENABLE_TESTS
  RC_INSTALL_ALL_EXTRAS
)

foreach(enabled_option ${enabled_option_list})
  set("${enabled_option}" true CACHE BOOL "Forced rapidcheck setting" FORCE)
endforeach()

foreach(disabled_option ${disabled_option_list})
  set("${disabled_option}" false CACHE BOOL "Forced rapidcheck setting" FORCE)
endforeach()

add_subdirectory(src EXCLUDE_FROM_ALL)

target_link_libraries(rapidcheck PRIVATE
  drlojekyll_disable_warnings
)

if(DRLOJEKYLL_ENABLE_SANITIZERS)
  target_link_libraries(rapidcheck PRIVATE
    drlojekyll_sanitizers
  )
endif()
