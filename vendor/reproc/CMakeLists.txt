# Copyright 2020, Trail of Bits, Inc. All rights reserved.

set(enabled_option_list
  REPROC_MULTITHREADED
  REPROC++
)

set(disabled_option_list
  REPROC_DEVELOP
  REPROC_EXAMPLES
  REPROC_TEST
  REPROC_WARNINGS
)

set(target_name_list
  reproc
  reproc++
)

foreach(enabled_option ${enabled_option_list})
  set("${enabled_option}" true CACHE BOOL "Forced reproc setting" FORCE)
endforeach()

foreach(disabled_option ${disabled_option_list})
  set("${disabled_option}" false CACHE BOOL "Forced reproc setting" FORCE)
endforeach()

add_subdirectory(src EXCLUDE_FROM_ALL)

foreach(target_name ${target_name_list})
  target_link_libraries("${target_name}" PRIVATE
    drlojekyll_disable_warnings
  )

  if(DRLOJEKYLL_ENABLE_SANITIZERS)
    target_link_libraries("${target_name}" PRIVATE
      drlojekyll_sanitizers
    )
  endif()
endforeach()
