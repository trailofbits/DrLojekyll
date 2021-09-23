# Copyright 2020, Trail of Bits, Inc. All rights reserved.

if(NOT TARGET reproc AND NOT TARGET reproc++)
  include("${CMAKE_CURRENT_LIST_DIR}/Findreproc.cmake")
endif()
