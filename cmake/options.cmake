# Copyright 2020, Trail of Bits, Inc. All rights reserved.

option(DRLOJEKYLL_ENABLE_VENDORED_LIBRARIES "Set to true to build against the vendored dependencies" ON)
option(DRLOJEKYLL_ENABLE_INSTALL "Set to true to enable the install targets" OFF)
option(DRLOJEKYLL_ENABLE_TESTS "Set to true to enable tests" OFF)
option(DRLOJEKYLL_ENABLE_LIBFUZZER "Set to true to enable fuzzing" OFF)
option(DRLOJEKYLL_ENABLE_SANITIZERS "Set to true to enable sanitizers" OFF)

if(DRLOJEKYLL_ENABLE_SANITIZERS AND DRLOJEKYLL_ENABLE_LIBFUZZER)
  message(FATAL_ERROR "Only enable one of the following two options: DRLOJEKYLL_ENABLE_SANITIZERS, DRLOJEKYLL_ENABLE_LIBFUZZER")
endif()
