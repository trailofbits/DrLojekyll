#pragma once

#include <gtest/gtest.h>

#if defined(__GNUC__)
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Wconversion"
#  pragma GCC diagnostic ignored "-Wsign-compare"
#endif

#include <rapidcheck/gtest.h>

#if defined(__GNUC__)
#  pragma GCC diagnostic pop
#endif

#include "drlojekyll_paths.h"
