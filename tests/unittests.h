#pragma once

#include <gtest/gtest.h>

#if defined(__GNUC__)
#  pragma GCC diagnostic ignored "-Wconversion"
#  pragma GCC diagnostic ignored "-Wsign-compare"
#elif defined(_MSC_VER)
//#  pragma warning (disable: )
#endif

#include <rapidcheck/gtest.h>

#if defined(__GNUC__)
#  pragma GCC diagnostic pop
#elif defined(_MSC_VER)
//#  pragma warning (pop)
#endif

#include "drlojekyll_paths.h"
