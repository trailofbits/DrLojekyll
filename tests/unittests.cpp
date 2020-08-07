// Here we have a unit test & property test suite for Dr. Lojekyll.
//
// We use Google Test for unit tests (https://github.com/google/googletest) and
// its test driver infrastructure.
//
// We use RapidCheck for property-based tests (https://github.com/emil-e/rapidcheck).
// RapidCheck includes integration support with Google Test, which is enabled
// here.
//
// Note: In RapidCheck test cases, you want to use RapidCheck assertions such
// as `RC_ASSERT`, instead of Google Test assertions.

#include <gtest/gtest.h>
#include <rapidcheck/gtest.h>

#include <drlojekyll/Util/BitManipulation.h>

TEST(BitManipulation, rotate_zero) {
  EXPECT_EQ(hyde::RotateRight64(42, 0), 42);
}

RC_GTEST_PROP(BitManipulation, prop_rotate_right_zero, (unsigned rot)) {
  RC_ASSERT(hyde::RotateRight64(0, rot) == 0);
}

RC_GTEST_PROP(BitManipulation, prop_rotate_right_by_zero, (uint64_t val)) {
  RC_ASSERT(hyde::RotateRight64(val, 0) == val);
}
