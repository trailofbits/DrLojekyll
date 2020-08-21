#include <drlojekyll/Util/BitManipulation.h>

#include "unittests.h"

// A simple Google Test example
TEST(BitManipulation, rotate_zero) {
  EXPECT_EQ(hyde::RotateRight64(42, 0), 42);
}

// A RapidCheck property-based example
RC_GTEST_PROP(BitManipulation, prop_rotate_right_zero, (unsigned rot)) {
  RC_ASSERT(hyde::RotateRight64(0, rot) == 0);
}

// Another RapidCheck property-based example
RC_GTEST_PROP(BitManipulation, prop_rotate_right_by_zero, (uint64_t val)) {
  RC_ASSERT(hyde::RotateRight64(val, 0) == val);
}
