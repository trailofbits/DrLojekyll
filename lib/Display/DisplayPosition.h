// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <drlojekyll/Display/DisplayPosition.h>

namespace hyde {
namespace display {

enum PositionStatus : uint64_t {
  kPositionStatusIndexOverflow = 1u << 0,
  kPositionStatusLineNumberOverflow = 1u << 1,
  kPositionStatusColumnNumberOverflow = 1u << 2,
  kPositionStatusDisplayIdOverflow = 1u << 3,
};

struct Position {
  uint64_t index:24;
  uint64_t line:12;
  uint64_t column:12;
  uint64_t display_id:12;
  uint64_t status:4;
};

union PositionInterpreter {
  uint64_t flat{0};
  Position position;
};

static_assert(sizeof(PositionInterpreter) == 8);

}  // namespace display
}  // namespace hyde
