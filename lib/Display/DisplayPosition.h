// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <drlojekyll/Display/DisplayPosition.h>

namespace hyde {
namespace display {

enum PositionStatus : uint16_t {
  kPositionStatusOK = 0u,
  kPositionStatusIndexOverflow = 1u << 0u,
  kPositionStatusLineNumberOverflow = 1u << 1u,
  kPositionStatusColumnNumberOverflow = 1u << 2u,
  kPositionStatusDisplayIdOverflow = 1u << 3u,
};

DEFINE_BOXED_TYPE(Index, uint32_t);
DEFINE_BOXED_TYPE(DisplayId, uint16_t);
DEFINE_BOXED_TYPE(Line, uint32_t);
DEFINE_BOXED_TYPE(Column, uint32_t);

struct Position final
    : public TypedOpaqueData<Index, DisplayId, PositionStatus, Line, Column> {

  void Emplace(uint64_t display_id, uint64_t index, uint64_t line, uint64_t col,
               PositionStatus status = kPositionStatusOK);
};

}  // namespace display
}  // namespace hyde
