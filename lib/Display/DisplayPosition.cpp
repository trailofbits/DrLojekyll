// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include "DisplayPosition.h"

#include <drlojekyll/Lex/Token.h>

namespace hyde {
namespace display {

void Position::Emplace(uint64_t display_id, uint64_t index, uint64_t line,
                       uint64_t col, PositionStatus status_) {
  Store<display::DisplayId>(display_id);
  Store<display::Index>(index);
  Store<display::Line>(line);
  Store<display::Column>(col);

  unsigned status = status_;
  if (Load<display::DisplayId>() != display_id) {
    Store<display::DisplayId>(0u);
    status |= display::kPositionStatusDisplayIdOverflow;
  }
  if (Load<display::Index>() != index) {
    Store<display::Index>(DisplayPosition::kInvalidIndex);
    status |= display::kPositionStatusIndexOverflow;
  }
  if (Load<display::Line>() != line) {
    Store<display::Line>(0u);
    status |= display::kPositionStatusLineNumberOverflow;
  }
  if (Load<display::Column>() != col) {
    Store<display::Column>(0u);
    status |= display::kPositionStatusColumnNumberOverflow;
  }

  Store<display::PositionStatus>(static_cast<PositionStatus>(status));
}

}  // namespace display

// Returns `true` if the display position is valid.
bool DisplayPosition::IsValid(void) const {
  if (As<display::Position>().Load<display::PositionStatus>() !=
      display::PositionStatus::kPositionStatusOK) {
    return false;
  } else {
    return HasData();
  }
}

// Returns `true` if the display position has data.
bool DisplayPosition::HasData(void) const {
  const auto pos = As<display::Position>();
  return 0 < pos.Load<display::Column>() && 0 < pos.Load<display::Line>() &&
         0 < pos.Load<display::DisplayId>();
}

// Return the display ID, or `~0U` (32 0s, followed by 32 1s) if invalid.
uint64_t DisplayPosition::DisplayId(void) const {
  const auto pos = As<display::Position>();
  const auto status = pos.Load<display::PositionStatus>();
  if (display::kPositionStatusDisplayIdOverflow & status) {
    return DisplayPosition::kInvalidDisplayId;
  } else if (HasData()) {
    return pos.Load<display::DisplayId>();
  } else {
    return DisplayPosition::kInvalidDisplayId;
  }
}

// Index of the character at this position, or `~0U` (32 0s, followed by
// 32 1s) if invalid.
uint64_t DisplayPosition::Index(void) const {
  const auto pos = As<display::Position>();
  const auto status = pos.Load<display::PositionStatus>();
  if (display::kPositionStatusIndexOverflow & status) {
    return DisplayPosition::kInvalidIndex;
  } else if (HasData()) {
    return pos.Load<display::Index>();
  } else {
    return DisplayPosition::kInvalidIndex;
  }
}

// Return the line number (starting at `1`) from the display referenced
// by this position, or `~0U` (32 0s, followed by 32 1s) if invalid.
uint64_t DisplayPosition::Line(void) const {
  const auto pos = As<display::Position>();
  const auto status = pos.Load<display::PositionStatus>();
  if (display::kPositionStatusLineNumberOverflow & status) {
    return DisplayPosition::kInvalidLine;
  } else if (HasData()) {
    return pos.Load<display::Line>();
  } else {
    return DisplayPosition::kInvalidLine;
  }
}

// Return the column number (starting at `1`) from the display referenced
// by this position, or `~0U` (32 0s, followed by 32 1s) if invalid.
uint64_t DisplayPosition::Column(void) const {
  const auto pos = As<display::Position>();
  const auto status = pos.Load<display::PositionStatus>();
  if (display::kPositionStatusColumnNumberOverflow & status) {
    return DisplayPosition::kInvalidColumn;
  } else if (HasData()) {
    return pos.Load<display::Column>();
  } else {
    return DisplayPosition::kInvalidColumn;
  }
}

DisplayPosition::DisplayPosition(uint64_t display_id, uint64_t index,
                                 uint64_t line, uint64_t column) {
  As<display::Position>().Emplace(display_id, index, line, column);
}

// Tries to compute the distance between two positions.
bool DisplayPosition::TryComputeDistanceTo(DisplayPosition to,
                                           int64_t *num_bytes,
                                           int64_t *num_lines,
                                           int64_t *num_cols) const {

  const auto from_pos = As<display::Position>();
  const auto to_pos = to.As<display::Position>();
  const auto status = from_pos.Load<display::PositionStatus>() |
                      to_pos.Load<display::PositionStatus>();

  if (status & display::kPositionStatusDisplayIdOverflow) {
    return false;
  }

  if (from_pos.Load<display::DisplayId>() !=
      to_pos.Load<display::DisplayId>()) {
    return false;
  }

  if (num_bytes) {
    if (status & display::kPositionStatusIndexOverflow) {
      return false;
    }
    const auto from_index =
        static_cast<int64_t>(from_pos.Load<display::Index>());
    const auto to_index = static_cast<int64_t>(to_pos.Load<display::Index>());
    *num_bytes = (to_index - from_index);
  }

  if (num_lines) {
    if (status & display::kPositionStatusLineNumberOverflow) {
      return false;
    }
    const auto from_index =
        static_cast<int64_t>(from_pos.Load<display::Line>());
    const auto to_index = static_cast<int64_t>(to_pos.Load<display::Line>());
    *num_lines = (to_index - from_index);
  }

  if (num_cols) {
    if (status & display::kPositionStatusColumnNumberOverflow) {
      return false;
    }
    const auto from_index =
        static_cast<int64_t>(from_pos.Load<display::Column>());
    const auto to_index = static_cast<int64_t>(to_pos.Load<display::Column>());
    *num_cols = (to_index - from_index);
  }

  return true;
}

DisplayRange::DisplayRange(const Token &from_, const Token &to_)
    : DisplayRange(from_.Position(), to_.NextPosition()) {}

bool DisplayRange::IsValid(void) const {
  const auto from_pos = from.As<display::Position>();
  const auto to_pos = to.As<display::Position>();
  const auto status = from_pos.Load<display::PositionStatus>() |
                      to_pos.Load<display::PositionStatus>();

  if (status != display::kPositionStatusOK) {
    return false;
  }

  if (from_pos.Load<display::DisplayId>() !=
      to_pos.Load<display::DisplayId>()) {
    return false;
  }

  const auto from_index = static_cast<int64_t>(from_pos.Load<display::Index>());
  const auto to_index = static_cast<int64_t>(to_pos.Load<display::Index>());

  // NOTE(pag): Ranges are exclusive.
  return from_index < to_index;
}

}  // namespace hyde
