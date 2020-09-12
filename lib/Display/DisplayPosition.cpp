// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include "DisplayPosition.h"

namespace hyde {

// Returns `true` if the display position is valid.
bool DisplayPosition::IsValid(void) const {
  display::PositionInterpreter interpreter = {opaque_data};
  if (interpreter.position.status) {
    return false;
  } else {
    return HasData();
  }
}

// Returns `true` if the display position has data.
bool DisplayPosition::HasData(void) const {
  display::PositionInterpreter interpreter = {opaque_data};
  return 0 < interpreter.position.column && 0 < interpreter.position.line &&
         0 < interpreter.position.display_id;
}

// Return the display ID, or `~0U` (32 0s, followed by 32 1s) if invalid.
uint64_t DisplayPosition::DisplayId(void) const {
  display::PositionInterpreter interpreter = {opaque_data};
  const auto status = interpreter.position.status;
  if (display::kPositionStatusDisplayIdOverflow & status) {
    return static_cast<uint32_t>(~0u);
  } else if (HasData()) {
    return static_cast<unsigned>(interpreter.position.display_id);
  } else {
    return static_cast<uint32_t>(~0u);
  }
}

// Index of the character at this position, or `~0U` (32 0s, followed by
// 32 1s) if invalid.
uint64_t DisplayPosition::Index(void) const {
  display::PositionInterpreter interpreter = {opaque_data};
  const auto status = interpreter.position.status;
  if (display::kPositionStatusIndexOverflow & status) {
    return static_cast<uint32_t>(~0u);
  } else if (HasData()) {
    return static_cast<unsigned>(interpreter.position.index);
  } else {
    return static_cast<uint32_t>(~0u);
  }
}

// Return the line number (starting at `1`) from the display referenced
// by this position, or `~0U` (32 0s, followed by 32 1s) if invalid.
uint64_t DisplayPosition::Line(void) const {
  display::PositionInterpreter interpreter = {opaque_data};
  const auto status = interpreter.position.status;
  if (display::kPositionStatusLineNumberOverflow & status) {
    return static_cast<uint32_t>(~0u);
  } else if (HasData()) {
    return static_cast<unsigned>(interpreter.position.line);
  } else {
    return static_cast<uint32_t>(~0u);
  }
}

// Return the column number (starting at `1`) from the display referenced
// by this position, or `~0U` (32 0s, followed by 32 1s) if invalid.
uint64_t DisplayPosition::Column(void) const {
  display::PositionInterpreter interpreter = {opaque_data};
  const auto status = interpreter.position.status;
  if (display::kPositionStatusColumnNumberOverflow & status) {
    return static_cast<uint32_t>(~0u);
  } else if (HasData()) {
    return static_cast<unsigned>(interpreter.position.column);
  } else {
    return static_cast<uint32_t>(~0u);
  }
}

#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Wconversion"
#endif

DisplayPosition::DisplayPosition(uint64_t display_id, uint64_t index,
                                 uint64_t line, uint64_t column) {
  display::PositionInterpreter interpreter = {};
  interpreter.position.line = line;
  interpreter.position.column = column;
  interpreter.position.index = index;
  interpreter.position.display_id = display_id;

  if (line != interpreter.position.line) {
    interpreter.position.line = 0;
    interpreter.position.line = ~interpreter.position.line;
    interpreter.position.status |= display::kPositionStatusLineNumberOverflow;
  }

  if (column != interpreter.position.column) {
    interpreter.position.column = 0;
    interpreter.position.column = ~interpreter.position.column;
    interpreter.position.status |= display::kPositionStatusColumnNumberOverflow;
  }

  if (index != interpreter.position.index) {
    interpreter.position.index = 0;
    interpreter.position.index = ~interpreter.position.index;
    interpreter.position.status |= display::kPositionStatusIndexOverflow;
  }

  if (display_id != interpreter.position.display_id) {
    interpreter.position.display_id = 0;
    interpreter.position.status |= display::kPositionStatusDisplayIdOverflow;
  }

  opaque_data = interpreter.flat;
}

#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic pop
#endif

// Tries to compute the distance between two positions.
bool DisplayPosition::TryComputeDistanceTo(DisplayPosition to, int *num_bytes,
                                           int *num_lines,
                                           int *num_cols) const {

  display::PositionInterpreter from_interpreter = {opaque_data};
  display::PositionInterpreter to_interpreter = {to.opaque_data};

  if (IsValid() && to.IsValid() &&
      from_interpreter.position.display_id ==
          to_interpreter.position.display_id) {

    if (num_bytes) {
      *num_bytes = static_cast<int>(to_interpreter.position.index) -
                   static_cast<int>(from_interpreter.position.index);
    }
    if (num_lines) {
      *num_lines = static_cast<int>(to_interpreter.position.line) -
                   static_cast<int>(from_interpreter.position.line);
    }
    if (num_cols) {
      *num_cols = static_cast<int>(to_interpreter.position.column) -
                  static_cast<int>(from_interpreter.position.column);
    }
    return true;

  } else if (!HasData() && to.IsValid()) {
    if (num_bytes) {
      *num_bytes = static_cast<int>(to_interpreter.position.index);
    }

    // NOTE(pag): Minus `1` to be relative to the first column of first line.
    if (num_lines) {
      *num_lines = static_cast<int>(to_interpreter.position.line) - 1;
    }

    // NOTE(pag): Minus `1` to be relative to the first column of first line.
    if (num_cols) {
      *num_cols = static_cast<int>(to_interpreter.position.column) - 1;
    }
    return true;

  } else {
    return false;
  }
}

bool DisplayRange::IsValid(void) const {
  display::PositionInterpreter from_interpreter = {from.opaque_data};
  display::PositionInterpreter to_interpreter = {to.opaque_data};
  return !from_interpreter.position.status && !to_interpreter.position.status &&
         from_interpreter.position.display_id ==
             to_interpreter.position.display_id &&
         from_interpreter.position.index < to_interpreter.position.index;
}

}  // namespace hyde
