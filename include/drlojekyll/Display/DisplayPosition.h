// Copyright 2019, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/Util/OpaqueData.h>

namespace hyde {

class Display;
class DisplayImpl;
class DisplayManager;
class DisplayRange;
class DisplayReader;
class Token;

// Position of a lexeme or AST node inside of a display (a abstraction over
// an input, which could be a file, but can be displayed back to a user given
// a configuration).
class DisplayPosition final : public OpaqueData {
 public:
  static constexpr uint64_t kInvalidDisplayId = ~0u;
  static constexpr uint64_t kInvalidIndex = ~0u;
  static constexpr uint64_t kInvalidLine = ~0u;
  static constexpr uint64_t kInvalidColumn = ~0u;

  DisplayPosition(void) = default;

  // Return the display ID, or `~0U` (32 0s, followed by 32 1s) if invalid.
  uint64_t DisplayId(void) const;

  // Index of the character at this position, or `~0U` (32 0s, followed by
  // 32 1s) if invalid.
  uint64_t Index(void) const;

  // Return the line number (starting at `1`) from the display referenced
  // by this position, or `~0U` (32 0s, followed by 32 1s) if invalid.
  uint64_t Line(void) const;

  // Return the column number (starting at `1`) from the display referenced
  // by this position, or `~0U` (32 0s, followed by 32 1s) if invalid.
  uint64_t Column(void) const;

  // Returns `true` if the display position is valid.
  bool IsValid(void) const;

  // Returns `true` if the display position has data.
  bool HasData(void) const;

  // Tries to compute the distance between two positions.
  bool TryComputeDistanceTo(DisplayPosition that, int64_t *num_bytes,
                            int64_t *num_lines, int64_t *num_cols) const;

  inline bool IsInvalid(void) const {
    return !IsValid();
  }

  using OpaqueData::operator ==;
  using OpaqueData::operator !=;

 private:
  friend class Display;
  friend class DisplayImpl;
  friend class DisplayManager;
  friend class DisplayRange;
  friend class DisplayReader;
  friend class Token;

  explicit DisplayPosition(uint64_t display_id, uint64_t index, uint64_t line,
                           uint64_t column);
};

// Exclusive range of characters in a display.
class DisplayRange {
 public:
  DisplayRange(void) = default;

  bool IsValid(void) const;

  inline bool IsInvalid(void) const {
    return !IsValid();
  }

  inline explicit DisplayRange(DisplayPosition from_, DisplayPosition to_)
      : from(from_),
        to(to_) {}

  inline DisplayPosition From(void) const {
    return from;
  }

  inline DisplayPosition To(void) const {
    return to;
  }

  // Tries to compute the distance between two positions.
  inline bool TryComputeDistance(int64_t *num_bytes, int64_t *num_lines,
                                 int64_t *num_cols) const {
    return from.TryComputeDistanceTo(to, num_bytes, num_lines, num_cols);
  }

 private:
  friend class Display;
  friend class DisplayManager;

  DisplayPosition from;
  DisplayPosition to;
};

}  // namespace hyde
