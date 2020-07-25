// Copyright 2019, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/Display/DisplayPosition.h>

#include <string_view>

namespace hyde {

class DisplayImpl;
class DisplayManager;
class DisplayReader;

// Manages the data associated with a display. Lets us read the data from the
// display.
class Display {
 public:
  ~Display(void) {}
  Display(const Display &) = default;
  Display(Display &&) noexcept = default;

  // Return the ID of this display.
  unsigned Id(void) const;

  // Tries to read a character from this display, given its position. Returns
  // `true` if successful and updates `*ch_out`.
  bool TryReadChar(DisplayPosition position, char *ch_out) const;

  // Tries to read a range of characters from the display. Returns `true` if
  // successful and updates `*data_out`.
  //
  // NOTE(pag): `*data_out` is valid for the lifetime of the `DisplayManager`
  //            from which this `Display` was created.
  bool TryReadData(DisplayRange range, std::string_view *data_out) const;

 private:
  friend class DisplayManager;
  friend class DisplayReader;

  inline explicit Display(DisplayImpl *impl_) : impl(impl_) {}

  Display(void) = delete;

  // Owned by the `DisplayManager` that created this `Display`.
  DisplayImpl *impl{nullptr};
};

}  // namespace hyde
