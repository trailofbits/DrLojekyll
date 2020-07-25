// Copyright 2019, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/Display/DisplayPosition.h>

#include <iosfwd>

namespace hyde {

class Display;
class DisplayImpl;

// Used to read characters from a display.
class DisplayReader {
 public:
  ~DisplayReader(void);

  explicit DisplayReader(const Display &display_);

  // Tries to read a character from the display. If successful, returns `true`
  // and updates `*ch_out`.
  bool TryReadChar(char *ch_out);

  // Unreads the last read character.
  void UnreadChar(void);

  // Returns the current display position.
  DisplayPosition CurrentPosition(void) const;

  // Returns `true` if there was an error, and if `os` is non-NULL, outputs
  // the error message to the `os` stream.
  bool TryGetErrorMessage(std::ostream *os) const;

 private:
  DisplayReader(void) = delete;

  DisplayImpl *display;
  uint64_t index{0};
  uint64_t line{1};
  uint64_t column{1};
};

}  // namespace hyde
