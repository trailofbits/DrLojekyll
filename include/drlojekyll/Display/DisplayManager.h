// Copyright 2019, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/Display/Display.h>

#include <cstdint>
#include <iosfwd>
#include <memory>
#include <string_view>

namespace hyde {

class DisplayConfiguration;
class Lexer;

// Manages one or more displays. In most cases, these are just files.
class DisplayManager {
 public:
  ~DisplayManager(void);

  DisplayManager(void);
  DisplayManager(const DisplayManager &) = default;
  DisplayManager(DisplayManager &&) noexcept = default;

  // Return the name of a display, given a position.
  std::string_view DisplayName(DisplayPosition position) const;

  // Open a buffer as a display.
  //
  // NOTE(pag): `data` must remain a valid reference for the lifetime of the
  //            `DisplayManager`.
  Display
  OpenBuffer(std::string_view data, const DisplayConfiguration &config) const;

  // Open a file, specified by its path. This will read the entire contents
  // of the file into a buffer.
  Display
  OpenPath(std::string_view path, const DisplayConfiguration &config) const;

  // Open an input stream.
  //
  // NOTE(pag): `is` must remain a valid reference for the lifetime of the
  //            `DisplayManager`.
  Display
  OpenStream(std::istream &is, const DisplayConfiguration &config) const;

  // Tries to read a character from a display, given its position. Returns
  // `true` if successful and updates `*ch_out`.
  bool TryReadChar(DisplayPosition position, char *ch_out) const;

  // Tries to read a range of characters from a display. Returns `true` if
  // successful and updates `*data_out`.
  //
  // NOTE(pag): `*data_out` is valid for the lifetime of this `DisplayManager`.
  bool TryReadData(DisplayRange range, std::string_view *data_out) const;

  // Try to displace `position` by `num_bytes`. If successful, modifies
  // `position` in place, and returns `true`, otherwise returns `false`.
  bool TryDisplacePosition(DisplayPosition &position, int num_bytes) const;

 private:
  friend class Lexer;

  class Impl;

  std::shared_ptr<Impl> impl;
};

}  // namespace hyde
