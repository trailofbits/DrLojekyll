// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <drlojekyll/Display/Display.h>

#include <cassert>
#include <memory>
#include <string>
#include <vector>

#include <drlojekyll/Display/DisplayConfiguration.h>

namespace hyde {
namespace display {
class DataStream;
}  // namespace display

class DisplayReader;
class DisplayManager;

// Implementation of `Display`.
class DisplayImpl {
 public:
  explicit DisplayImpl(unsigned id_,
                       const DisplayConfiguration &config_,
                       display::DataStream *stream_);

  ~DisplayImpl(void);

  // Tries to read the character at index `index`.
  bool TryReadChar(uint64_t index, char *ch_out);

  // Tries to get the position of the character at index `index`.
  bool TryGetPosition(uint64_t index, DisplayPosition *pos_out);

  inline std::string_view Name(void) const {
    return config.name;
  }

 private:
  friend class Display;
  friend class DisplayReader;

  // Identifier of this display.
  const unsigned id;

  // Current configuration of the display.
  const DisplayConfiguration config;

  // Stream for fetching characters.
  std::unique_ptr<display::DataStream> stream;

  // Where the data of this display is stored. This stores the byte values
  // after things like tab expansion, based off of `config`.
  std::string data;

  // The position of every 256th byte.
  std::vector<DisplayPosition> waypoints;

  // Tracks column position of the next characters read from `stream`.
  unsigned next_line{1};
  unsigned next_column{1};

  DisplayImpl(void) = delete;
};

}  // namespace hyde
