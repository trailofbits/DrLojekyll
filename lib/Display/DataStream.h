// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <iosfwd>
#include <string_view>

namespace hyde {
namespace display {

// Interface providing streaming access to sequences of character bytes from
// some location (e.g. a file, a stream, or a buffer).
class DataStream {
 public:
  virtual ~DataStream(void);

  // Read data from the stream. Returns `true` if data was read, and updates
  // `data_out`. `data_out` is valid until the next call to `ReadData`. Returns
  // `false` upon error, or when no more data can be read.
  virtual bool ReadData(std::string_view *data_out) = 0;

  // Returns `true` if there was an error, and if `os` is non-NULL, outputs
  // the error message to the `os` stream.
  virtual bool TryGetErrorMessage(std::ostream *os) const = 0;
};

}  // namespace display
}  // namespace hyde
