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

// Stream that lets one read from data buffer.
class StringViewStream final : public display::DataStream {
 public:
  explicit StringViewStream(const std::string_view data_);

  virtual ~StringViewStream(void);

  bool ReadData(std::string_view *data_out) override;

  bool TryGetErrorMessage(std::ostream *) const override;

  void MarkAsDone(void) {
    done = true;
  }

 private:
  std::string_view data;
  bool done{false};
};

}  // namespace display
}  // namespace hyde
