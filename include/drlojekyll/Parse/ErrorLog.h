// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <drlojekyll/Parse/Error.h>

namespace hyde {

class DisplayManager;
class DisplayPosition;
class DisplayRange;

// Keeps track of a log of errors.
class ErrorLog {
 public:
  explicit ErrorLog(const DisplayManager &dm_);

  // Add the given error message.
  void Append(Error error) const;

  // Add a new error message related to a line:column offset.
  Error Append(const DisplayPosition &pos) const;

  // Add a new error message related to a highlighted range of tokens.
  Error Append(const DisplayRange &range) const;

  // Add a new error message related to a highlighted range of tokens, with one
  // character in particular being referenced.
  Error Append(const DisplayRange &range,
               const DisplayPosition &pos_in_range) const;

  // Add an new error message related to a highlighted range of tokens, with a
  // sub-range in particular being referenced.
  Error Append(const DisplayRange &range, const DisplayRange &sub_range) const;

  // Add a new error message related to a highlighted range of tokens, with a
  // sub-range in particular being referenced, where the error itself is at
  // `pos_in_range`.
  Error Append(const DisplayRange &range, const DisplayRange &sub_range,
               const DisplayPosition &pos_in_range) const;

  // Check if the log is empty.
  bool IsEmpty(void) const;

  // Returns the number of errors in the log.
  unsigned Size(void) const;

  // Render the formatted errors to a stream, along with any attached notes.
  void Render(std::ostream &os, const ErrorColorScheme &color_scheme =
                                    Error::kDefaultColorScheme) const;

 private:
  class Impl;

  std::shared_ptr<Impl> impl;
};

}  // namespace hyde
