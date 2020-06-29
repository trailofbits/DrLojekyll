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
  ~ErrorLog(void);

  explicit ErrorLog(const DisplayManager &dm_);

  // An error message related to a line:column offset.
  void Append(Error error) const;

  // An error message related to a line:column offset.
  Error Append(const DisplayPosition &pos) const;

  // An error message related to a highlighted range of tokens.
  Error Append(const DisplayRange &range) const;

  // An error message related to a highlighted range of tokens, with one
  // character in particular being referenced.
  Error Append(const DisplayRange &range,
               const DisplayPosition &pos_in_range) const;

  // An error message related to a highlighted range of tokens, with a sub-range
  // in particular being referenced.
  Error Append(const DisplayRange &range, const DisplayRange &sub_range) const;

  // An error message related to a highlighted range of tokens, with a sub-range
  // in particular being referenced, where the error itself is at
  // `pos_in_range`.
  Error Append(const DisplayRange &range, const DisplayRange &sub_range,
               const DisplayPosition &pos_in_range) const;

  // Check if the log is empty.
  bool IsEmpty(void) const;

  // Returns the number of errors in the log.
  unsigned Size(void) const;

  // Render the formatted errors to a stream, along with any attached notes.
  void Render(
      std::ostream &os,
      const ErrorColorScheme &color_scheme=Error::kDefaultColorScheme) const;

 private:
  class Impl;

  std::shared_ptr<Impl> impl;
};

}  // namespace hyde
