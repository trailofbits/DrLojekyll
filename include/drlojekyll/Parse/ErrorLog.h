// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <drlojekyll/Parse/Error.h>

namespace hyde {

// Keeps track of a log of errors.
class ErrorLog {
 public:
  ~ErrorLog(void);
  ErrorLog(void);

  // Add an error to the log.
  void Append(Error error) const;

  // Check if the log is empty.
  bool IsEmpty(void) const;

  // Render the formatted errors to a stream, along with any attached notes.
  void Render(
      std::ostream &os,
      const ErrorColorScheme &color_scheme=Error::kDefaultColorScheme) const;

 private:
  class Impl;

  std::shared_ptr<Impl> impl;
};

}  // namespace hyde
