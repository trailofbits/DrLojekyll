// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include <drlojekyll/Parse/ErrorLog.h>

#include <type_traits>
#include <vector>

#include "Error.h"

namespace hyde {

class ErrorLog::Impl {
 public:
  std::vector<Error> errors;
};

ErrorLog::~ErrorLog(void) {}

ErrorLog::ErrorLog(void)
    : impl(std::make_shared<Impl>()) {}

// Add an error to the log.
void ErrorLog::Append(Error error) const {
  impl->errors.emplace_back(std::move(error));
}

// Check if the log is empty.
bool ErrorLog::IsEmpty(void) const {
  return impl->errors.empty();
}

// Render the formatted errors to a stream, along with any attached notes.
void ErrorLog::Render(
    std::ostream &os, const ErrorColorScheme &color_scheme) const {
  for (auto &error : impl->errors) {
    error.Render(os, color_scheme);
  }
}

}  // namespace hyde
