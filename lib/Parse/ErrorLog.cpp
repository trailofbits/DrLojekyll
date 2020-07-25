// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include <drlojekyll/Parse/ErrorLog.h>

#include <type_traits>
#include <vector>

#include "Error.h"

namespace hyde {

class ErrorLog::Impl {
 public:
  explicit Impl(const DisplayManager &dm_) : dm(dm_) {}

  const DisplayManager dm;
  std::vector<Error> errors;
};

ErrorLog::~ErrorLog(void) {}

ErrorLog::ErrorLog(const DisplayManager &dm_)
    : impl(std::make_shared<Impl>(dm_)) {}

// An error message related to a line:column offset.
Error ErrorLog::Append(const DisplayPosition &pos) const {
  Error err(impl->dm, pos);
  impl->errors.push_back(err);
  return err;
}

// An error message related to a highlighted range of tokens.
Error ErrorLog::Append(const DisplayRange &range) const {
  Error err(impl->dm, range);
  impl->errors.push_back(err);
  return err;
}

// An error message related to a highlighted range of tokens, with one
// character in particular being referenced.
Error ErrorLog::Append(const DisplayRange &range,
                       const DisplayPosition &pos_in_range) const {
  Error err(impl->dm, range, pos_in_range);
  impl->errors.push_back(err);
  return err;
}

// An error message related to a highlighted range of tokens, with a sub-range
// in particular being referenced.
Error ErrorLog::Append(const DisplayRange &range,
                       const DisplayRange &sub_range) const {
  Error err(impl->dm, range, sub_range);
  impl->errors.push_back(err);
  return err;
}

// An error message related to a highlighted range of tokens, with a sub-range
// in particular being referenced, where the error itself is at
// `pos_in_range`.
Error ErrorLog::Append(const DisplayRange &range, const DisplayRange &sub_range,
                       const DisplayPosition &pos_in_range) const {
  Error err(impl->dm, range, sub_range, pos_in_range);
  impl->errors.push_back(err);
  return err;
}

// Add an error to the log.
void ErrorLog::Append(Error error) const {
  impl->errors.emplace_back(std::move(error));
}

// Check if the log is empty.
bool ErrorLog::IsEmpty(void) const {
  return impl->errors.empty();
}

// Returns the number of errors in the log.
unsigned ErrorLog::Size(void) const {
  return static_cast<unsigned>(impl->errors.size());
}

// Render the formatted errors to a stream, along with any attached notes.
void ErrorLog::Render(std::ostream &os,
                      const ErrorColorScheme &color_scheme) const {
  for (auto &error : impl->errors) {
    error.Render(os, color_scheme);
  }
}

}  // namespace hyde
