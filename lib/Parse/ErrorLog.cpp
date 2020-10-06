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

ErrorLog::ErrorLog(const DisplayManager &dm_)
    : impl(std::make_shared<Impl>(dm_)) {}

Error ErrorLog::Append(const DisplayPosition &pos) const {
  Error err(impl->dm, pos);
  impl->errors.push_back(err);
  return err;
}

Error ErrorLog::Append(const DisplayRange &range) const {
  Error err(impl->dm, range);
  impl->errors.push_back(err);
  return err;
}

Error ErrorLog::Append(const DisplayRange &range,
                       const DisplayPosition &pos_in_range) const {
  Error err(impl->dm, range, pos_in_range);
  impl->errors.push_back(err);
  return err;
}

Error ErrorLog::Append(const DisplayRange &range,
                       const DisplayRange &sub_range) const {
  Error err(impl->dm, range, sub_range);
  impl->errors.push_back(err);
  return err;
}

Error ErrorLog::Append(const DisplayRange &range, const DisplayRange &sub_range,
                       const DisplayPosition &pos_in_range) const {
  Error err(impl->dm, range, sub_range, pos_in_range);
  impl->errors.push_back(err);
  return err;
}

void ErrorLog::Append(Error error) const {
  impl->errors.emplace_back(std::move(error));
}

Error ErrorLog::Append() const {
  Error err(impl->dm);
  impl->errors.push_back(err);
  return err;
}

bool ErrorLog::IsEmpty(void) const {
  return impl->errors.empty();
}

unsigned ErrorLog::Size(void) const {
  return static_cast<unsigned>(impl->errors.size());
}

void ErrorLog::Render(std::ostream &os,
                      const ErrorColorScheme &color_scheme) const {
  for (auto &error : impl->errors) {
    error.Render(os, color_scheme);
  }
}

}  // namespace hyde
