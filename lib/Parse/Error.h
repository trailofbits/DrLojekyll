// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <drlojekyll/Parse/Error.h>

#include <cstddef>
#include <sstream>
#include <string>
#include <string_view>

namespace hyde {

class DisplayManager;

class ErrorImpl {
 public:
  std::shared_ptr<ErrorImpl> next;

  const DisplayManager *display_manager{nullptr};
  std::string_view path;
  std::stringstream message;
  std::string_view highlight_range;
  uint64_t line{0};
  uint64_t column{0};
  uint64_t underline_length{0};
  size_t carat{std::string::npos};
};

}  // namespace hyde
