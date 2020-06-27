// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <drlojekyll/Parse/Error.h>
#include <drlojekyll/Display/DisplayManager.h>

#include <cstddef>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

namespace hyde {

class ErrorImpl {
 public:
  explicit ErrorImpl(const DisplayManager &display_manager_)
      : display_manager(display_manager_) {}

  std::shared_ptr<ErrorImpl> next;

  const DisplayManager display_manager;
  std::string_view path;
  std::stringstream message;

  std::string pre_source;
  std::string source;
  size_t post_source_start{0};
  size_t post_source_len{0};
  std::vector<bool> is_error;
  uint64_t hightlight_line{0};
  uint64_t line{0};
  uint64_t column{0};
};

}  // namespace hyde
