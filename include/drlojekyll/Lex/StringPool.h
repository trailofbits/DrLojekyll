// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <memory>
#include <string_view>

namespace hyde {

// Basic string pool implementation. Used for variables, atoms, and strings.
class StringPool {
 public:
  ~StringPool(void);
  StringPool(void);

  // Intern a string into the pool, returning its offset in the pool.
  unsigned InternString(std::string_view data) const;

  // Read out some string.
  bool TryReadString(
      unsigned index, unsigned len, std::string_view *data_out) const;

 private:
  class Impl;

  std::shared_ptr<Impl> impl;
};

}  // namespace
