// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <memory>
#include <string_view>

namespace hyde {

// Basic string pool implementation. Used for body_variables, atoms, and strings.
class StringPool {
 public:
  ~StringPool(void);
  StringPool(void);

  // Default IDs.
  unsigned UnderscoreId(void) const;
  unsigned LiteralTrueId(void) const;
  unsigned LiteralFalseId(void) const;

  // Intern a code block into the pool, returning its ID.
  unsigned InternCode(std::string_view code) const;

  // Read out some code block given its ID.
  bool TryReadCode(unsigned id, std::string_view *code_out) const;

  // Intern a string into the pool, returning its offset in the pool.
  unsigned InternString(std::string_view data, bool force = false) const;

  // Read out some string given its index and length.
  bool TryReadString(unsigned index, unsigned len,
                     std::string_view *data_out) const;

 private:
  class Impl;

  std::shared_ptr<Impl> impl;
};

}  // namespace hyde
