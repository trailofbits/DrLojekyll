// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

#include <memory>

namespace hyde {

struct SuperSet {};

// Simple set for managing equality checks.
class EqualitySet {
 public:
  EqualitySet(void);
  ~EqualitySet(void);
  EqualitySet(const EqualitySet &that, SuperSet);

  void Insert(const void *a_, const void *b_) noexcept;
  void Remove(const void *a_, const void *b_) noexcept;
  bool Contains(const void *a_, const void *b_) const noexcept;
  void Clear(void) noexcept;

 private:
  EqualitySet &operator=(const EqualitySet &) = delete;

  class Impl;
  std::unique_ptr<Impl> impl;
};

}  // namespace hyde
