// Copyright 2019, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/Parse/Parse.h>

#include <memory>

namespace hyde {

// An iterator for iterating over all transitively imported modules. This
// iterates in the order of which module declarations are resolved, i.e. from
// the deepest, earlier module, all the way out to the root module (last).
class ParsedModuleIterator {
 public:
  class Impl;
  class Iterator;

  explicit ParsedModuleIterator(const ParsedModule &module);

  Iterator begin(void) const;
  Iterator end(void) const;

 private:
  ParsedModuleIterator(void) = delete;

  const ParsedModule impl;
};

class ParsedModuleIterator::Iterator {
 public:
  ParsedModule operator*(void) const;

  inline bool operator!=(const Iterator &that) const {
    return index != that.index;
  }

  inline Iterator &operator++(void) {
    ++index;
    return *this;
  }

  inline Iterator operator++(int) {
    auto ret = *this;
    ++index;
    return ret;
  }

 private:
  friend class ParsedModuleIterator;

  inline Iterator(const ParsedModule &impl_, unsigned index_)
      : impl(impl_),
        index(index_) {}

  Iterator(void) = delete;

  const ParsedModule impl;
  unsigned index;
};

}  // namespace hyde
