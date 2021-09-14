// Copyright 2019, Trail of Bits. All rights reserved.

#pragma once

#include <cstdint>
#include <type_traits>

namespace hyde {

template <typename PublicT, typename PrivateT>
class Node {
 public:
  using PublicType = PublicT;
  using PrivateType = PrivateT;

  inline Node(PrivateT *impl_) : impl(impl_) {}

  inline bool operator==(const PublicT &that) const noexcept {
    return impl == that.impl;
  }

  inline bool operator!=(const PublicT &that) const noexcept {
    return impl != that.impl;
  }

  inline bool operator<(const PublicT &that) const noexcept {
    return impl < that.impl;
  }

  uintptr_t UniqueId(void) const noexcept {
    return reinterpret_cast<uintptr_t>(impl);
  }

  inline uint64_t Hash(void) const {
    return reinterpret_cast<uintptr_t>(impl);
  }

  PrivateT *impl;
};

}  // namespace hyde
