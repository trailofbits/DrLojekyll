// Copyright 2021, Trail of Bits. All rights reserved.

#pragma once

#include <functional>

#include "Util.h"

namespace hyde {
namespace rt {

template <typename T>
struct InternRef {
 public:
  HYDE_RT_ALWAYS_INLINE InternRef(const T &ref_) noexcept
      : ref(const_cast<T *>(&ref_)) {}

  HYDE_RT_ALWAYS_INLINE InternRef(const T *ref_) noexcept
      : ref(const_cast<T *>(ref_)) {}

  T *ref;

  const T &operator*(void) const noexcept {
    return *ref;
  }

  const T *operator->(void) const noexcept {
    return ref;
  }

  HYDE_RT_ALWAYS_INLINE bool operator<(InternRef<T> that) const noexcept {
    return ref < that.ref;
  }

  HYDE_RT_ALWAYS_INLINE bool operator<(const T &that) const noexcept {
    return std::less<T>{}(*ref, that);
  }

  HYDE_RT_ALWAYS_INLINE bool operator==(InternRef<T> that) const noexcept {
    return ref == that.ref;
  }

  HYDE_RT_ALWAYS_INLINE bool operator==(const T &that) const noexcept {
    return std::equal_to<T>{}(*ref, that);
  }

  HYDE_RT_ALWAYS_INLINE bool operator!=(InternRef<T> that) const noexcept {
    return ref != that.ref;
  }

  HYDE_RT_ALWAYS_INLINE bool operator!=(const T &that) const noexcept {
    return !std::equal_to<T>{}(*ref, that);
  }


 private:
  InternRef(void) = delete;
};

}  // namespace rt
}  // namespace hyde
