// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <cstddef>

#include "Util.h"

namespace hyde {
namespace rt {

enum : size_t {
  k1MiB = 1ull << 20ull,
  k2MiB = k1MiB * 2ull,
  kSlabSize = k2MiB
};

class Slab;

void LockSlab(void *ptr, uint32_t num_bytes) noexcept;
void UnlockSlab(void *ptr, uint32_t num_bytes) noexcept;

template <typename T>
class SlabLocker {
 public:
  HYDE_RT_ALWAYS_INLINE SlabLocker(void *, uint32_t) {}
};

template <typename T>
class SlabLocker<Mutable<T>> {
 public:
  HYDE_RT_ALWAYS_INLINE SlabLocker(void *ptr_, uint32_t num_bytes_)
      : ptr(ptr_),
        num_bytes(num_bytes_) {
    LockSlab(ptr_, num_bytes_);
  }

  HYDE_RT_ALWAYS_INLINE ~SlabLocker(void) {
    UnlockSlab(ptr, num_bytes);
  }

 private:
  void *const ptr;
  const uint32_t num_bytes;
};

}  // namespace rt
}  // namespace hyde
