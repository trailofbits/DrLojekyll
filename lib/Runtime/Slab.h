// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <drlojekyll/Runtime/Slab.h>

#include <atomic>
#include <cstdint>

namespace hyde {
namespace rt {

class SlabManager;

// A slab is an aligned region in memory holding a byte array containing
// serialized data. We rely on a slab's alignment in memory to be able to
// find a slab head given an internal pointer to that slab. Slabs also have
// an accompanying reference count, which corresponds to the number of open
// references to internal areas in the slab.
class Slab {
 public:
  struct {
    // Next slab in either a maybe-free list, or in a discontiguous list for
    // a vector.
    union {
      Slab *next{nullptr};
      uint64_t padding;
    } u;

    // Reference count on how many concurrent users might have this slab open.
    // If we have reference to some data in a slab, then that slab, and any
    // slabs that follow from `u.next`, are considered to be alive.
    //
    // NOTE(pag): The ref count is not incremented/decremented for vectors, but
    //            instead for references extracted from vectors.
    std::atomic<uint64_t> ref_count{0};

    // How many bytes in this slab have been used?
    std::atomic<uint32_t> num_used_bytes{0};

  } header;

  // The raw data backing this slab.
  uint8_t data[kSlabSize - sizeof(header)];

  inline bool IsReferenced(
      std::memory_order order=std::memory_order_acquire) const noexcept {
    return 0u < header.ref_count.load(order);
  }

  inline void IncRef(
      std::memory_order order=std::memory_order_release) noexcept {
    header.ref_count.fetch_add(1u, order);
  }

  inline void DefRef(
      std::memory_order order=std::memory_order_release) noexcept {
    header.ref_count.fetch_sub(1u, order);
  }

  inline uint32_t Size(
      std::memory_order order=std::memory_order_acquire) const noexcept {
    return header.num_used_bytes.load(order);
  }

  inline const uint8_t *Begin(void) const {
    return &(data[0]);
  }

  inline uint8_t *Begin(void) {
    return &(data[0]);
  }

  inline const uint8_t *End(void) const {
    return &(data[sizeof(data)]);
  }

  // Compute the address of a `Slab` given an address inside of the `Slab`.
  // We rely on slabs being aligned, and enforce this using `posix_memalign`.
  static inline Slab *Containing(const void *ptr) noexcept {
    auto addr = reinterpret_cast<uintptr_t>(ptr);
    auto slab_addr = addr & ~(kSlabSize - 1ull);
    return reinterpret_cast<Slab *>(slab_addr);
  }

  static void *operator new(size_t, SlabManager &manager) noexcept;
  static void operator delete(void *ptr) noexcept;
};

static_assert(sizeof(Slab) == kSlabSize);
static_assert(__builtin_offsetof(Slab, header.u.next) == 0u);
static_assert(__builtin_offsetof(Slab, header.u.padding) == 0u);
static_assert(__builtin_offsetof(Slab, header.ref_count) == 8u);
static_assert(__builtin_offsetof(Slab, header.num_used_bytes) == 16u);

}  // namespace rt
}  // namespace hyde
