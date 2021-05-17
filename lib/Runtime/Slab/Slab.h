// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <drlojekyll/Runtime/Slab.h>

#include <atomic>
#include <cassert>
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
  explicit Slab(SlabManager &manager, bool is_persistent);

  static constexpr auto kShiftedNextOffsetShift = 2;

  struct {

    // Next slab in either a maybe-free list, or in a discontiguous list for
    // a vector. This is a offset that is relative to the address of this slab.
    // This works for both persistent and ephemeral slabs because persistent
    // slabs will all be contiguous, and so their offsets will all keep things
    // in the range of the persistent map, and for ephemeral maps, this will
    // just
    union {
      uint64_t opaque{0};

      struct {
        uint64_t is_persistent : 1;

        // Do we have a next offset?
        int64_t has_next : 1;

        // Slabs are always 2 MiB aligned, so the low bits of an offset will
        // always be zero. In the case of ephemeral slabs, the slabs could be
        // anywhere in memory, so we need to use an `int64_t` displacement.
        int64_t shifted_next_offset : 62;

      } __attribute__((packed)) s;
    } __attribute__((packed)) u;

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
      std::memory_order order = std::memory_order_acquire) const noexcept {
    if (header.u.s.is_persistent) {
      return true;
    } else {
      return 0u < header.ref_count.load(order);
    }
  }

  inline void
  IncRef(std::memory_order order = std::memory_order_release) noexcept {
    if (!IsPersistent()) {
      header.ref_count.fetch_add(1u, order);
    }
  }

  inline void
  DecRef(std::memory_order order = std::memory_order_release) noexcept {
    if (!IsPersistent()) {
      auto old_val = header.ref_count.fetch_sub(1u, order);
      assert(0 < old_val);
      (void) old_val;
    }
  }

  inline uint32_t
  Size(std::memory_order order = std::memory_order_acquire) const noexcept {
    return header.num_used_bytes.load(order);
  }

  inline const uint8_t *Begin(void) const noexcept {
    return &(data[0]);
  }

  inline uint8_t *Begin(void) noexcept {
    return &(data[0]);
  }

  inline const uint8_t *End(void) const noexcept {
    return &(data[sizeof(data)]);
  }

  inline uint8_t *
  LogicalEnd(std::memory_order order = std::memory_order_acquire) noexcept {
    return &(data[Size(order)]);
  }

  inline bool IsPersistent(void) const noexcept {
    return header.u.s.is_persistent;
  }

  inline Slab *Next(void) const noexcept {
    int64_t has_next_mask = static_cast<int64_t>(header.u.s.has_next);
    int64_t offset = header.u.s.shifted_next_offset <<
                     Slab::kShiftedNextOffsetShift;
    auto addr = reinterpret_cast<intptr_t>(this);
    return reinterpret_cast<Slab *>((addr + offset) & has_next_mask);
  }

  inline void SetNext(Slab *that) noexcept {
    int64_t this_addr = reinterpret_cast<intptr_t>(this);
    int64_t that_addr = reinterpret_cast<intptr_t>(that);
    int64_t diff = that_addr - this_addr;
    assert(header.u.s.is_persistent == that->header.u.s.is_persistent);
    assert(!that->header.u.s.has_next);
    assert(!header.u.s.has_next);
    header.u.s.has_next = -1;
#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Wconversion"
#endif
    header.u.s.shifted_next_offset = static_cast<int64_t>(
        diff >> Slab::kShiftedNextOffsetShift);
#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic pop
#endif
  }

  // Compute the address of a `Slab` given an address inside of the `Slab`.
  // We rely on slabs being aligned, and enforce this using `posix_memalign`.
  static inline Slab *Containing(const void *ptr) noexcept {

    // We do `- 1ull` so that we can find the slab given a pointer that is
    // equal to the slab's maximum address, i.e. one byte past the end of the
    // slab. It is convenient to be able to increment by one byte to get past
    // a `TupleState` inside of a table's vector.
    auto addr = reinterpret_cast<uintptr_t>(ptr) - 1ull;

    auto slab_addr = addr & ~(kSlabSize - 1ull);
    return reinterpret_cast<Slab *>(slab_addr);
  }

  static void *operator new(size_t, SlabManager &manager,
                            bool is_persistent) noexcept;

  static void operator delete(void *ptr) noexcept;

 private:
  Slab(void) = delete;
};

static constexpr auto kSlabDataSize =
    sizeof(reinterpret_cast<Slab *>(NULL)->data);

static_assert(sizeof(Slab) == kSlabSize);
static_assert(__builtin_offsetof(Slab, header.u) == 0u);
static_assert(__builtin_offsetof(Slab, header.u.s) == 0u);
static_assert(__builtin_offsetof(Slab, header.ref_count) == 8u);
static_assert(__builtin_offsetof(Slab, header.num_used_bytes) == 16u);

}  // namespace rt
}  // namespace hyde
