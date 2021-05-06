// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <cstddef>
#include <memory>

namespace hyde {
namespace rt {

class SlabStorage;

void ShutDownSlabStorage(SlabStorage *);

}  // namespace rt
}  // namespace hyde
namespace std {
template <>
struct default_delete<::hyde::rt::SlabStorage> {
  inline void operator()(::hyde::rt::SlabStorage *ptr) const noexcept {
    ::hyde::rt::ShutDownSlabStorage(ptr);
  }
};
}  // namespace std
namespace hyde {
namespace rt {

// Create a new slab storage engine.
std::unique_ptr<SlabStorage> CreateSlabStorage(unsigned num_workers=1u);

struct SlabStats {
  size_t num_allocated_slabs{0};
  size_t num_free_slabs{0};
  size_t num_open_slabs{0};
};

// Perform garbage collection. Mostly useful for testing purposes.
SlabStats GarbageCollect(SlabStorage &storage);

}  // namespace rt
}  // namespace hyde
