// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#include "SlabStorage.h"

#include "Slab.h"

namespace hyde {
namespace rt {

std::unique_ptr<SlabStorage> CreateSlabStorage(unsigned num_workers) {
  return std::make_unique<SlabStorage>(num_workers);
}

void ShutDownSlabStorage(SlabStorage *ptr) {
  delete ptr;
}

SlabStats GarbageCollect(SlabStorage &storage) {

  SlabStats stats;

  auto count_num_used = [&] (Slab *slab) {
    for (; slab; slab = slab->header.u.next) {
      stats.num_open_slabs += 1u;
    }
  };

  auto count_num_free = [&] (Slab *slab) {
    for (; slab; slab = slab->header.u.next) {
      if (slab->IsReferenced()) {
        count_num_used(slab);
        break;
      } else {
        stats.num_free_slabs += 1u;
      }
    }
  };

  std::scoped_lock locker(storage.manager.all_slabs_lock,
                          storage.manager.maybe_free_slabs_lock);

  stats.num_allocated_slabs = storage.manager.all_slabs.size();
  for (auto slab : storage.manager.maybe_free_slabs) {
    count_num_free(slab);
  }

  return stats;
}

}  // namespace rt
}  // namespace hyde
