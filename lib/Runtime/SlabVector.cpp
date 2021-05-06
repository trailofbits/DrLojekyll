// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#include <drlojekyll/Runtime/SlabVector.h>

#include "SlabStorage.h"

namespace hyde {
namespace rt {

SlabVector::SlabVector(SlabStorage &storage, unsigned worker_id_)
    : SlabList(),
      manager(storage.manager),
      worker_id(worker_id_) {}

void SlabVector::Clear(void) {
  if (auto slab = first) {
    first = nullptr;
    last = nullptr;
    std::unique_lock<std::mutex> locker(manager.maybe_free_slabs_lock);
    manager.maybe_free_slabs.push_back(slab);
    manager.has_free_slab_heads.store(true, std::memory_order_release);
  }
}

}  // namespace rt
}  // namespace hyde
