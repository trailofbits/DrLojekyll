// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#include "SlabManager.h"

#include "Slab.h"

namespace hyde {
namespace rt {

SlabManager::SlabManager(unsigned num_workers_)
    : num_workers(num_workers_),
      has_free_slab_heads(false) {
  all_slabs.reserve(4096u);
  maybe_free_slabs.reserve(4096u);
}

SlabManager::~SlabManager(void) {
  {
    std::unique_lock<std::mutex> locker(maybe_free_slabs_lock);
    maybe_free_slabs.clear();
  }
  {
    std::unique_lock<std::mutex> locker(all_slabs_lock);
    all_slabs.clear();
  }
}

}  // namespace rt
}  // namespace hyde
