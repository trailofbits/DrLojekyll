// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#include "Slab.h"

#include <cstdio>
#include <cstdlib>

#include "SlabManager.h"

namespace hyde {
namespace rt {

void *Slab::operator new(size_t, SlabManager &manager) noexcept {
  Slab *ret_slab = nullptr;
  if (manager.has_free_slab_heads.load(std::memory_order_acquire)) {

    std::unique_lock<std::mutex> locker(manager.maybe_free_slabs_lock);

    unsigned to_remove = 0u;
    for (size_t max_i = manager.maybe_free_slabs.size(), i = 0; i < max_i; ++i) {

      auto &found_slab = manager.maybe_free_slabs[i];

      // This slot has a null entry, schedule it for removal.
      if (!found_slab) {
        ++to_remove;
        std::swap(found_slab, manager.maybe_free_slabs[max_i - to_remove]);
        continue;
      }

      // This slab is still referenced.
      if (found_slab->IsReferenced()) {
        continue;
      }

      ret_slab = found_slab;
      found_slab = ret_slab->header.u.next;

      // `ret_slab->header.next` is null, schedule it for removal.
      if (!found_slab) {
        ++to_remove;
        std::swap(found_slab, manager.maybe_free_slabs[max_i - to_remove]);
      }
      break;
    }

    // Clean up our list.
    if (to_remove) {
      while (!manager.maybe_free_slabs.back()) {
        manager.maybe_free_slabs.pop_back();
      }
      manager.has_free_slab_heads.store(
          !manager.maybe_free_slabs.empty(), std::memory_order_release);
    }
  }

  // We have nothing in a free list, so go and allocate some new memory.
  if (!ret_slab) {
    void *ptr = nullptr;
    if (posix_memalign(&ptr, k2MiB, k2MiB)) {
      perror("Failed to perform 2 MiB aligned allocation");
      abort();
    }

    ret_slab = reinterpret_cast<Slab *>(ptr);
    std::unique_lock<std::mutex> locker(manager.all_slabs_lock);
    manager.all_slabs.emplace_back(ret_slab);
  }

  ret_slab->header.u.next = nullptr;
  return ret_slab;
}

void Slab::operator delete(void *ptr) noexcept {
  free(ptr);
}

}  // namespace rt
}  // namespace hyde
