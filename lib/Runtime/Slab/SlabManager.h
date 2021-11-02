// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <drlojekyll/Runtime/SlabManager.h>

#include <atomic>
#include <mutex>
#include <vector>

#include "Slab.h"

namespace hyde {
namespace rt {

class SlabManager {
 public:
  ~SlabManager(void);

  SlabManager(unsigned num_workers_, int fd_, uint64_t file_size_,
              void *real_base_, uint64_t real_max_size_, void *base_,
              uint64_t max_size_);

  // Number of worker threads permitted.
  const unsigned num_workers;

  // If this is a file-backed slab store, then the `fd` will be something other
  // than `-1`.
  const int fd;
  const uint64_t base_file_size;

  // These may be different than below if `mmap` gave us back an address that
  // wasn't `Slab`-sized-aligned.
  void *const real_base;
  const uint64_t real_max_size;

  // The slab base address. For an in-memory slab store, this will be `nullptr`,
  // and all slab offsets will be the actual addresses of slabs. For persistent
  // slab stores, this will be the base of a file-backed `mmap`, and all slab
  // offsets will either
  Slab *const base;
  const uint64_t max_size;

  std::mutex file_size_lock;
  uint64_t file_size;

  // List of all allocated slabs of memory.
  std::mutex all_slabs_lock;
  std::vector<std::unique_ptr<Slab>> all_slabs;

  // List of possibly free slabs of memory.
  std::mutex maybe_free_slabs_lock;
  std::vector<Slab *> maybe_free_slabs;
  std::atomic<bool> has_free_slab_heads;

  // Allocate an ephemeral slab.
  void *AllocateEphemeralSlab(void);

  // Allocate a persistent slab.
  void *AllocatePersistentSlab(void);

 private:
  SlabManager(const SlabManager &) = delete;
  SlabManager(SlabManager &&) noexcept = delete;
  SlabManager &operator=(const SlabManager &) = delete;
  SlabManager &operator=(SlabManager &&) noexcept = delete;
  SlabManager(void) = delete;
};

}  // namespace rt
}  // namespace hyde