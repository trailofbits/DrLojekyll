// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <drlojekyll/Runtime/SlabStorage.h>

#include <atomic>
#include <mutex>
#include <vector>

#include "Slab.h"

namespace hyde {
namespace rt {

class SlabStorage {
 public:
  ~SlabStorage(void);

  SlabStorage(unsigned num_workers_, int fd_, uint64_t file_size_,
              void *base_, uint64_t max_size_);

  // Number of worker threads permitted.
  const unsigned num_workers;

  // If this is a file-backed slab store, then the `fd` will be something other
  // than `-1`.
  const int fd;
  const uint64_t base_file_size;
  std::atomic<uint64_t> current_file_size;

  uint8_t * const base;
  const uint64_t max_size;

  // List of all allocated slabs of memory.
  std::mutex all_slabs_lock;
  std::vector<std::unique_ptr<Slab>> all_slabs;

  // List of possibly free slabs of memory.
  std::mutex maybe_free_slabs_lock;
  std::vector<Slab *> maybe_free_slabs;
  std::atomic<bool> has_free_slab_heads;

 private:
  SlabStorage(const SlabStorage &) = delete;
  SlabStorage(SlabStorage &&) noexcept = delete;
  SlabStorage &operator=(const SlabStorage &) = delete;
  SlabStorage &operator=(SlabStorage &&) noexcept = delete;
  SlabStorage(void) = delete;
};

}  // namespace rt
}  // namespace hyde
