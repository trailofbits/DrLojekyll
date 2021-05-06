// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <memory>
#include <mutex>
#include <vector>

namespace hyde {
namespace rt {

class Slab;
class SlabVector;

// Manages slabs of memory.
class SlabManager {
 public:
  explicit SlabManager(unsigned num_workers_);
  ~SlabManager(void);

  // Number of worker threads.
  const unsigned num_workers;

  // List of all allocated slabs of memory.
  std::mutex all_slabs_lock;
  std::vector<std::unique_ptr<Slab>> all_slabs;

  // List of possibly free slabs of memory.
  std::mutex maybe_free_slabs_lock;
  std::vector<Slab *> maybe_free_slabs;
  std::atomic<bool> has_free_slab_heads;

 private:
  SlabManager(void) = delete;
};

}  // namespace rt
}  // namespace hyde
