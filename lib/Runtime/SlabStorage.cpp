// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#include <drlojekyll/Runtime/SlabStorage.h>

#include <cassert>

#include "Slab.h"

namespace hyde {
namespace rt {

SlabStorage::SlabStorage(SlabManagerPtr manager_)
    : manager(std::move(manager_)),
      super_block(*manager, 0u  /* worker_id */) {}

SlabStorage::~SlabStorage(void) {}

SlabList SlabStorage::GetTableSlabs(unsigned id) const noexcept {
  for (auto [found_id, first_slab_ptr, last_slab_ptr] : super_block) {
    if (found_id == id) {
      return {std::move(first_slab_ptr), std::move(last_slab_ptr)};
    }
  }

  return {nullptr, nullptr};
}

void SlabStorage::PutTableSlabs(unsigned id, const SlabList &list) noexcept {
  if (!list.first) {
    return;
  }

  for (auto [found_id, first_slab_ptr, last_slab_ptr] : super_block) {
    if (found_id == id) {
      assert(first_slab_ptr == list.first);
      last_slab_ptr = list.last;
      return;
    }
  }

  super_block.Add(id, list.first, list.last);
}

}  // namespace rt
}  // namespace hyde
