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

std::pair<SlabList, uint64_t>
SlabStorage::GetTableSlabs(unsigned id) const noexcept {
  for (auto [found_id, first_slab_ptr,
             last_slab_ptr, num_rows_ref] : super_block) {
    if (found_id == id) {
      SlabList list(std::move(first_slab_ptr), std::move(last_slab_ptr));
      return {std::move(list), static_cast<uint64_t>(num_rows_ref)};
    }
  }

  return {{nullptr, nullptr}, {}};
}

void SlabStorage::PutTableSlabs(unsigned id, const SlabList &list,
                                uint64_t num_rows) noexcept {
  if (!list.first) {
    return;
  }

  for (auto [found_id, first_slab_ptr,
             last_slab_ptr, num_rows_ref] : super_block) {
    if (found_id == id) {
      assert(first_slab_ptr == list.first);
      last_slab_ptr = list.last;
      num_rows_ref = num_rows;
      return;
    }
  }

  super_block.Add(id, list.first, list.last, num_rows);
}

}  // namespace rt
}  // namespace hyde
