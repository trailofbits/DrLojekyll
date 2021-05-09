// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include "SlabManager.h"
#include "SlabVector.h"
#include "SlabTable.h"

namespace hyde {
namespace rt {

class SlabStorage : public SlabManager {
 public:
  using SlabManager::SlabManager;

  template <typename T, typename... Ts>
  PersistentTypedSlabVector<T, Ts...> GetOrCreateTable(unsigned id) {
    for (auto [found_id , first_slab_ptr, last_slab_ptr] : super_block) {
      if (found_id == id) {
        SlabList ls(first_slab_ptr, last_slab_ptr);
        return PersistentTypedSlabVector<T, Ts...>(*this, std::move(ls), 0u);
      }
    }
  }

 private:
  // Triples of (id, first slab, last slab).
  PersistentTypedSlabVector<unsigned, Slab *, Slab *> super_block;
};

}  // namespace rt
}  // namespace hyde
