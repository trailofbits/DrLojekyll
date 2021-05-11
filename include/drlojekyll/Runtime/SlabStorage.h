// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include "SlabManager.h"
#include "SlabVector.h"

namespace hyde {
namespace rt {

class SlabStorage {
 public:
  SlabStorage(SlabManagerPtr manager_);

  ~SlabStorage(void);

 private:
  template <unsigned>
  friend class SlabTable;

  friend class SlabTableBase;
  friend class SlabVector;

  SlabList GetTableSlabs(unsigned id) const noexcept;

  void PutTableSlabs(unsigned id, const SlabList &list) noexcept;

  SlabManagerPtr manager;

  // Triples of (id, first slab, last slab).
  PersistentTypedSlabVector<unsigned, Slab *, Mutable<Slab *>> super_block;
};

}  // namespace rt
}  // namespace hyde
