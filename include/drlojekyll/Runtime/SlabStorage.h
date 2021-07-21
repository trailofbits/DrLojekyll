// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <utility>

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

  std::pair<SlabList, uint64_t> GetTableSlabs(unsigned id) const noexcept;

  void PutTableSlabs(unsigned id, const SlabList &list,
                     uint64_t num_rows) noexcept;

  SlabManagerPtr manager;

  // Triples of (id, first slab, last slab).
  PersistentTypedSlabVector<
      unsigned  /* Table ID */,
      Slab *  /* First slab */,
      Mutable<Slab *>  /* Last slab */,
      Mutable<uint64_t>  /* Number of rows */> super_block;
};

}  // namespace rt
}  // namespace hyde
