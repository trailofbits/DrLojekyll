// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#include <drlojekyll/Runtime/SlabReference.h>

#include "Slab.h"
#include "SlabManager.h"

namespace hyde {
namespace rt {

HYDE_RT_FLATTEN
SlabReference::SlabReference(uint8_t *data, uint32_t) noexcept {
  data_ptr = data;
  if (HYDE_RT_LIKELY(data_ptr)) {
    Slab::Containing(data_ptr)->IncRef();
  }
}

HYDE_RT_FLATTEN
SlabReference::SlabReference(const SlabReference &that) noexcept {
  data_ptr = that.Data();
  if (HYDE_RT_LIKELY(data_ptr)) {
    Slab::Containing(data_ptr)->IncRef();
  }
}

HYDE_RT_FLATTEN
SlabReference &SlabReference::operator=(SlabReference &&that) noexcept {
  if (auto data = Data()) {
    Slab::Containing(data)->DecRef();
  }

  data_ptr = that.data_ptr;
  that.data_ptr = nullptr;
  return *this;
}

HYDE_RT_FLATTEN
SlabReference &SlabReference::operator=(const SlabReference &that) noexcept {
  uint8_t *const this_data = Data();
  uint8_t *const that_data = that.Data();

  if (that_data) {
    Slab::Containing(that_data)->IncRef();
  }

  if (this_data) {
    Slab::Containing(this_data)->DecRef();
  }

  //  u.opaque = that.u.opaque;
  data_ptr = that_data;
  return *this;
}

HYDE_RT_FLATTEN
void SlabReference::Clear(void) noexcept {
  if (uint8_t *const data = Data()) {
    Slab::Containing(data)->DecRef();

    //    u.opaque = 0;
    data_ptr = nullptr;
  }
}

HYDE_RT_FLATTEN
SizedSlabReference &
SizedSlabReference::operator=(SizedSlabReference &&that) noexcept {
  if (auto data = Data()) {
    Slab::Containing(data)->DecRef();
  }

  //  u.opaque = that.u.opaque;

  data_ptr = that.data_ptr;
  num_bytes = that.num_bytes;

  //  that.u.opaque = 0;
  that.data_ptr = nullptr;
  that.num_bytes = 0;
  return *this;
}

HYDE_RT_FLATTEN
SizedSlabReference &
SizedSlabReference::operator=(const SizedSlabReference &that) noexcept {
  uint8_t *const this_data = Data();
  uint8_t *const that_data = that.Data();

  if (that_data) {
    Slab::Containing(that_data)->IncRef();
  }

  if (this_data) {
    Slab::Containing(this_data)->DecRef();
  }

  //  u.opaque = that.u.opaque;
  data_ptr = that_data;
  num_bytes = that.num_bytes;

  return *this;
}

}  // namespace rt
}  // namespace hyde
