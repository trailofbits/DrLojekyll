// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#include <drlojekyll/Runtime/SlabReference.h>

#include "Slab.h"
#include "SlabManager.h"

namespace hyde {
namespace rt {

HYDE_RT_FLATTEN
SlabReference::SlabReference(uint8_t *data, uint32_t, uint32_t hash) noexcept {
  u.opaque = 0;
  if (HYDE_RT_LIKELY(data)) {
    u.p.data_addr = reinterpret_cast<intptr_t>(data);
    u.p.hash = static_cast<uint16_t>(hash);
    Slab::Containing(data)->IncRef();
  }
}

HYDE_RT_FLATTEN
SlabReference::SlabReference(const SlabReference &that) noexcept {
  u.opaque = that.u.opaque;
  if (HYDE_RT_LIKELY(auto data = Data())) {
    Slab::Containing(data)->IncRef();
  }
}

HYDE_RT_FLATTEN
SlabReference &SlabReference::operator=(SlabReference &&that) noexcept {
  if (auto data = Data()) {
    Slab::Containing(data)->DecRef();
  }

  u.opaque = that.u.opaque;
  that.u.opaque = 0u;
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

  u.opaque = that.u.opaque;
  return *this;
}

HYDE_RT_FLATTEN
void SlabReference::Clear(void) noexcept {
  if (uint8_t *const data = Data()) {
    Slab::Containing(data)->DecRef();
    u.opaque = 0;
  }
}

HYDE_RT_FLATTEN
SizedSlabReference &SizedSlabReference::operator=(
    SizedSlabReference &&that) noexcept {
  if (auto data = Data()) {
    Slab::Containing(data)->DecRef();
  }

  u.opaque = that.u.opaque;

  num_bytes = that.num_bytes;
  hash = that.hash;

  that.u.opaque = 0;
  that.num_bytes = 0;
  that.hash = 0;
  return *this;
}

HYDE_RT_FLATTEN
SizedSlabReference &SizedSlabReference::operator=(
    const SizedSlabReference &that) noexcept {
  uint8_t *const this_data = Data();
  uint8_t *const that_data = that.Data();

  if (that_data) {
    Slab::Containing(that_data)->IncRef();
  }

  if (this_data) {
    Slab::Containing(this_data)->DecRef();
  }

  u.opaque = that.u.opaque;
  num_bytes = that.num_bytes;
  hash = that.hash;
  return *this;
}

}  // namespace rt
}  // namespace hyde
