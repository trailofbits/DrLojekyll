// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#include <drlojekyll/Runtime/SlabReference.h>

#include "Slab.h"
#include "SlabManager.h"

namespace hyde {
namespace rt {

HYDE_RT_FLATTEN SlabReference::SlabReference(const uint8_t *read_ptr_,
                                             uint32_t num_bytes_) noexcept
    : read_ptr(read_ptr_),
      num_bytes(num_bytes_) {
  Slab::Containing(read_ptr_)->IncRef();
}

HYDE_RT_FLATTEN
SlabReference &SlabReference::operator=(SlabReference &&that) noexcept {
  if (auto this_read_ptr = read_ptr) {
    Slab::Containing(this_read_ptr)->DefRef();
  }

  read_ptr = that.read_ptr;
  num_bytes = that.num_bytes;

  that.read_ptr = nullptr;
  that.num_bytes = 0;
  return *this;
}

HYDE_RT_FLATTEN
SlabReference &SlabReference::operator=(const SlabReference &that) noexcept {
  const uint8_t *const this_read_ptr = read_ptr;
  const uint8_t *const that_read_ptr = that.read_ptr;

  if (that_read_ptr) {
    Slab::Containing(that_read_ptr)->IncRef();
  }

  if (this_read_ptr) {
    Slab::Containing(this_read_ptr)->DefRef();
  }

  read_ptr = that_read_ptr;
  num_bytes = that.num_bytes;
  return *this;
}

HYDE_RT_FLATTEN
void SlabReference::Clear(void) noexcept {
  if (const uint8_t *const this_read_ptr = read_ptr) {
    Slab::Containing(this_read_ptr)->DefRef();
    read_ptr = nullptr;
    num_bytes = 0;
  }
}

}  // namespace rt
}  // namespace hyde
