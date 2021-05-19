// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#include <drlojekyll/Runtime/SlabList.h>

#ifndef _WIN32
#  include <sys/mman.h>
#endif

#include <cassert>

#include "Slab.h"
#include "SlabManager.h"

namespace hyde {
namespace rt {

UnsafeSlabListWriter::UnsafeSlabListWriter(SlabManager &manager_,
                                           SlabList &buffer, bool is_persistent)
    : UnsafeByteWriter(nullptr),
      manager(manager_),
      last_ptr(&(buffer.last)) {

  Slab *slab = *last_ptr;

  // If the slab list is empty, then the expectation of creating a writer is
  // that we intend to write to the slab list, so immediately create the first
  // slab.
  if (!slab) {
    slab = new (manager, is_persistent) Slab(manager, is_persistent);
    buffer.first = slab;
    write_ptr = slab->Begin();

  // We want to continue on where we left off.
  } else {
    assert(slab->IsPersistent() <= is_persistent);
    write_ptr = slab->LogicalEnd();
  }

  *last_ptr = slab;
  max_write_ptr = slab->End();
}


HYDE_RT_FLATTEN void UnsafeSlabListWriter::UpdateSlabSize(void) {
  const auto slab = Slab::Containing(write_ptr - 1u);
  const auto used_size = static_cast<uint32_t>(write_ptr - slab->Begin());
  assert(used_size <= kSlabDataSize);
  assert(used_size > slab->Size());
  slab->header.num_used_bytes.store(used_size, std::memory_order_release);
}

HYDE_RT_FLATTEN void UnsafeSlabListWriter::UpdateWritePointer(void) {
  UpdateSlabSize();
  const auto last_slab = *last_ptr;
  assert(nullptr != last_slab);

  const auto is_persistent = last_slab->IsPersistent();

#ifndef _WIN32
  if (is_persistent) {
    msync(last_slab, sizeof(Slab), MS_ASYNC);
  }
#endif

  const auto slab = new (manager, is_persistent) Slab(manager, is_persistent);
  last_slab->SetNext(slab);
  *last_ptr = slab;
  write_ptr = slab->Begin();
  max_write_ptr = slab->End();
}

UnsafeSlabListReader::UnsafeSlabListReader(SlabList slab_list) noexcept {
  if (auto slab = slab_list.first) {
    const auto num_used_bytes = slab->Size();
    assert(num_used_bytes <= kSlabDataSize);
    read_ptr = slab->Begin();
    max_read_ptr = &(read_ptr[num_used_bytes]);
  } else {
    read_ptr = nullptr;
    max_read_ptr = nullptr;
  }
}

UnsafeSlabListReader::UnsafeSlabListReader(
    uint8_t *ref_read_ptr, uint32_t /* ref_num_bytes  */) noexcept {
  const auto slab = Slab::Containing(ref_read_ptr - 1u);
  read_ptr = ref_read_ptr;
  max_read_ptr = slab->LogicalEnd();
  assert(slab->Begin() <= ref_read_ptr);
  assert(ref_read_ptr <= max_read_ptr);

  //  max_read_ptr = &(read_ptr[ref_num_bytes]);
  //  if (max_read_ptr > slab->End()) {
  //    max_read_ptr = slab->LogicalEnd();
  //  }
}

HYDE_RT_FLATTEN bool UnsafeSlabListReader::UpdateReadPointer(void) noexcept {
  if (HYDE_RT_UNLIKELY(!read_ptr)) {
    return false;
  }

  auto slab = Slab::Containing(max_read_ptr - 1u);
  auto new_max_read_ptr = slab->LogicalEnd();

  // This slab was extended during iteration, so update ourselves to its new
  // ending position.
  if (max_read_ptr < new_max_read_ptr) {
    max_read_ptr = new_max_read_ptr;
    return true;

  // We're at the end of the slab for this reader.
  } else {

    // There is a next slab, we can jump to it.
    slab = slab->Next();
    if (slab) {
      read_ptr = slab->Begin();
      max_read_ptr = slab->LogicalEnd();
      return true;

    } else {
      read_ptr = max_read_ptr;
      return false;
    }
  }
}

void SlabListWriter::SkipSlow(uint32_t num_bytes) {
  const auto diff = static_cast<uint32_t>(max_write_ptr - write_ptr);
  write_ptr = const_cast<uint8_t *>(max_write_ptr);
  num_bytes -= diff;
  UpdateWritePointer();
  if (num_bytes) {
    Skip(num_bytes);
  }
}

HYDE_RT_FLATTEN
void SlabListReader::SkipSlow(uint32_t num_bytes) {
  const auto diff = static_cast<uint32_t>(max_read_ptr - read_ptr);
  if (diff) {
    read_ptr = max_read_ptr;
    num_bytes -= diff;
    UpdateReadPointer();
    if (num_bytes) {
      Skip(num_bytes);
    }
  } else if (UpdateReadPointer()) {
    Skip(num_bytes);
  }
}

}  // namespace rt
}  // namespace hyde
