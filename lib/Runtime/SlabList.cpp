// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#include <drlojekyll/Runtime/SlabList.h>

#include <cassert>

#include "Slab.h"
#include "SlabStorage.h"

namespace hyde {
namespace rt {

UnsafeSlabListWriter::UnsafeSlabListWriter(
    SlabStorage &manager_, SlabList &buffer)
    : manager(manager_),
      last_ptr(&(buffer.last)) {

  Slab *slab = nullptr;

  // If the slab list is empty, then the expectation of creating a writer is
  // that we intend to write to the slab list, so immediately create the first
  // slab.
  if (!buffer.first) {
    slab = new (manager) Slab;
    buffer.first = slab;
    write_ptr = slab->Begin();

  // We want to continue on where we left off.
  } else {
    slab = buffer.last;
    write_ptr = &(slab->data[slab->Size()]);
  }

  *last_ptr = slab;
  last_next_ptr = &(slab->header.u.next);
  max_write_ptr = slab->End();
}


HYDE_RT_FLATTEN void UnsafeSlabListWriter::UpdateSlabSize(void) {
  const auto slab = Slab::Containing(write_ptr - 1u);
  const auto used_size = static_cast<uint32_t>(write_ptr - slab->Begin());
  assert(used_size <= sizeof(Slab().data));
  assert(used_size > slab->Size());
  slab->header.num_used_bytes.store(used_size, std::memory_order_release);
}

HYDE_RT_FLATTEN void UnsafeSlabListWriter::UpdateWritePointer(void) {
  UpdateSlabSize();
  const auto slab = new (manager) Slab;
  *last_next_ptr = slab;
  *last_ptr = slab;
  last_next_ptr = &(slab->header.u.next);
  write_ptr = slab->Begin();
  max_write_ptr = slab->End();
}

HYDE_RT_FLATTEN UnsafeSlabListReader::UnsafeSlabListReader(
    SlabList slab_list) noexcept {
  if (auto slab = slab_list.first) {
    const auto num_used_bytes = slab->Size();
    assert(num_used_bytes <= sizeof(Slab().data));
    read_ptr = slab->Begin();
    max_read_ptr = &(read_ptr[num_used_bytes]);
  } else {
    read_ptr = nullptr;
    max_read_ptr = nullptr;
  }
}

HYDE_RT_FLATTEN UnsafeSlabListReader::UnsafeSlabListReader(
    const uint8_t *ref_read_ptr, uint32_t ref_num_bytes) noexcept {
  const auto slab = Slab::Containing(ref_read_ptr - 1u);
  read_ptr = ref_read_ptr;
  assert(slab->Begin() <= ref_read_ptr);
  assert(ref_read_ptr < slab->End());
  max_read_ptr = &(read_ptr[ref_num_bytes]);
  if (max_read_ptr > slab->End()) {
    max_read_ptr = &(slab->data[slab->Size()]);
  }
}

HYDE_RT_FLATTEN void UnsafeSlabListReader::UpdateReadPointer(void) noexcept {
  auto slab_ptr = Slab::Containing(max_read_ptr - 1u);
  auto num_bytes = static_cast<uint32_t>(max_read_ptr - slab_ptr->Begin());
  auto new_num_bytes = slab_ptr->Size();

  // This slab was extended during iteration, so update ourselves to its new
  // ending position.
  if (num_bytes < new_num_bytes) {
    max_read_ptr = &(slab_ptr->data[new_num_bytes]);

  // We're at the end of the slab for this reader.
  } else {

    // There is a next slab, we can jump to it.
    slab_ptr = slab_ptr->header.u.next;
    if (slab_ptr) {
      new_num_bytes = slab_ptr->Size();
      read_ptr = slab_ptr->Begin();
      max_read_ptr = &(read_ptr[new_num_bytes]);

    } else {
      read_ptr = max_read_ptr;
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
  read_ptr = max_read_ptr;
  num_bytes -= diff;
  UpdateReadPointer();
  if (num_bytes) {
    Skip(num_bytes);
  }
}

}  // namespace rt
}  // namespace hyde
