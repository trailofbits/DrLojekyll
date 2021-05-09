// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

namespace hyde {
namespace rt {

// Represents an opaque pointer. The serialization format of a pointer is as
// a displacement from its storage location.
class SlabPointer {
 protected:
  void *data;
};

template <>
static constexpr bool kHasTrivialFixedSizeSerialization<SlabPointer> = true;

template <>
static constexpr size_t kFixedSerializationSize<SlabPointer> = 8u;



// Methods to overload for serializing data
template <SlabListReader, typename Writer, typename DataT>
struct Serializer;

// Slab pointers are treated like plain old values.
template <>
class TypedSlabReference<SlabPointer> {
 public:
  explicit TypedSlabReference(const uint8_t *read_ptr_,
                              uint32_t num_bytes_) noexcept;
};

}  // namespace rt
}  // namespace hyde
