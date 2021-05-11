// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <tuple>
#include <utility>

#include "SlabReference.h"

namespace hyde {
namespace rt {

struct RawReference {
  uint8_t *data;
  uint32_t num_bytes;
  uint32_t hash;
};

// A slab tuple is like a slab reference, except that it doesn't do any
// reference counting.
template <typename... Ts>
class SlabTuple {
 public:
  template <typename... Offsets>
  HYDE_RT_ALWAYS_INLINE SlabTuple(Offsets... elems_) noexcept
      : elems{elems_...} {}

  template <size_t kIndex>
  auto get(void) const noexcept {
    using T = std::tuple_element_t<kIndex, std::tuple<Ts...>>;
    return ::hyde::rt::TypedSlabReference<T>(
        elems[kIndex].data, elems[kIndex].num_bytes, elems[kIndex].hash);
  }

  const RawReference elems[sizeof...(Ts)];
};

using ByteCountingSlabListReader = ByteCountingReader<SlabListReader>;

template <typename T, typename... Ts>
class TupleBuilder {
 public:
  HYDE_RT_ALWAYS_INLINE explicit TupleBuilder(const SlabList &slab_list_)
      : reader(slab_list_) {}

  HYDE_RT_ALWAYS_INLINE
  explicit TupleBuilder(uint8_t *read_ptr_, uint32_t num_bytes_)
      : reader(read_ptr_, num_bytes_) {}

  [[gnu::hot]] HYDE_RT_FLATTEN SlabTuple<T, Ts...> Build(void) {
    return BuildImpl<0>();
  }

 private:

  template <size_t kIndex, typename... Indices>
  SlabTuple<T, Ts...> BuildImpl(Indices... indices) {
    using E = std::tuple_element_t<kIndex, std::tuple<T, Ts...>>;
    using ValT = typename ValueType<E>::Type;

    uint8_t *const elem_read_ptr = reader.read_ptr;

    alignas(ValT) char dummy_data[sizeof(ValT)];
    Serializer<ByteCountingSlabListReader, NullWriter, ValT>::Read(
        reader, *reinterpret_cast<ValT *>(dummy_data));

    const uint32_t elem_size = static_cast<uint32_t>(reader.num_bytes);
    reader.num_bytes = 0;

    // TODO(pag): Hashing the bytes.

    if constexpr (kIndex < sizeof...(Ts)) {
      return BuildImpl<kIndex + 1u, Indices..., RawReference>(
          indices..., RawReference{elem_read_ptr, elem_size, 0u});
    } else {
      return SlabTuple<T, Ts...>(indices...,
                                 RawReference{elem_read_ptr, elem_size, 0u});
    }
  }

 protected:

  ByteCountingSlabListReader reader;
};

}  // namespace rt
}  // namespace hyde
namespace std {

template <typename... Ts>
struct tuple_size<::hyde::rt::SlabTuple<Ts...>> {
 public:
  static constexpr auto value = sizeof...(Ts);
};

template <size_t kIndex, typename... Ts>
struct tuple_element<kIndex, ::hyde::rt::SlabTuple<Ts...>> {
 public:
  using T = std::tuple_element_t<kIndex, std::tuple<Ts...>>;
  using type = ::hyde::rt::TypedSlabReference<T>;
};

template <size_t kIndex, typename... Ts>
HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE static auto
get(const ::hyde::rt::SlabTuple<Ts...> &tuple) noexcept {
  return tuple.template get<kIndex>();
}

}  // namespace std
