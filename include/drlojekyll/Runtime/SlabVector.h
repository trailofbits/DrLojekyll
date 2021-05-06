// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <tuple>
#include <utility>

#include "SlabTuple.h"

namespace hyde {
namespace rt {

class SlabVectorIteratorEnd {};

// An append-only vector of serialized bytes, implemented using a `SlabList`.
// What distinguishes a `SlabList` from a `SlabVector` is that a `SlabVector`
// has actual lifetime semantics. That is, when a slab vector is destroyed, it
// is responsible for freeing the backing slabs.
class SlabVector : public SlabList {
 public:
  SlabVector(SlabStorage &storage_, unsigned worker_id_);

  SlabVector(SlabVector &&that) noexcept
      : SlabList(std::forward<SlabList>(that)),
        storage(that.storage),
        worker_id(that.worker_id) {}

  HYDE_RT_ALWAYS_INLINE ~SlabVector(void) {
    Clear();
  }

  void Clear(void);

  SlabStorage &storage;
  const unsigned worker_id;

 private:
  // We don't permit copying of slab vectors, only moving. That way the
  // ownership of the backing slab lists does not get confusing.
  SlabVector(const SlabVector &) = delete;
  SlabVector &operator=(const SlabVector &) = delete;
};

template <typename T, typename... Ts>
class TypedSlabVectorVectorIterator {
 public:
  HYDE_RT_ALWAYS_INLINE
  TypedSlabVectorVectorIterator(const SlabList &slab_list_)
      : reader(slab_list_) {}

  using SelfType = TypedSlabVectorVectorIterator<T, Ts...>;

  HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE
  bool operator!=(SlabVectorIteratorEnd) noexcept {
    if (HYDE_RT_LIKELY(reader.SoftHasMore())) {
      return true;
    } else {
      return reader.HardHasMore();
    }
  }

  HYDE_RT_ALWAYS_INLINE void operator++(void) const noexcept {}

  using CountingReader = ByteCountingReader<SlabListReader>;

  HYDE_RT_FLATTEN SlabTuple<T, Ts...> operator*(void) {
    reader.num_bytes = 0u;
    return Read<0>();
  }

 private:

  using IndexT = std::pair<const uint8_t *, uint32_t>;

  template <size_t kIndex, typename... Indices>
  [[gnu::hot]] HYDE_RT_FLATTEN
  SlabTuple<T, Ts...> Read(Indices... indices) {
    using E = std::tuple_element_t<kIndex, std::tuple<T, Ts...>>;

    const uint8_t * const elem_read_ptr = reader.read_ptr;

    alignas(E) char dummy_data[sizeof(E)];
    Serializer<CountingReader, NullWriter, E>::Read(
        reader, *reinterpret_cast<E *>(dummy_data));

    const uint32_t elem_size = static_cast<uint32_t>(reader.num_bytes);
    reader.num_bytes = 0;

    if constexpr (kIndex < sizeof...(Ts)) {
      return Read<kIndex + 1u, Indices..., IndexT>(
          indices...,
          IndexT{elem_read_ptr, elem_size});
    } else {
      return SlabTuple<T, Ts...>(
          indices...,
          IndexT{elem_read_ptr, elem_size});
    }
  }

  ByteCountingReader<SlabListReader> reader;
};

// An append-only vector of serialiazed tuples of type `Ts`.
template <typename T, typename... Ts>
class TypedSlabVector : public SlabVector {
 public:
  using SlabVector::SlabVector;

//  // Add a single serialized element
//  HYDE_RT_FLATTEN void Add(Ts... ts) noexcept {
//    using Tuple = std::tuple<Ts...>;
//    const Tuple row(std::move(ts)...);
//    ByteCountingWriter counter;
//    Serializer<NullReader, ByteCountingWriter, Tuple>::WriteValue(counter, row);
//    SlabListWriter writer(manager, *this);
//    if (HYDE_RT_LIKELY(writer.CanWriteUnsafely(counter.num_bytes))) {
//      Serializer<NullReader, UnsafeSlabListWriter, Tuple>::WriteValue(
//          writer, row);
//    } else {
//      Serializer<NullReader, SlabListWriter, Tuple>::WriteValue(writer, row);
//    }
//  }

  HYDE_RT_ALWAYS_INLINE
  TypedSlabVectorVectorIterator<T, Ts...> begin(void) const {
    return TypedSlabVectorVectorIterator<T, Ts...>(*this);
  }

  HYDE_RT_ALWAYS_INLINE
  SlabVectorIteratorEnd end(void) const {
    return {};
  }

  template <typename InputT, typename... InputTs>
  HYDE_RT_FLATTEN void Add(const InputT &t, const InputTs&... ts) noexcept {
    static_assert(sizeof...(Ts) == sizeof...(InputTs));

    SlabListWriter writer(storage, *this);
    AddImpl<0, SlabListWriter, InputT, InputTs...>(writer, t, ts...);
  }

 protected:
  template <size_t kIndex, typename Writer, typename InputT,
            typename... InputTs>
  void AddImpl(Writer &writer, const InputT &elem,
               const InputTs &... elems) noexcept {

    using Nth =
        typename std::tuple_element_t<kIndex, std::tuple<T, Ts...>>;

    // It's a value, so write it.
    if constexpr (std::is_same_v<InputT, Nth>) {
      Serializer<NullReader, Writer, InputT>::Write(writer, elem);

    // It's a reference to something we've already serialized.
    } else if constexpr (std::is_same_v<InputT, TypedSlabReference<Nth>>) {
      Serializer<NullReader, Writer, SlabReference>::Write(
          writer, elem);

    } else if constexpr (std::is_constructible_v<Nth, InputT>) {
      Nth real_elem(elem);
      Serializer<NullReader, Writer, Nth>::Write(writer, real_elem);

    } else {
      __builtin_unreachable();
    }

    if constexpr (0u < sizeof...(InputTs)) {
      AddImpl<kIndex + 1u, Writer, InputTs...>(writer, elems...);
    }
  }
};

// An append-only vector of serialiazed tuples of type `Ts`. This variant
// is "persistent", i.e. data added is persistently stored.
template <typename T, typename... Ts>
class PersistentTypedSlabVector : public TypedSlabVector<T, Ts...> {
 public:
  template <typename InputT, typename... InputTs>
  HYDE_RT_FLATTEN void Add(const InputT &t, const InputTs&... ts) noexcept {
    static_assert(sizeof...(Ts) == sizeof...(InputTs));

    using Parent = TypedSlabVector<T, Ts...>;

    SlabListWriter writer(this->Parent::storage,
                          *this, true  /* is_persistent */);
    this->Parent::template AddImpl<0, SlabListWriter, InputT, InputTs...>(
        writer, t, ts...);
  }
};

}  // namespace rt
}  // namespace hyde
