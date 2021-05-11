// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <tuple>
#include <utility>

#include "SlabTuple.h"

namespace hyde {
namespace rt {

class SlabStorage;
class SlabVectorIteratorEnd {};

// An append-only vector of serialized bytes, implemented using a `SlabList`.
// What distinguishes a `SlabList` from a `SlabVector` is that a `SlabVector`
// has actual lifetime semantics. That is, when a slab vector is destroyed, it
// is responsible for freeing the backing slabs.
class SlabVector : public SlabList {
 public:
  SlabVector(SlabStorage &storage_, unsigned worker_id_);
  SlabVector(SlabManager &storage_, unsigned worker_id_);

  SlabVector(SlabVector &&that) noexcept
      : SlabList(std::forward<SlabList>(that)),
        storage(that.storage),
        worker_id(that.worker_id) {}

  SlabVector(SlabManager &storage_, SlabList &&that,
             unsigned worker_id_) noexcept
      : SlabList(std::forward<SlabList>(that)),
        storage(storage_),
        worker_id(worker_id_) {}

  HYDE_RT_ALWAYS_INLINE ~SlabVector(void) {
    Clear();
  }

  void Clear(void);

  SlabManager &storage;
  const unsigned worker_id;

 protected:
  // We don't permit copying of slab vectors, only moving. That way the
  // ownership of the backing slab lists does not get confusing.
  SlabVector(const SlabVector &) = delete;
  SlabVector &operator=(const SlabVector &) = delete;
};

template <typename T, typename... Ts>
class TypedSlabVectorVectorIterator : protected TupleBuilder<T, Ts...> {
 public:
  using TupleBuilder<T, Ts...>::TupleBuilder;

  using SelfType = TypedSlabVectorVectorIterator<T, Ts...>;

  HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE bool
  operator!=(SlabVectorIteratorEnd) noexcept {
    if (HYDE_RT_LIKELY(this->reader.SoftHasMore())) {
      return true;
    } else {
      return this->reader.HardHasMore();
    }
  }

  HYDE_RT_ALWAYS_INLINE void operator++(void) const noexcept {}

  using CountingReader = ByteCountingReader<SlabListReader>;

  HYDE_RT_FLATTEN SlabTuple<T, Ts...> operator*(void) {
    return this->Build();
  }
};

// An append-only vector of serialiazed tuples of type `Ts`.
template <typename T, typename... Ts>
class TypedSlabVector : public SlabVector {
 public:
  using SlabVector::SlabVector;

  using TupleT = SlabTuple<T, Ts...>;

  HYDE_RT_ALWAYS_INLINE
  TypedSlabVectorVectorIterator<T, Ts...> begin(void) const {
    return TypedSlabVectorVectorIterator<T, Ts...>(*this);
  }

  HYDE_RT_ALWAYS_INLINE
  SlabVectorIteratorEnd end(void) const {
    return {};
  }

  template <typename InputT, typename... InputTs>
  HYDE_RT_FLATTEN void Add(const InputT &t, const InputTs &...ts) noexcept {
    static_assert(sizeof...(Ts) == sizeof...(InputTs));

    SlabListWriter writer(storage, *this, false /* is_persistent */);
    AddImpl<0, SlabListWriter, InputT, InputTs...>(writer, t, ts...);
  }

 protected:
  template <size_t kIndex, typename Writer, typename InputT,
            typename... InputTs>
  uint8_t *AddImpl(Writer &writer, const InputT &elem,
                   const InputTs &...elems) noexcept {
    uint8_t *ret = nullptr;
    using Nth = typename std::tuple_element_t<kIndex, std::tuple<T, Ts...>>;

    using ValT = typename ValueType<Nth>::Type;

    // We're trying to add in a pointer, therefore we should only allow
    // a `Address<T>` value.
    if constexpr (std::is_pointer_v<ValT>) {
      if constexpr (kIsAddress<InputT>) {
        auto ptr = ExtractAddress(elem);
        ret = Serializer<NullReader, Writer, InputT>::Write(writer, ptr);

      } else {
        __builtin_unreachable();
      }
    // It's a value, so write it.
    } else if constexpr (std::is_same_v<InputT, ValT>) {
      ret = Serializer<NullReader, Writer, ValT>::Write(writer, elem);

    // It's a reference to something we've already serialized.
    } else if constexpr (std::is_same_v<InputT, TypedSlabReference<Nth>>) {
      ret = Serializer<NullReader, Writer, TypedSlabReference<Nth>>::Write(
          writer, elem);

    // It's a reference to something we've already serialized.
    } else if constexpr (std::is_same_v<InputT, TypedSlabReference<ValT>>) {
      ret = Serializer<NullReader, Writer, TypedSlabReference<ValT>>::Write(
          writer, elem);

    } else if constexpr (std::is_constructible_v<Nth, InputT>) {
      Nth real_elem(elem);
      ret = Serializer<NullReader, Writer, Nth>::Write(writer, real_elem);

    } else if constexpr (std::is_constructible_v<ValT, InputT>) {
      ValT real_elem(elem);
      ret = Serializer<NullReader, Writer, ValT>::Write(writer, real_elem);

    } else {
      __builtin_unreachable();
    }

    if constexpr (0u < sizeof...(InputTs)) {
      AddImpl<kIndex + 1u, Writer, InputTs...>(writer, elems...);
    }

    return ret;
  }
};

// An append-only vector of serialiazed tuples of type `Ts`. This variant
// is "persistent", i.e. data added is persistently stored.
template <typename T, typename... Ts>
class PersistentTypedSlabVector : public TypedSlabVector<T, Ts...> {
 public:
  using Parent = TypedSlabVector<T, Ts...>;
  using Parent::Parent;
  using typename Parent::TupleT;

  template <typename InputT, typename... InputTs>
  HYDE_RT_FLATTEN void Add(const InputT &t, const InputTs &...ts) noexcept {
    static_assert(sizeof...(Ts) == sizeof...(InputTs));

    SlabListWriter writer(this->Parent::storage, *this,
                          true /* is_persistent */);
    this->Parent::template AddImpl<0, SlabListWriter, InputT, InputTs...>(
        writer, t, ts...);
  }

  // Add a tuple, returning a reference to it.
  template <typename InputT, typename... InputTs>
  HYDE_RT_FLATTEN TupleT ReturnAddedTuple(
      const InputT &t, const InputTs &...ts) noexcept {

    static_assert(sizeof...(Ts) == sizeof...(InputTs));

    ByteCountingWriter counting_writer;
    (void) this->Parent::template AddImpl<0, ByteCountingWriter, InputT, InputTs...>(
        counting_writer, t, ts...);

    SlabListWriter writer(this->Parent::storage, *this,
                          true /* is_persistent */);

    uint8_t *ret = nullptr;

    if (writer.CanWriteUnsafely(counting_writer.num_bytes)) {
      ret = this->Parent::template AddImpl<0, UnsafeSlabListWriter, InputT, InputTs...>(
          writer, t, ts...);
    } else {
      ret = this->Parent::template AddImpl<0, SlabListWriter, InputT, InputTs...>(
          writer, t, ts...);
    }

    TupleBuilder<T, Ts...> builder(ret, counting_writer.num_bytes);
    return builder.Build();
  }
};

}  // namespace rt
}  // namespace hyde
