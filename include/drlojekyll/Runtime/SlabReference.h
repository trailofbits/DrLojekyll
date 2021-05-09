// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <cstdint>

#include "SlabList.h"
#include "Serializer.h"

namespace hyde {
namespace rt {

template <typename T>
class TypedSlabReference;

// An untyped, counted reference into a slab. The reference counter is
// implicitly tracked by being able to compute the address of the slab
// containing `data`.
class SlabReference {
 public:
  HYDE_RT_ALWAYS_INLINE ~SlabReference(void) noexcept {
    Clear();
  }

  explicit SlabReference(uint8_t *read_ptr_, uint32_t num_bytes_,
                         uint32_t hash_) noexcept;

  HYDE_RT_INLINE SlabReference(void) noexcept
      : u{} {}

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE
  SlabReference(SlabReference &&that) noexcept {
    u.opaque = that.u.opaque;
    that.u.opaque = 0;
  }

  SlabReference(const SlabReference &that) noexcept;

  SlabReference &operator=(SlabReference &&that) noexcept;
  SlabReference &operator=(const SlabReference &that) noexcept;

  // Clear this reference.
  [[gnu::hot]] void Clear(void) noexcept;

 protected:
  template <typename, typename, typename>
  friend struct Serializer;

  template <typename>
  friend class TypedSlabReference;

  friend class SlabListReader;
  friend class UnsafeSlabListReader;

  HYDE_RT_INLINE uint8_t *Data(void) const noexcept {
    return reinterpret_cast<uint8_t *>(u.p.data_addr);
  }

  HYDE_RT_INLINE uint64_t Handle(void) const noexcept {
    return u.opaque;
  }

  HYDE_RT_INLINE uint32_t Hash(void) const noexcept {
    return u.p.hash;
  }

  // Packed representation that bring along the address of some data, as well
  // as a truncated hash.
  union {
    uint64_t opaque{0u};
    struct {
      uint64_t hash:16;
      int64_t data_addr:48;
    } __attribute__((packed)) p;
  } __attribute__((packed)) u;

  static_assert(sizeof(u) == sizeof(uint64_t));
};

// A sized slab reference is a reference to a variable-sized data structure.
class SizedSlabReference : public SlabReference {
 public:

  HYDE_RT_FLATTEN HYDE_RT_INLINE
  explicit SizedSlabReference(uint8_t *data_, uint32_t num_bytes_,
                              uint32_t hash_) noexcept
      : SlabReference(data_, num_bytes_, hash_),
        num_bytes(num_bytes_),
        hash(hash_) {}

  HYDE_RT_FLATTEN HYDE_RT_INLINE SizedSlabReference(void) noexcept
      : SlabReference() {}

  HYDE_RT_INLINE void Clear(void) noexcept {
    this->SlabReference::Clear();
    num_bytes = 0u;
    hash = 0u;
  }

  HYDE_RT_INLINE uint32_t Hash(void) const noexcept {
    return hash;
  }

  SizedSlabReference &operator=(SizedSlabReference &&that) noexcept;
  SizedSlabReference &operator=(const SizedSlabReference &that) noexcept;

 protected:
  using SlabReference::Data;
  using SlabReference::Handle;

  uint32_t num_bytes{0u};
  uint32_t hash{0u};
};

//// Serialize a slab reference.
//template <typename Reader, typename Writer, typename T>
//struct Serializer<Reader, Writer, TypedSlabReference<T>> {
// public:
//
//  HYDE_RT_FLATTEN HYDE_RT_INLINE
//  static void Write(Writer &writer, const TypedSlabReference<T> &ref);
//
//  // We're being asked to form a reference to the underlying bytes, but we
//  // don't know the type, so we can't go and decode/figure out the size of
//  // the type.
//  HYDE_RT_INLINE
//  static void Read(Reader &reader, SlabReference &ref) {
//    __builtin_unreachable();
//  }
//};

template <typename T>
using TypedSlabReferenceBase =
    std::conditional_t<kHasTrivialFixedSizeSerialization<T>,
                       SlabReference, SizedSlabReference>;

// Operations provided for a pointer to some typed data stored in a type-
// erased way in a `Slab`.
template <typename T>
class TypedSlabReferenceOps : public TypedSlabReferenceBase<T> {
 public:
  using TypedSlabReferenceBase<T>::TypedSlabReferenceBase;
  using TypedSlabReferenceBase<T>::operator=;
  using TypedSlabReferenceBase<T>::Hash;
  using TypedSlabReferenceBase<T>::Data;
  using TypedSlabReferenceBase<T>::Handle;
  using TypedSlabReferenceBase<T>::Clear;

  using ValT = typename ValueType<T>::Type;
  using Reader = typename std::conditional<kCanReadWriteUnsafely<ValT>,
                                           UnsafeSlabListReader,
                                           SlabListReader>::type;
  using SizeReader = ByteCountingReader<Reader>;

  TypedSlabReferenceOps(uint8_t *ptr, uint32_t num_bytes_, uint32_t hash_)
      : TypedSlabReferenceBase<T>(ptr, num_bytes_, hash_) {}

  // Returns the serialized size in bytes of something.
  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_INLINE
  uint32_t SizeInBytes(void) const noexcept {
    if constexpr (kHasTrivialFixedSizeSerialization<T>) {
      return kFixedSerializationSize<T>;
    } else {
      return this->SizedSlabReference::num_bytes;
    }
  }

  // Read the current state of the value, converting it to its high-level type.
  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_INLINE
  operator ValT(void) const noexcept {
    ValT val;
    const auto num_bytes = SizeInBytes();
    const auto data = Data();
    Reader reader(data);
    {
      SlabLocker<T> locker(data, num_bytes);
      Serializer<Reader, NullWriter, ValT>::Read(reader, val);
    }
    return val;
  }

  bool operator==(const TypedSlabReference<T> &that) const noexcept {
    if (Handle() == that.Handle()) {
      return true;
    }

    // Equality is the most common operation, so we'd like to optimize for it.
    if (Hash() != that.Hash()) {
      return false;
    }

    const auto num_bytes = SizeInBytes();
    const auto that_num_bytes = that.SizeInBytes();
    if (num_bytes != that_num_bytes) {
      return false;
    }

    const auto data = Data();

    using EqualityWriter = ByteEqualityComparingWriter<Reader>;
    EqualityWriter writer(data, num_bytes);
    {
      SlabLocker<T> locker(data, num_bytes);
      Serializer<NullReader, EqualityWriter, TypedSlabReference<T>>::Write(
          writer, that);
    }
    return writer.equal;
  }

  bool operator<(const TypedSlabReference<T> &that) const noexcept {
    if (Handle() == Handle()) {
      return false;
    }

    const auto num_bytes = SizeInBytes();
    const auto that_num_bytes = that.SizeInBytes();
    if (num_bytes < that_num_bytes) {
      return true;

    } else if (num_bytes > that_num_bytes) {
      return false;
    }

    const auto data = Data();

    using LessThanWriter = ByteLessThanComparingWriter<Reader>;
    LessThanWriter writer(data, num_bytes);
    {
      SlabLocker<T> locker(data, num_bytes);
      Serializer<NullReader, LessThanWriter, TypedSlabReference<T>>::Write(
          writer, that);
    }
    return writer.less;
  }

  bool operator>(const TypedSlabReference<T> &that) const noexcept {
    if (Handle() == Handle()) {
      return false;
    }

    const auto num_bytes = SizeInBytes();
    const auto that_num_bytes = that.SizeInBytes();
    if (num_bytes < that_num_bytes) {
      return false;
    } else if (num_bytes > that_num_bytes) {
      return true;
    }

    const auto data = Data();

    using GreaterThanWriter = ByteGreaterThanComparingWriter<Reader>;
    GreaterThanWriter writer(data, num_bytes);
    {
      SlabLocker<T> locker(data, num_bytes);
      Serializer<NullReader, GreaterThanWriter, TypedSlabReference<T>>::Write(
          writer, that);
    }
    return writer.greater;
  }

  bool operator==(const ValT &that) const noexcept {
    ByteCountingWriter counting_writer;
    Serializer<NullReader, ByteCountingWriter, ValT>::Write(
        counting_writer, that);

    const auto num_bytes = SizeInBytes();
    if (counting_writer.num_bytes != num_bytes) {
      return false;
    }

    const auto data = Data();

    using EqualityWriter = ByteEqualityComparingWriter<Reader>;
    EqualityWriter writer(data, num_bytes);
    {
      SlabLocker<T> locker(data, num_bytes);
      Serializer<NullReader, EqualityWriter, ValT>::Write(writer, that);
    }
    return writer.equal;
  }

  bool operator<(const ValT &that) const noexcept {
    ByteCountingWriter counting_writer;
    Serializer<NullReader, ByteCountingWriter, ValT>::Write(
        counting_writer, that);

    const auto num_bytes = SizeInBytes();
    if (counting_writer.num_bytes < num_bytes) {
      return false;
    } else if (counting_writer.num_bytes > num_bytes) {
      return true;
    }

    const auto data = Data();

    using LessThanWriter = ByteLessThanComparingWriter<Reader>;
    LessThanWriter writer(data, num_bytes);
    {
      SlabLocker<T> locker(data, num_bytes);
      Serializer<NullReader, LessThanWriter, ValT>::Write(writer, that);
    }
    return writer.less;
  }

  bool operator>(const ValT &that) const noexcept {
    ByteCountingWriter counting_writer;
    Serializer<NullReader, ByteCountingWriter, ValT>::Write(
        counting_writer, that);

    const auto num_bytes = SizeInBytes();
    if (counting_writer.num_bytes < num_bytes) {
      return true;
    } else if (counting_writer.num_bytes > num_bytes) {
      return false;
    }

    const auto data = Data();

    using GreaterThanWriter = ByteGreaterThanComparingWriter<Reader>;
    GreaterThanWriter writer(data, num_bytes);
    {
      SlabLocker<T> locker(data, num_bytes);
      Serializer<NullReader, GreaterThanWriter, ValT>::Write(writer, that);
    }
    return writer.greater;
  }
};

// A typed (really, type-erased) reference to some area in a slab.
template <typename T>
class TypedSlabReference : public TypedSlabReferenceOps<T> {
 public:
  using TypedSlabReferenceOps<T>::TypedSlabReferenceOps;
  using TypedSlabReferenceOps<T>::operator=;
  using TypedSlabReferenceOps<T>::operator==;
  using TypedSlabReferenceOps<T>::operator<;
  using TypedSlabReferenceOps<T>::operator>;
  using TypedSlabReferenceOps<T>::Hash;
  using TypedSlabReferenceOps<T>::Data;
  using TypedSlabReferenceOps<T>::Handle;
  using TypedSlabReferenceOps<T>::Clear;
  using TypedSlabReferenceOps<T>::SizeInBytes;
};

template <typename T>
class TypedSlabReference<T *> : public TypedSlabReferenceOps<T *> {
 public:
  using TypedSlabReferenceOps<T *>::TypedSlabReferenceOps;
  using TypedSlabReferenceOps<T *>::operator=;
  using TypedSlabReferenceOps<T *>::operator==;
  using TypedSlabReferenceOps<T *>::operator<;
  using TypedSlabReferenceOps<T *>::operator>;
  using TypedSlabReferenceOps<T *>::Hash;
  using TypedSlabReferenceOps<T *>::Data;
  using TypedSlabReferenceOps<T *>::Handle;
  using TypedSlabReferenceOps<T *>::Clear;
  using TypedSlabReferenceOps<T *>::SizeInBytes;
  using ValT = typename TypedSlabReferenceOps<T *>::ValT;
  using Reader = typename TypedSlabReferenceOps<T *>::Reader;

  // Follow the pointer, returning us a new reference.
  TypedSlabReference<T> operator*(void) const noexcept {
    const auto data = Data();
    const auto num_bytes = SizeInBytes();

    uint8_t *ptr = nullptr;
    Reader reader(data);
    {
      SlabLocker<T> locker(data, num_bytes);
      Serializer<Reader, NullWriter, uint8_t *>::Read(reader, ptr);
    }

    // TODO(pag): Hashes.
    if constexpr (kHasTrivialFixedSizeSerialization<T>) {
      return TypedSlabReference<T>(ptr, kFixedSerializationSize<T>, 0);

    } else {
      ByteCountingReader<SlabListReader> counting_reader(ptr, 0u);
      using IndirectValT = typename ValueType<T>::Type;
      alignas(IndirectValT) uint8_t dummy_data[sizeof(IndirectValT)];
      Serializer<ByteCountingReader<SlabListReader>, NullWriter, IndirectValT>::Read(
          reader, *reinterpret_cast<IndirectValT *>(dummy_data));
      return TypedSlabReference<T>(ptr, reader.num_bytes, 0);
    }
  }
};

template <typename T>
class TypedSlabReference<Addressable<T>> : public TypedSlabReferenceOps<T> {
 public:
  using TypedSlabReferenceOps<T>::TypedSlabReferenceOps;
  using TypedSlabReferenceOps<T>::operator=;
  using TypedSlabReferenceOps<T>::operator==;
  using TypedSlabReferenceOps<T>::operator<;
  using TypedSlabReferenceOps<T>::operator>;
  using TypedSlabReferenceOps<T>::Hash;
  using TypedSlabReferenceOps<T>::Data;
  using TypedSlabReferenceOps<T>::Handle;
  using TypedSlabReferenceOps<T>::Clear;
  using TypedSlabReferenceOps<T>::SizeInBytes;
  using ValT = typename TypedSlabReferenceOps<T>::ValT;
  using Reader = typename TypedSlabReferenceOps<T>::Reader;

  Address<ValT> operator&(void) const noexcept {
    return {Data()};
  }
};

template <typename T>
class TypedSlabReference<Mutable<T>> : public TypedSlabReferenceOps<T> {
 public:
  using TypedSlabReferenceOps<T>::TypedSlabReferenceOps;
  using TypedSlabReferenceOps<T>::operator==;
  using TypedSlabReferenceOps<T>::operator<;
  using TypedSlabReferenceOps<T>::operator>;
  using TypedSlabReferenceOps<T>::Hash;
  using TypedSlabReferenceOps<T>::Data;
  using TypedSlabReferenceOps<T>::Handle;
  using TypedSlabReferenceOps<T>::Clear;
  using TypedSlabReferenceOps<T>::SizeInBytes;
  using ValT = typename TypedSlabReferenceOps<T>::ValT;
  using Reader = typename TypedSlabReferenceOps<T>::Reader;

  // Mutable things are also addressable.
  Address<ValT> operator&(void) const noexcept {
    return {Data()};
  }

  // Permit updating of a value. The value must be a trivial fixed size that
  // does not cross two slabs. It almost must be marked as being mutable.
  void operator=(const ValT &new_val) noexcept {
    static_assert(kHasTrivialFixedSizeSerialization<ValT>);
    static_assert(kCanReadWriteUnsafely<ValT>);
    static constexpr auto num_bytes = kFixedSerializationSize<ValT>;

    const auto data = Data();
    UnsafeByteWriter writer(data);

    SlabLocker<T> locker(data, num_bytes);
    Serializer<NullReader, UnsafeByteWriter, ValT>::Write(writer, new_val);
  }

  using TypedSlabReferenceOps<T>::operator=;
};

#define HYDE_RT_DEFINE_SLAB_VALUE(type) \
    template <> \
    class TypedSlabReference<type> { \
     public: \
      static constexpr bool kIsValue = true; \
      TypedSlabReference(void) = default; \
      HYDE_RT_ALWAYS_INLINE TypedSlabReference(type val_) \
          : val(val_) {} \
      HYDE_RT_ALWAYS_INLINE TypedSlabReference( \
          uint8_t *read_ptr, uint32_t, uint32_t) { \
        SlabListReader reader(read_ptr); \
        Serializer<UnsafeSlabListReader, NullWriter, type>::Read(reader, val); \
      } \
      void operator&(void) const noexcept { \
        __builtin_unreachable(); \
      } \
      HYDE_RT_ALWAYS_INLINE \
      bool operator==(const type that_val) const noexcept { \
        return val == that_val; \
      } \
      HYDE_RT_ALWAYS_INLINE \
      bool operator!=(const type that_val) const noexcept { \
        return val != that_val; \
      } \
      HYDE_RT_ALWAYS_INLINE \
      bool operator<(const type that_val) const noexcept { \
        return val < that_val; \
      } \
      HYDE_RT_ALWAYS_INLINE \
      bool operator>(const type that_val) const noexcept { \
        return val > that_val; \
      } \
      HYDE_RT_ALWAYS_INLINE \
      bool operator<=(const type that_val) const noexcept { \
        return val <= that_val; \
      } \
      HYDE_RT_ALWAYS_INLINE \
      bool operator>=(const type that_val) const noexcept { \
        return val >= that_val; \
      } \
      HYDE_RT_ALWAYS_INLINE \
      operator type(void) const noexcept { \
        return val; \
      } \
      type val; \
    }

HYDE_RT_DEFINE_SLAB_VALUE(char);
HYDE_RT_DEFINE_SLAB_VALUE(bool);
HYDE_RT_DEFINE_SLAB_VALUE(uint8_t);
HYDE_RT_DEFINE_SLAB_VALUE(uint16_t);
HYDE_RT_DEFINE_SLAB_VALUE(uint32_t);
HYDE_RT_DEFINE_SLAB_VALUE(uint64_t);
HYDE_RT_DEFINE_SLAB_VALUE(int8_t);
HYDE_RT_DEFINE_SLAB_VALUE(int16_t);
HYDE_RT_DEFINE_SLAB_VALUE(int32_t);
HYDE_RT_DEFINE_SLAB_VALUE(int64_t);
HYDE_RT_DEFINE_SLAB_VALUE(float);
HYDE_RT_DEFINE_SLAB_VALUE(double);

template <typename Reader, typename Writer, typename DataT>
struct Serializer<Reader, Writer, TypedSlabReference<DataT>> {
 public:
  using RefT = TypedSlabReference<DataT>;

  HYDE_RT_INLINE
  static void Write(Writer &writer, const RefT &ref) {
    if constexpr (RefT::kIsValue) {
      Serializer<Reader, Writer, DataT>::Write(writer, ref.val);
    } else {
      const auto num_bytes = ref.SizeInBytes();
      if constexpr (kIsByteCountingWriter<Writer>) {
        writer.Skip(num_bytes);

      } else {
        SlabListReader reader(ref.Data(), num_bytes);
        TransferData(writer, reader, num_bytes);
      }
    }
  }

  HYDE_RT_INLINE
  static void Read(Reader &reader, RefT &ref) {
    if constexpr (RefT::kIsValue) {
      Serializer<Reader, Writer, DataT>::Read(reader, ref.val);

    // The caller has already done size checking for us.
    } else if constexpr (kIsSlabListReader<Reader>) {
      alignas(DataT) uint8_t dummy_data[sizeof(DataT)];

      ByteCountingReader<Reader> counting_reader(reader);

      Serializer<ByteCountingReader<Reader>, NullWriter, DataT>::Read(
          counting_reader, *reinterpret_cast<DataT *>(dummy_data));
      RefT made_ref(reader, counting_reader.num_bytes);
      ref = std::move(made_ref);
      reader.Skip(counting_reader.num_bytes);

    // Counting the number of bytes.
    } else if constexpr (kIsByteCountingReader<Reader>) {
      alignas(DataT) uint8_t dummy_data[sizeof(DataT)];
      Serializer<Reader, NullWriter, DataT>::Read(
          reader, *reinterpret_cast<DataT *>(dummy_data));

    } else {
      __builtin_unreachable();
    }
  }
};

// A reference to a reference is just a reference.
template <typename T>
class TypedSlabReference<TypedSlabReference<T>>
    : public TypedSlabReference<T> {};

}  // namespace rt
}  // namespace hyde
