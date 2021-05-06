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

  explicit SlabReference(const uint8_t *read_ptr_,
                         uint32_t num_bytes_) noexcept;

  HYDE_RT_INLINE SlabReference(void) noexcept
      : read_ptr{nullptr},
        num_bytes(0u) {}

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE
  SlabReference(SlabReference &&that) noexcept
      : read_ptr(that.read_ptr),
        num_bytes(that.num_bytes) {
    that.read_ptr = nullptr;
    that.num_bytes = 0u;
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE
  SlabReference(const SlabReference &that) noexcept
      : SlabReference(that.read_ptr, that.num_bytes) {}

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE
  SlabReference(const UnsafeSlabListReader &reader, uint32_t num_bytes_)
      : SlabReference(reader.read_ptr, num_bytes_) {}

  SlabReference &operator=(SlabReference &&that) noexcept;
  SlabReference &operator=(const SlabReference &that) noexcept;

  // Clear this reference.
  [[gnu::hot]] void Clear(void) noexcept;

 private:
  template <typename, typename, typename>
  friend struct Serializer;

  template <typename>
  friend class TypedSlabReference;

  friend class SlabListReader;
  friend class UnsafeSlabListReader;

  // Points to the first by of the serialized `T` inside of one or more slabs.
  const uint8_t *read_ptr;
  uint32_t num_bytes;
};

// Serialize a slab reference.
template <typename Reader, typename Writer>
struct Serializer<Reader, Writer, SlabReference> {
 public:
  HYDE_RT_FLATTEN HYDE_RT_INLINE
  static void Write(Writer &writer, const SlabReference &ref) {
    if constexpr (std::is_same_v<Writer, ByteCountingWriter>) {
      writer.Skip(ref.num_bytes);

    } else {
      const auto num_bytes = ref.num_bytes;
      SlabListReader reader(ref.read_ptr, num_bytes);
      if (HYDE_RT_LIKELY(reader.CanReadUnsafely(num_bytes))) {
        for (auto i = 0u; i < num_bytes; ++i) {
          writer.WriteU8(reader.UnsafeSlabListReader::ReadU8());
        }
      } else {
        for (auto i = 0u; i < num_bytes; ++i) {
          writer.WriteU8(reader.ReadU8());
        }
      }
    }
  }

  // We're being asked to form a reference to the underlying bytes, but we
  // don't know the type, so we can't go and decode/figure out the size of
  // the type.
  HYDE_RT_INLINE
  static void Read(Reader &reader, SlabReference &ref) {
    __builtin_unreachable();
  }
};

// A typed (really, type-erased) reference to some area in a slab.
template <typename T>
class TypedSlabReference : public SlabReference {
 public:
  using SlabReference::SlabReference;
  using SlabReference::operator=;

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_INLINE operator T(void) const noexcept {
    SlabListReader reader(read_ptr, num_bytes);
    T val;
    if (HYDE_RT_LIKELY(reader.CanReadUnsafely(num_bytes))) {
      Serializer<UnsafeSlabListReader, NullWriter, T>::Read(reader, val);
    } else {
      Serializer<SlabListReader, NullWriter, T>::Read(reader, val);
    }
    return val;
  }

  bool operator==(const T &that) const noexcept {
    ByteCountingWriter counting_writer;
    Serializer<NullReader, ByteCountingWriter, T>::Write(counting_writer, that);

    if (counting_writer.num_bytes != num_bytes) {
      return false;
    }

    UnsafeSlabListReader reader(read_ptr, num_bytes);
    if (HYDE_RT_LIKELY(reader.CanReadUnsafely(num_bytes))) {
      using UnsafeEqualityWriter = ByteEqualityComparingWriter<UnsafeSlabListReader>;
      UnsafeEqualityWriter writer(read_ptr, num_bytes);
      Serializer<NullReader, UnsafeEqualityWriter, T>::Write(writer, that);
      return writer.equal;

    } else {
      using EqualityWriter = ByteEqualityComparingWriter<SlabListReader>;
      EqualityWriter writer(read_ptr, num_bytes);
      Serializer<NullReader, EqualityWriter, T>::Write(writer, that);
      return writer.equal;
    }
  }

  bool operator<(const T &that) const noexcept {
    ByteCountingWriter counting_writer;
    Serializer<NullReader, ByteCountingWriter, T>::Write(counting_writer, that);

    if (counting_writer.num_bytes < num_bytes) {
      return false;
    } else if (counting_writer.num_bytes > num_bytes) {
      return true;
    }

    UnsafeSlabListReader reader(read_ptr, num_bytes);
    if (HYDE_RT_LIKELY(reader.CanReadUnsafely(num_bytes))) {
      using UnsafeEqualityWriter =
          ByteLessThanComparingWriter<UnsafeSlabListReader>;
      UnsafeEqualityWriter writer(read_ptr, num_bytes);
      Serializer<NullReader, UnsafeEqualityWriter, T>::Write(writer, that);
      return writer.less;

    } else {
      using EqualityWriter = ByteLessThanComparingWriter<SlabListReader>;
      EqualityWriter writer(read_ptr, num_bytes);
      Serializer<NullReader, EqualityWriter, T>::Write(writer, that);
      return writer.less;
    }
  }

  bool operator>(const T &that) const noexcept {
    ByteCountingWriter counting_writer;
    Serializer<NullReader, ByteCountingWriter, T>::Write(counting_writer, that);

    if (counting_writer.num_bytes < num_bytes) {
      return true;
    } else if (counting_writer.num_bytes > num_bytes) {
      return false;
    }

    UnsafeSlabListReader reader(read_ptr, num_bytes);
    if (HYDE_RT_LIKELY(reader.CanReadUnsafely(num_bytes))) {
      using UnsafeEqualityWriter =
          ByteGreaterThanComparingWriter<UnsafeSlabListReader>;
      UnsafeEqualityWriter writer(read_ptr, num_bytes);
      Serializer<NullReader, UnsafeEqualityWriter, T>::Write(writer, that);
      return writer.greater;

    } else {
      using EqualityWriter = ByteGreaterThanComparingWriter<SlabListReader>;
      EqualityWriter writer(read_ptr, num_bytes);
      Serializer<NullReader, EqualityWriter, T>::Write(writer, that);
      return writer.greater;
    }
  }
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
          const uint8_t *read_ptr, uint32_t num_bytes) { \
        SlabListReader reader(read_ptr, static_cast<uint32_t>(sizeof(type))); \
        Serializer<UnsafeSlabListReader, NullWriter, type>::Read(reader, val); \
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
      Serializer<Reader, Writer, SlabReference>::Write(writer, ref);
    }
  }

  HYDE_RT_INLINE
  static void Read(Reader &reader, RefT &ref) {
    if constexpr (RefT::kIsValue) {
      Serializer<Reader, Writer, DataT>::Read(reader, ref.val);

    // The caller has already done size checking for us.
    } else if constexpr (std::is_same_v<Reader, UnsafeSlabListReader> ||
                         std::is_same_v<Reader, SlabListReader>) {
      ByteCountingReader<Reader> counting_reader(reader);

      Serializer<ByteCountingReader<Reader>, NullWriter, DataT>::Read(
          counting_reader, *reinterpret_cast<DataT *>(nullptr));
      RefT made_ref(reader, counting_reader.num_bytes);
      ref = std::move(made_ref);
      reader.Skip(counting_reader.num_bytes);

    } else {
      __builtin_unreachable();
    }
  }
};

}  // namespace rt
}  // namespace hyde
