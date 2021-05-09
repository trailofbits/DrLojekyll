// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <array>
#include <cstdint>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "Util.h"

namespace hyde {
namespace rt {

// An unserializer that always returns default-initialzied values.
struct NullReader {
 public:
  HYDE_RT_ALWAYS_INLINE void *ReadPointer(void) { return {}; }
  HYDE_RT_ALWAYS_INLINE uint32_t ReadSize(void) { return {}; }
  HYDE_RT_ALWAYS_INLINE double ReadF64(void) { return {}; }
  HYDE_RT_ALWAYS_INLINE float ReadF32(void) { return {}; }
  HYDE_RT_ALWAYS_INLINE uint64_t ReadU64(void) { return {}; }
  HYDE_RT_ALWAYS_INLINE uint32_t ReadU32(void) { return {}; }
  HYDE_RT_ALWAYS_INLINE uint16_t ReadU16(void) { return {}; }
  HYDE_RT_ALWAYS_INLINE uint8_t ReadU8(void) { return {}; }
  HYDE_RT_ALWAYS_INLINE bool ReadB(void) { return {}; }
  HYDE_RT_ALWAYS_INLINE int64_t ReadI64(void) { return {}; }
  HYDE_RT_ALWAYS_INLINE uint32_t ReadI32(void) { return {}; }
  HYDE_RT_ALWAYS_INLINE int16_t ReadI16(void) { return {}; }
  HYDE_RT_ALWAYS_INLINE int8_t ReadI8(void) { return {}; }
  HYDE_RT_ALWAYS_INLINE void Skip(uint32_t) {}
};

// A serializer that writes out nothing.
struct NullWriter {
 public:
  HYDE_RT_ALWAYS_INLINE void WritePointer(void *) {}
  HYDE_RT_ALWAYS_INLINE void WriteSize(uint32_t) {}
  HYDE_RT_ALWAYS_INLINE void WriteF64(double) {}
  HYDE_RT_ALWAYS_INLINE void WriteF32(float) {}
  HYDE_RT_ALWAYS_INLINE void WriteU64(uint64_t) {}
  HYDE_RT_ALWAYS_INLINE void WriteU32(uint32_t) {}
  HYDE_RT_ALWAYS_INLINE void WriteU16(uint16_t) {}
  HYDE_RT_ALWAYS_INLINE void WriteU8(uint8_t) {}
  HYDE_RT_ALWAYS_INLINE void WriteB(bool) {}
  HYDE_RT_ALWAYS_INLINE void WriteI64(int64_t) {}
  HYDE_RT_ALWAYS_INLINE void WriteI32(int32_t) {}
  HYDE_RT_ALWAYS_INLINE void WriteI16(int16_t) {}
  HYDE_RT_ALWAYS_INLINE void WriteI8(int8_t) {}
  HYDE_RT_ALWAYS_INLINE void Skip(uint32_t) {}
};

// A writer for writing bytes into a continuous backing buffer. No bounds
// checking is performed.
class UnsafeByteWriter {
 public:
  HYDE_RT_ALWAYS_INLINE explicit UnsafeByteWriter(uint8_t *write_ptr_)
      : write_ptr(write_ptr_) {}

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE
  void WritePointer(void *ptr) noexcept {
    auto addr = reinterpret_cast<intptr_t>(ptr);
    auto write_addr = reinterpret_cast<intptr_t>(write_ptr);
    WriteI64(addr - write_addr);
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE
  void WriteF64(double d) noexcept {
    WriteU64(reinterpret_cast<const uint64_t &>(d));
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE
  void WriteF32(float f) noexcept {
    WriteU32(reinterpret_cast<const uint32_t &>(f));
  }

  [[gnu::hot]] HYDE_RT_ALWAYS_INLINE
  void WriteU64(uint64_t q) noexcept {
    const auto ptr = write_ptr;
    write_ptr += 8;
    ptr[0] = static_cast<uint8_t>(q >> 0);
    ptr[1] = static_cast<uint8_t>(q >> 8);
    ptr[2] = static_cast<uint8_t>(q >> 16);
    ptr[3] = static_cast<uint8_t>(q >> 24);
    ptr[4] = static_cast<uint8_t>(q >> 32);
    ptr[5] = static_cast<uint8_t>(q >> 40);
    ptr[6] = static_cast<uint8_t>(q >> 48);
    ptr[7] = static_cast<uint8_t>(q >> 56);
  }

  [[gnu::hot]] HYDE_RT_ALWAYS_INLINE
  void WriteU32(uint32_t d) noexcept {
    const auto ptr = write_ptr;
    write_ptr += 4;
    ptr[0] = static_cast<uint8_t>(d >> 0);
    ptr[1] = static_cast<uint8_t>(d >> 8);
    ptr[2] = static_cast<uint8_t>(d >> 16);
    ptr[3] = static_cast<uint8_t>(d >> 24);
  }

  [[gnu::hot]] HYDE_RT_ALWAYS_INLINE
  void WriteU16(uint16_t h) noexcept {
    const auto ptr = write_ptr;
    write_ptr += 2;
    ptr[0] = static_cast<uint8_t>(h >> 0);
    ptr[1] = static_cast<uint8_t>(h >> 8);
  }

  [[gnu::hot]] HYDE_RT_ALWAYS_INLINE
  void WriteU8(uint8_t b) noexcept {
    *write_ptr++ = b;
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE
  void WriteI64(int64_t q) noexcept {
    WriteU64(static_cast<uint64_t>(q));
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE
  void WriteI32(int32_t w) noexcept {
    WriteU32(static_cast<uint32_t>(w));
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE
  void WriteI16(int16_t h) noexcept {
    WriteU16(static_cast<uint16_t>(h));
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE
  void WriteI8(int8_t b) noexcept {
    WriteU8(static_cast<uint8_t>(b));
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE
  void WriteB(bool b) noexcept {
    WriteU8(static_cast<uint8_t>(!!b));
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE
  void WriteSize(uint32_t d) noexcept {
    WriteU32(d);
  }

  [[gnu::hot]] HYDE_RT_ALWAYS_INLINE void Skip(uint32_t num_bytes) noexcept {
    write_ptr += num_bytes;
  }

  uint8_t *write_ptr{nullptr};
};

// A serializing writer that ignores the values being written, and instead
// counts the number of bytes that will be written.
struct ByteCountingWriter {
 public:
  HYDE_RT_ALWAYS_INLINE void WritePointer(void *) {
    num_bytes += 8;
  }

  HYDE_RT_ALWAYS_INLINE void WriteSize(uint32_t) {
    num_bytes += 4;
  }

  HYDE_RT_ALWAYS_INLINE void WriteF64(double) {
    num_bytes += 8;
  }

  HYDE_RT_ALWAYS_INLINE void WriteF32(float) {
    num_bytes += 4;
  }

  HYDE_RT_ALWAYS_INLINE void WriteU64(uint64_t) {
    num_bytes += 8;
  }

  HYDE_RT_ALWAYS_INLINE void WriteU32(uint32_t) {
    num_bytes += 4;
  }

  HYDE_RT_ALWAYS_INLINE void WriteU16(uint16_t) {
    num_bytes += 2;
  }

  HYDE_RT_ALWAYS_INLINE void WriteU8(uint8_t) {
    num_bytes += 1;
  }

  HYDE_RT_ALWAYS_INLINE void WriteB(bool) {
    num_bytes += 1;
  }

  HYDE_RT_ALWAYS_INLINE void WriteI64(int64_t) {
    num_bytes += 8;
  }

  HYDE_RT_ALWAYS_INLINE void WritI32(int32_t) {
    num_bytes += 4;
  }

  HYDE_RT_ALWAYS_INLINE void WriteI16(int16_t) {
    num_bytes += 2;
  }

  HYDE_RT_ALWAYS_INLINE void WriteI8(int8_t) {
    num_bytes += 1;
  }

  HYDE_RT_ALWAYS_INLINE void Skip(uint32_t n) {
    num_bytes += n;
  }

  size_t num_bytes{0};
};

template <typename T>
static constexpr bool kIsByteCountingWriter = false;

template <>
static constexpr bool kIsByteCountingWriter<ByteCountingWriter> = true;

// A serializing writer that ignores the values being written, and instead
// performs an element-wise equality comparison.
template <typename Reader>
struct ByteEqualityComparingWriter : public Reader {
 public:
  using Reader::Reader;

  HYDE_RT_ALWAYS_INLINE void WritePointer(void *rhs) {
    if (!equal) {
      equal = static_cast<const uint8_t *>(Reader::ReadPointer()) ==
              static_cast<const uint8_t *>(rhs);
    }
  }

  HYDE_RT_ALWAYS_INLINE void WriteSize(uint32_t rhs) {
    if (equal) {
      equal = Reader::ReadSize() == rhs;
    }
  }

  HYDE_RT_ALWAYS_INLINE void WriteF64(double rhs) {
    if (equal) {
      equal = Reader::ReadF64() == rhs;
    }
  }

  HYDE_RT_ALWAYS_INLINE void WriteF32(float rhs) {
    if (equal) {
      equal = Reader::ReadF32() == rhs;
    }
  }

  HYDE_RT_ALWAYS_INLINE void WriteU64(uint64_t rhs) {
    if (equal) {
      equal = Reader::ReadU64() == rhs;
    }
  }

  HYDE_RT_ALWAYS_INLINE void WriteU32(uint32_t rhs) {
    if (equal) {
      equal = Reader::ReadU32() == rhs;
    }
  }

  HYDE_RT_ALWAYS_INLINE void WriteU16(uint16_t rhs) {
    if (equal) {
      equal = Reader::ReadU16() == rhs;
    }
  }

  HYDE_RT_ALWAYS_INLINE void WriteU8(uint8_t rhs) {
    if (equal) {
      equal = Reader::ReadU8() == rhs;
    }
  }

  HYDE_RT_ALWAYS_INLINE void WriteB(bool rhs) {
    if (equal) {
      equal = Reader::ReadB() == rhs;
    }
  }

  HYDE_RT_ALWAYS_INLINE void WriteI64(int64_t rhs) {
    if (equal) {
      equal = Reader::ReadI64() == rhs;
    }
  }

  HYDE_RT_ALWAYS_INLINE void WritI32(int32_t rhs) {
    if (equal) {
      equal = Reader::ReadI32() == rhs;
    }
  }

  HYDE_RT_ALWAYS_INLINE void WriteI16(int16_t rhs) {
    if (equal) {
      equal = Reader::ReadI16() == rhs;
    }
  }

  HYDE_RT_ALWAYS_INLINE void WriteI8(int8_t rhs) {
    if (equal) {
      equal = Reader::ReadI8() == rhs;
    }
  }

  HYDE_RT_ALWAYS_INLINE void Skip(uint32_t n) {
    if (equal) {
      Reader::Skip(n);
    }
  }

  bool equal{true};
};

// A serializing writer that ignores the values being written, and instead
// performs an element-wise less-than comparison.
template <typename Reader>
struct ByteLessThanComparingWriter : public Reader {
 public:
  using Reader::Reader;

  HYDE_RT_ALWAYS_INLINE void WritePointer(void *rhs) {
    if (!less) {
      less = static_cast<const uint8_t *>(Reader::ReadPointer()) <
             static_cast<const uint8_t *>(rhs);
    }
  }

  HYDE_RT_ALWAYS_INLINE void WriteSize(uint32_t rhs) {
    if (!less) {
      less = Reader::ReadSize() < rhs;
    }
  }

  HYDE_RT_ALWAYS_INLINE void WriteF64(double rhs) {
    if (!less) {
      less = Reader::ReadF64() < rhs;
    }
  }

  HYDE_RT_ALWAYS_INLINE void WriteF32(float rhs) {
    if (!less) {
      less = Reader::ReadF32() < rhs;
    }
  }

  HYDE_RT_ALWAYS_INLINE void WriteU64(uint64_t rhs) {
    if (!less) {
      less = Reader::ReadU64() < rhs;
    }
  }

  HYDE_RT_ALWAYS_INLINE void WriteU32(uint32_t rhs) {
    if (!less) {
      less = Reader::ReadU32() < rhs;
    }
  }

  HYDE_RT_ALWAYS_INLINE void WriteU16(uint16_t rhs) {
    if (!less) {
      less = Reader::ReadU16() < rhs;
    }
  }

  HYDE_RT_ALWAYS_INLINE void WriteU8(uint8_t rhs) {
    if (!less) {
      less = Reader::ReadU8() < rhs;
    }
  }

  HYDE_RT_ALWAYS_INLINE void WriteB(bool rhs) {
    if (!less) {
      less = Reader::ReadB() < rhs;
    }
  }

  HYDE_RT_ALWAYS_INLINE void WriteI64(int64_t rhs) {
    if (!less) {
      less = Reader::ReadI64() < rhs;
    }
  }

  HYDE_RT_ALWAYS_INLINE void WritI32(int32_t rhs) {
    if (!less) {
      less = Reader::ReadI32() < rhs;
    }
  }

  HYDE_RT_ALWAYS_INLINE void WriteI16(int16_t rhs) {
    if (!less) {
      less = Reader::ReadI16() < rhs;
    }
  }

  HYDE_RT_ALWAYS_INLINE void WriteI8(int8_t rhs) {
    if (!less) {
      less = Reader::ReadI8() < rhs;
    }
  }

  HYDE_RT_ALWAYS_INLINE void Skip(uint32_t n) {
    if (!less) {
      Reader::Skip(n);
    }
  }

  bool less{false};
};

// A serializing writer that ignores the values being written, and instead
// performs an element-wise greater-than comparison.
template <typename Reader>
struct ByteGreaterThanComparingWriter : public Reader {
 public:
  using Reader::Reader;

  HYDE_RT_ALWAYS_INLINE void WritePointer(void *rhs) {
    if (!greater) {
      greater = static_cast<const uint8_t *>(Reader::ReadPointer()) >
                static_cast<const uint8_t *>(rhs);
    }
  }

  HYDE_RT_ALWAYS_INLINE void WriteSize(uint32_t rhs) {
    if (!greater) {
      greater = Reader::ReadSize() > rhs;
    }
  }

  HYDE_RT_ALWAYS_INLINE void WriteF64(double rhs) {
    if (!greater) {
      greater = Reader::ReadF64() > rhs;
    }
  }

  HYDE_RT_ALWAYS_INLINE void WriteF32(float rhs) {
    if (!greater) {
      greater = Reader::ReadF32() > rhs;
    }
  }

  HYDE_RT_ALWAYS_INLINE void WriteU64(uint64_t rhs) {
    if (!greater) {
      greater = Reader::ReadU64() > rhs;
    }
  }

  HYDE_RT_ALWAYS_INLINE void WriteU32(uint32_t rhs) {
    if (!greater) {
      greater = Reader::ReadU32() > rhs;
    }
  }

  HYDE_RT_ALWAYS_INLINE void WriteU16(uint16_t rhs) {
    if (!greater) {
      greater = Reader::ReadU16() > rhs;
    }
  }

  HYDE_RT_ALWAYS_INLINE void WriteU8(uint8_t rhs) {
    if (!greater) {
      greater = Reader::ReadU8() > rhs;
    }
  }

  HYDE_RT_ALWAYS_INLINE void WriteB(bool rhs) {
    if (!greater) {
      greater = Reader::ReadB() > rhs;
    }
  }

  HYDE_RT_ALWAYS_INLINE void WriteI64(int64_t rhs) {
    if (!greater) {
      greater = Reader::ReadI64() > rhs;
    }
  }

  HYDE_RT_ALWAYS_INLINE void WritI32(int32_t rhs) {
    if (!greater) {
      greater = Reader::ReadI32() > rhs;
    }
  }

  HYDE_RT_ALWAYS_INLINE void WriteI16(int16_t rhs) {
    if (!greater) {
      greater = Reader::ReadI16() > rhs;
    }
  }

  HYDE_RT_ALWAYS_INLINE void WriteI8(int8_t rhs) {
    if (!greater) {
      greater = Reader::ReadI8() > rhs;
    }
  }

  HYDE_RT_ALWAYS_INLINE void Skip(uint32_t n) {
    if (!greater) {
      Reader::Skip(n);
    }
  }

  bool greater{false};
};

// A reader that actually figures out how many serialized bytes something will
// require (`NullReader`), or that something occupied (using a real `Reader`)
template <typename SubReader>
struct ByteCountingReader : public SubReader {
 public:
  using SubReader::SubReader;

  HYDE_RT_ALWAYS_INLINE void *ReadPointer(void) {
    num_bytes += 8u;
    return SubReader::ReadPointer();
  }

  // This is the one special case where we actual do the read.
  HYDE_RT_ALWAYS_INLINE uint32_t ReadSize(void) {
    num_bytes += 4u;
    return SubReader::ReadSize();
  }

  HYDE_RT_ALWAYS_INLINE double ReadF64(void) {
    num_bytes += 8u;
    SubReader::Skip(8u);
    return {};
  }

  HYDE_RT_ALWAYS_INLINE float ReadF32(void) {
    num_bytes += 4u;
    SubReader::Skip(4u);
    return {};
  }

  HYDE_RT_ALWAYS_INLINE uint64_t ReadU64(void) {
    num_bytes += 8u;
    SubReader::Skip(8u);
    return {};
  }

  HYDE_RT_ALWAYS_INLINE uint32_t ReadU32(void) {
    num_bytes += 4u;
    SubReader::Skip(4u);
    return {};
  }

  HYDE_RT_ALWAYS_INLINE uint16_t ReadU16(void) {
    num_bytes += 2u;
    SubReader::Skip(2u);
    return {};
  }

  HYDE_RT_ALWAYS_INLINE uint8_t ReadU8(void) {
    num_bytes += 1u;
    SubReader::Skip(1u);
    return {};
  }

  HYDE_RT_ALWAYS_INLINE bool ReadB(void) {
    num_bytes += 1u;
    SubReader::Skip(1u);
    return {};
  }

  HYDE_RT_ALWAYS_INLINE int64_t ReadI64(void) {
    num_bytes += 8u;
    SubReader::Skip(8u);
    return {};
  }

  HYDE_RT_ALWAYS_INLINE int32_t ReadI32(void) {
    num_bytes += 4u;
    SubReader::Skip(4u);
    return {};
  }

  HYDE_RT_ALWAYS_INLINE int16_t ReadI16(void) {
    num_bytes += 2u;
    SubReader::Skip(2u);
    return {};
  }

  HYDE_RT_ALWAYS_INLINE int8_t ReadI8(void) {
    num_bytes += 1u;
    SubReader::Skip(1u);
    return {};
  }

  HYDE_RT_ALWAYS_INLINE void Skip(uint32_t n) {
    num_bytes += n;
    SubReader::Skip(n);
  }

  size_t num_bytes{0};
};

template <typename T>
static constexpr bool kIsByteCountingReader = false;

template <typename T>
static constexpr bool kIsByteCountingReader<ByteCountingReader<T>> = true;

// Methods to overload for serializing data
template <typename Reader, typename Writer, typename DataT>
struct Serializer;

template <typename T>
static constexpr bool kHasTrivialFixedSizeSerialization =
    Serializer<NullReader, NullWriter, T>::kIsFixedSize;

template <typename T>
static constexpr size_t kFixedSerializationSize =
    Serializer<NullReader, NullWriter, T>::SizeInBytes();

template <typename T>
static constexpr bool kHasTrivialFixedSizeSerialization<Mutable<T>> =
    kHasTrivialFixedSizeSerialization<T>;

template <typename T>
static constexpr size_t kFixedSerializationSize<Mutable<T>> =
    kFixedSerializationSize<T>;

template <typename T>
static constexpr bool kHasTrivialFixedSizeSerialization<Addressable<T>> =
    kHasTrivialFixedSizeSerialization<T>;

template <typename T>
static constexpr size_t kFixedSerializationSize<Addressable<T>> =
    kFixedSerializationSize<T>;

// Methods to overload for serializing data
template <typename Reader, typename Writer, typename DataT>
struct Serializer<Reader, Writer, const DataT>
    : Serializer<Reader, Writer, DataT> {};

template <typename Reader, typename Writer, typename DataT>
struct Serializer<Reader, Writer, Mutable<DataT>>
    : Serializer<Reader, Writer, DataT> {};

template <typename Reader, typename Writer, typename DataT>
struct Serializer<Reader, Writer, Addressable<DataT>>
    : Serializer<Reader, Writer, DataT> {};

template <typename Reader, typename Writer, typename DataT>
struct Serializer<Reader, Writer, Address<DataT>>
    : Serializer<Reader, Writer, DataT *> {};

#define HYDE_RT_SERIALIZER_NAMESPACE_BEGIN
#define HYDE_RT_SERIALIZER_NAMESPACE_END
#define HYDE_RT_DEFINE_UNSAFE_SERIALIZER_PRIV(type) \
    template <> \
    static constexpr bool kCanReadWriteUnsafely<type> = true

#define DRLOJEKYLL_MAKE_FUNDAMENTAL_SERIALIZER(type, cast_op, cast_type, method_suffix, size) \
    HYDE_RT_SERIALIZER_NAMESPACE_BEGIN \
    template <typename Reader, typename Writer> \
    struct Serializer<Reader, Writer, type> { \
      static constexpr bool kIsFixedSize = true; \
      HYDE_RT_FLATTEN HYDE_RT_INLINE \
      static void Write(Writer &writer, type data) { \
        writer.Write ## method_suffix (cast_op<cast_type>(data)); \
      } \
      HYDE_RT_FLATTEN HYDE_RT_INLINE \
      static void Read(Reader &reader, type &out) { \
        out = cast_op<type>(reader.Read ## method_suffix()); \
      } \
      static constexpr uint32_t SizeInBytes(void) noexcept { \
        return size; \
      } \
    }; \
    HYDE_RT_DEFINE_UNSAFE_SERIALIZER_PRIV(type); \
    HYDE_RT_SERIALIZER_NAMESPACE_END

template <typename T>
static constexpr bool kCanReadWriteUnsafely = false;

#if CHAR_MIN < 0
DRLOJEKYLL_MAKE_FUNDAMENTAL_SERIALIZER(char, static_cast, int8_t, I8, 1)
#else
DRLOJEKYLL_MAKE_FUNDAMENTAL_SERIALIZER(char, static_cast, uint8_t, U8, 1)
#endif

DRLOJEKYLL_MAKE_FUNDAMENTAL_SERIALIZER(bool, static_cast, bool, B, 1)

DRLOJEKYLL_MAKE_FUNDAMENTAL_SERIALIZER(uint8_t, static_cast, uint8_t, U8, 1)
DRLOJEKYLL_MAKE_FUNDAMENTAL_SERIALIZER(int8_t, static_cast, int8_t, I8, 1)

DRLOJEKYLL_MAKE_FUNDAMENTAL_SERIALIZER(uint16_t, static_cast, uint16_t, U16, 2)
DRLOJEKYLL_MAKE_FUNDAMENTAL_SERIALIZER(int16_t, static_cast, int16_t, I16, 2)

DRLOJEKYLL_MAKE_FUNDAMENTAL_SERIALIZER(uint32_t, static_cast, uint32_t, U32, 4)
DRLOJEKYLL_MAKE_FUNDAMENTAL_SERIALIZER(int32_t, static_cast, int32_t, I32, 4)

DRLOJEKYLL_MAKE_FUNDAMENTAL_SERIALIZER(uint64_t, static_cast, uint64_t, U64, 8)
DRLOJEKYLL_MAKE_FUNDAMENTAL_SERIALIZER(int64_t, static_cast, int64_t, I64, 8)

DRLOJEKYLL_MAKE_FUNDAMENTAL_SERIALIZER(float, static_cast, float, F32, 4)
DRLOJEKYLL_MAKE_FUNDAMENTAL_SERIALIZER(double, static_cast, double, F64, 8)

template<typename Reader, typename Writer, typename T>
struct Serializer<Reader, Writer, T *> {
  static constexpr bool kIsFixedSize = true;
  static void Write(Writer &writer, T *data) {
    writer.WritePointer(reinterpret_cast<void*>(data));
  }

  static void Read(Reader &reader, T *&out) {
    out = reinterpret_cast<T*>(reader.ReadPointer());
  }
  static constexpr uint32_t SizeInBytes(void) noexcept {
    return 8;
  }
};

template <typename T>
static constexpr bool kCanReadWriteUnsafely<T *> = true;

#undef HYDE_RT_DEFINE_UNSAFE_SERIALIZER_PRIV
#define HYDE_RT_DEFINE_UNSAFE_SERIALIZER_PRIV(type)

#undef HYDE_RT_SERIALIZER_NAMESPACE_BEGIN
#undef HYDE_RT_SERIALIZER_NAMESPACE_END

template <typename Reader, typename ContainerType, typename ElementType>
struct LinearContainerReader {
 public:
  HYDE_RT_FLATTEN HYDE_RT_INLINE
  static void Read(Reader &reader, ContainerType &ret) {
    const auto size = reader.ReadSize();
    ret.resize(size);
    ElementType * const begin = &(ret[0]);
    for (uint32_t i = 0; i < size; ++i) {
      Serializer<Reader, NullWriter, ElementType>::Read(reader, begin[i]);
    }
  }
};

// Special case for the byte counting reader: we don't want to really have to
// construct any actual containers or anything like that, and instead just
// count the bytes.
template <typename SubReader, typename ContainerType, typename ElementType>
struct LinearContainerReader<ByteCountingReader<SubReader>,
                             ContainerType, ElementType> {
 public:
  using Reader = ByteCountingReader<SubReader>;

  HYDE_RT_FLATTEN HYDE_RT_INLINE
  static void Read(Reader &reader, ContainerType &) {
    const auto size = reader.ReadSize();
    if (kHasTrivialFixedSizeSerialization<ElementType>) {
      reader.Skip(static_cast<uint32_t>(
          size * kFixedSerializationSize<ElementType>));
    } else {
      alignas(ElementType) char dummy_data[sizeof(ElementType)];
      for (uint32_t i = 0; i < size; ++i) {
        Serializer<Reader, NullWriter, ElementType>::Read(
            reader, *reinterpret_cast<ElementType *>(dummy_data));
      }
    }
  }
};

// The generic linear container writer.
template <typename Writer, typename ContainerType, typename ElementType>
struct LinearContainerWriter {
 public:

  HYDE_RT_FLATTEN HYDE_RT_INLINE
  static void Write(Writer &writer, const ContainerType &data) {
    const auto size = static_cast<uint32_t>(data.size());
    writer.WriteSize(size);

    // NOTE(pag): Induction variable based `for` loop so that a a byte counting
    //            writer can elide the `for` loop entirely and count `size`.
    const ElementType * const begin = &(data[0]);
    for (uint32_t i = 0; i < size; ++i) {
      Serializer<NullReader, Writer, ElementType>::Write(writer, begin[i]);
    }
  }
};

// A specialization of a linear container writer that knows that all we want
// to do is count bytes.
template <typename ContainerType, typename ElementType>
struct LinearContainerWriter<ByteCountingWriter, ContainerType, ElementType> {
 public:

  HYDE_RT_FLATTEN HYDE_RT_INLINE
  static void Write(ByteCountingWriter &writer, const ContainerType &data) {
    const auto size = static_cast<uint32_t>(data.size());
    writer.WriteSize(size);

    if (kHasTrivialFixedSizeSerialization<ElementType>) {
      writer.Skip(static_cast<uint32_t>(
          size * kFixedSerializationSize<ElementType>));

    } else {
      const ElementType * const begin = &(data[0]);
      for (uint32_t i = 0; i < size; ++i) {
        Serializer<NullReader, ByteCountingWriter, ElementType>::Write(
            writer, begin[i]);
      }
    }
  }
};

template <typename Reader, typename Writer, typename ContainerType,
          typename ElementType>
struct LinearContainerSerializer
    : public LinearContainerReader<Reader, ContainerType, ElementType>,
      public LinearContainerWriter<Writer, ContainerType, ElementType> {
  static constexpr bool kIsFixedSize = false;

  static constexpr uint32_t SizeInBytes(void) noexcept {
    return 0u;
  }
};

template <typename Reader, typename Writer, typename T,
          typename VectorAllocator>
struct Serializer<Reader, Writer, std::vector<T, VectorAllocator>>
    : public LinearContainerSerializer<
          Reader,
          Writer,
          std::vector<T, VectorAllocator>,
          T> {};

template <typename Reader, typename Writer, typename T, typename StringTraits,
          typename StringAllocator>
struct Serializer<Reader, Writer,
                  std::basic_string<T, StringTraits, StringAllocator>>
    : public LinearContainerSerializer<
          Reader,
          Writer,
          std::basic_string<T, StringTraits, StringAllocator>,
          T> {};

// Serialize an indexed type like `std::tuple`, `std::pair`, or `std::array`.
template <typename Reader, typename Writer, typename Val, size_t kIndex,
          size_t kMaxIndex>
struct IndexedSerializer {

  HYDE_RT_FLATTEN HYDE_RT_INLINE
  static void Write(Writer &writer, const Val &data) {
    if constexpr (kIndex < kMaxIndex) {
      const auto &elem = std::get<kIndex>(data);
      using ElemT = std::remove_const_t<std::remove_reference_t<decltype(elem)>>;
      Serializer<Reader, Writer, ElemT>::Write(writer, elem);
      if constexpr ((kIndex + 1u) < kMaxIndex) {
        IndexedSerializer<NullReader, Writer, Val, kIndex + 1u, kMaxIndex>::Write(
            writer, data);
      }
    }
  }

  HYDE_RT_FLATTEN HYDE_RT_INLINE
  static void Read(Reader &writer, Val &data) {
    if constexpr (kIndex < kMaxIndex) {
      auto &elem = std::get<kIndex>(data);
      using ElemT = std::remove_reference_t<decltype(elem)>;
      Serializer<Reader, Writer, ElemT>::Read(writer, elem);
      if constexpr ((kIndex + 1u) < kMaxIndex) {
        IndexedSerializer<Reader, NullWriter, Val, kIndex + 1u, kMaxIndex>::Read(
            writer, data);
      }
    }
  }
};

template <typename Reader, typename Writer, typename A, typename B>
struct Serializer<Reader, Writer, std::pair<A, B>>
    : public IndexedSerializer<Reader, Writer, std::pair<A, B>, 0, 2> {

  static constexpr bool kIsFixedSize =
      kHasTrivialFixedSizeSerialization<A> &&
      kHasTrivialFixedSizeSerialization<B>;

  static constexpr uint32_t SizeInBytes(void) noexcept {
    if constexpr (kIsFixedSize) {
      return kFixedSerializationSize<A> + kFixedSerializationSize<B>;
    } else {
      return 0u;
    }
  }
};

template <typename Reader, typename Writer, typename... Elems>
struct Serializer<Reader, Writer, std::tuple<Elems...>>
    : public IndexedSerializer<Reader, Writer, std::tuple<Elems...>,
                               0, std::tuple_size_v<std::tuple<Elems...>>> {

  static constexpr bool kIsFixedSize =
      (kHasTrivialFixedSizeSerialization<Elems> && ... && true);

  static constexpr uint32_t SizeInBytes(void) noexcept {
    if constexpr (kIsFixedSize) {
      return (kFixedSerializationSize<Elems> + ... + 0u);
    } else {
      return 0u;
    }
  }
};

template <typename Reader, typename Writer, typename ElemT, size_t kSize>
struct Serializer<Reader, Writer, std::array<ElemT, kSize>>
    : public IndexedSerializer<Reader, Writer, std::array<ElemT, kSize>,
                               0, kSize> {
  static constexpr bool kIsFixedSize =
      kHasTrivialFixedSizeSerialization<ElemT>;


  static constexpr uint32_t SizeInBytes(void) noexcept {
    if constexpr (kHasTrivialFixedSizeSerialization<ElemT>) {
      return static_cast<uint32_t>(kFixedSerializationSize<ElemT> * kSize);
    } else {
      return 0u;
    }
  }
};

}  // namespace rt
}  // namespace hyde

#define HYDE_RT_SERIALIZER_NAMESPACE_BEGIN namespace hyde::rt {
#define HYDE_RT_SERIALIZER_NAMESPACE_END }
