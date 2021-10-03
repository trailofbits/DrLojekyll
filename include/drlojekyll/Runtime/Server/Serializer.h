// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <array>
#include <cstdint>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#define XXH_INLINE_ALL
#include "xxhash.h"

#include <drlojekyll/Runtime/Endian.h>
#include <drlojekyll/Runtime/Int.h>
#include "Reference.h"
#include "Util.h"

namespace hyde {
namespace rt {

enum class TupleState : uint8_t;

// An unserializer that always returns default-initialzied values.
struct NullReader {
 public:
  HYDE_RT_ALWAYS_INLINE void *ReadPointer(void) {
    return {};
  }
  HYDE_RT_ALWAYS_INLINE uint32_t ReadSize(void) {
    return {};
  }
  HYDE_RT_ALWAYS_INLINE double ReadF64(void) {
    return {};
  }
  HYDE_RT_ALWAYS_INLINE float ReadF32(void) {
    return {};
  }
  HYDE_RT_ALWAYS_INLINE uint64_t ReadU64(void) {
    return {};
  }
  HYDE_RT_ALWAYS_INLINE uint32_t ReadU32(void) {
    return {};
  }
  HYDE_RT_ALWAYS_INLINE uint16_t ReadU16(void) {
    return {};
  }
  HYDE_RT_ALWAYS_INLINE uint8_t ReadU8(void) {
    return {};
  }
  HYDE_RT_ALWAYS_INLINE bool ReadB(void) {
    return {};
  }
  HYDE_RT_ALWAYS_INLINE int64_t ReadI64(void) {
    return {};
  }
  HYDE_RT_ALWAYS_INLINE uint32_t ReadI32(void) {
    return {};
  }
  HYDE_RT_ALWAYS_INLINE int16_t ReadI16(void) {
    return {};
  }
  HYDE_RT_ALWAYS_INLINE int8_t ReadI8(void) {
    return {};
  }
  HYDE_RT_ALWAYS_INLINE void Skip(uint32_t num_bytes) {
    assert(0u < num_bytes);
  }
};

// A serializer that writes out nothing.
struct NullWriter {
 public:
  HYDE_RT_ALWAYS_INLINE uint8_t *Current(void) const noexcept {
    return nullptr;
  }
  HYDE_RT_ALWAYS_INLINE uint8_t *WritePointer(void *) noexcept {
    return nullptr;
  }
  HYDE_RT_ALWAYS_INLINE uint8_t *WriteSize(uint32_t) noexcept {
    return nullptr;
  }
  HYDE_RT_ALWAYS_INLINE uint8_t *WriteF64(double) noexcept { return nullptr; }
  HYDE_RT_ALWAYS_INLINE uint8_t *WriteF32(float) noexcept { return nullptr; }
  HYDE_RT_ALWAYS_INLINE uint8_t *WriteU64(uint64_t) noexcept { return nullptr; }
  HYDE_RT_ALWAYS_INLINE uint8_t *WriteU32(uint32_t) noexcept { return nullptr; }
  HYDE_RT_ALWAYS_INLINE uint8_t *WriteU16(uint16_t) noexcept { return nullptr; }
  HYDE_RT_ALWAYS_INLINE uint8_t *WriteU8(uint8_t) noexcept { return nullptr; }
  HYDE_RT_ALWAYS_INLINE uint8_t *WriteB(bool) noexcept { return nullptr; }
  HYDE_RT_ALWAYS_INLINE uint8_t *WriteI64(int64_t) noexcept { return nullptr; }
  HYDE_RT_ALWAYS_INLINE uint8_t *WriteI32(int32_t) noexcept { return nullptr; }
  HYDE_RT_ALWAYS_INLINE uint8_t *WriteI16(int16_t) noexcept { return nullptr; }
  HYDE_RT_ALWAYS_INLINE uint8_t *WriteI8(int8_t) noexcept { return nullptr; }
  HYDE_RT_ALWAYS_INLINE uint8_t *Skip(uint32_t num_bytes) noexcept {
    assert(0u < num_bytes);
    return nullptr;
  }
  HYDE_RT_ALWAYS_INLINE void EnterFixedSizeComposite(uint32_t) noexcept {}
  HYDE_RT_ALWAYS_INLINE void EnterVariableSizedComposite(uint32_t) noexcept {}
  HYDE_RT_ALWAYS_INLINE void ExitComposite(void) noexcept {}
};

// A writer for writing bytes into a continuous backing buffer. No bounds
// checking is performed.
template <typename Self>
class ByteWriter {
 public:
  HYDE_RT_ALWAYS_INLINE explicit ByteWriter(uint8_t *write_ptr_)
      : write_ptr(write_ptr_) {}

  HYDE_RT_ALWAYS_INLINE uint8_t *Current(void) const noexcept {
    return write_ptr;
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE
  uint8_t *WritePointer(void *ptr) noexcept {
    auto addr = reinterpret_cast<intptr_t>(ptr);
    auto write_addr = reinterpret_cast<intptr_t>(write_ptr);
    return WriteI64(addr - write_addr);
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE
  uint8_t *WriteF64(double d) noexcept {
    uint64_t q = {};
    *(new (&q) double) = d;
    const auto ptr =
        static_cast<Self *>(this)->WriteU8(static_cast<uint8_t>(q >> 0));
    static_cast<Self *>(this)->WriteU8(static_cast<uint8_t>(q >> 8));
    static_cast<Self *>(this)->WriteU8(static_cast<uint8_t>(q >> 16));
    static_cast<Self *>(this)->WriteU8(static_cast<uint8_t>(q >> 24));
    static_cast<Self *>(this)->WriteU8(static_cast<uint8_t>(q >> 32));
    static_cast<Self *>(this)->WriteU8(static_cast<uint8_t>(q >> 40));
    static_cast<Self *>(this)->WriteU8(static_cast<uint8_t>(q >> 48));
    static_cast<Self *>(this)->WriteU8(static_cast<uint8_t>(q >> 56));
    return ptr;
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE
  uint8_t *WriteF32(float f) noexcept {
    uint32_t d = {};
    *(new (&d) float) = f;
    const auto ptr =
        static_cast<Self *>(this)->WriteU8(static_cast<uint8_t>(d >> 0));
    static_cast<Self *>(this)->WriteU8(static_cast<uint8_t>(d >> 8));
    static_cast<Self *>(this)->WriteU8(static_cast<uint8_t>(d >> 16));
    static_cast<Self *>(this)->WriteU8(static_cast<uint8_t>(d >> 24));
    return ptr;
  }

  [[gnu::hot]] HYDE_RT_ALWAYS_INLINE uint8_t *WriteU64(uint64_t q) noexcept {
    const auto ptr =
        static_cast<Self *>(this)->WriteU8(static_cast<uint8_t>(q >> 0));
    static_cast<Self *>(this)->WriteU8(static_cast<uint8_t>(q >> 8));
    static_cast<Self *>(this)->WriteU8(static_cast<uint8_t>(q >> 16));
    static_cast<Self *>(this)->WriteU8(static_cast<uint8_t>(q >> 24));
    static_cast<Self *>(this)->WriteU8(static_cast<uint8_t>(q >> 32));
    static_cast<Self *>(this)->WriteU8(static_cast<uint8_t>(q >> 40));
    static_cast<Self *>(this)->WriteU8(static_cast<uint8_t>(q >> 48));
    static_cast<Self *>(this)->WriteU8(static_cast<uint8_t>(q >> 56));
    return ptr;
  }

  [[gnu::hot]] HYDE_RT_ALWAYS_INLINE uint8_t *WriteU32(uint32_t d) noexcept {
    const auto ptr =
        static_cast<Self *>(this)->WriteU8(static_cast<uint8_t>(d >> 0));
    static_cast<Self *>(this)->WriteU8(static_cast<uint8_t>(d >> 8));
    static_cast<Self *>(this)->WriteU8(static_cast<uint8_t>(d >> 16));
    static_cast<Self *>(this)->WriteU8(static_cast<uint8_t>(d >> 24));
    return ptr;
  }

  [[gnu::hot]] HYDE_RT_ALWAYS_INLINE uint8_t *WriteU16(uint16_t h) noexcept {
    const auto ptr =
        static_cast<Self *>(this)->WriteU8(static_cast<uint8_t>(h >> 0));
    static_cast<Self *>(this)->WriteU8(static_cast<uint8_t>(h >> 8));
    return ptr;
  }

  [[gnu::hot]] HYDE_RT_ALWAYS_INLINE uint8_t *WriteU8(uint8_t b) noexcept {
    const auto ptr = write_ptr;
    *write_ptr++ = b;
    return ptr;
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE
  uint8_t *WriteI64(int64_t q) noexcept {
    return WriteU64(static_cast<uint64_t>(q));
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE
  uint8_t *WriteI32(int32_t w) noexcept {
    return WriteU32(static_cast<uint32_t>(w));
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE
  uint8_t *WriteI16(int16_t h) noexcept {
    return WriteU16(static_cast<uint16_t>(h));
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE
  uint8_t *WriteI8(int8_t b) noexcept {
    return static_cast<Self *>(this)->WriteU8(static_cast<uint8_t>(b));
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE
  uint8_t *WriteB(bool b) noexcept {
    return static_cast<Self *>(this)->WriteU8(static_cast<uint8_t>(!!b));
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE
  uint8_t *WriteSize(uint32_t d) noexcept {
    return WriteU32(d);
  }

  [[gnu::hot]] HYDE_RT_ALWAYS_INLINE
  uint8_t *Skip(uint32_t num_bytes) noexcept {
    assert(0u < num_bytes);
    auto ptr = write_ptr;
    write_ptr += num_bytes;
    return ptr;
  }

  HYDE_RT_ALWAYS_INLINE void EnterFixedSizeComposite(uint32_t) {}
  HYDE_RT_ALWAYS_INLINE void EnterVariableSizedComposite(uint32_t) {}
  HYDE_RT_ALWAYS_INLINE void ExitComposite(void) {}

  uint8_t *write_ptr{nullptr};
};

// A writer for writing bytes into a continuous backing buffer. No bounds
// checking is performed.
class UnsafeByteWriter : public ByteWriter<UnsafeByteWriter> {
 public:
  using ByteWriter<UnsafeByteWriter>::WriteU8;

  HYDE_RT_ALWAYS_INLINE explicit UnsafeByteWriter(uint8_t *write_ptr_)
      : ByteWriter(write_ptr_) {}
};

template <typename Self>
class ByteReader {
 public:
  // Constructor for use by a `SlabReference`.
  explicit ByteReader(const uint8_t *read_ptr_) noexcept
      : read_ptr(read_ptr_) {}

  [[gnu::hot]] HYDE_RT_ALWAYS_INLINE void *ReadPointer(void) noexcept {
    const auto read_addr = reinterpret_cast<intptr_t>(read_ptr);
    const auto disp = ReadI64();
    return reinterpret_cast<void *>(read_addr + disp);
  }

  [[gnu::hot]] HYDE_RT_ALWAYS_INLINE double ReadF64(void) noexcept {
    alignas(double) uint8_t data[8u];
    data[0] = static_cast<Self *>(this)->ReadU8();
    data[1] = static_cast<Self *>(this)->ReadU8();
    data[2] = static_cast<Self *>(this)->ReadU8();
    data[3] = static_cast<Self *>(this)->ReadU8();
    data[4] = static_cast<Self *>(this)->ReadU8();
    data[5] = static_cast<Self *>(this)->ReadU8();
    data[6] = static_cast<Self *>(this)->ReadU8();
    data[7] = static_cast<Self *>(this)->ReadU8();
    return *(new (data) double);
  }

  [[gnu::hot]] HYDE_RT_ALWAYS_INLINE float ReadF32(void) noexcept {
    alignas(float) uint8_t data[4u];
    data[0] = static_cast<Self *>(this)->ReadU8();
    data[1] = static_cast<Self *>(this)->ReadU8();
    data[2] = static_cast<Self *>(this)->ReadU8();
    data[3] = static_cast<Self *>(this)->ReadU8();
    return *(new (data) float);
  }

  [[gnu::hot]] HYDE_RT_ALWAYS_INLINE uint64_t ReadU64(void) noexcept {
    alignas(uint64_t) uint8_t data[8u];
    data[0] = static_cast<Self *>(this)->ReadU8();
    data[1] = static_cast<Self *>(this)->ReadU8();
    data[2] = static_cast<Self *>(this)->ReadU8();
    data[3] = static_cast<Self *>(this)->ReadU8();
    data[4] = static_cast<Self *>(this)->ReadU8();
    data[5] = static_cast<Self *>(this)->ReadU8();
    data[6] = static_cast<Self *>(this)->ReadU8();
    data[7] = static_cast<Self *>(this)->ReadU8();
    return *(new (data) uint64_t);
  }

  [[gnu::hot]] HYDE_RT_ALWAYS_INLINE uint32_t ReadU32(void) noexcept {
    alignas(uint32_t) uint8_t data[4u];
    data[0] = static_cast<Self *>(this)->ReadU8();
    data[1] = static_cast<Self *>(this)->ReadU8();
    data[2] = static_cast<Self *>(this)->ReadU8();
    data[3] = static_cast<Self *>(this)->ReadU8();
    return *(new (data) uint32_t);
  }

  [[gnu::hot]] HYDE_RT_ALWAYS_INLINE uint16_t ReadU16(void) noexcept {
    alignas(uint16_t) uint8_t data[2];
    data[0] = static_cast<Self *>(this)->ReadU8();
    data[1] = static_cast<Self *>(this)->ReadU8();
    return *(new (data) uint16_t);
  }

  [[gnu::hot]] HYDE_RT_ALWAYS_INLINE uint8_t ReadU8(void) noexcept {
    return *read_ptr++;
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE int64_t
  ReadI64(void) noexcept {
    return static_cast<int64_t>(ReadU64());
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE int32_t
  ReadI32(void) noexcept {
    return static_cast<int32_t>(ReadU32());
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE int16_t
  ReadI16(void) noexcept {
    return static_cast<int16_t>(ReadU16());
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE int8_t
  ReadI8(void) noexcept {
    return static_cast<int8_t>(ReadU8());
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE bool ReadB(void) noexcept {
    return !!static_cast<Self *>(this)->ReadU8();
  }
  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE uint32_t
  ReadSize(void) noexcept {
    return ReadU32();
  }

  [[gnu::hot]] HYDE_RT_ALWAYS_INLINE void Skip(uint32_t num_bytes) noexcept {
    read_ptr += num_bytes;
  }

  const uint8_t *read_ptr;
};

// A reader for reading the discontinuous data in a `SlabList`. This reader is
// considered unsafe because no bounds checking is performed.
class UnsafeByteReader : public ByteReader<UnsafeByteReader> {
 public:
  using ByteReader<UnsafeByteReader>::ReadU8;

  HYDE_RT_ALWAYS_INLINE explicit UnsafeByteReader(const uint8_t *read_ptr_)
      : ByteReader(read_ptr_) {}

};

// A reader for reading the discontinuous data in a `SlabList`. This reader is
// considered unsafe because no bounds checking is performed.
class ByteRangeReader : public UnsafeByteReader {
 public:
  // Constructor for use by a `SlabReference`.
  explicit ByteRangeReader(const uint8_t *read_ptr_, size_t num_bytes) noexcept
      : UnsafeByteReader(read_ptr_),
        max_read_ptr(&(read_ptr_[num_bytes])) {}


  [[gnu::hot]] HYDE_RT_ALWAYS_INLINE void *ReadPointer(void) noexcept {
    if (&(read_ptr[7]) >= max_read_ptr) {
      error = true;
      return nullptr;
    } else {
      return UnsafeByteReader::ReadPointer();
    }
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE double
  ReadF64(void) noexcept {
    if (&(read_ptr[7]) >= max_read_ptr) {
      error = true;
      return {};
    } else {
      return UnsafeByteReader::ReadF64();
    }
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE float
  ReadF32(void) noexcept {
    if (&(read_ptr[3]) >= max_read_ptr) {
      error = true;
      return {};
    } else {
      return UnsafeByteReader::ReadF32();
    }
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE uint64_t
  ReadU64(void) noexcept {
    if (&(read_ptr[7]) >= max_read_ptr) {
      error = true;
      return {};
    } else {
      return UnsafeByteReader::ReadU64();
    }
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE uint32_t
  ReadU32(void) noexcept {
    if (&(read_ptr[3]) >= max_read_ptr) {
      error = true;
      return {};
    } else {
      return UnsafeByteReader::ReadU32();
    }
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE uint16_t
  ReadU16(void) noexcept {
    if (&(read_ptr[1]) >= max_read_ptr) {
      error = true;
      return {};
    } else {
      return UnsafeByteReader::ReadU16();
    }
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE uint8_t
  ReadU8(void) noexcept {
    if (read_ptr >= max_read_ptr) {
      error = true;
      return {};
    } else {
      return UnsafeByteReader::ReadU8();
    }
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE int64_t
  ReadI64(void) noexcept {
    return static_cast<int64_t>(ReadU64());
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE int32_t
  ReadI32(void) noexcept {
    return static_cast<int32_t>(ReadU32());
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE int16_t
  ReadI16(void) noexcept {
    return static_cast<int16_t>(ReadU16());
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE int8_t
  ReadI8(void) noexcept {
    return static_cast<int8_t>(ReadU8());
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE bool ReadB(void) noexcept {
    return !!ReadU8();
  }

  [[gnu::hot]] void Skip(uint32_t num_bytes) noexcept {
    read_ptr = &(read_ptr[num_bytes]);
    if (read_ptr > max_read_ptr) {
      error = true;
    }
  }

  const uint8_t *max_read_ptr;
  bool error{false};
};

struct HashingBase {
 public:
  HYDE_RT_ALWAYS_INLINE HashingBase(void) {
    Reset();
  }

  HYDE_RT_ALWAYS_INLINE void Reset(void) noexcept {
    XXH64_reset(&state, 0);
  }

  HYDE_RT_ALWAYS_INLINE uint64_t Digest(void) noexcept {
    return XXH64_digest(&state);
  }

  XXH64_state_t state;

  union {
    uint64_t u64;
    int64_t i64;
    double f64;
    float f32;
    uint8_t data[8];
  } u;

  static_assert(sizeof(u) == sizeof(uint64_t));
};

struct HashingWriter : public HashingBase {
 public:

  HYDE_RT_ALWAYS_INLINE uint8_t *Current(void) const noexcept {
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WritePointer(void *p) {
    u.u64 = reinterpret_cast<uintptr_t>(p);
    XXH64_update(&state, u.data, sizeof(u.data));
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteSize(uint32_t num_bytes) {
    u.u64 = num_bytes;
    XXH64_update(&state, u.data, sizeof(u.data));
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteF64(double d) {
    u.f64 = d;
    XXH64_update(&state, u.data, sizeof(u.data));
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteF32(float f) {
    u.u64 = 0;
    u.f32 = f;
    XXH64_update(&state, u.data, sizeof(u.data));
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteU64(uint64_t q) {
    u.u64 = q;
    XXH64_update(&state, u.data, sizeof(u.data));
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteU32(uint32_t d) {
    u.u64 = d;
    XXH64_update(&state, u.data, sizeof(u.data));
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteU16(uint16_t h) {
    u.u64 = h;
    XXH64_update(&state, u.data, sizeof(u.data));
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteU8(uint8_t b) {
    u.u64 = b;
    XXH64_update(&state, u.data, sizeof(u.data));
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteB(bool b) {
    u.u64 = b;
    XXH64_update(&state, u.data, sizeof(u.data));
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteI64(int64_t q) {
    u.i64 = q;
    XXH64_update(&state, u.data, sizeof(u.data));
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteI32(int32_t d) {
    u.i64 = d;
    XXH64_update(&state, u.data, sizeof(u.data));
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteI16(int16_t h) {
    u.i64 = h;
    XXH64_update(&state, u.data, sizeof(u.data));
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteI8(int8_t b) {
    u.i64 = b;
    XXH64_update(&state, u.data, sizeof(u.data));
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *Skip(uint32_t n) {
    assert(0u < n);
    u.u64 = n;
    XXH64_update(&state, u.data, sizeof(u.data));
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE void EnterFixedSizeComposite(uint32_t) {}
  HYDE_RT_ALWAYS_INLINE void EnterVariableSizedComposite(uint32_t) {}
  HYDE_RT_ALWAYS_INLINE void ExitComposite(void) {}

  HYDE_RT_ALWAYS_INLINE void Reset(void) noexcept {
    XXH64_reset(&state, 0);
  }

  HYDE_RT_ALWAYS_INLINE uint64_t Digest(void) noexcept {
    return XXH64_digest(&state);
  }
};

// A reader that computes a hash as it reads.
template <typename SubReader>
struct HashingReader : public SubReader, HashingBase {
 public:
  using SubReader::SubReader;

  HYDE_RT_ALWAYS_INLINE void *ReadPointer(void) {
    auto ret = SubReader::ReadPointer();
    u.u64 = reinterpret_cast<uintptr_t>(ret);
    XXH64_update(&state, u.data, sizeof(u.data));
    return ret;
  }

  // This is the one special case where we actual do the read.
  HYDE_RT_ALWAYS_INLINE uint32_t ReadSize(void) {
    auto ret = SubReader::ReadSize();
    u.u64 = ret;
    XXH64_update(&state, u.data, sizeof(u.data));
    return ret;
  }

  HYDE_RT_ALWAYS_INLINE double ReadF64(void) {
    auto ret = SubReader::ReadF64();
    u.f64 = ret;
    XXH64_update(&state, u.data, sizeof(u.data));
    return ret;
  }

  HYDE_RT_ALWAYS_INLINE float ReadF32(void) {
    auto ret = SubReader::ReadF32();
    u.f32 = ret;
    XXH64_update(&state, u.data, sizeof(u.data));
    return ret;
  }

  HYDE_RT_ALWAYS_INLINE uint64_t ReadU64(void) {
    auto ret = SubReader::ReadU64();
    u.u64 = ret;
    XXH64_update(&state, u.data, sizeof(u.data));
    return ret;
  }

  HYDE_RT_ALWAYS_INLINE uint32_t ReadU32(void) {
    auto ret = SubReader::ReadU32();
    u.u64 = ret;
    XXH64_update(&state, u.data, sizeof(u.data));
    return ret;
  }

  HYDE_RT_ALWAYS_INLINE uint16_t ReadU16(void) {
    auto ret = SubReader::ReadU16();
    u.u64 = ret;
    XXH64_update(&state, u.data, sizeof(u.data));
    return ret;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t ReadU8(void) {
    auto ret = SubReader::ReadU8();
    u.u64 = ret;
    XXH64_update(&state, u.data, sizeof(u.data));
    return ret;
  }

  HYDE_RT_ALWAYS_INLINE bool ReadB(void) {
    auto ret = SubReader::ReadB();
    u.u64 = ret;
    XXH64_update(&state, u.data, sizeof(u.data));
    return ret;
  }

  HYDE_RT_ALWAYS_INLINE int64_t ReadI64(void) {
    auto ret = SubReader::ReadI64();
    u.i64 = ret;
    XXH64_update(&state, u.data, sizeof(u.data));
    return ret;
  }

  HYDE_RT_ALWAYS_INLINE int32_t ReadI32(void) {
    auto ret = SubReader::ReadI32();
    u.i64 = ret;
    XXH64_update(&state, u.data, sizeof(u.data));
    return ret;
  }

  HYDE_RT_ALWAYS_INLINE int16_t ReadI16(void) {
    auto ret = SubReader::ReadI16();
    u.i64 = ret;
    XXH64_update(&state, u.data, sizeof(u.data));
    return ret;
  }

  HYDE_RT_ALWAYS_INLINE int8_t ReadI8(void) {
    auto ret = SubReader::ReadI8();
    u.i64 = ret;
    XXH64_update(&state, u.data, sizeof(u.data));
    return ret;
  }

  HYDE_RT_ALWAYS_INLINE void Skip(uint32_t n) {
    assert(0u < n);
    SubReader::Skip(n);
    u.u64 = n;
    XXH64_update(&state, u.data, sizeof(u.data));
  }
};

// A serializing writer that ignores the values being written, and instead
// counts the number of bytes that will be written.
template <typename SubWriter>
struct ByteCountingWriterProxy : public SubWriter {
 public:
  using SubWriter::SubWriter;

  HYDE_RT_ALWAYS_INLINE uint8_t *WritePointer(void *v) {
    num_bytes += 8;
    return SubWriter::WritePointer(v);
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteSize(uint32_t v) {
    num_bytes += 4;
    return SubWriter::WriteSize(v);
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteF64(double v) {
    num_bytes += 8;
    return SubWriter::WriteF64(v);
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteF32(float v) {
    num_bytes += 4;
    return SubWriter::WriteF32(v);
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteU64(uint64_t v) {
    num_bytes += 8;
    return SubWriter::WriteU64(v);
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteU32(uint32_t v) {
    num_bytes += 4;
    return SubWriter::WriteU32(v);
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteU16(uint16_t v) {
    num_bytes += 2;
    return SubWriter::WriteU16(v);
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteU8(uint8_t v) {
    num_bytes += 1;
    return SubWriter::WriteU8(v);
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteB(bool v) {
    num_bytes += 1;
    return SubWriter::WriteB(v);
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteI64(int64_t v) {
    num_bytes += 8;
    return SubWriter::WriteI64(v);
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteI32(int32_t v) {
    num_bytes += 4;
    return SubWriter::WriteI32(v);
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteI16(int16_t v) {
    num_bytes += 2;
    return SubWriter::WriteI16(v);
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteI8(int8_t v) {
    num_bytes += 1;
    return SubWriter::WriteI8(v);
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *Skip(uint32_t n) {
    assert(0u < n);
    num_bytes += n;
    return SubWriter::Skip(n);
  }

  uint32_t num_bytes{0};
};

using ByteCountingWriter = ByteCountingWriterProxy<NullWriter>;

template <typename T>
static constexpr bool kIsByteCountingWriter = false;

template <>
inline constexpr bool kIsByteCountingWriter<ByteCountingWriter> = true;

// A serializing writer that ignores the values being written, and instead
// performs an element-wise equality comparison.
template <typename Reader>
struct ByteEqualityComparingWriter : public Reader {
 public:
  using Reader::Reader;

  HYDE_RT_ALWAYS_INLINE uint8_t *Current(void) const noexcept {
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WritePointer(void *rhs) {
    if (!equal) {
      equal = static_cast<const uint8_t *>(Reader::ReadPointer()) ==
              static_cast<const uint8_t *>(rhs);
    }
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteSize(uint32_t rhs) {
    if (equal) {
      equal = Reader::ReadSize() == rhs;
    }
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteF64(double rhs) {
    if (equal) {
      equal = Reader::ReadF64() == rhs;
    }
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteF32(float rhs) {
    if (equal) {
      equal = Reader::ReadF32() == rhs;
    }
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteU64(uint64_t rhs) {
    if (equal) {
      equal = Reader::ReadU64() == rhs;
    }
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteU32(uint32_t rhs) {
    if (equal) {
      equal = Reader::ReadU32() == rhs;
    }
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteU16(uint16_t rhs) {
    if (equal) {
      equal = Reader::ReadU16() == rhs;
    }
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteU8(uint8_t rhs) {
    if (equal) {
      equal = Reader::ReadU8() == rhs;
    }
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteB(bool rhs) {
    if (equal) {
      equal = Reader::ReadB() == rhs;
    }
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteI64(int64_t rhs) {
    if (equal) {
      equal = Reader::ReadI64() == rhs;
    }
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteI32(int32_t rhs) {
    if (equal) {
      equal = Reader::ReadI32() == rhs;
    }
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteI16(int16_t rhs) {
    if (equal) {
      equal = Reader::ReadI16() == rhs;
    }
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteI8(int8_t rhs) {
    if (equal) {
      equal = Reader::ReadI8() == rhs;
    }
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *Skip(uint32_t n) {
    assert(0u < n);
    if (equal) {
      Reader::Skip(n);
    }
    return nullptr;
  }

  bool equal{true};
};

// A serializing writer that ignores the values being written, and instead
// performs an element-wise less-than comparison.
template <typename Reader>
struct ByteLessThanComparingWriter : public Reader {
 public:
  using Reader::Reader;

  HYDE_RT_ALWAYS_INLINE uint8_t *Current(void) const noexcept {
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WritePointer(void *rhs) {
    if (!less) {
      less = static_cast<const uint8_t *>(Reader::ReadPointer()) <
             static_cast<const uint8_t *>(rhs);
    }
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteSize(uint32_t rhs) {
    if (!less) {
      less = Reader::ReadSize() < rhs;
    }
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteF64(double rhs) {
    if (!less) {
      less = Reader::ReadF64() < rhs;
    }
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteF32(float rhs) {
    if (!less) {
      less = Reader::ReadF32() < rhs;
    }
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteU64(uint64_t rhs) {
    if (!less) {
      less = Reader::ReadU64() < rhs;
    }
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteU32(uint32_t rhs) {
    if (!less) {
      less = Reader::ReadU32() < rhs;
    }
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteU16(uint16_t rhs) {
    if (!less) {
      less = Reader::ReadU16() < rhs;
    }
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteU8(uint8_t rhs) {
    if (!less) {
      less = Reader::ReadU8() < rhs;
    }
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteB(bool rhs) {
    if (!less) {
      less = Reader::ReadB() < rhs;
    }
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteI64(int64_t rhs) {
    if (!less) {
      less = Reader::ReadI64() < rhs;
    }
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteI32(int32_t rhs) {
    if (!less) {
      less = Reader::ReadI32() < rhs;
    }
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteI16(int16_t rhs) {
    if (!less) {
      less = Reader::ReadI16() < rhs;
    }
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteI8(int8_t rhs) {
    if (!less) {
      less = Reader::ReadI8() < rhs;
    }
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *Skip(uint32_t n) {
    assert(0u < n);
    if (!less) {
      Reader::Skip(n);
    }
    return nullptr;
  }

  bool less{false};
};

// A serializing writer that ignores the values being written, and instead
// performs an element-wise greater-than comparison.
template <typename Reader>
struct ByteGreaterThanComparingWriter : public Reader {
 public:
  using Reader::Reader;

  HYDE_RT_ALWAYS_INLINE uint8_t *WritePointer(void *rhs) {
    if (!greater) {
      greater = static_cast<const uint8_t *>(Reader::ReadPointer()) >
                static_cast<const uint8_t *>(rhs);
    }
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteSize(uint32_t rhs) {
    if (!greater) {
      greater = Reader::ReadSize() > rhs;
    }
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteF64(double rhs) {
    if (!greater) {
      greater = Reader::ReadF64() > rhs;
    }
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteF32(float rhs) {
    if (!greater) {
      greater = Reader::ReadF32() > rhs;
    }
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteU64(uint64_t rhs) {
    if (!greater) {
      greater = Reader::ReadU64() > rhs;
    }
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteU32(uint32_t rhs) {
    if (!greater) {
      greater = Reader::ReadU32() > rhs;
    }
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteU16(uint16_t rhs) {
    if (!greater) {
      greater = Reader::ReadU16() > rhs;
    }
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteU8(uint8_t rhs) {
    if (!greater) {
      greater = Reader::ReadU8() > rhs;
    }
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteB(bool rhs) {
    if (!greater) {
      greater = Reader::ReadB() > rhs;
    }
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteI64(int64_t rhs) {
    if (!greater) {
      greater = Reader::ReadI64() > rhs;
    }
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteI32(int32_t rhs) {
    if (!greater) {
      greater = Reader::ReadI32() > rhs;
    }
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteI16(int16_t rhs) {
    if (!greater) {
      greater = Reader::ReadI16() > rhs;
    }
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *WriteI8(int8_t rhs) {
    if (!greater) {
      greater = Reader::ReadI8() > rhs;
    }
    return nullptr;
  }

  HYDE_RT_ALWAYS_INLINE uint8_t *Skip(uint32_t n) {
    assert(0u < n);
    if (!greater) {
      Reader::Skip(n);
    }
    return nullptr;
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
    assert(0u < n);
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
  inline constexpr bool kCanReadWriteUnsafely<type> = true

#define DRLOJEKYLL_MAKE_FUNDAMENTAL_SERIALIZER(type, cast_op, cast_type, \
                                               method_suffix, size) \
  HYDE_RT_SERIALIZER_NAMESPACE_BEGIN \
  template <typename Reader, typename Writer> \
  struct Serializer<Reader, Writer, type> { \
    static constexpr bool kIsFixedSize = true; \
    HYDE_RT_FLATTEN HYDE_RT_INLINE static uint8_t *Write(Writer &writer, \
                                                         type data) { \
      return writer.Write##method_suffix(cast_op<cast_type>(data)); \
    } \
    HYDE_RT_FLATTEN HYDE_RT_INLINE static void Read(Reader &reader, \
                                                    type &out) { \
      out = cast_op<type>(reader.Read##method_suffix()); \
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

DRLOJEKYLL_MAKE_FUNDAMENTAL_SERIALIZER(TupleState, static_cast, uint8_t, U8, 1)

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

template <typename Reader, typename Writer, typename T>
struct Serializer<Reader, Writer, T *> {
  static constexpr bool kIsFixedSize = true;
  static uint8_t *Write(Writer &writer, T *data) {
    return writer.WritePointer(reinterpret_cast<void *>(data));
  }

  static void Read(Reader &reader, T *&out) {
    out = reinterpret_cast<T *>(reader.ReadPointer());
  }

  static constexpr uint32_t SizeInBytes(void) noexcept {
    return 8;
  }
};

#ifndef HYDE_RT_MISSING_INT128
template <typename Reader, typename Writer>
struct Serializer<Reader, Writer, int128_t> {
  static constexpr bool kIsFixedSize = true;
  static uint8_t *Write(Writer &writer, int128_t val) {
    alignas(int128_t) uint8_t data[16u];
    *(new (data) int128_t) = val;

    if constexpr (HYDE_RT_LITTLE_ENDIAN) {
      auto ret = writer.WriteU8(data[0]);
      for (auto i = 1u; i < 16u; ++i) {
        writer.WriteU8(data[i]);
      }
      return ret;
    } else {
      auto ret = writer.WriteU8(data[16u - 1u]);
      for (auto i = 2u; i <= 16u; ++i) {
        writer.WriteU8(data[16u - i]);
      }
      return ret;
    }
  }

  static void Read(Reader &reader, int128_t &out) {
    alignas(int128_t) uint8_t data[16u];
    for (auto i = 0u; i < 16u; ++i) {
      data[i] = reader.ReadU8();
    }
    out = *(new (data) int128_t);
  }

  static constexpr uint32_t SizeInBytes(void) noexcept {
    return static_cast<unsigned>(sizeof(int32_t));
  }
};

template <typename Reader, typename Writer>
struct Serializer<Reader, Writer, uint128_t> {
  static constexpr bool kIsFixedSize = true;

  static uint8_t *Write(Writer &writer, uint128_t val) {
    alignas(uint128_t) uint8_t data[16u];
    *(new (data) uint128_t) = val;

    if constexpr (HYDE_RT_LITTLE_ENDIAN) {
      auto ret = writer.WriteU8(data[0]);
      for (auto i = 1u; i < 16u; ++i) {
        writer.WriteU8(data[i]);
      }
      return ret;
    } else {
      auto ret = writer.WriteU8(data[16u - 1u]);
      for (auto i = 2u; i <= 16u; ++i) {
        writer.WriteU8(data[16u - i]);
      }
      return ret;
    }
  }

  static void Read(Reader &reader, uint128_t &out) {
    alignas(uint128_t) uint8_t data[16];
    for (auto i = 0u; i < 16u; ++i) {
      data[i] = reader.ReadU8();
    }
    out = *(new (data) uint128_t);
  }

  static constexpr uint32_t SizeInBytes(void) noexcept {
    return static_cast<unsigned>(sizeof(uint32_t));
  }
};
#endif  // HYDE_RT_MISSING_INT128

template <typename Reader, typename Writer, typename DataT>
struct Serializer<Reader, Writer, InternRef<DataT>> {
 public:
  using RefT = InternRef<DataT>;

  HYDE_RT_INLINE
  static uint8_t *Write(Writer &writer, RefT ref) {
    return Serializer<Reader, Writer, DataT>::Write(writer, *(ref.ref));
  }

  HYDE_RT_INLINE
  static void Read(Reader &reader, RefT) {
    abort();
  }
};

template <typename Reader, typename Writer, typename DataT>
struct Serializer<Reader, Writer, const InternRef<DataT> &>
    : public Serializer<Reader, Writer, InternRef<DataT>> {};

template <typename T>
static constexpr bool kCanReadWriteUnsafely<T *> = true;

#undef HYDE_RT_DEFINE_UNSAFE_SERIALIZER_PRIV
#define HYDE_RT_DEFINE_UNSAFE_SERIALIZER_PRIV(type)

#undef HYDE_RT_SERIALIZER_NAMESPACE_BEGIN
#undef HYDE_RT_SERIALIZER_NAMESPACE_END

template <typename Reader, typename ContainerType, typename ElementType>
struct LinearContainerReader {
 public:
  HYDE_RT_FLATTEN HYDE_RT_INLINE static void Read(Reader &reader,
                                                  ContainerType &ret) {
    const auto size = reader.ReadSize();
    ret.resize(size);
    if (size) {
      ElementType *const begin = &(ret[0]);
      for (uint32_t i = 0; i < size; ++i) {
        Serializer<Reader, NullWriter, ElementType>::Read(reader, begin[i]);
      }
    }
  }
};

// Special case for the byte counting reader: we don't want to really have to
// construct any actual containers or anything like that, and instead just
// count the bytes.
template <typename SubReader, typename ContainerType, typename ElementType>
struct LinearContainerReader<ByteCountingReader<SubReader>, ContainerType,
                             ElementType> {
 public:
  using Reader = ByteCountingReader<SubReader>;

  HYDE_RT_FLATTEN HYDE_RT_INLINE static void Read(Reader &reader,
                                                  ContainerType &) {
    if (const auto size = reader.ReadSize(); size) {
      if (kHasTrivialFixedSizeSerialization<ElementType>) {
        reader.Skip(
            static_cast<uint32_t>(size * kFixedSerializationSize<ElementType>));
      } else {
        alignas(ElementType) char dummy_data[sizeof(ElementType)];
        for (uint32_t i = 0; i < size; ++i) {
          Serializer<Reader, NullWriter, ElementType>::Read(
              reader, *reinterpret_cast<ElementType *>(dummy_data));
        }
      }
    }
  }
};

// The generic linear container writer.
template <typename Writer, typename ContainerType, typename ElementType>
struct LinearContainerWriter {
 public:
  HYDE_RT_FLATTEN HYDE_RT_INLINE static uint8_t *Write(
      Writer &writer, const ContainerType &data) {
    const auto size = static_cast<uint32_t>(data.size());
    writer.EnterVariableSizedComposite(size);
    auto ret = writer.WriteSize(size);

    // NOTE(pag): Induction variable based `for` loop so that a a byte counting
    //            writer can elide the `for` loop entirely and count `size`.
    if (size) {
      const ElementType *const begin = &(data[0]);
      for (uint32_t i = 0; i < size; ++i) {
        Serializer<NullReader, Writer, ElementType>::Write(writer, begin[i]);
      }
    }

    writer.ExitComposite();
    return ret;
  }
};

// A specialization of a linear container writer that knows that all we want
// to do is count bytes.
template <typename ContainerType, typename ElementType>
struct LinearContainerWriter<ByteCountingWriter, ContainerType, ElementType> {
 public:
  HYDE_RT_FLATTEN HYDE_RT_INLINE static uint8_t *Write(
      ByteCountingWriter &writer, const ContainerType &data) {
    const auto size = static_cast<uint32_t>(data.size());
    writer.EnterVariableSizedComposite(size);
    auto ret = writer.WriteSize(size);
    if (size) {
      if (kHasTrivialFixedSizeSerialization<ElementType>) {
        writer.Skip(
            static_cast<uint32_t>(size * kFixedSerializationSize<ElementType>));

      } else {
        const ElementType *const begin = &(data[0]);
        for (uint32_t i = 0; i < size; ++i) {
          Serializer<NullReader, ByteCountingWriter, ElementType>::Write(
              writer, begin[i]);
        }
      }
    }
    writer.ExitComposite();
    return ret;
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
    : public LinearContainerSerializer<Reader, Writer,
                                       std::vector<T, VectorAllocator>, T> {};

template <typename Reader, typename Writer, typename T, typename StringTraits,
          typename StringAllocator>
struct Serializer<Reader, Writer,
                  std::basic_string<T, StringTraits, StringAllocator>>
    : public LinearContainerSerializer<
          Reader, Writer, std::basic_string<T, StringTraits, StringAllocator>,
          T> {};

// Serialize an indexed type like `std::tuple`, `std::pair`, or `std::array`.
template <typename Reader, typename Writer, typename Val, size_t kIndex,
          size_t kMaxIndex>
struct IndexedSerializer {

  HYDE_RT_FLATTEN HYDE_RT_INLINE static uint8_t *Write(Writer &writer,
                                                       const Val &data) {
    if constexpr (kIndex == 0u) {
      writer.EnterFixedSizeComposite(kMaxIndex);
    }
    if constexpr (kIndex < kMaxIndex) {
      const auto &elem = std::get<kIndex>(data);
      using ElemT =
          std::remove_const_t<std::remove_reference_t<decltype(elem)>>;
      auto ret = Serializer<Reader, Writer, ElemT>::Write(writer, elem);
      if constexpr ((kIndex + 1u) < kMaxIndex) {
        IndexedSerializer<NullReader, Writer, Val, kIndex + 1u,
                          kMaxIndex>::Write(writer, data);
      } else {
        writer.ExitComposite();
      }
      return ret;
    } else {
      return nullptr;
    }
  }

  HYDE_RT_FLATTEN HYDE_RT_INLINE static void Read(Reader &writer, Val &data) {
    if constexpr (kIndex < kMaxIndex) {
      auto &elem = std::get<kIndex>(data);
      using ElemT = std::remove_reference_t<decltype(elem)>;
      Serializer<Reader, Writer, ElemT>::Read(writer, elem);
      if constexpr ((kIndex + 1u) < kMaxIndex) {
        IndexedSerializer<Reader, NullWriter, Val, kIndex + 1u,
                          kMaxIndex>::Read(writer, data);
      }
    }
  }
};

template <typename Reader, typename Writer, typename A, typename B>
struct Serializer<Reader, Writer, std::pair<A, B>>
    : public IndexedSerializer<Reader, Writer, std::pair<A, B>, 0, 2> {

  static constexpr bool kIsFixedSize = kHasTrivialFixedSizeSerialization<A> &&
                                       kHasTrivialFixedSizeSerialization<B>;

  static constexpr uint32_t SizeInBytes(void) noexcept {
    if constexpr (kIsFixedSize) {
      return kFixedSerializationSize<A> + kFixedSerializationSize<B>;
    } else {
      return 0u;
    }
  }
};

template <typename Reader, typename Writer, typename Elem, typename... Elems>
struct Serializer<Reader, Writer, std::tuple<Elem, Elems...>>
    : public IndexedSerializer<Reader, Writer, std::tuple<Elem, Elems...>, 0,
                               std::tuple_size_v<std::tuple<Elem, Elems...>>> {

  static constexpr bool kIsFixedSize =
      kHasTrivialFixedSizeSerialization<Elem> &&
      (kHasTrivialFixedSizeSerialization<Elems> && ... && true);

  static constexpr uint32_t SizeInBytes(void) noexcept {
    if constexpr (kIsFixedSize) {
      return kFixedSerializationSize<Elem> +
             (kFixedSerializationSize<Elems> + ... + 0u);
    } else {
      return 0u;
    }
  }
};

template <typename Reader, typename Writer>
struct Serializer<Reader, Writer, std::tuple<>> {

  static constexpr bool kIsFixedSize = true;

  static constexpr uint32_t SizeInBytes(void) noexcept {
    return 0u;
  }

  HYDE_RT_FLATTEN HYDE_RT_INLINE static uint8_t *Write(
      Writer &writer, std::tuple<>) {
    return writer.Current();
  }

  HYDE_RT_FLATTEN HYDE_RT_INLINE static void Read(
      Reader &writer, std::tuple<> &) {}
};

template <typename Reader, typename Writer, typename ElemT, size_t kSize>
struct Serializer<Reader, Writer, std::array<ElemT, kSize>>
    : public IndexedSerializer<Reader, Writer, std::array<ElemT, kSize>, 0,
                               kSize> {
  static constexpr bool kIsFixedSize = kHasTrivialFixedSizeSerialization<ElemT>;


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
