// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <algorithm>
#include <cstdint>
#include <new>

#include <drlojekyll/Runtime/Serializer.h>

#include "Slab.h"

namespace hyde {
namespace rt {

class Slab;
class SlabListWriter;
class SlabListReader;
class SlabManager;
class SlabReference;
class SlabStorage;
class SlabVector;
class UnsafeSlabListWriter;
class UnsafeSlabListReader;

template <typename, typename...>
class TypedSlabVectorVectorIterator;

// A discontinuous storage region of bytes.
class SlabList {
 public:
  SlabList(const SlabList &) = default;
  SlabList &operator=(const SlabList &) = default;

  HYDE_RT_ALWAYS_INLINE SlabList(SlabList &&that) noexcept
      : first(that.first),
        last(that.last) {
    that.first = nullptr;
    that.last = nullptr;
  }

  HYDE_RT_ALWAYS_INLINE SlabList &operator=(SlabList &&that) noexcept {
    first = that.first;
    last = that.last;
    that.first = nullptr;
    that.last = nullptr;
    return *this;
  }

  HYDE_RT_ALWAYS_INLINE void Swap(SlabList &that) noexcept {
    std::swap(first, that.first);
    std::swap(last, that.last);
  }

 private:
  friend class SlabListWriter;
  friend class SlabListReader;
  friend class SlabStorage;
  friend class SlabVector;
  friend class UnsafeSlabListWriter;
  friend class UnsafeSlabListReader;

  HYDE_RT_ALWAYS_INLINE SlabList(Slab *first_, Slab *last_) noexcept
      : first(first_),
        last(last_) {}

  HYDE_RT_ALWAYS_INLINE SlabList(std::pair<Slab *, Slab *> p) noexcept
      : first(p.first),
        last(p.second) {}


  SlabList(void) = default;

  Slab *first{nullptr};
  Slab *last{nullptr};
};

// A writer for writing bytes into a discontinuous backing buffer. Individual
// fundamental types are always stored sequentially. This writer is considered
// unsafe because no bounds checking is performed.
class UnsafeSlabListWriter : public UnsafeByteWriter {
 public:
  explicit UnsafeSlabListWriter(SlabManager &, SlabList &,
                                bool is_persistent = false);

  [[gnu::hot]] HYDE_RT_ALWAYS_INLINE ~UnsafeSlabListWriter(void) {
    UpdateSlabSize();
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE
  uint8_t *WritePointer(void *ptr) noexcept {
    auto addr = reinterpret_cast<intptr_t>(ptr);
    auto write_addr = reinterpret_cast<intptr_t>(write_ptr);
    return WriteI64(addr - write_addr);
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE
  uint8_t *WriteF64(double d) noexcept {
    const auto ptr = write_ptr;
    write_ptr += 8;
#ifndef HYDE_RT_DISALLOW_UNALIGNED_ACCESS
    *(new (ptr) double) = d;
#else
    const auto q = reinterpret_cast<uint64_t &>(d);
    ptr[0] = static_cast<uint8_t>(q >> 0);
    ptr[1] = static_cast<uint8_t>(q >> 8);
    ptr[2] = static_cast<uint8_t>(q >> 16);
    ptr[3] = static_cast<uint8_t>(q >> 24);
    ptr[4] = static_cast<uint8_t>(q >> 32);
    ptr[5] = static_cast<uint8_t>(q >> 40);
    ptr[6] = static_cast<uint8_t>(q >> 48);
    ptr[7] = static_cast<uint8_t>(q >> 56);
#endif
    return ptr;
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE
  uint8_t *WriteF32(float f) noexcept {
    const auto ptr = write_ptr;
    write_ptr += 4;
#ifndef HYDE_RT_DISALLOW_UNALIGNED_ACCESS
    *(new (ptr) float) = f;
#else
    const auto d = reinterpret_cast<uint32_t &>(f);
    ptr[0] = static_cast<uint8_t>(d >> 0);
    ptr[1] = static_cast<uint8_t>(d >> 8);
    ptr[2] = static_cast<uint8_t>(d >> 16);
    ptr[3] = static_cast<uint8_t>(d >> 24);
#endif
    return ptr;
  }

  [[gnu::hot]] HYDE_RT_ALWAYS_INLINE uint8_t *WriteU64(uint64_t q) noexcept {
    const auto ptr = write_ptr;
    write_ptr += 8;
#ifndef HYDE_RT_DISALLOW_UNALIGNED_ACCESS
    *(new (ptr) uint64_t) = q;
#else
    ptr[0] = static_cast<uint8_t>(q >> 0);
    ptr[1] = static_cast<uint8_t>(q >> 8);
    ptr[2] = static_cast<uint8_t>(q >> 16);
    ptr[3] = static_cast<uint8_t>(q >> 24);
    ptr[4] = static_cast<uint8_t>(q >> 32);
    ptr[5] = static_cast<uint8_t>(q >> 40);
    ptr[6] = static_cast<uint8_t>(q >> 48);
    ptr[7] = static_cast<uint8_t>(q >> 56);
#endif
    return ptr;
  }

  [[gnu::hot]] HYDE_RT_ALWAYS_INLINE uint8_t *WriteU32(uint32_t d) noexcept {
    const auto ptr = write_ptr;
    write_ptr += 4;
#ifndef HYDE_RT_DISALLOW_UNALIGNED_ACCESS
    *(new (ptr) uint32_t) = d;
#else
    ptr[0] = static_cast<uint8_t>(d >> 0);
    ptr[1] = static_cast<uint8_t>(d >> 8);
    ptr[2] = static_cast<uint8_t>(d >> 16);
    ptr[3] = static_cast<uint8_t>(d >> 24);
#endif
    return ptr;
  }

  [[gnu::hot]] HYDE_RT_ALWAYS_INLINE uint8_t *WriteU16(uint16_t h) noexcept {
    const auto ptr = write_ptr;
    write_ptr += 2;
#ifndef HYDE_RT_DISALLOW_UNALIGNED_ACCESS
    *(new (ptr) uint16_t) = h;
#else
    ptr[0] = static_cast<uint8_t>(h >> 0);
    ptr[1] = static_cast<uint8_t>(h >> 8);
#endif
    return ptr;
  }

  [[gnu::hot]] HYDE_RT_ALWAYS_INLINE uint8_t *WriteU8(uint8_t b) noexcept {
    auto ptr = write_ptr;
    write_ptr += 1u;
    ptr[0] = b;
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
    return WriteU8(static_cast<uint8_t>(b));
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE
  uint8_t *WriteB(bool b) noexcept {
    return WriteU8(static_cast<uint8_t>(!!b));
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

  [[gnu::hot]] HYDE_RT_ALWAYS_INLINE bool
  CanWriteUnsafely(size_t num_bytes) const noexcept {
    return static_cast<size_t>(max_write_ptr - write_ptr) >= num_bytes;
  }

 protected:
  SlabManager &manager;
  Slab **const last_ptr;
  const uint8_t *max_write_ptr;

  [[gnu::hot]] void UpdateWritePointer(void);
  [[gnu::hot]] void UpdateSlabSize(void);
};

// A writer for writing bytes into a discontinuous backing buffer. Individual
// fundamental types are always stored sequentially. This writer is considered
// safe because bounds checking is performed.
class SlabListWriter : public UnsafeSlabListWriter {
 public:
  using UnsafeSlabListWriter::UnsafeSlabListWriter;

  // Bytes should only be writable once.
  SlabListWriter(const SlabListWriter &) = delete;
  SlabListWriter(SlabListWriter &&) noexcept = delete;
  SlabListWriter &operator=(const SlabListWriter &) = delete;
  SlabListWriter &operator=(SlabListWriter &&) noexcept = delete;

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE
  uint8_t *WritePointer(void *ptr) noexcept {
    if (&(write_ptr[7]) >= max_write_ptr) {
      UpdateWritePointer();
    }
    return UnsafeSlabListWriter::WritePointer(ptr);
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE
  uint8_t *WriteF64(double d) noexcept {
    if (&(write_ptr[7]) >= max_write_ptr) {
      UpdateWritePointer();
    }
    return UnsafeSlabListWriter::WriteF64(d);
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE
  uint8_t *WriteF32(float f) noexcept {
    if (&(write_ptr[3]) >= max_write_ptr) {
      UpdateWritePointer();
    }
    return UnsafeSlabListWriter::WriteF32(f);
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE
  uint8_t *WriteU64(uint64_t q) noexcept {
    if (&(write_ptr[7]) >= max_write_ptr) {
      UpdateWritePointer();
    }
    return UnsafeSlabListWriter::WriteU64(q);
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE
  uint8_t *WriteU32(uint32_t d) noexcept {
    if (&(write_ptr[3]) >= max_write_ptr) {
      UpdateWritePointer();
    }
    return UnsafeSlabListWriter::WriteU32(d);
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE
  uint8_t *WriteU16(uint16_t h) noexcept {
    if (&(write_ptr[1]) >= max_write_ptr) {
      UpdateWritePointer();
    }
    return UnsafeSlabListWriter::WriteU16(h);
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE
  uint8_t *WriteU8(uint8_t b) noexcept {
    if (write_ptr >= max_write_ptr) {
      UpdateWritePointer();
    }
    return UnsafeSlabListWriter::WriteU8(b);
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE
  uint8_t *WriteI64(int64_t q) noexcept {
    return WriteU64(static_cast<uint64_t>(q));
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE
  uint8_t *WriteI32(int32_t q) noexcept {
    return WriteU32(static_cast<uint32_t>(q));
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE
  uint8_t *WriteI16(int16_t q) noexcept {
    return WriteU16(static_cast<uint16_t>(q));
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE
  uint8_t *WriteI8(int8_t q) noexcept {
    return WriteU8(static_cast<uint8_t>(q));
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE
  uint8_t *WriteB(bool b) noexcept {
    return WriteU8(static_cast<uint8_t>(!!b));
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE
  uint8_t *WriteSize(uint32_t d) noexcept {
    return WriteU32(d);
  }

  [[gnu::hot]] uint8_t *Skip(uint32_t num_bytes) noexcept {
    assert(0u < num_bytes);

    if (HYDE_RT_UNLIKELY(write_ptr == max_write_ptr)) {
      UpdateWritePointer();
    }

    uint8_t * const ret = write_ptr;
    const auto new_write_ptr = &(write_ptr[num_bytes]);
    if (HYDE_RT_UNLIKELY(new_write_ptr > max_write_ptr)) {
      SkipSlow(num_bytes);
    } else {
      write_ptr = new_write_ptr;
    }

    return ret;
  }

 private:
  void SkipSlow(uint32_t num_bytes);

  SlabListWriter(void) = delete;
};

// A reader for reading the discontinuous data in a `SlabList`. This reader is
// considered unsafe because no bounds checking is performed.
class UnsafeSlabListReader {
 public:
  explicit UnsafeSlabListReader(const UnsafeSlabListReader &) = default;

  explicit UnsafeSlabListReader(SlabList) noexcept;

  // Constructor for use by a `SlabReference`.
  explicit UnsafeSlabListReader(uint8_t *read_ptr_,
                                uint32_t num_bytes = 0) noexcept;

  // Have we reached the soft limit, i.e. the end of the current slab.
  [[gnu::hot]] HYDE_RT_ALWAYS_INLINE bool SoftHasMore(void) const noexcept {
    return read_ptr < max_read_ptr;
  }

  // Have we reached hard limit, i.e. the end of the slab list.
  HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE bool HardHasMore(void) noexcept {
    UpdateReadPointer();
    return SoftHasMore();
  }

  [[gnu::hot]] HYDE_RT_ALWAYS_INLINE bool
  CanReadUnsafely(size_t num_bytes) const noexcept {
    const auto read_addr = reinterpret_cast<uintptr_t>(read_ptr);
    const auto max_read_addr = (read_addr + kSlabSize) & ~(kSlabSize - 1u);
    return (max_read_addr - read_addr) >= num_bytes;
  }

  [[gnu::hot]] HYDE_RT_ALWAYS_INLINE void *ReadPointer(void) noexcept {
    const auto read_addr = reinterpret_cast<intptr_t>(read_ptr);
    const auto disp = ReadI64();
    return reinterpret_cast<void *>(read_addr + disp);
  }

  [[gnu::hot]] HYDE_RT_ALWAYS_INLINE double ReadF64(void) noexcept {
    const auto ptr = read_ptr;
    read_ptr += 8;
#ifndef HYDE_RT_DISALLOW_UNALIGNED_ACCESS
    return *(new (ptr) double);
#else
    alignas(double) uint8_t data[8u];
    data[0] = ptr[0];
    data[1] = ptr[1];
    data[2] = ptr[2];
    data[3] = ptr[3];
    data[4] = ptr[4];
    data[5] = ptr[5];
    data[6] = ptr[6];
    data[7] = ptr[7];
    return *(new (data) double);
#endif
  }

  [[gnu::hot]] HYDE_RT_ALWAYS_INLINE float ReadF32(void) noexcept {
    const auto ptr = read_ptr;
    read_ptr += 4;
#ifndef HYDE_RT_DISALLOW_UNALIGNED_ACCESS
    return *(new (ptr) float);
#else
    alignas(float) uint8_t data[4u];
    data[0] = ptr[0];
    data[1] = ptr[1];
    data[2] = ptr[2];
    data[3] = ptr[3];
    return *(new (data) float);
#endif
  }

  [[gnu::hot]] HYDE_RT_ALWAYS_INLINE uint64_t ReadU64(void) noexcept {
    const auto ptr = read_ptr;
    read_ptr += 8;
#ifndef HYDE_RT_DISALLOW_UNALIGNED_ACCESS
    return *(new (ptr) uint64_t);
#else
    alignas(uint64_t) uint8_t data[8u];
    data[0] = ptr[0];
    data[1] = ptr[1];
    data[2] = ptr[2];
    data[3] = ptr[3];
    data[4] = ptr[4];
    data[5] = ptr[5];
    data[6] = ptr[6];
    data[7] = ptr[7];
    return *(new (data) uint64_t);
#endif
  }

  [[gnu::hot]] HYDE_RT_ALWAYS_INLINE uint32_t ReadU32(void) noexcept {
    const auto ptr = read_ptr;
    read_ptr += 4;
#ifndef HYDE_RT_DISALLOW_UNALIGNED_ACCESS
    return *(new (ptr) uint32_t);
#else
    alignas(uint32_t) uint8_t data[4u];
    data[0] = ptr[0];
    data[1] = ptr[1];
    data[2] = ptr[2];
    data[3] = ptr[3];
    return *(new (data) uint32_t);
#endif
  }

  [[gnu::hot]] HYDE_RT_ALWAYS_INLINE uint16_t ReadU16(void) noexcept {
    const auto ptr = read_ptr;
    read_ptr += 2;
#ifndef HYDE_RT_DISALLOW_UNALIGNED_ACCESS
    return *(new (ptr) uint16_t);
#else
    alignas(uint16_t) uint8_t data[2u];
    data[0] = ptr[0];
    data[1] = ptr[1];
    return *(new (data) uint16_t);
#endif
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
    return !!ReadU8();
  }
  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE uint32_t
  ReadSize(void) noexcept {
    return ReadU32();
  }

  [[gnu::hot]] HYDE_RT_ALWAYS_INLINE void Skip(uint32_t num_bytes) noexcept {
#ifndef NDEBUG
    const auto read_addr = reinterpret_cast<uintptr_t>(read_ptr);
    const auto slab_addr = read_addr & ~(kSlabSize - 1ull);
    const auto next_slab_addr = slab_addr + kSlabSize;
    assert(slab_addr < read_addr);
    assert(read_addr < next_slab_addr);
#endif
    read_ptr += num_bytes;
    assert(read_addr <= next_slab_addr);
  }

  uint8_t *read_ptr;

 protected:

  friend class SlabList;
  friend class SlabReference;

  template <typename, typename...>
  friend class TypedSlabVectorVectorIterator;

  [[gnu::hot]] bool UpdateReadPointer(void) noexcept;

  uint8_t *max_read_ptr;
};

// A reader for reading the discontinuous data in a `SlabList`.
class SlabListReader : public UnsafeSlabListReader {
 public:
  using UnsafeSlabListReader::UnsafeSlabListReader;

  [[gnu::hot]] HYDE_RT_ALWAYS_INLINE void *ReadPointer(void) noexcept {
    if (&(read_ptr[7]) >= max_read_ptr) {
      UpdateReadPointer();
    }
    return UnsafeSlabListReader::ReadPointer();
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE double
  ReadF64(void) noexcept {
    if (&(read_ptr[7]) >= max_read_ptr) {
      UpdateReadPointer();
    }
    return UnsafeSlabListReader::ReadF64();
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE float
  ReadF32(void) noexcept {
    if (&(read_ptr[3]) >= max_read_ptr) {
      UpdateReadPointer();
    }
    return UnsafeSlabListReader::ReadF32();
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE uint64_t
  ReadU64(void) noexcept {
    if (&(read_ptr[7]) >= max_read_ptr) {
      UpdateReadPointer();
    }
    return UnsafeSlabListReader::ReadU64();
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE uint32_t
  ReadU32(void) noexcept {
    if (&(read_ptr[3]) >= max_read_ptr) {
      UpdateReadPointer();
    }
    return UnsafeSlabListReader::ReadU32();
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE uint16_t
  ReadU16(void) noexcept {
    if (&(read_ptr[1]) >= max_read_ptr) {
      UpdateReadPointer();
    }
    return UnsafeSlabListReader::ReadU16();
  }

  [[gnu::hot]] HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE uint8_t
  ReadU8(void) noexcept {
    if (read_ptr >= max_read_ptr) {
      UpdateReadPointer();
    }
    return UnsafeSlabListReader::ReadU8();
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
    const auto new_read_ptr = &(read_ptr[num_bytes]);
    if (HYDE_RT_UNLIKELY(new_read_ptr > max_read_ptr)) {
      SkipSlow(num_bytes);
    } else {
      read_ptr = new_read_ptr;
    }
  }

 private:
  template <typename, typename...>
  friend class TypedSlabVectorVectorIterator;

  void SkipSlow(uint32_t num_bytes);

  SlabListReader(void) = delete;
};

template <typename T>
static constexpr bool kIsSlabListReader = false;

template <>
static constexpr bool kIsSlabListReader<UnsafeSlabListReader> = true;

template <>
static constexpr bool kIsSlabListReader<SlabListReader> = true;

// Transfer data from one reader to a writer.
template <typename Writer, typename Reader>
static void TransferData(Writer &writer, Reader &reader, uint32_t num_bytes) {
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

}  // namespace rt
}  // namespace hyde
