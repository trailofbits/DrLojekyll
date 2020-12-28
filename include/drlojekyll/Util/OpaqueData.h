// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

#include <cstdint>
#include <type_traits>

namespace hyde {

// Extract a value from `a:b`.
template <uint64_t kOffsetBits, uint64_t kSizeBits>
static uint64_t LoadFromPair(uint64_t a, uint64_t b) {
  static_assert((kOffsetBits + kSizeBits) <= 128u);

  static constexpr uint64_t kBitMask =
      kSizeBits == 64 ? ~0ull : (1ull << kSizeBits) - 1ull;

  // Extracting from `b`.
  if constexpr (kOffsetBits >= 64u) {
    return LoadFromPair<kOffsetBits - 64u, kSizeBits>(b, 0);

  // Extracting from `a`.
  } else if constexpr ((kOffsetBits + kSizeBits) <= 64u) {
    return (a >> kOffsetBits) & kBitMask;

  // Extract `b:a`, i.e. high bits from `a` to form the low bits of our
  // extracted value, combined with the low bits from `b` to form the high
  // bits of our extracted value.
  } else {
    static constexpr uint64_t kNumABits = 64u - kOffsetBits;
    static constexpr uint64_t kNumBBits = kOffsetBits + kSizeBits - 64u;
    static constexpr uint64_t kLowBitMask = (1ull << kNumABits) - 1ull;
    static constexpr uint64_t kHighBitMask = (1ull << kNumBBits) - 1ull;

    const uint64_t low_bits = (a >> kOffsetBits) & kLowBitMask;
    const uint64_t high_bits = b & kHighBitMask;
    const uint64_t val = low_bits | (high_bits << kNumABits);
    return val;
  }
}

// Store `val` into `a:b`.
template <uint64_t kOffsetBits, uint64_t kSizeBits>
static void StoreToPair(uint64_t &a, uint64_t &b, uint64_t val) {
  static_assert((kOffsetBits + kSizeBits) <= 128u);

  static constexpr uint64_t kBitMask =
      kSizeBits == 64 ? ~0ull : (1ull << kSizeBits) - 1ull;

  // Storing to `b`.
  if constexpr (kOffsetBits >= 64u) {
    StoreToPair<kOffsetBits - 64u, kSizeBits>(b, a, val);

  // Storing to `a`.
  } else if constexpr ((kOffsetBits + kSizeBits) <= 64u) {
    static constexpr uint64_t kHighBitMask = (kBitMask << kOffsetBits);
    a = (a & ~kHighBitMask) | ((val & kBitMask) << kOffsetBits);

  // Extract `b:a`, i.e. high bits from `a` to form the low bits of our
  // extracted value, combined with the low bits from `b` to form the high
  // bits of our extracted value.
  } else {
    static constexpr uint64_t kNumABits = 64u - kOffsetBits;
    static constexpr uint64_t kNumBBits = kOffsetBits + kSizeBits - 64u;

    static constexpr uint64_t kLowBitMask = (1ull << kNumABits) - 1ull;
    static constexpr uint64_t kHighBitMask = (1ull << kNumBBits) - 1ull;

    const auto val_low = val & kLowBitMask;
    const auto val_high = (val >> (64u - kOffsetBits)) & kHighBitMask;

    a = (a & ~(kLowBitMask << kOffsetBits)) | (val_low << kOffsetBits);
    b = (b & ~kHighBitMask) | val_high;
  }
}

template <typename Desired, uint64_t kOffset, typename... All>
struct Extractor;

template <typename Desired, uint64_t kOffset, typename First, typename... Rest>
struct Extractor<Desired, kOffset, First, Rest...> final {
 public:
  static constexpr auto kSizeInBits = sizeof(First) * 8u;

  inline static uint64_t Load(uint64_t a, uint64_t b) noexcept {
    if constexpr (std::is_same_v<Desired, First>) {
      return LoadFromPair<kOffset, kSizeInBits>(a, b);
    } else {
      return Extractor<Desired, kOffset + kSizeInBits, Rest...>::Load(a, b);
    }
  }

  inline static void Store(uint64_t &a, uint64_t &b, uint64_t val) noexcept {
    if constexpr (std::is_same_v<Desired, First>) {
      StoreToPair<kOffset, kSizeInBits>(a, b, val);
    } else {
      Extractor<Desired, kOffset + kSizeInBits, Rest...>::Store(a, b, val);
    }
  }
};

template <typename Desired, uint64_t kOffset, typename Final>
struct Extractor<Desired, kOffset, Final> final {
 public:
  static constexpr auto kSizeInBits = sizeof(Final) * 8u;

  inline static uint64_t Load(uint64_t a, uint64_t b) noexcept {
    static_assert(std::is_same_v<Desired, Final>);
    return LoadFromPair<kOffset, kSizeInBits>(a, b);
  }

  inline static void Store(uint64_t &a, uint64_t &b, uint64_t val) noexcept {
    static_assert(std::is_same_v<Desired, Final>);
    StoreToPair<kOffset, kSizeInBits>(a, b, val);
  }
};

struct OpaqueData {
 public:
  template <typename DerivedType>
  inline DerivedType As(void) const noexcept {
    static_assert(std::is_convertible_v<DerivedType *, OpaqueData *>);
    return reinterpret_cast<const DerivedType &>(*this);
  }

  template <typename DerivedType>
  inline DerivedType &As(void) noexcept {
    static_assert(std::is_convertible_v<DerivedType *, OpaqueData *>);
    return reinterpret_cast<DerivedType &>(*this);
  }

  inline bool operator==(OpaqueData that) const noexcept {
    return a == that.a && b == that.b;
  }

  inline bool operator!=(OpaqueData that) const noexcept {
    return a != that.a || b != that.b;
  }

 protected:
  uint64_t a{0}, b{0};
};

template <typename... Args>
struct TypedOpaqueData : public OpaqueData {

  static_assert((sizeof(Args) + ... + 0u) <= 16u);

  template <typename T>
  T Load(void) const noexcept {
    return static_cast<T>(Extractor<T, 0, Args...>::Load(a, b));
  }

  template <typename T, typename U>
  void Store(U val) noexcept {
    Extractor<T, 0, Args...>::Store(a, b, static_cast<uint64_t>(val));
  }
};

template <typename T>
struct BoxedType {
  inline BoxedType(T val_) : val(val_) {}

  inline BoxedType(uint64_t val_) : val(static_cast<T>(val_)) {}

  inline void operator=(T val_) noexcept {
    val = val_;
  }

  inline void operator=(uint64_t val_) noexcept {
    val = static_cast<T>(val_);
  }

  inline operator T(void) const {
    return val;
  }

  T val;
};

template <>
struct BoxedType<uint64_t> {
  inline BoxedType(uint64_t val_) : val(val_) {}

  inline void operator=(uint64_t val_) noexcept {
    val = val_;
  }

  inline operator uint64_t(void) const {
    return val;
  }
  uint64_t val;
};

template <>
struct BoxedType<uint32_t> {
  inline BoxedType(uint64_t val_) : val(static_cast<uint32_t>(val_)) {}

  inline void operator=(uint64_t val_) noexcept {
    val = static_cast<uint32_t>(val_);
  }

  inline operator uint32_t(void) const {
    return val;
  }
  uint32_t val;
};

template <>
struct BoxedType<uint16_t> {
  inline BoxedType(uint64_t val_) : val(static_cast<uint16_t>(val_)) {}

  inline void operator=(uint64_t val_) noexcept {
    val = static_cast<uint16_t>(val_);
  }

  inline operator uint16_t(void) const {
    return val;
  }
  uint16_t val;
};

template <>
struct BoxedType<uint8_t> {
  inline BoxedType(uint64_t val_) : val(static_cast<uint8_t>(val_)) {}

  inline void operator=(uint64_t val_) noexcept {
    val = static_cast<uint8_t>(val_);
  }

  inline operator uint8_t(void) const {
    return val;
  }

  uint8_t val;
};

#define DEFINE_BOXED_TYPE(name, underlying_type) \
  struct name final : public ::hyde::BoxedType<underlying_type> { \
    using BoxedType::BoxedType; \
  }; \
  static_assert(sizeof(name) == sizeof(underlying_type))

}  // namespace hyde
