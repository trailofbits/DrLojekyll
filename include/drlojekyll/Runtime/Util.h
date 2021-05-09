// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <cassert>
#include <cstddef>
#include <type_traits>

namespace hyde {
namespace rt {

#ifdef NDEBUG
# define HYDE_RT_LIKELY(...) __builtin_expect(!!(__VA_ARGS__), 1)
# define HYDE_RT_UNLIKELY(...) __builtin_expect(!!(__VA_ARGS__), 0)
# define HYDE_RT_INLINE inline
# define HYDE_RT_ALWAYS_INLINE [[gnu::always_inline]] HYDE_RT_INLINE
# define HYDE_RT_FLATTEN [[gnu::flatten]]
#else
# define HYDE_RT_LIKELY(...) __VA_ARGS__
# define HYDE_RT_UNLIKELY(...) __VA_ARGS__
# define HYDE_RT_INLINE
# define HYDE_RT_ALWAYS_INLINE HYDE_RT_INLINE
# define HYDE_RT_FLATTEN
#endif

template <unsigned... kIds>
struct IdList {};

template <typename T>
struct ValueType {
  using Type = T;
};

// A mutable wrapper around something else.
template <typename T>
class Mutable;

template <typename T>
static constexpr bool kIsMutable = false;

template <typename T>
static constexpr bool kIsMutable<Mutable<T>> = true;

template <typename T>
struct ValueType<Mutable<T>> : public ValueType<T> {};

template <typename T>
class Addressable;

template <typename T>
static constexpr bool kIsAddressable = false;

template <typename T>
static constexpr bool kIsAddressable<Mutable<T>> = true;

template <typename T>
static constexpr bool kIsAddressable<Addressable<T>> = true;

template <typename T>
struct ValueType<Addressable<T>> : public ValueType<T> {};

template <typename T>
struct Address {
  void *data;
};

template <typename T>
static constexpr bool kIsAddress = false;

template <typename T>
static constexpr bool kIsAddress<Address<T>> = true;

template <>
static constexpr bool kIsAddress<std::nullptr_t> = true;

template <typename T>
HYDE_RT_ALWAYS_INLINE
static T *ExtractAddress(Address<T> a) {
  return reinterpret_cast<T *>(a.data);
}

template <typename T>
HYDE_RT_ALWAYS_INLINE
static T *ExtractAddress(std::nullptr_t) {
  return nullptr;
}

}  // namespace rt
}  // namespace hyde
