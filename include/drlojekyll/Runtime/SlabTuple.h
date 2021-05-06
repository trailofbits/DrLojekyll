// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <tuple>
#include <utility>

#include "SlabReference.h"

namespace hyde {
namespace rt {

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
    return ::hyde::rt::TypedSlabReference<T>(elems[kIndex].first,
                                             elems[kIndex].second);
  }

  const std::pair<const uint8_t *, uint32_t> elems[sizeof...(Ts)];
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
HYDE_RT_FLATTEN HYDE_RT_ALWAYS_INLINE static auto get(
    const ::hyde::rt::SlabTuple<Ts...> &tuple) noexcept {
  return tuple.template get<kIndex>();
}

}  // namespace std
