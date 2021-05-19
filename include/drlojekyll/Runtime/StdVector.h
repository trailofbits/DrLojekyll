// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <algorithm>
#include <cstddef>
#include <tuple>
#include <deque>

#include <drlojekyll/Runtime/Runtime.h>

namespace hyde {
namespace rt {

class StdStorage;

template <typename... ElemTypes>
class StdVector {
 private:

  using TupleType = std::tuple<ElemTypes...>;
  std::deque<TupleType> entries;

 public:
  using Self = StdVector<ElemTypes...>;

  HYDE_RT_ALWAYS_INLINE void Add(ElemTypes... elems) noexcept {
    entries.emplace_back(std::move(elems)...);
  }

  HYDE_RT_ALWAYS_INLINE size_t Size(void) const noexcept {
    return entries.size();
  }

  HYDE_RT_ALWAYS_INLINE void SortAndUnique(void) noexcept {
//    std::sort(entries.begin(), entries.end());
//    auto it = std::unique(entries.begin(), entries.end());
//    entries.erase(it, entries.end());
  }

  HYDE_RT_ALWAYS_INLINE void Swap(Self &that) noexcept {
    entries.swap(that.entries);
  }

  HYDE_RT_ALWAYS_INLINE void Clear(void) noexcept {
    entries.clear();
  }

  HYDE_RT_ALWAYS_INLINE
  auto begin(void) const noexcept -> decltype(this->entries.begin()) {
    return entries.begin();
  }

  HYDE_RT_ALWAYS_INLINE
  auto end(void) const noexcept -> decltype(this->entries.end()) {
    return entries.end();
  }
};

template <typename... ElemTypes>
class Vector<StdStorage, ElemTypes...>
    : public StdVector<ElemTypes...> {
 public:
  HYDE_RT_ALWAYS_INLINE Vector(StdStorage &) {}
};

template <typename... ElemTypes>
class SerializedVector<StdStorage, ElemTypes...>
    : public StdVector<ElemTypes...> {
 public:
  HYDE_RT_ALWAYS_INLINE SerializedVector(StdStorage &) {}
};


}  // namespace rt
}  // namespace hyde
