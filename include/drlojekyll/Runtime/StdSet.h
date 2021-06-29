// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <algorithm>
#include <cstddef>
#include <tuple>
#include <unordered_set>

#include <drlojekyll/Runtime/Runtime.h>

namespace hyde {
namespace rt {

class StdStorage;

template <typename... ElemTypes>
class StdSet {
 private:
  using SelfType = StdSet<ElemTypes...>;
  using TupleType = std::tuple<ElemTypes...>;

  StdSet(const SelfType &) = delete;
  SelfType &operator=(const SelfType &) = delete;

  struct Hash {
   public:
    uint64_t operator()(const TupleType &tuple) const noexcept {
      HashingWriter writer;
      Serializer<NullReader, HashingWriter, TupleType>::Write(writer, tuple);
      return writer.Digest();
    }
  };

  std::unordered_set<TupleType, Hash> entries;

 public:
  using Self = StdSet<ElemTypes...>;

  StdSet(void) = default;
  StdSet(SelfType &&that) noexcept
      : entries(std::move(that.entries)) {}

  SelfType &operator=(SelfType &&) noexcept = default;

  HYDE_RT_ALWAYS_INLINE TupleType &Add(ElemTypes... elems) noexcept {
    auto [it, added] = entries.emplace(std::move(elems)...);
    (void) added;
    return it->second;
  }

  HYDE_RT_ALWAYS_INLINE size_t Size(void) const noexcept {
    return entries.size();
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
class Set<StdStorage, ElemTypes...>
    : public StdSet<ElemTypes...> {
 public:

  using BaseType = StdSet<ElemTypes...>;
  using SelfType = Set<StdStorage, ElemTypes...>;

  HYDE_RT_ALWAYS_INLINE Set(SelfType &&that_) noexcept
      : BaseType(std::move(that_)) {}

  HYDE_RT_ALWAYS_INLINE
  explicit Set(StdStorage &, unsigned)
      : BaseType() {}

 private:
  Set(const SelfType &) = delete;
  SelfType operator=(const SelfType &) = delete;
};

}  // namespace rt
}  // namespace hyde
