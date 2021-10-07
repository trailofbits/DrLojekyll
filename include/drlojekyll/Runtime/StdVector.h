// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <algorithm>
#include <cstddef>
#include <tuple>
#include <deque>

#include "Runtime.h"

namespace hyde {
namespace rt {

class StdStorage;

template <typename ElemType, typename ParamType>
struct InternType;


template <typename ElemType>
struct InternType<ElemType, ElemType> {
  static inline ElemType Intern(StdStorage &storage, ElemType &&val) {
    return std::forward<ElemType>(val);
  }
};

#define DR_MAKE_CASTED_INTERN_TYPE(to_type, from_type) \
    template <> \
    struct InternType<to_type, from_type> { \
      static inline to_type Intern(StdStorage &storage, from_type val) { \
        return static_cast<to_type>(val); \
      } \
    };

DR_MAKE_CASTED_INTERN_TYPE(uint8_t, int)
DR_MAKE_CASTED_INTERN_TYPE(uint16_t, int)
DR_MAKE_CASTED_INTERN_TYPE(uint32_t, int)
DR_MAKE_CASTED_INTERN_TYPE(uint64_t, int)
DR_MAKE_CASTED_INTERN_TYPE(int8_t, int)
DR_MAKE_CASTED_INTERN_TYPE(int16_t, int)
DR_MAKE_CASTED_INTERN_TYPE(int64_t, int)

#undef DR_MAKE_CASTED_INTERN_TYPE

template <typename ValType>
struct InternType<InternRef<ValType>, ValType> {
  static inline InternRef<ValType> Intern(StdStorage &storage,
                                          ValType val) {
    return storage.Intern(std::move(val));
  }
};

template <typename ValType>
struct InternType<InternRef<ValType>, ValType &&> {
  static inline InternRef<ValType> Intern(StdStorage &storage,
                                          ValType &&val) {
    return storage.Intern(std::forward<ValType>(val));
  }
};

template <typename... ElemTypes>
class StdVector {
 private:
  using SelfType = StdVector<ElemTypes...>;

  StdStorage &storage;

  StdVector(const SelfType &) = delete;
  SelfType &operator=(const SelfType &) = delete;

  using TupleType = std::tuple<ElemTypes...>;
  std::deque<TupleType> entries;

 public:
  using Self = StdVector<ElemTypes...>;

  explicit StdVector(StdStorage &storage_)
      : storage(storage_) {}

  StdVector(SelfType &&that) noexcept
      : storage(that.storage),
        entries(std::move(that.entries)) {}

  SelfType &operator=(SelfType &&) noexcept = default;

  template <typename... ParamTypes>
  HYDE_RT_ALWAYS_INLINE void Add(ParamTypes... params) noexcept {
    entries.emplace_back(
        std::move(
            InternType<ElemTypes, ParamTypes>::Intern(
                storage, std::move(params)))...);
  }

  HYDE_RT_ALWAYS_INLINE size_t Size(void) const noexcept {
    return entries.size();
  }

  HYDE_RT_ALWAYS_INLINE void SortAndUnique(void) noexcept {
    std::sort(entries.begin(), entries.end());
    auto it = std::unique(entries.begin(), entries.end());
    entries.erase(it, entries.end());
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

  using BaseType = StdVector<ElemTypes...>;
  using SelfType = Vector<StdStorage, ElemTypes...>;

  HYDE_RT_ALWAYS_INLINE Vector(SelfType &&that_) noexcept
      : BaseType(std::move(that_)) {}

  HYDE_RT_ALWAYS_INLINE
  explicit Vector(StdStorage &storage_, unsigned)
      : BaseType(storage_) {}

 private:
  Vector(const SelfType &) = delete;
  SelfType operator=(const SelfType &) = delete;
};

}  // namespace rt
}  // namespace hyde
