// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

#include <atomic>
#include <cstdint>
#include <unordered_map>

namespace hyde {
namespace rt {

struct UUID {
  uint64_t low;
  uint64_t high;
};

union ASCII {
  uint64_t opaque_qwords[64 / sizeof(uint64_t)];
  char opaque_bytes[64];
};

union UTF8 {
  uint64_t opaque_qwords[64 / sizeof(uint64_t)];
  char opaque_bytes[64];
};

union Bytes {
  uint64_t opaque_qwords[64 / sizeof(uint64_t)];
  char opaque_bytes[64];
};

class ProgramBase {
 public:
  virtual ~ProgramBase(void);

  ProgramBase(unsigned worker_id_, unsigned num_workers_);

  virtual void Init(void) noexcept = 0;
  virtual void Step(unsigned selector, void *data) noexcept = 0;

 protected:
  const unsigned __worker_id;
  const unsigned __num_workers;
  const uint64_t __num_workers_mask;
};

template <typename T>
class Program : ProgramBase {
 public:
  virtual ~Program(void) = default;

  inline Program(unsigned worker_id_, unsigned num_workers_,
          T *workers_[])
      : ProgramBase(worker_id_, num_workers_),
        __workers(workers_) {}

  template <typename... KeyTypes>
  static uint64_t Hash(uint64_t version, KeyTypes... keys) noexcept {
    return 0;
  }

  virtual void Init(void) noexcept = 0;
  virtual void Step(unsigned selector, void *data) noexcept = 0;

 protected:
  T * const * const __workers;
};

// Template for hashing multiple values.
template <typename... KeyTypes>
struct Hash {
  inline static uint64_t Update(uint64_t hash, KeyTypes... keys) noexcept {
    auto apply_each = [&hash] (auto val) {
      hash = Hash<decltype(val)>::Update(hash, val);
    };
    int force[] = {(apply_each(keys), 0)...};
    (void) force;
    return hash;
  }
};

#define HASH_MIX(a, b) \
    ((((a) << 37) * 0x85ebca6bull) ^ \
     (((a) >> 43) * 0xc2b2ae35ull) ^ \
      ((b) * 0xcc9e2d51ull))

#define MAKE_HASH_IMPL(type, utype, cast) \
    template <> \
    struct Hash<type> { \
     public: \
      inline static uint64_t Update(uint64_t hash, type val_) noexcept { \
        const uint64_t val = cast<utype>(val_); \
        return HASH_MIX(hash, val); \
      } \
    }

MAKE_HASH_IMPL(int8_t, uint8_t, static_cast);
MAKE_HASH_IMPL(uint8_t, uint8_t, static_cast);
MAKE_HASH_IMPL(int16_t, uint16_t, static_cast);
MAKE_HASH_IMPL(uint16_t, uint16_t, static_cast);
MAKE_HASH_IMPL(int32_t, uint32_t, static_cast);
MAKE_HASH_IMPL(uint32_t, uint32_t, static_cast);
MAKE_HASH_IMPL(int64_t, uint64_t, static_cast);
MAKE_HASH_IMPL(uint64_t, uint64_t, static_cast);
MAKE_HASH_IMPL(float, uint32_t &, reinterpret_cast);
MAKE_HASH_IMPL(double, uint64_t &, reinterpret_cast);

#undef MAKE_HASH_IMPL

template <>
struct Hash<UUID> {
 public:
  inline static uint64_t Update(uint64_t hash, UUID uuid) noexcept {
    const auto high = HASH_MIX(hash, uuid.high);
    return HASH_MIX(high, uuid.low);
  }
};

template <>
struct Hash<ASCII> {
 public:
  inline static uint64_t Update(uint64_t hash, ASCII str) noexcept {
    _Pragma("unroll")
    for (auto qword : str.opaque_qwords) {
      hash = HASH_MIX(hash, qword);
    }
    return hash;
  }
};

template <>
struct Hash<UTF8> {
 public:
  inline static uint64_t Update(uint64_t hash, UTF8 str) noexcept {
    _Pragma("unroll")
    for (auto qword : str.opaque_qwords) {
      hash = HASH_MIX(hash, qword);
    }
    return hash;
  }
};

template <>
struct Hash<Bytes> {
 public:
  inline static uint64_t Update(uint64_t hash, Bytes str) noexcept {
    _Pragma("unroll")
    for (auto qword : str.opaque_qwords) {
      hash = HASH_MIX(hash, qword);
    }
    return hash;
  }
};

template <typename T>
struct Hash<const T &> : public Hash<T> {};

template <typename T>
struct Hash<T &> : public Hash<T> {};

template <typename T>
struct Hash<T &&> : public Hash<T> {};

// The `alignas` will force it to have size 64, but be empty. As an empty
// struct, it will benefit from the empty base class optimization, so additional
// fields in derived structs will fill the 64 bytes.
struct alignas(64) AggregateConfiguration {};

template <typename... Keys>
struct ConfigVars {};

template <typename... Keys>
struct GroupVars {};

struct NoGroupVars {};
struct NoConfigVars {};

template <typename AggregatorType, typename GroupTuple, typename KeyTuple>
class Aggregate;

template <typename AggregatorType, typename... ConfigVarTypes>
class Aggregate<AggregatorType, NoGroupVars, ConfigVars<ConfigVarTypes...>> {
 public:
  AggregatorType &operator()(
      uint64_t hash,
      ConfigVarTypes&&... config_vars) noexcept {

  }
};

template <typename AggregatorType, typename... GroupVarTypes>
class Aggregate<AggregatorType, ConfigVars<GroupVarTypes...>, NoConfigVars> {
 public:
  AggregatorType &operator()(
      uint64_t hash,
      GroupVarTypes&&... group_vars) noexcept {

  }
};

template <typename AggregatorType, typename... GroupVarTypes, typename... ConfigVarTypes>
class Aggregate<AggregatorType, ConfigVars<GroupVarTypes...>, GroupVars<ConfigVarTypes...>> {
 public:
  AggregatorType &operator()(
      uint64_t hash,
      GroupVarTypes&&... group_vars,
      ConfigVarTypes&&... config_vars) noexcept {

  }
};

template <typename... Keys>
struct PivotVars {};  // Equi-join.

struct NoPivotVars {};  // Cross-product.

template <typename... Keys>
struct SourceVars {};

struct NoSourceVars {};

template <typename PivotSetType, unsigned kNumSources, typename... SourceSetTypes>
class Join;

// Equi-join over one or more keys.
template <typename... PivotKeyTypes, unsigned kNumSources, typename... SourceSetTypes>
class Join<PivotVars<PivotKeyTypes...>, kNumSources, SourceSetTypes...> {
 public:
  static_assert(kNumSources == sizeof...(SourceSetTypes));
};

// Cross-product.
template <unsigned kNumSources, typename... SourceSetTypes>
class Join<NoPivotVars, kNumSources, SourceSetTypes...> {
 public:
  static_assert(kNumSources == sizeof...(SourceSetTypes));
};

// Set.
template <typename... Keys>
class Set {
 public:
  bool Add(uint64_t hash, unsigned added, Keys&&... keys) {

  }
};

// Key/value mapping with a merge operator.
template <typename...>
class Map;

template <typename... Keys>
struct KeyVars {};

template <typename... Values>
struct ValueVars {};

template <typename... Keys, typename... Values>
class Map<KeyVars<Keys...>, ValueVars<Values...>> {

};

}  // namespace rt
}  // namespace hyde
