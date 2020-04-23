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

struct String {
  char opaque[64];
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

template <typename KeyType, typename... RemainingKeyTypes>
struct Hash {

};

#define HASH_MIX(a, b) \
    ((((a) << 37) * 0x85ebca6bull) ^ \
     (((a) >> 43) * 0xc2b2ae35ull) ^ \
      ((b) * 0xcc9e2d51ull))

#define MAKE_HASH_IMPL(type, utype) \
    template <> \
    struct Hash<type> { \
     public: \
      inline static uint64_t Update(uint64_t hash, type val_) noexcept { \
        const uint64_t val = static_cast<utype>(val_); \
        return HASH_MIX(hash, val); \
      } \
    }

MAKE_HASH_IMPL(int8_t, uint8_t);
MAKE_HASH_IMPL(uint8_t, uint8_t);
MAKE_HASH_IMPL(int16_t, uint16_t);
MAKE_HASH_IMPL(uint16_t, uint16_t);
MAKE_HASH_IMPL(int32_t, uint32_t);
MAKE_HASH_IMPL(uint32_t, uint32_t);
MAKE_HASH_IMPL(int64_t, uint64_t);
MAKE_HASH_IMPL(uint64_t, uint64_t);

#undef MAKE_HASH_IMPL



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

template <typename AggregatorType, typename...GroupVarTypes>
class Aggregate<AggregatorType, ConfigVars<GroupVarTypes...>, NoConfigVars> {
 public:
  AggregatorType &operator()(
      uint64_t hash,
      GroupVarTypes&&... group_vars) noexcept {

  }
};

template <typename AggregatorType, typename...GroupVarTypes, typename... ConfigVarTypes>
class Aggregate<AggregatorType, ConfigVars<GroupVarTypes...>, GroupVars<ConfigVarTypes...>> {
 public:
  AggregatorType &operator()(
      uint64_t hash,
      GroupVarTypes&&... group_vars,
      ConfigVarTypes&&... config_vars) noexcept {

  }
};

}  // namespace rt
}  // namespace hyde
