// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

#include <atomic>
#include <cstdint>

namespace hyde {
namespace rt {

struct UUID {
  uint64_t low;
  uint64_t high;
};

struct String {
  uint64_t opaque;
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

// The `alignas` will force it to have size 64, but be empty. As an empty
// struct, it will benefit from the empty base class optimization, so additional
// fields in derived structs will fill the 64 bytes.
struct alignas(64) AggregateConfiguration {};

template <typename ValueType, typename... KeyTypes>
class Map {
 public:
  ValueType &operator()(uint64_t hash, KeyTypes... keys) noexcept {

  }
};

}  // namespace rt
}  // namespace hyde
