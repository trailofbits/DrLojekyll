// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <vector>

template <typename T>
class hyde_rt_AggregateState : public T {};

template <typename T>
class hyde_rt_DifferentialAggregateState : public hyde_rt_AggregateState<T> {};

namespace hyde {
namespace rt {

union UUID {
  std::byte opaque_bytes[16];
  struct {
    uint64_t low;
    uint64_t high;
  } __attribute__((packed)) opaque_pair;

  inline bool operator<(UUID other) const noexcept {
    return memcmp(opaque_bytes, other.opaque_bytes, 16) < 0;
  }
};

union ASCII {
  uint64_t opaque_qwords[64 / sizeof(uint64_t)];
  std::byte opaque_bytes[64];

  inline bool operator<(UUID other) const noexcept {
    return memcmp(opaque_bytes, other.opaque_bytes, 16) < 0;
  }
};

union UTF8 {
  uint64_t opaque_qwords[64 / sizeof(uint64_t)];
  std::byte opaque_bytes[64];

  inline bool operator<(UUID other) const noexcept {
    return memcmp(opaque_bytes, other.opaque_bytes, 16) < 0;
  }
};

union Bytes {
  uint64_t opaque_qwords[64 / sizeof(uint64_t)];
  std::byte opaque_bytes[64];

  inline bool operator<(UUID other) const noexcept {
    return memcmp(opaque_bytes, other.opaque_bytes, 16) < 0;
  }
};

// A work list that lets us generically store tuple data and the "case" that
// should handle the tuple data.
template <size_t kNumCases>
class WorkList {
 private:
  static constexpr bool kIdIsI8 = static_cast<uint8_t>(kNumCases) == kNumCases;
  static constexpr bool kIdIsI16 =
      static_cast<uint16_t>(kNumCases) == kNumCases;

  enum : size_t { kMinSize = 4096u };

 public:
  using IdType = typename std::conditional<
      kIdIsI8, uint8_t,
      typename std::conditional<kIdIsI16, uint16_t, uint32_t>::type>::type;

  static_assert(sizeof(IdType) < kMinSize);

  WorkList(void) : begin_(new std::byte[kMinSize]), end_(&(begin_[kMinSize])) {
    Clear();
  }

  ~WorkList(void) {
    delete[] begin_;
  }

  void Clear(void) noexcept {
    *reinterpret_cast<IdType *>(begin_) = static_cast<IdType>(kNumCases);
    curr_ = &(begin_[sizeof(IdType)]);
    load_ = 0;
  }

  template <typename... Args>
  void EmplaceBack(Args... args, IdType case_id) noexcept {
    using Tuple = std::tuple<Args...>;
    constexpr auto needed_space = sizeof(Tuple) + sizeof(IdType) + sizeof(bool);
    if (&(curr_[needed_space]) >= end_) {
      Resize();
    }

    *reinterpret_cast<Tuple *>(curr_) = std::make_tuple<Args...>(args...);
    curr_ += sizeof(Tuple);

    *reinterpret_cast<IdType *>(curr_) = case_id;
    curr_ += sizeof(IdType);

    load_ += sizeof(Tuple);
  }

  IdType PopCase(void) noexcept {
    curr_ -= sizeof(IdType);
    return *reinterpret_cast<IdType *>(curr_);
  }

  template <typename... Args>
  const std::tuple<Args...> &PopTuple(void) noexcept {
    using Tuple = std::tuple<Args...>;
    curr_ -= sizeof(Tuple);
    return *reinterpret_cast<Tuple *>(curr_);
  }

  size_t Load(void) const {
    return load_;
  }

 private:
  using SelfType = WorkList<kNumCases>;

  WorkList(const SelfType &) = delete;
  WorkList(SelfType &&) noexcept = delete;

  [[gnu::noinline]] void Resize(void) {
    auto curr_size = end_ - begin_;
    auto new_size = ((curr_size * 5u) / 3u) + kMinSize;
    auto new_begin = new std::byte[new_size];
    std::move(begin_, end_, new_begin);
    delete[] begin_;
    curr_ = &(new_begin[curr_ - begin_]);
    end_ = &(new_begin[new_size]);
    begin_ = new_begin;
  }

  std::byte *begin_;
  std::byte *curr_{nullptr};
  std::byte *end_;
  uint64_t load_{0};
};

template <typename... Args>
class Generator {
 public:
  using Tuple = std::tuple<Args...>;

  Generator(void)
      : begin_(&(inline_[0])),
        curr_(begin_),
        end_(&(inline_[sizeof(inline_) / sizeof(Tuple)])) {}

  ~Generator(void) {
    if (begin_ != &(inline_[0])) {
      delete[] begin_;
    }
  }

  void Reset(void) {
    curr_ = begin_;
  }

  void Emit(Args... args) {
    if (curr_ >= end_) {
      Resize();
    }
    *curr_++ = std::make_tuple<Args...>(args...);
  }

  void Sort(void) noexcept {
    std::sort(begin_, curr_);
    curr_ = std::unique(begin_, curr_);
  }

  void Erase(const Generator<Args...> &that) {
    curr_ = std::set_difference(begin_, curr_, that.begin_, that.curr_, begin_);
  }

  const Tuple *begin(void) const noexcept {
    return begin_;
  }

  const Tuple *end(void) const noexcept {
    return curr_;
  }

 private:
  [[gnu::noinline]] void Resize(void) {
    auto curr_size = end_ - begin_;
    auto new_size = curr_size * 2u;
    auto new_begin = new Tuple[new_size];
    std::move(begin_, end_, new_begin);
    if (begin_ != &(inline_[0])) {
      delete[] begin_;
    }
    curr_ = &(new_begin[curr_size]);
    end_ = &(new_begin[new_size]);
    begin_ = new_begin;
  }

  Tuple *begin_;
  Tuple *curr_;
  Tuple *end_;
  Tuple inline_[16];
};

//class ProgramBase {
// public:
//  virtual ~ProgramBase(void);
//
//  ProgramBase(unsigned worker_id_, unsigned num_workers_);
//
//  virtual void Init(void) noexcept = 0;
//  virtual void Step(unsigned selector, void *data) noexcept = 0;
//
// protected:
//  const unsigned __worker_id;
//  const unsigned __num_workers;
//  const uint64_t __num_workers_mask;
//};

// Template for hashing multiple values.
template <typename... KeyTypes>
struct Hash {
  inline static uint64_t Update(KeyTypes... keys) noexcept {
    uint64_t hash = 0;
    auto apply_each = [&hash](auto val) {
      hash = Hash<decltype(val)>::Update(hash, val);
    };
    int force[] = {(apply_each(keys), 0)...};
    (void) force;
    return hash;
  }
};

// TODO(pag): Make the hash function follow a hilbert-curve-like construction.
//
//            Or have it do some kind of order preservation.
#define HASH_MIX(a, b) \
  ((((a) << 37) * 0x85ebca6bull) ^ (((a) >> 43) * 0xc2b2ae35ull) ^ \
   ((b) *0xcc9e2d51ull))

#define MAKE_HASH_IMPL(type, utype, cast) \
  template <> \
  struct Hash<type> { \
   public: \
    inline static uint64_t Update(uint64_t hash, type val_) noexcept { \
      const uint64_t val = cast<utype>(val_); \
      return HASH_MIX(hash, val); \
    } \
  }

MAKE_HASH_IMPL(std::byte, unsigned char, static_cast);
MAKE_HASH_IMPL(bool, uint8_t, static_cast);
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

using i8 = int8_t;
using i16 = int16_t;
using i32 = int32_t;
using i64 = int64_t;

using u8 = uint8_t;
using u16 = uint16_t;
using u32 = uint32_t;
using u64 = uint64_t;

using f32 = float;
using f64 = double;

using uuid = UUID;

using bytes = Bytes;
using utf8 = UTF8;
using ascii = ASCII;

template <>
struct Hash<UUID> {
 public:
  inline static uint64_t Update(uint64_t hash, UUID uuid) noexcept {
    const auto high = HASH_MIX(hash, uuid.opaque_pair.high);
    return HASH_MIX(high, uuid.opaque_pair.low);
  }
};

template <>
struct Hash<ASCII> {
 public:
  inline static uint64_t Update(uint64_t hash, ASCII str) noexcept {
    _Pragma("unroll") for (auto qword : str.opaque_qwords) {
      hash = HASH_MIX(hash, qword);
    }
    return hash;
  }
};

template <>
struct Hash<UTF8> {
 public:
  inline static uint64_t Update(uint64_t hash, UTF8 str) noexcept {
    _Pragma("unroll") for (auto qword : str.opaque_qwords) {
      hash = HASH_MIX(hash, qword);
    }
    return hash;
  }
};

template <>
struct Hash<Bytes> {
 public:
  inline static uint64_t Update(uint64_t hash, Bytes str) noexcept {
    _Pragma("unroll") for (auto qword : str.opaque_qwords) {
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

template <typename AggregatorType>
class Aggregate<AggregatorType, NoConfigVars, NoGroupVars> {
 public:
  AggregatorType *Get(void) noexcept {}
};

template <typename AggregatorType, typename... ConfigVarTypes>
class Aggregate<AggregatorType, ConfigVars<ConfigVarTypes...>, NoGroupVars> {
 public:
  AggregatorType *Get(ConfigVarTypes &&... config_vars) noexcept {}
};

template <typename AggregatorType, typename... GroupVarTypes>
class Aggregate<AggregatorType, NoConfigVars, GroupVars<GroupVarTypes...>> {
 public:
  AggregatorType *Get(GroupVarTypes &&... group_vars) noexcept {}
};

template <typename AggregatorType, typename... GroupVarTypes,
          typename... ConfigVarTypes>
class Aggregate<AggregatorType, ConfigVars<GroupVarTypes...>,
                GroupVars<ConfigVarTypes...>> {
 public:
  AggregatorType *Get(GroupVarTypes &&... group_vars,
                      ConfigVarTypes &&... config_vars) noexcept {}
};

template <typename... Keys>
struct PivotVars {};  // Equi-join.

struct NoPivotVars {};  // Cross-product.

template <typename... Keys>
struct SourceVars {};

struct NoSourceVars {};

template <typename PivotSetType, unsigned kNumSources,
          typename... SourceSetTypes>
class Join;

// Storage model for K/V mapping of an equi-join:
//
//  prefix:<pivots>            -> <size1>:<size2>:<size3>:<bloom filter>
//  prefix:<pivots>:<N>:<...>  -> <entry>
//  prefix:<pivots>:<N>:<...>  -> <entry>
//  prefix:<pivots>:<N>:<...>  -> <entry>
//  prefix:<pivots>:<M>:<...>  -> <entry>

template <typename...>
struct JoinEntry;

namespace detail {

template <typename...>
struct MapSourceVars;

template <typename... Vars>
struct MapSourceVars<SourceVars<Vars...>> {
  using Type = std::tuple<std::vector<std::tuple<Vars...>>>;
};

template <typename...>
struct ConcatTupleTypes;

template <typename... LVars>
struct ConcatTupleTypes<std::tuple<LVars...>> {
  using Type = std::tuple<LVars...>;
};

template <typename... LVars, typename... RVars>
struct ConcatTupleTypes<std::tuple<LVars...>, std::tuple<RVars...>> {
  using Type = std::tuple<LVars..., RVars...>;
};

template <typename... Vars, typename... Rest>
struct MapSourceVars<SourceVars<Vars...>, Rest...> {
  using Type =
      typename ConcatTupleTypes<std::tuple<std::vector<std::tuple<Vars...>>>,
                                typename MapSourceVars<Rest...>::Type>::Type;
};

}  // namespace detail

template <>
struct JoinEntry<NoPivotVars, NoSourceVars> {};

template <typename... PVars>
struct JoinEntry<PivotVars<PVars...>, NoSourceVars> {
  std::tuple<PVars...> key;
  std::tuple<> values;
};

template <typename... PVars, typename... SVarSets>
struct JoinEntry<PivotVars<PVars...>, SVarSets...> {
  std::tuple<PVars...> key;
  typename detail::MapSourceVars<SVarSets...>::Type values;
};

template <typename... SVarSets>
struct JoinEntry<NoPivotVars, SVarSets...> {
  std::tuple<> key;
  typename detail::MapSourceVars<SVarSets...>::Type values;
};

//// Cross-product with no source vars.
//template <unsigned kNumSources>
//class Join<NoPivotVars, kNumSources, NoSourceVars> {
// public:
//  static_assert(false, "Not possible");
//};


// Equi-join over one or more keys.
template <typename... PivotKeyTypes, unsigned kNumSources,
          typename... SourceSetTypes>
class Join<PivotVars<PivotKeyTypes...>, kNumSources, SourceSetTypes...> {
 public:
  static_assert(kNumSources == sizeof...(SourceSetTypes));
  using Entry = JoinEntry<PivotVars<PivotKeyTypes...>, SourceSetTypes...>;
};

// Cross-product.
template <unsigned kNumSources, typename... SourceSetTypes>
class Join<NoPivotVars, kNumSources, SourceSetTypes...> {
 public:
  static_assert(kNumSources == sizeof...(SourceSetTypes));
  using Entry = JoinEntry<NoPivotVars, SourceSetTypes...>;
};


// Set of values. Add-only.
template <typename... Keys>
class Set {
 public:
  // Returns `true` if the entry was added.
  bool Add(Keys &&... keys) {
    return true;  // TODO(pag): Implement me.
  }
};

// Set. Really this is more like a key/value map, mapping the tuple to `RC`,
// which is a bitset tracking which sources contributed this value.
template <typename RC, typename... Keys>
class DifferentialSet {
 public:
  // Returns `true` if the entry was added.
  bool Add(Keys &&... keys, RC insert) {
    return true;  // TODO(pag): Implement me.
  }

  // Returns `true` if the entry was deleted.
  bool Remove(Keys &&... keys, RC clear) {
    return true;  // TODO(pag): Implement me.
  }
};

// Key/value mapping with a merge operator.
template <typename...>
class Map;

// Key/value mapping with a merge operator, that supports removals.
template <typename...>
class DifferentialMap;

// Key/value mapping, where we map a single key to many values.
template <typename...>
class MultiMap;

// Key/value mapping, where we map a single key to many values.
template <typename...>
class DifferentialMultiMap;

struct EmptyKeyVars {};

template <typename... Keys>
struct KeyVars {};

template <typename... Values>
struct ValueVars {};

// A "proper" key/value mapping.
template <typename... Keys, typename... Values>
class Map<KeyVars<Keys...>, ValueVars<Values...>> {
 public:
  using Tuple = std::tuple<Values..., bool>;

  Tuple Get(Keys... keys) const noexcept {}

  inline void Update(Keys... keys, Values... vals) noexcept {}

  inline void Insert(Keys... keys, Values... vals) noexcept {}
};

// Really, a glorified global variable.
template <typename... Values>
class Map<EmptyKeyVars, ValueVars<Values...>> {
 public:
  using Tuple = std::tuple<Values..., bool>;

  inline Tuple Get(void) const noexcept {
    return val;
  }

  inline void Put(Values... vals) noexcept {
    Tuple(vals..., true).swap(val);
  }

 private:
  Tuple val;
};

// A "proper" key/value mapping.
template <typename... Keys, typename... Values>
class DifferentialMap<KeyVars<Keys...>, ValueVars<Values...>> {
 public:
  using Tuple = std::tuple<Values..., bool>;

  Tuple Get(Keys... keys) const noexcept {}

  inline void Put(Keys... keys, Values... vals) noexcept {}

  inline void Erase(Keys... keys) noexcept {}
};

// Really, a glorified global variable.
template <typename... Values>
class DifferentialMap<EmptyKeyVars, ValueVars<Values...>> {
 public:
  using Tuple = std::tuple<Values..., bool>;

  inline Tuple Get(void) const noexcept {
    return val;
  }

  inline void Put(Values... vals) noexcept {
    Tuple(vals..., true).swap(val);
  }

  inline void Erase(void) noexcept {
    Tuple().swap(val);
  }

 private:
  Tuple val;
};


template <typename... Keys, typename... Values>
class MultiMap<KeyVars<Keys...>, ValueVars<Values...>> {
 public:
  bool Get(Keys... keys, Generator<Values...> &vals_gen) const noexcept {
    return false;
  }

  inline void Put(Keys... keys, Generator<Values...> &vals_gen) noexcept {}
};

template <typename... Keys, typename... Values>
class DifferentialMultiMap<KeyVars<Keys...>, ValueVars<Values...>> {
 public:
  bool Get(Keys... keys, Generator<Values...> &vals_gen) const noexcept {
    return false;
  }

  void Put(Keys... keys, Generator<Values...> &vals_gen) noexcept {}

  inline void Erase(Keys... keys) noexcept {}
};

template <typename Tag>
struct InlineRedefinition {
  static constexpr bool kIsDefined = false;
};

template <typename Tag, typename Ret, typename... ArgTypes>
inline static auto InlineDefinition(Ret (*func)(ArgTypes...))
    -> Ret (*)(ArgTypes...) {
  if constexpr (InlineRedefinition<Tag>::kIsDefined) {
    return InlineRedefinition<Tag>::template Run<Ret, ArgTypes...>;
  } else {
    return func;
  }
}

#define AGGREGATOR_CONFIG(name, binding_pattern) \
  template <typename> \
  class hyde_rt_AggregateState<name##_##binding_pattern##_config>

#define DIFF_AGGREGATOR_CONFIG(name, binding_pattern) \
  template <typename> \
  class hyde_rt_DifferentialAggregateState<name##_##binding_pattern##_config>

#define AGGREGATOR_INIT(name, binding_pattern, ...) \
  struct name##_##binding_pattern##_config; \
  extern "C" void name##_##binding_pattern##_init( \
      hyde_rt_AggregateState<name##_##binding_pattern##_config> &self, \
      #__VA_ARGS__)

#define AGGREGATOR_ADD(name, binding_pattern, ...) \
  struct name##_##binding_pattern##_config; \
  extern "C" void name##_##binding_pattern##_add( \
      hyde_rt_AggregateState<name##_##binding_pattern##_config> &self, \
      #__VA_ARGS__)

#define DIFF_AGGREGATOR_INIT(name, binding_pattern, ...) \
  struct name##_##binding_pattern##_config; \
  extern "C" void name##_##binding_pattern##_init( \
      hyde_rt_DifferentialAggregateState<name##_##binding_pattern##_config> \
          &self, \
      #__VA_ARGS__)

#define DIFF_AGGREGATOR_ADD(name, binding_pattern, ...) \
  struct name##_##binding_pattern##_config; \
  extern "C" void name##_##binding_pattern##_add( \
      hyde_rt_DifferentialAggregateState<name##_##binding_pattern##_config> \
          &self, \
      #__VA_ARGS__)

#define DIFF_AGGREGATOR_REMOVE(name, binding_pattern, ...) \
  struct name##_##binding_pattern##_result; \
  struct name##_##binding_pattern##_config; \
  extern "C" void name##_##binding_pattern##_remove( \
      hyde_rt_DifferentialAggregateState<name##_##binding_pattern##_config> \
          &self, \
      #__VA_ARGS__)

#define MERGE(name, type, prev_var, proposed_var) \
  extern "C" type name##_merge(type prev_var, type proposed_var)

#define MAP(name, binding_pattern, ...) \
  struct name##_##binding_pattern##_mapper \
      : public name##_##binding_pattern##_generator { \
    void Run(__VA_ARGS__); \
  }; \
  template <> \
  struct InlineRedefinition<name##_##binding_pattern##_tag> { \
    using __GenType = name##_##binding_pattern##_generator; \
    using __SelfType = name##_##binding_pattern##_mapper; \
    static constexpr bool kIsDefined = true; \
    template <typename Ret, typename... Args> \
    static Ret Run(__GenType &__self, Args... __args) { \
      return reinterpret_cast<__SelfType &>(__self).Run(__args...); \
    } \
  }; \
  void name##_##binding_pattern##_mapper ::Run(__VA_ARGS__)

}  // namespace rt
}  // namespace hyde
