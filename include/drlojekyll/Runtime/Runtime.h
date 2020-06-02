// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

#include <atomic>
#include <cstdint>
#include <unordered_map>

#if defined(__clang__) || defined(__GNUC__)
# define DR_INLINE [[gnu::always_inline]] inline
#elif defined(_MSVC_LANG)
# define DR_INLINE __forceinline
#else
# define DR_INLINE inline
#endif

namespace hyde {
namespace rt {

union UUID {
  uint8_t opaque_bytes[16];
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
  uint8_t opaque_bytes[64];

  inline bool operator<(UUID other) const noexcept {
    return memcmp(opaque_bytes, other.opaque_bytes, 16) < 0;
  }
};

union UTF8 {
  uint64_t opaque_qwords[64 / sizeof(uint64_t)];
  char opaque_bytes[64];

  inline bool operator<(UUID other) const noexcept {
    return memcmp(opaque_bytes, other.opaque_bytes, 16) < 0;
  }
};

union Bytes {
  uint64_t opaque_qwords[64 / sizeof(uint64_t)];
  char opaque_bytes[64];

  inline bool operator<(UUID other) const noexcept {
    return memcmp(opaque_bytes, other.opaque_bytes, 16) < 0;
  }
};

template <typename... Args>
class Rows {
 public:
  using SelfType = Rows<Args...>;
  using TupleType = std::tuple<Args...>;

  ~Rows(void) {
    if (begin_) {
      delete [] begin_;
    }
  }

  Rows(SelfType &&that) noexcept
      : begin_(that.begin_),
        next_(that.next_),
        end_(that.end_),
        is_sorted(that.is_sorted),
        is_empty(that.is_empty) {
    that.begin_ = nullptr;
    that.next_ = nullptr;
    that.end_ = nullptr;
    that.is_sorted = true;
    that.is_empty = true;
  }

  SelfType Release(void) {
    return SelfType(std::move(*this));
  }

  SelfType &operator=(SelfType &&that) noexcept {
    next_ = begin_;
    is_sorted = true;
    is_empty = true;

    std::swap(begin_, that.begin_);
    std::swap(next_, that.next_);
    std::swap(end_, that.end_);
    std::swap(is_sorted, that.is_sorted);
    std::swap(is_empty, that.is_empty);

    return *this;
  }

  const TupleType *begin(void) const {
    return begin_;
  }

  const TupleType *end(void) const {
    return next_;
  }

  uint64_t Size(void) const {
    return static_cast<uint64_t>(end_ - begin_);
  }

  void Emplace(Args&&... vals) {

    if (next_ >= end_) {
      auto curr_size = Size();
      auto new_size = ((curr_size * 5u) / 3u) + 4096u;
      auto new_begin = new std::tuple<Args...>[new_size];
      std::move(begin_, end_, new_begin);
      delete [] begin_;
      begin_ = new_begin;
      next_ = &(new_begin[curr_size]);
      end_ = &(new_begin[new_size]);
    }

    *next_++ = std::make_tuple<Args...>(vals...);

    if (is_empty) {
      is_sorted = true;
      is_empty = false;

    } else if (is_sorted) {
      const auto prev_tuple = next_[-2];
      const auto curr_tuple = next_[-1];
      if (prev_tuple == curr_tuple) {
        --next_;

      } else if (prev_tuple > curr_tuple) {
        is_sorted = false;
      }
    }
  }

  void Sort(void) {
    if (!is_sorted && !is_empty) {
      std::sort(begin_, next_);
      next_ = std::unique(begin_, next_);
      is_sorted = true;
    }
  }

  static void RemoveCommon(SelfType &a, SelfType &b) {
    a.Sort();
    b.Sort();

    auto a_it = a.begin_;
    auto a_end = a.next_;
    auto a_result = a.begin_;

    auto b_it = b.begin_;
    auto b_end = b.next_;
    auto b_result = b.begin_;

    for (; a_it != a_end && b_it != b_end; ) {
      const auto a = *a_it;
      const auto b = *b_it;
      if (a < b) {
        if (a_it != a_result) {
          *a_result = std::move(a);
          ++a_result;
        }
        ++a_it;

      } else if (b < a) {
        if (b_it != b_result) {
          *b_result = std::move(b);
          ++b_result;
        }
        ++b_it;
      }
    }

    a.next_ = a_result;
    b.next_ = b_result;

    a.is_empty = a.begin_ == a.next_;
    b.is_empty = b.begin_ == b.next_;
  }

  void RemoveCommon(Rows<Args...> &b) {
    return RemoveCommon(*this, b);
  }

  void Clear(void) {
    is_empty = true;
    is_sorted = true;
    next_ = begin_;
  }

  bool IsEmpty(void) const {
    return is_empty;
  }

 private:
  Rows(const SelfType &) = delete;
  SelfType &operator=(const SelfType &) = delete;

  TupleType *begin_{nullptr};
  TupleType *next_{nullptr};
  TupleType *end_{nullptr};
  bool is_sorted{true};
  bool is_empty{true};
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

// Template for hashing multiple values.
template <typename... KeyTypes>
struct Hash {
  inline static uint64_t Update(KeyTypes... keys) noexcept {
    uint64_t hash = 0;
    auto apply_each = [&hash] (auto val) {
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

template <typename AggregatorType>
class Aggregate<AggregatorType, NoConfigVars, NoGroupVars> {
 public:
  AggregatorType *Get(void) noexcept {

  }
};

template <typename AggregatorType, typename... ConfigVarTypes>
class Aggregate<AggregatorType, ConfigVars<ConfigVarTypes...>, NoGroupVars> {
 public:
  AggregatorType *Get(
      ConfigVarTypes&&... config_vars) noexcept {

  }
};

template <typename AggregatorType, typename... GroupVarTypes>
class Aggregate<AggregatorType, NoConfigVars, GroupVars<GroupVarTypes...>> {
 public:
  AggregatorType *Get(
      GroupVarTypes&&... group_vars) noexcept {

  }
};

template <typename AggregatorType, typename... GroupVarTypes, typename... ConfigVarTypes>
class Aggregate<AggregatorType, ConfigVars<GroupVarTypes...>, GroupVars<ConfigVarTypes...>> {
 public:
  AggregatorType *Get(
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
  using Type = typename ConcatTupleTypes<
      std::tuple<std::vector<std::tuple<Vars...>>>,
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
template <typename... PivotKeyTypes, unsigned kNumSources, typename... SourceSetTypes>
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


// Set. Really this is more like a key/value map, mapping the tuple to `RC`,
// which is a bitset tracking which sources contributed this value.
template <typename RC, typename... Keys>
class Set {
 public:

  // Returns `true` if the entry was added.
  bool Add(Keys&&... keys, RC insert) {

  }

  // Returns `true` if the entry was deleted.
  bool Remove(Keys&&... keys, RC clear) {

  }

};

// Key/value mapping with a merge operator.
template <typename...>
class Map;

struct EmptyKeyVars {};

template <typename... Keys>
struct KeyVars {};

template <typename... Values>
struct ValueVars {};

// A "proper" key/value mapping.
template <typename... Keys, typename... Values>
class Map<KeyVars<Keys...>, ValueVars<Values...>> {
 public:
  std::tuple<bool, Values...> Get(Keys... keys) const noexcept {

  }

  inline void Update(Keys... keys, Values... vals) noexcept {

  }

  inline void Insert(Keys... keys, Values... vals) noexcept {

  }

  inline void Erase(Keys... keys) noexcept {

  }
};

// Really, a glorified global variable.
template <typename ...Values>
class Map<EmptyKeyVars, ValueVars<Values...>> {
 public:
  inline std::tuple<bool, Values...> Get(void) const noexcept {
    return val;
  }

  inline void Insert(Values... vals) noexcept {
    std::make_tuple<Values..., bool>(vals..., true).swap(val);
  }

  inline void Update(Values... vals) noexcept {
    std::make_tuple<Values..., bool>(vals..., true).swap(val);
  }

  inline void Erase(void) noexcept {
    std::tuple<Values..., bool>().swap(val);
  }

 private:
  std::tuple<Values..., bool> val;
};

#define CONFIGURE_AGGREGATOR(name, binding_pattern, ...) \
  struct name ## _ ## binding_pattern ## _result; \
  struct name ## _ ## binding_pattern ## _config; \
  extern "C" \
  void name ## _ ## binding_pattern ## _init( \
      name ## _ ## binding_pattern ## _config &self, \
      #__VA_ARGS__)

#define UPDATE_AGGREGATOR(name, binding_pattern, ...) \
  struct name ## _ ## binding_pattern ## _result; \
  struct name ## _ ## binding_pattern ## _config; \
  extern "C" \
  void name ## _ ## binding_pattern ## _update( \
      name ## _ ## binding_pattern ## _config &self, \
      bool add, \
      #__VA_ARGS__)


#define MERGE_VALUES(name, type, prev_var, proposed_var) \
    extern "C" type name ## _merge(type prev_var, type proposed_var)

}  // namespace rt
}  // namespace hyde
