#include <drlojekyll/Runtime.h>

#include <cassert>
#include <cstring>
#include <functional>
#include <initializer_list>
#include <iomanip>
#include <ios>
#include <iostream>
#include <map>
#include <string>
#include <tuple>
#include <type_traits>
#include <vector>


namespace hyde {
namespace rt {

// Tag type for usage of standard containers
struct std_containers {};

// Alias for a serialized buffer type
using StdSerialBuffer = std::vector<uint8_t>;

// Basic buffered data writer for writing fundamental types into a byte buffer.
struct BufferedWriter {
 public:
  explicit BufferedWriter(StdSerialBuffer &key_storage_)
      : key_storage(key_storage_) {}

  inline void WriteF64(double d) {
    WriteU64(reinterpret_cast<const uint64_t &>(d));
  }

  inline void WriteF32(float d) {
    WriteU32(reinterpret_cast<const uint32_t &>(d));
  }

  inline void WriteU64(uint64_t d) {
    key_storage.push_back(static_cast<uint8_t>(d));
    key_storage.push_back(static_cast<uint8_t>(d >> 8));
    key_storage.push_back(static_cast<uint8_t>(d >> 16));
    key_storage.push_back(static_cast<uint8_t>(d >> 24));

    key_storage.push_back(static_cast<uint8_t>(d >> 32));
    key_storage.push_back(static_cast<uint8_t>(d >> 40));
    key_storage.push_back(static_cast<uint8_t>(d >> 48));
    key_storage.push_back(static_cast<uint8_t>(d >> 56));
  }

  inline void WriteU32(uint32_t d) {
    key_storage.push_back(static_cast<uint8_t>(d >> 24));
    key_storage.push_back(static_cast<uint8_t>(d >> 16));
    key_storage.push_back(static_cast<uint8_t>(d >> 8));
    key_storage.push_back(static_cast<uint8_t>(d));
  }

  inline void WriteU16(uint16_t h) {
    key_storage.push_back(static_cast<uint8_t>(h >> 8));
    key_storage.push_back(static_cast<uint8_t>(h));
  }

  inline void WriteU8(uint8_t b) {
    key_storage.push_back(b);
  }

 private:
  StdSerialBuffer &key_storage;
};


// Holds a reference to the first element in a grouping of data elements.
// Can be used for creating a std:: container of serialized groupings or
// for referencing a specific grouping of elements.
template <typename... Ts>
class SerializedTupleRef<std_containers, Ts...> {
 public:
  explicit SerializedTupleRef(const StdSerialBuffer &backing_store_,
                              index_t start_offset)
      : backing_store(backing_store_),
        orig_offset(start_offset) {}

//  const std::tuple<SerialRef<StdSerialBuffer, Ts>..., index_t> Get() const {
//    return _Get<Ts...>(orig_offset);
//  }

  std::tuple<Ts..., index_t> GetReified() const {
    return _GetReified<Ts...>(orig_offset);
  }

 private:
//  // TODO(ekilmer): This code below is duplicated between _Get and _GetReified
//  // for the most part, but I'm not sure the most elegant way to combine it.
//  template <typename E>
//  const std::tuple<SerialRef<StdSerialBuffer, E>, index_t>
//  _Get(const index_t offset) const {
//    assert(offset < backing_store.size() &&
//           "Offset is greater than backing store");
//    auto ref = SerialRef<StdSerialBuffer, E>(backing_store, offset);
//    return {ref, offset + ref.ElementSize()};
//  }
//
//  template <typename E, typename... Es>
//  std::enable_if_t<sizeof...(Es) != 0,
//                   const std::tuple<SerialRef<StdSerialBuffer, E>,
//                                    SerialRef<StdSerialBuffer, Es>..., index_t>>
//  _Get(const index_t offset) const {
//    const auto [ref, new_offset] = _Get<E>(offset);
//    return std::tuple_cat(std::make_tuple(ref), _Get<Es...>(new_offset));
//  }

//  template <typename E>
//  std::tuple<E, index_t> _GetReified(const index_t offset) const {
//    assert(offset < backing_store.size() &&
//           "Offset is greater than backing store");
//
//
//  }

  template <typename E, typename... Es>
  std::tuple<E, Es..., index_t> _GetReified(const index_t offset) const {
    auto [val, next_offset] = SerialRef<StdSerialBuffer, E>(
        backing_store, offset).Reify();

    if constexpr (0u < sizeof...(Es)) {
      std::tuple<Es..., index_t> next_vals = _GetReified<Es...>(next_offset);
      return std::tuple_cat(std::make_tuple(val), next_vals);
    } else {
      return {val, next_offset};
    }
  }

  const StdSerialBuffer &backing_store;
  const index_t orig_offset;
};

// Serialize a tuple of values
template <typename... Ts>
StdSerialBuffer SerializeValue(const std::tuple<Ts...> &t) {
  StdSerialBuffer value_data;
  BufferedWriter value_writer(value_data);

  std::apply(
      [&](const Ts &...ts) {
        KeyValueWriter<BufferedWriter, Value<TypeIdentity<Ts>>...>::WriteValue(
            value_writer, ts...);
      },
      t);
  return value_data;
}

template <typename... Ts>
StdSerialBuffer SerializeValues(std::initializer_list<std::tuple<Ts...>> l) {
  StdSerialBuffer value_data;
  for (const auto &t : l) {
    auto value = SerializeValue(t);
    value_data.reserve(value_data.size() +
                       std::distance(value.begin(), value.end()));
    value_data.insert(value_data.end(), value.begin(), value.end());
  }
  return value_data;
}

template <typename... Ts, typename... Tuples>
StdSerialBuffer SerializeValues(const std::tuple<Ts...> &t,
                                const Tuples &...tuples) {
  auto value_data = SerializeValue(t);

  if constexpr (sizeof...(Ts) != 0) {
    auto value = SerializeValues(tuples...);
    value_data.reserve(value_data.size() +
                       std::distance(value.begin(), value.end()));
    value_data.insert(value_data.end(), value.begin(), value.end());
  }

  return value_data;
}

// A VectorRef is a vector that holds a collection of references to a tuple of types that can be Reified
template <typename... Ts>
class VectorRef<std_containers, Ts...> {
 public:
  explicit VectorRef(const StdSerialBuffer &backing_store_)
      : backing_store(backing_store_) {}

  std::tuple<Ts..., index_t> Get(index_t offset) const {
    return SerializedTupleRef<std_containers, Ts...>(
        backing_store, offset).GetReified();
  }

  void Add(Ts... ts) {
    backing_store.reserve((sizeof(Ts) + ... + backing_store.size()));
    _Add(ts...);
  }

  index_t size(void) const noexcept {
    return static_cast<index_t>(backing_store.size());
  }

 private:
  template <typename... Es>
  void _Add() {}

  template <typename E, typename... Es>
  void _Add(E ele, Es... eles) {
    alignas(E) std::array<std::byte, sizeof(ele)> tmp;
    new (&tmp) E(ele);

    backing_store.insert(backing_store.end(), tmp.begin(), tmp.end());

    // recurse
    _Add(eles...);
  }

  const StdSerialBuffer &backing_store;
};

template <typename... Ts>
class Vector<std_containers, Ts...> {
 private:
  std::vector<std::tuple<Ts...>> backing_store;

 public:
  std::tuple<Ts..., index_t> Get(index_t index) const {
    return std::tuple_cat(backing_store[index],
                          std::make_tuple<index_t>(index + 1u));
  }

  void Add(Ts... args) {
    backing_store.emplace_back(std::make_tuple(std::move(args)...));
  }

  index_t size(void) const noexcept {
    return static_cast<index_t>(backing_store.size());
  }

  auto begin(void) const -> decltype(this->backing_store.begin()) {
    return backing_store.begin();
  }

  auto end(void) const -> decltype(this->backing_store.end()) {
    return backing_store.end();
  }

  void clear(void) {
    backing_store.clear();
  }

  void swap(Vector<std_containers, Ts...> &that) {
    backing_store.swap(that.backing_store);
  }

  void SortAndUnique(void) {

  }
};

// A SerializedVector owns its own backing store, unlike a VectorRef that only
// references another backing store. Both hold serialized data.
// TODO(ekilmer): There should be some way to reduce this duplication
template <typename... Ts>
class SerializedVector<std_containers, Ts...> {
 public:

  auto size(void) const {
    return backing_store.size();
  }

  const std::tuple<Ts..., index_t> Get(index_t offset) const {
    return SerializedTupleRef<std_containers, Ts...>(backing_store, offset)
        .GetReified();
  }

  // Add a single serialized element
  void Add(Ts... ts) {
    BufferedWriter writer(backing_store);
    Serializer<NullWriter, BufferedWriter, std::tuple<Ts...>>::WriteValue(
        writer, std::make_tuple<Ts...>(std::move(ts)...));
  }

  void clear() {
    backing_store.clear();
  }

  void swap(SerializedVector<std_containers, std::tuple<Ts...>> &store) {
    backing_store.swap(store.backing_store);
  }

  const auto begin() const {
    return backing_store.begin();
  }

  const auto end() const {
    return backing_store.end();
  }

 private:

  StdSerialBuffer backing_store;
};

// Database index class using standard containers. `Columns` will be a list
// of `Key<T>` or `Val<T>`, where each `T` is a column descriptor, and
// `KeyColumns` and `ValColumns` will be lists of column descriptors.
template <typename TableId, unsigned kIndexId, typename... Columns,
          typename... KeyColumns, typename... ValColumns>
class Index<std_containers, TableId, kIndexId, TypeList<Columns...>,
            TypeList<KeyColumns...>, TypeList<ValColumns...>> {
 public:
  explicit Index(std_containers &) : backing_store(){};

  void Add(const typename ValueType<Columns>::type &...cols) {
    key_data.clear();
    BufferedWriter key_writer(key_data);
    KeyValueWriter<BufferedWriter, Columns...>::WriteKeySort(
        key_writer, cols...);
    KeyValueWriter<BufferedWriter, Columns...>::WriteKeyUnique(
        key_writer, cols...);

    BufferedWriter data_writer(backing_store[key_data]);
    KeyValueWriter<BufferedWriter, Columns...>::WriteValue(
        data_writer, cols...);
  }

  const VectorRef<std_containers, typename ValueType<ValColumns>::type...>
  Get(const typename ValueType<KeyColumns>::type &...cols) const {
    key_data.clear();
    BufferedWriter key_writer(key_data);
    KeyValueWriter<BufferedWriter, KeyColumns...>::WriteKeySort(
        key_writer, cols...);
    KeyValueWriter<BufferedWriter, KeyColumns...>::WriteKeyUnique(
        key_writer, cols...);

    auto it = backing_store.find(key_data);
    if (it == backing_store.end()) {
      return kEmptyVecRef;
    } else {
      return VectorRef<std_containers, typename ValueType<ValColumns>::type...>(
          it->second);
    }
  }

  bool KeyExists(const typename ValueType<KeyColumns>::type &...cols) const {
    key_data.clear();
    BufferedWriter key_writer(key_data);
    KeyValueWriter<BufferedWriter, KeyColumns...>::WriteKeySort(
        key_writer, cols...);
    KeyValueWriter<BufferedWriter, KeyColumns...>::WriteKeyUnique(
        key_writer, cols...);
    return  backing_store.find(key_data) != backing_store.end();
  }

 private:

  // Working buffer for writing key data when doing lookups.
  mutable StdSerialBuffer key_data;

  // Stores serialized Key/Value objects
  std::map<StdSerialBuffer, StdSerialBuffer> backing_store;

  static const StdSerialBuffer kEmptyIndexBackingBuffer;

  static const VectorRef<std_containers, typename ValueType<ValColumns>::type...>
      kEmptyVecRef;
};

template <typename TableId, unsigned kIndexId, typename... Columns,
          typename... KeyColumns, typename... ValColumns>
const StdSerialBuffer
Index<std_containers, TableId, kIndexId, TypeList<Columns...>,
      TypeList<KeyColumns...>, TypeList<ValColumns...>>::kEmptyIndexBackingBuffer{};

template <typename TableId, unsigned kIndexId, typename... Columns,
          typename... KeyColumns, typename... ValColumns>
const VectorRef<std_containers, typename ValueType<ValColumns>::type...>
Index<std_containers, TableId, kIndexId, TypeList<Columns...>,
      TypeList<KeyColumns...>, TypeList<ValColumns...>>::kEmptyVecRef(kEmptyIndexBackingBuffer);

template <typename kTableId, typename... Indices, typename... Columns>
class Table<std_containers, kTableId, TypeList<Indices...>,
            TypeList<Columns...>> {
 public:
  Table(std_containers &, Indices &...indices_)
      : backing_store(),
        indices(indices_...) {}

  //uint8_t GetState(SerialRef<StdSerialBuffer, Columns> &...cols) {

  // Get the state of the specified columns (key)
  uint8_t GetState(const Columns &...cols) const {
    StdSerialBuffer key_data = SerializeKey(cols...);

    // Don't insert if it's not already present
    if (KeyExists(key_data)) {
      return backing_store.at(key_data);
    } else {
      return kStateAbsent;
    }
  }

  // For use when indices are aliased to the Table. Gets the state
  bool Get(const Columns &...cols) const {
    return GetState(cols...);
  }

  bool KeyExists(const Columns &...cols) {
    return KeyExists(SerializeKey(cols...));
  }

  void SetState(const Columns &...cols, uint8_t val) {
    backing_store[SerializeKey(cols...)] = val;
  }

  // Transition from_state to to_state and return whether this actually happened.
  // Inserts column values if not already present
  bool TransitionState(TupleState from_state, TupleState to_state,
                       const Columns &...cols) {
    StdSerialBuffer key_data = SerializeKey(cols...);

    static_assert(kStateAbsent == 0,
                  "Default initialized state should be 0 (kStateAbsent)");
    auto prev_state = backing_store[key_data];
    auto state = prev_state & kStateMask;
    auto present_bit = prev_state & kStatePresentBit;

    bool matches_from_state = false;
    switch (from_state) {
      case TupleState::kAbsent:
        matches_from_state = state == kStateAbsent;
        break;
      case TupleState::kPresent:
        matches_from_state = state == kStatePresent;
        break;
      case TupleState::kUnknown:
        matches_from_state = state == kStateUnknown;
        break;
      case TupleState::kAbsentOrUnknown:
        matches_from_state =
            (state == kStateAbsent) || (state == kStateUnknown);
        break;
      default: assert(false && "Unknown FromState TupleState");
    }

    if (matches_from_state) {

      // 4 is kPresentBit value
      // See Python CodeGen for ProgramTransitionStateRegion
      switch (to_state) {
        case TupleState::kAbsent:
          backing_store[key_data] = kStateAbsent | kStatePresentBit;
          break;
        case TupleState::kPresent:
          backing_store[key_data] = kStatePresent | kStatePresentBit;
          break;
        case TupleState::kUnknown:
          backing_store[key_data] = kStateUnknown | kStatePresentBit;
          break;
        case TupleState::kAbsentOrUnknown:
          backing_store[key_data] = kStateUnknown | kStatePresentBit;
          assert(false && "Invalid ToState TupleState");
          break;
        default: assert(false && "Unknown ToState TupleState");
      }

      if (!present_bit) {
        UpdateIndices(cols...);
      }

      return true;
    }

    return false;
  }

  bool TransitionState(TupleState from_state, TupleState to_state,
                       const SerialRef<StdSerialBuffer, Columns> &...cols) {
    return TransitionState(from_state, to_state, cols.Reify()...);
  }

  std::vector<SerializedTupleRef<std_containers, Columns...>>
  Keys() {
    std::vector<SerializedTupleRef<std_containers, Columns...>>
        keys;
    for (auto &[key, _] : backing_store) {
      keys.emplace_back(
          SerializedTupleRef<std_containers, Columns...>(key, 0));
    }
    return keys;
  }

 private:
  std::map<StdSerialBuffer, uint8_t> backing_store;
  std::tuple<std::reference_wrapper<Indices>...> indices;

  bool KeyExists(const StdSerialBuffer key_data) const {
    return (backing_store.count(key_data) != 0);
  }

  // Serialize columns into a Key that can be used to look up the value in our backing_store
  StdSerialBuffer SerializeKey(const Columns &...cols) const {
    StdSerialBuffer key_data;
    BufferedWriter key_writer(key_data);

    // BufferedWriter data_writer(value_data);
    KeyValueWriter<BufferedWriter, Key<TypeIdentity<Columns>>...>::WriteKeySort(
        key_writer, cols...);
    KeyValueWriter<BufferedWriter,
                   Key<TypeIdentity<Columns>>...>::WriteKeyUnique(key_writer,
                                                                  cols...);

    // KeyValueWriter<BufferedWriter, Columns...>::WriteKeyData(data_writer, cols...);

    return key_data;
  }

  template <index_t I = 0>
  void UpdateIndices(const Columns &...cols) {

    // If we have iterated through all elements
    if constexpr (I == sizeof...(Indices)) {

      // No indices to update
      return;
    } else {

      // Add to this index
      std::get<I>(indices).get().Add(cols...);

      // Then go for next
      UpdateIndices<I + 1>(cols...);
    }
  }
};

template <class T>
inline index_t hash_combine(index_t seed, T const &v) {
  seed ^= seed >> 16u;
  seed *= 0x85ebca6bu;
  seed ^= seed >> 13u;
  seed *= 0xc2b2ae35u;
  seed ^= seed >> 16u;

  seed ^= std::hash<T>{}(v);
  return seed;
}

namespace {

// Recursive template code derived from Matthieu M.
template <class Tuple, index_t Index = std::tuple_size<Tuple>::value - 1>
struct HashValueImpl {
  static index_t apply(index_t seed, Tuple const &tuple) {
    seed = HashValueImpl<Tuple, Index - 1>::apply(seed, tuple);
    return hash_combine(seed, std::get<Index>(tuple));
  }
};

template <class Tuple>
struct HashValueImpl<Tuple, 0> {
  static index_t apply(index_t seed, Tuple const &tuple) {
    return hash_combine(seed, std::get<0>(tuple));
  }
};
}  // namespace


}  // namespace rt
}  // namespace hyde
namespace std {

template <typename... Ts>
struct hash<std::tuple<Ts...>> {
  ::hyde::rt::index_t operator()(std::tuple<Ts...> const &tt) const {
    return ::hyde::rt::HashValueImpl<std::tuple<Ts...>>::apply(0xc6ef3720u, tt);
  }
};

}  // namespace
