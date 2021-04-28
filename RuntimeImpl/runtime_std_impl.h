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

  void AppendF64(double d) {
    return AppendU64(reinterpret_cast<const uint64_t &>(d));
  }

  void AppendF32(float d) {
    return AppendU32(reinterpret_cast<const uint32_t &>(d));
  }

  void AppendU64(uint64_t d) {
    key_storage.push_back(static_cast<uint8_t>(d));
    key_storage.push_back(static_cast<uint8_t>(d >> 8));
    key_storage.push_back(static_cast<uint8_t>(d >> 16));
    key_storage.push_back(static_cast<uint8_t>(d >> 24));

    key_storage.push_back(static_cast<uint8_t>(d >> 32));
    key_storage.push_back(static_cast<uint8_t>(d >> 40));
    key_storage.push_back(static_cast<uint8_t>(d >> 48));
    key_storage.push_back(static_cast<uint8_t>(d >> 56));
  }

  void AppendU32(uint32_t d) {
    key_storage.push_back(static_cast<uint8_t>(d >> 24));
    key_storage.push_back(static_cast<uint8_t>(d >> 16));
    key_storage.push_back(static_cast<uint8_t>(d >> 8));
    key_storage.push_back(static_cast<uint8_t>(d));
  }

  void AppendU16(uint16_t h) {
    key_storage.push_back(static_cast<uint8_t>(h >> 8));
    key_storage.push_back(static_cast<uint8_t>(h));
  }

  void AppendU8(uint8_t b) {
    key_storage.push_back(b);
  }

 private:
  StdSerialBuffer &key_storage;
};


// Holds a reference to the first element in a grouping of data elements.
// Can be used for creating a std:: container of serialized groupings or for referencing a specific grouping
// of elements.
template <typename... Ts>
class SerializedTupleRef<std_containers, std::tuple<Ts...>> {
 public:
  explicit SerializedTupleRef(const StdSerialBuffer &backing_store_,
                              size_t start_offset)
      : backing_store(backing_store_),
        orig_offset(start_offset) {}

  const std::tuple<SerialRef<StdSerialBuffer, Ts>..., size_t> Get() const {
    return _Get<Ts...>(orig_offset);
  }

  std::tuple<Ts..., size_t> GetReified() const {
    return _GetReified<Ts...>(orig_offset);
  }

 private:
  // TODO(ekilmer): This code below is duplicated between _Get and _GetReified for the most part, but I'm not sure the most elegant way to combine it.
  template <typename E>
  const std::tuple<SerialRef<StdSerialBuffer, E>, size_t>
  _Get(const size_t offset) const {
    assert(offset < backing_store.size() &&
           "Offset is greater than backing store");
    auto ref = SerialRef<StdSerialBuffer, E>(backing_store, offset);
    return {ref, offset + ref.ElementSize()};
  }

  template <typename E, typename... Es>
  std::enable_if_t<sizeof...(Es) != 0,
                   const std::tuple<SerialRef<StdSerialBuffer, E>,
                                    SerialRef<StdSerialBuffer, Es>..., size_t>>
  _Get(const size_t offset) const {
    const auto [ref, new_offset] = _Get<E>(offset);
    return std::tuple_cat(std::make_tuple(ref), _Get<Es...>(new_offset));
  }

  template <typename E>
  std::tuple<E, size_t> _GetReified(const size_t offset) const {
    assert(offset < backing_store.size() &&
           "Offset is greater than backing store");
    auto ref = SerialRef<StdSerialBuffer, E>(backing_store, offset);
    return {ref.Reify(), offset + ref.ElementSize()};
  }

  template <typename E, typename... Es>
  std::enable_if_t<sizeof...(Es) != 0, std::tuple<E, Es..., size_t>>
  _GetReified(const size_t offset) const {
    const auto [ref, new_offset] = _GetReified<E>(offset);
    return std::tuple_cat(std::make_tuple(ref), _GetReified<Es...>(new_offset));
  }

  const StdSerialBuffer &backing_store;
  const size_t orig_offset;
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
class VectorRef<std_containers, std::tuple<Ts...>> {
 public:
  explicit VectorRef(StdSerialBuffer &backing_store_)
      : backing_store(backing_store_) {}

  std::tuple<SerialRef<StdSerialBuffer, Ts>..., size_t> Get(size_t offset) {
    return SerializedTupleRef<std_containers, std::tuple<Ts...>>(backing_store,
                                                                 offset)
        .Get();
  }

  std::tuple<SerialRef<StdSerialBuffer, Ts>..., size_t>
  operator[](size_t offset) {
    return Get(offset);
  }

  void Add(Ts... ts) {
    backing_store.reserve((sizeof(Ts) + ... + backing_store.size()));
    _Add(ts...);
  }

  auto size() {
    return backing_store.size();
  }

  StdSerialBuffer &backing_store;
  size_t offset;

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
};

// A SerializedVector owns its own backing store, unlike a VectorRef that only
// references another backing store. Both hold serialized data.
// TODO(ekilmer): There should be some way to reduce this duplication
template <typename... Ts>
class SerializedVector<std_containers, Ts...> {
 public:

  auto size() const {
    return backing_store.size();
  }

  const std::tuple<Ts..., size_t> Get(size_t offset) const {
    return SerializedTupleRef<std_containers, std::tuple<Ts...>>(backing_store,
                                                                 offset)
        .GetReified();
  }

  const std::tuple<Ts..., size_t> operator[](size_t offset) const {
    return Get(offset);
  }

  // Add a single serialized element
  void Add(Ts... ts) {
    BufferedWriter writer(backing_store);
    Serializer<BufferedWriter, std::tuple<Ts...>>::AppendValue(
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

// Database index class using standard containers
template <typename TableId, unsigned kIndexId, typename... Columns>
class Index<std_containers, TableId, kIndexId, Columns...> {
 public:
  using values_tuple_t = typename filtered_tuple<IsValue, Columns...>::type;
  using keys_tuple_t = typename filtered_tuple<IsKey, Columns...>::type;

  explicit Index(std_containers &) : backing_store(){};

  void Add(const typename ValueType<Columns>::type &...cols) {

    // First, write/serialize all keys/values
    StdSerialBuffer key_data = SerializeKey(cols...);
    StdSerialBuffer value_data = SerializeValue(cols...);

    // Second, look up whether the key is already present
    auto search = backing_store.find(key_data);
    if (search != backing_store.end()) {
      auto &key = search->first;
      auto &value = search->second;

      // Only append value (or search for it to make sure we don't double add)
      value.reserve(value.size() +
                    std::distance(value_data.begin(), value_data.end()));
      value.insert(value.end(), value_data.begin(), value_data.end());
    } else {
      backing_store[key_data] = value_data;
    }
  }

  // Checked statically whether the types of params passed are actually same
  // types as Keys
  template <typename... Ts>
  VectorRef<std_containers, values_tuple_t> Get(const Ts &...cols) {
    StdSerialBuffer key_data;
    BufferedWriter key_writer(key_data);

    // BufferedWriter data_writer(value_data);
    KeyValueWriter<BufferedWriter, Key<TypeIdentity<Ts>>...>::WriteKeySort(
        key_writer, cols...);
    KeyValueWriter<BufferedWriter, Key<TypeIdentity<Ts>>...>::WriteKeyUnique(
        key_writer, cols...);

    // KeyValueWriter<BufferedWriter, Columns...>::WriteKeyData(data_writer, cols...);

    return VectorRef<std_containers, values_tuple_t>(backing_store[key_data]);
  }

 private:
  // TODO(ekilmer): These serialization methods would be useful as standalones as well...

  template <typename... Ts>
  StdSerialBuffer SerializeKey(const Ts &...cols) const {
    static_assert(std::is_same_v<keys_tuple_t, std::tuple<Ts...>>);
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

  StdSerialBuffer
  SerializeKey(const typename ValueType<Columns>::type &...cols) {
    StdSerialBuffer key_data;
    BufferedWriter key_writer(key_data);
    KeyValueWriter<BufferedWriter, Columns...>::WriteKeySort(key_writer,
                                                             cols...);
    KeyValueWriter<BufferedWriter, Columns...>::WriteKeyUnique(key_writer,
                                                               cols...);

    // KeyValueWriter<BufferedWriter, Columns...>::WriteKeyData(data_writer, cols...);
    return key_data;
  }

  StdSerialBuffer
  SerializeValue(const typename ValueType<Columns>::type &...cols) {

    StdSerialBuffer value_data;
    BufferedWriter data_writer(value_data);
    KeyValueWriter<BufferedWriter, Columns...>::WriteValue(data_writer,
                                                           cols...);
    return value_data;
  }

  // stores serialized Key/Value objects
  std::map<StdSerialBuffer, StdSerialBuffer> backing_store;
};

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

  std::vector<SerializedTupleRef<std_containers, std::tuple<Columns...>>>
  Keys() {
    std::vector<SerializedTupleRef<std_containers, std::tuple<Columns...>>>
        keys;
    for (auto &[key, _] : backing_store) {
      keys.emplace_back(
          SerializedTupleRef<std_containers, std::tuple<Columns...>>(key, 0));
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

  template <size_t I = 0>
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

/* ****************************************** */
/* START: https://stackoverflow.com/a/7115547 */
/* Hashing for std::tuple                     */

template <typename TT>
struct hash {
  size_t operator()(TT const &tt) const {
    return std::hash<TT>()(tt);
  }
};

template <class T>
inline void hash_combine(std::size_t &seed, T const &v) {
  seed ^= hash<T>()(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
}

namespace {

// Recursive template code derived from Matthieu M.
template <class Tuple, size_t Index = std::tuple_size<Tuple>::value - 1>
struct HashValueImpl {
  static void apply(size_t &seed, Tuple const &tuple) {
    HashValueImpl<Tuple, Index - 1>::apply(seed, tuple);
    hash_combine(seed, std::get<Index>(tuple));
  }
};

template <class Tuple>
struct HashValueImpl<Tuple, 0> {
  static void apply(size_t &seed, Tuple const &tuple) {
    hash_combine(seed, std::get<0>(tuple));
  }
};
}  // namespace

template <typename... TT>
struct hash<std::tuple<TT...>> {
  size_t operator()(std::tuple<TT...> const &tt) const {
    size_t seed = 0;
    HashValueImpl<std::tuple<TT...>>::apply(seed, tt);
    return seed;
  }
};
/* END: https://stackoverflow.com/a/7115547 */
/* **************************************** */

}  // namespace rt
}  // namespace hyde
