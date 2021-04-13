#include <drlojekyll/Runtime.h>

#include <cassert>
#include <cstring>
#include <functional>
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

// Alias for a serialized buffer type
using StdSerialBuffer = std::vector<uint8_t>;

// Basic buffered data writer for writing fundamental types into a byte buffer.
struct BufferedWriter {
 public:
  explicit BufferedWriter(StdSerialBuffer &key_storage_)
      : key_storage(key_storage_) {}

  void AppendI32(int32_t h) {
    AppendU32(static_cast<uint32_t>(h));
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

template <typename Writer>
struct Serializer<Writer, int32_t> {
  static inline void AppendKeySort(Writer &writer, const int32_t &data) {
    writer.AppendI32(data);
  }
  static inline void AppendKeyUnique(Writer &writer, const int32_t &data) {}
  static inline void AppendKeyData(Writer &writer, const int32_t &data) {}
  static inline void AppendValue(Writer &writer, int32_t data) {
    writer.AppendI32(data);
  }
};

template <typename Writer>
struct Serializer<Writer, uint64_t> {
  static inline void AppendKeySort(Writer &writer, const uint64_t &data) {
    writer.AppendU64(data);
  }
  static inline void AppendKeyUnique(Writer &writer, const uint64_t &data) {}
  static inline void AppendKeyData(Writer &writer, const uint64_t &data) {}
  static inline void AppendValue(Writer &writer, uint64_t data) {
    writer.AppendU64(data);
  }
};

template <typename Writer>
struct Serializer<Writer, StdSerialBuffer> {
  static inline void AppendKeySort(Writer &writer,
                                   const StdSerialBuffer &data) {
    const auto len = static_cast<uint32_t>(data.size());
    if (!len) {
      writer.AppendU8(0);

    } else {
      if (len >= 0xFFu) {
        writer.AppendU8(static_cast<uint8_t>(0xFF));
      } else {
        writer.AppendU8(static_cast<uint8_t>(len));
      }

      writer.AppendU8(static_cast<uint8_t>(32 - __builtin_clz(len)));

      const auto bytes = reinterpret_cast<const uint8_t *>(data.data());
      for (auto i = 0u; i < 6u; ++i) {
        const auto in_range =
            static_cast<uint8_t>(static_cast<int8_t>((i < len) << 7) >> 7);
        writer.AppendU8(bytes[i & in_range] & in_range);
      }
    }
  }

  static inline void AppendKeyUnique(Writer &writer,
                                     const StdSerialBuffer &data) {
    const auto len = static_cast<uint32_t>(data.size());
    const auto bytes = reinterpret_cast<const uint8_t *>(data.data());
    for (auto i = 6u; i < len; ++i) {
      writer.AppendU8(bytes[i]);
    }
  }

  static inline void AppendKeyData(Writer &writer,
                                   const StdSerialBuffer &data) {
    const auto len = static_cast<uint32_t>(data.size());
    writer.AppendU32(len);
  }

  static inline void AppendValue(Writer &writer, const StdSerialBuffer &data) {
    const auto len = static_cast<uint32_t>(data.size());
    const auto bytes = reinterpret_cast<const uint8_t *>(data.data());
    for (auto i = 0u; i < len; ++i) {
      writer.AppendU8(bytes[i]);
    }
  }
};

// Reference to a BackingStore container that holds a contiguous serialized form
// of type T at an offset. The object T is reified from BackingStore using
// the starting memory address at the offset until the size of the object
// instance (where the size should be encoded in the serialized form if the type
// is not a fundamental type).
template <typename BackingStore, typename T>
struct SerialRef {
  explicit SerialRef(const BackingStore &store_, size_t offset_)
      : store(store_),
        offset(offset_) {}

  using Type = T;

  // Size of the element T for its contiguous serialized form
  size_t ElementSize() const {
    if constexpr (std::is_fundamental_v<T>) {
      return sizeof(T);
    } else {
      assert(0 && "ElementSize is only supported for fundamental types.");
    }
  }

  const T Reify() const {
    if constexpr (std::is_fundamental_v<T>) {
      return ReifyFundamental();
    } else {
      assert(0 && "Reify is only supported for fundamental types.");
    }
  }

 private:
  // Fundamental types are returned by value
  const T ReifyFundamental() const {
    T tmp;
    memcpy(&tmp, &(store.at(offset)), ElementSize());
    return tmp;
  }

  const BackingStore &store;
  size_t offset;
};

template <typename BackingStore, class T>
class SerializedTupleRef;

template <typename... Ts>
class SerializedTupleRef<StdSerialBuffer, std::tuple<Ts...>> {
 public:
  explicit SerializedTupleRef(const StdSerialBuffer &backing_store_,
                              size_t start_offset)
      : backing_store(backing_store_),
        orig_offset(start_offset) {}

  const std::tuple<SerialRef<StdSerialBuffer, Ts>..., size_t> Get() {
    size_t tmp = orig_offset;
    return _Get<Ts...>(tmp);
  }

 private:
  template <typename E>
  std::tuple<SerialRef<StdSerialBuffer, E>, size_t> _Get(size_t &offset) {
    assert(offset < backing_store.size() &&
           "Offset is greater than backing store");
    auto ref = SerialRef<StdSerialBuffer, E>(backing_store, offset);
    offset += ref.ElementSize();
    return {ref, offset};
  }

  template <typename E, typename... Es>
  std::enable_if_t<sizeof...(Es) != 0,
                   std::tuple<SerialRef<StdSerialBuffer, E>,
                              SerialRef<StdSerialBuffer, Es>..., size_t>>
  _Get(size_t &offset) {
    return std::tuple_cat(_Get<E>(offset), _Get<Es...>(offset));
  }

  const StdSerialBuffer &backing_store;
  const size_t orig_offset;
};

template <typename BackingStore, class T>
class VectorRef;

template <typename... Ts>
class VectorRef<StdSerialBuffer, std::tuple<Ts...>> {
 public:
  explicit VectorRef(StdSerialBuffer &backing_store_)
      : backing_store(backing_store_) {}

  std::tuple<SerialRef<StdSerialBuffer, Ts>..., size_t> Get(size_t offset) {
    return SerializedTupleRef<StdSerialBuffer, std::tuple<Ts...>>(backing_store,
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

  size_t size() {
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

struct std_containers {};

template <typename TableId, unsigned kIndexId, typename... Columns>
class Index<std_containers, TableId, kIndexId, Columns...> {
 public:
  using values_tuple_t = typename filtered_tuple<IsValue, Columns...>::type;
  using keys_tuple_t = typename filtered_tuple<IsKey, Columns...>::type;

  explicit Index(std_containers &) : backing_store(){};

  void Add(const typename ValueType<Columns>::type &...cols) {

    // First, write/serialize all keys/values
    StdSerialBuffer key_data;
    StdSerialBuffer value_data;
    BufferedWriter key_writer(key_data);
    BufferedWriter data_writer(value_data);
    KeyValueWriter<BufferedWriter, Columns...>::WriteKeySort(key_writer,
                                                             cols...);
    KeyValueWriter<BufferedWriter, Columns...>::WriteKeyUnique(key_writer,
                                                               cols...);

    // KeyValueWriter<BufferedWriter, Columns...>::WriteKeyData(data_writer, cols...);
    KeyValueWriter<BufferedWriter, Columns...>::WriteValue(data_writer,
                                                           cols...);

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
  VectorRef<StdSerialBuffer, values_tuple_t> Get(const Ts &...cols) {
    static_assert(std::is_same_v<keys_tuple_t, std::tuple<Ts...>>);
    StdSerialBuffer key_data;
    BufferedWriter key_writer(key_data);

    // BufferedWriter data_writer(value_data);
    KeyValueWriter<BufferedWriter, Key<TypeIdentity<Ts>>...>::WriteKeySort(
        key_writer, cols...);
    KeyValueWriter<BufferedWriter, Key<TypeIdentity<Ts>>...>::WriteKeyUnique(
        key_writer, cols...);

    // KeyValueWriter<BufferedWriter, Columns...>::WriteKeyData(data_writer, cols...);

    return VectorRef<StdSerialBuffer, values_tuple_t>(backing_store[key_data]);
  }

  // Get from serialized buffer key form
  // VectorRef<values_tuple> Get(const StdSerialBuffer &key_data) {
  //   return VectorRef(backing_store[key_data]);
  // }

 private:
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

  uint8_t GetState(const Columns &...cols) {
    StdSerialBuffer key_data;
    BufferedWriter key_writer(key_data);

    // BufferedWriter data_writer(value_data);
    KeyValueWriter<BufferedWriter, Key<TypeIdentity<Columns>>...>::WriteKeySort(
        key_writer, cols...);
    KeyValueWriter<BufferedWriter,
                   Key<TypeIdentity<Columns>>...>::WriteKeyUnique(key_writer,
                                                                  cols...);

    // KeyValueWriter<BufferedWriter, Columns...>::WriteKeyData(data_writer, cols...);

    return backing_store[key_data];
  }

  bool Get(const Columns &...cols) const {
    StdSerialBuffer key_data;
    BufferedWriter key_writer(key_data);

    // BufferedWriter data_writer(value_data);
    KeyValueWriter<BufferedWriter, Key<TypeIdentity<Columns>>...>::WriteKeySort(
        key_writer, cols...);
    KeyValueWriter<BufferedWriter,
                   Key<TypeIdentity<Columns>>...>::WriteKeyUnique(key_writer,
                                                                  cols...);

    // KeyValueWriter<BufferedWriter, Columns...>::WriteKeyData(data_writer, cols...);

    return backing_store.count(key_data);
  }

  bool KeyExists(const Columns &...cols) {
    StdSerialBuffer key_data;
    BufferedWriter key_writer(key_data);

    // BufferedWriter data_writer(value_data);
    KeyValueWriter<BufferedWriter, Key<TypeIdentity<Columns>>...>::WriteKeySort(
        key_writer, cols...);
    KeyValueWriter<BufferedWriter,
                   Key<TypeIdentity<Columns>>...>::WriteKeyUnique(key_writer,
                                                                  cols...);

    // KeyValueWriter<BufferedWriter, Columns...>::WriteKeyData(data_writer, cols...);

    return backing_store.count(key_data);
  }

  void SetState(const Columns &...cols, uint8_t val) {
    StdSerialBuffer key_data;
    BufferedWriter key_writer(key_data);

    // BufferedWriter data_writer(value_data);
    KeyValueWriter<BufferedWriter, Key<TypeIdentity<Columns>>...>::WriteKeySort(
        key_writer, cols...);
    KeyValueWriter<BufferedWriter,
                   Key<TypeIdentity<Columns>>...>::WriteKeyUnique(key_writer,
                                                                  cols...);

    // KeyValueWriter<BufferedWriter, Columns...>::WriteKeyData(data_writer, cols...);

    backing_store[key_data] = val;
  }

  bool TransitionState(const Columns &...cols) {
    StdSerialBuffer key_data;
    BufferedWriter key_writer(key_data);

    // BufferedWriter data_writer(value_data);
    KeyValueWriter<BufferedWriter, Key<TypeIdentity<Columns>>...>::WriteKeySort(
        key_writer, cols...);
    KeyValueWriter<BufferedWriter,
                   Key<TypeIdentity<Columns>>...>::WriteKeyUnique(key_writer,
                                                                  cols...);

    // KeyValueWriter<BufferedWriter, Columns...>::WriteKeyData(data_writer, cols...);

    auto prev_state = backing_store[key_data];
    auto state = prev_state & 3;
    auto present_bit = prev_state & 4;

    if (state == 0 || state == 2) {
      backing_store[key_data] = 1 | 4;
      if (!present_bit) {
        UpdateIndices(cols...);
      }
      return true;
    }

    return false;
  }

  bool TransitionState(SerialRef<StdSerialBuffer, Columns> &...cols) {
    return TransitionState(cols.Reify()...);
  }

  std::vector<SerializedTupleRef<StdSerialBuffer, std::tuple<Columns...>>>
  Keys() {
    std::vector<SerializedTupleRef<StdSerialBuffer, std::tuple<Columns...>>>
        keys;
    for (auto &[key, _] : backing_store) {
      keys.emplace_back(
          SerializedTupleRef<StdSerialBuffer, std::tuple<Columns...>>(key, 0));
    }
    return keys;
  }

 private:
  std::map<StdSerialBuffer, uint8_t> backing_store;
  std::tuple<std::reference_wrapper<Indices>...> indices;

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

}  // namespace rt
}  // namespace hyde
