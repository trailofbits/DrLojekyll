#include <drlojekyll/Runtime.h>

#include <map>
#include <string>


namespace hyde {
namespace rt {

template <typename Writer>
struct WriteData<Writer, int32_t> {
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
struct WriteData<Writer, std::string> {
  static inline void AppendKeySort(Writer &writer, const std::string &data) {
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

  static inline void AppendKeyUnique(Writer &writer, const std::string &data) {
    const auto len = static_cast<uint32_t>(data.size());
    const auto bytes = reinterpret_cast<const uint8_t *>(data.data());
    for (auto i = 6u; i < len; ++i) {
      writer.AppendU8(bytes[i]);
    }
  }

  static inline void AppendKeyData(Writer &writer, const std::string &data) {
    const auto len = static_cast<uint32_t>(data.size());
    writer.AppendU32(len);
  }

  static inline void AppendValue(Writer &writer, const std::string &data) {
    const auto len = static_cast<uint32_t>(data.size());
    const auto bytes = reinterpret_cast<const uint8_t *>(data.data());
    for (auto i = 0u; i < len; ++i) {
      writer.AppendU8(bytes[i]);
    }
  }
};

struct std_containers {};

template <typename TableId, unsigned kIndexId, typename... Columns>
class Index<std_containers, TableId, kIndexId, Columns...> {
 public:
  using values_tuple = typename filtered<IsValue, Columns...>::type;
  using keys_tuple = typename filtered<IsKey, Columns...>::type;

  explicit Index(std_containers &) : backing_store(){};

  void Add(const typename ValueType<Columns>::type &...cols) {}

  // Checked statically whether the types of params passed are actually same
  // types as Keys
  template <typename... T>
  std::vector<values_tuple> Get(const T &...cols) {
    static_assert(std::is_same_v<keys_tuple, std::tuple<T...>>);
    return std::vector<values_tuple>();
  }

 private:
  // stores serialized Key/Value objects
  std::map<std::string, std::string> backing_store;
};

template <typename kTableId, typename... Columns>
class Table<std_containers, kTableId, Columns...> {
 public:
  template <class... IndexT>
  Table(std_containers &, IndexT &...indices) : backing_store() {}

  uint8_t GetState(const Columns &...cols) {
    return 0;
  }

  uint8_t Get(const Columns &...cols) {
    return GetState(cols...);
  }

  bool KeyExists(const Columns &...cols) {
    return true;
  }

  uint8_t SetState(const Columns &...cols, uint8_t val) {
    return 0;
  }

  bool TransitionState(const Columns &...cols) {
    return true;
  }

 private:
  std::map<std::string, uint8_t> backing_store;
};

}  // namespace rt
}  // namespace hyde
