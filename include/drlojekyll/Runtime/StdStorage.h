// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <memory>
#include <unordered_set>

#include "Runtime.h"

namespace hyde {
namespace rt {

struct InternedValue {
 public:

  template <typename T>
  static bool CompareValues(void *a_opaque, void *b_opaque) {
    const T &a = *reinterpret_cast<const T *>(a_opaque);
    const T &b = *reinterpret_cast<const T *>(b_opaque);
    return a == b;
  }

  static void DestroyTemporary(void *);

  template <typename T>
  static void DestroyPersistent(void *opaque) {
    delete reinterpret_cast<T *>(opaque);
  }

  mutable void *data{nullptr};
  mutable void (*destroy_data)(void *);
  uint64_t hash{0};
  bool (*compare_values)(void *, void *);
  unsigned serialized_length{0};

  ~InternedValue(void) {
    destroy_data(data);
  }
};

struct HashInternedValue {
 public:
  inline bool operator()(const InternedValue &a) const noexcept {
    return a.hash;
  }
};

struct CompareInternedValues {
 public:
  inline bool operator()(const InternedValue &a,
                         const InternedValue &b) const noexcept {
    if (a.data == b.data) {
      return true;
    } else if (a.hash != b.hash ||
               a.compare_values != b.compare_values ||
               a.serialized_length != b.serialized_length) {
      return false;
    } else {
      return (a.compare_values)(a.data, b.data);
    }
  }
};

class StdStorage {
 public:
  StdStorage(void);
  ~StdStorage(void);

//  // Intern a value.
//  template <typename T>
//  const T &Intern(T &&val) {
//    using Writer = ByteCountingWriterProxy<HashingWriter>;
//    Writer writer;
//    Serializer<NullReader, Writer, T>::Write(writer, val);
//
//    InternedValue dummy_val;
//    dummy_val.data = &val;
//    dummy_val.destroy_data = &InternedValue::DestroyTemporary;
//    dummy_val.hash = writer.Digest();
//    dummy_val.serialized_length = writer.num_bytes;
//    dummy_val.compare_values = &InternedValue::CompareValues<T>;
//
//    auto [it, added] = interned_data.emplace(std::move(dummy_val));
//    if (added) {
//      const InternedValue &persist_val = *it;
//      persist_val.data = new T(std::forward<T>(val));
//      persist_val.destroy_data = &InternedValue::DestroyPersistent<T>;
//    }
//
//    return *reinterpret_cast<const T *>(it->data);
//  }
//
//  template <typename T, typename ParamT>
//  inline const T &Intern(ParamT val) {
//    return Intern(T(val));
//  }

 private:
  std::unordered_set<InternedValue, HashInternedValue,
                     CompareInternedValues> interned_data;
};

}  // namespace rt
}  // namespace hyde
