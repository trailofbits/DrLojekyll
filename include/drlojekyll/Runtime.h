// Copyright 2021, Trail of Bits. All rights reserved.

#pragma once

#include <string>

namespace hyde {
namespace rt {

template <typename T>
class Key;

template <typename T>
class Value;

template <typename KVColDesc>
struct ValueType;

template <typename ColDesc>
struct ValueType<Key<ColDesc>> {
  using Type = typename ColDesc::Type;
};

template <typename ColDesc>
struct ValueType<Value<ColDesc>> {
  using Type = typename ColDesc::Type;
};

template <typename StorageT, typename TableId, typename... Columns>
class Table;

template <typename StorageT, typename TableId, const unsigned kIndexId,
          typename... Columns>
class Index;

// Method on an index for AddToIndex that takes in all columns and does the
// right thing for updating based on Key, Value of column Ids


using UTF8 = std::string_view;
using Any = void;

/* **************************************** */
/* START https://stackoverflow.com/a/264088 */

// Templated function <T, Sign> named 'name' that checks whether the type `T`
// has a member function named 'func' with signature `Sign`.
// See stackoverflow link for usage.
#define HAS_MEM_FUNC(func, name) \
  template <typename T, typename Sign> \
  struct name { \
    typedef char yes[1]; \
    typedef char no[2]; \
    template <typename U, U> \
    struct type_check; \
    template <typename _1> \
    static yes &chk(type_check<Sign, &_1::func> *); \
    template <typename> \
    static no &chk(...); \
    static bool const value = sizeof(chk<T>(0)) == sizeof(yes); \
  }

template <bool C, typename T = void>
struct enable_if {
  typedef T type;
};

template <typename T>
struct enable_if<false, T> {};

HAS_MEM_FUNC(merge_into, has_merge_into);

/* END  https://stackoverflow.com/a/264088 */
/* *************************************** */

}  // namespace rt
}  // namespace hyde
