// Copyright 2021, Trail of Bits. All rights reserved.

#pragma once

#include <string>

namespace hyde {
namespace rt {

template <typename... T>
class Key;

template <typename... T>
class Value;

template <const int ColumnId, typename T>
class Column;

// StorageEngine should be some type of mapping from Cols to int
template <typename StorageEngine, const int Id, typename... Cols>
class Table;

template <typename StorageEngine, const int TableId, const int IndexId,
          typename KeyT, typename ValueT>
class Index;


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
