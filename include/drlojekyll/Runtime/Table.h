// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include "Column.h"

namespace hyde {
namespace rt {

template <unsigned kTableId>
struct TableDescriptor;

// The `kTableId` is used to reference an auto-generated table descriptor, which
// looks roughly like this:
//
//      template <>
//      struct TableDescriptor<7> {
//        using ColumnIds = IdList<8, 9>;
//        using IndexIds = IdList<149>;
//      };
template <typename Storage, unsigned kTableId>
class Table;

}  // namespace rt
}  // namespace hyde
