// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include "Table.h"

#include <map>

#include "SlabStorage.h"

namespace hyde {
namespace rt {

enum TupleState : uint8_t;

template <unsigned kTableId>
class Table<SlabStorage, kTableId> {
 public:
  Table(SlabStorage &storage);

 private:
  std::map<TypedSlabReference<std::tuple<T, Ts...>>, TupleState> state;
};

}  // namespace rt
}  // namespace hyde
