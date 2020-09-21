// Copyright 2020, Trail of Bits. All rights reserved.

#include <vector>

#include "Query.h"

namespace hyde {

// Finalize column ID values. Column ID values relate to lexical scope, to
// some extent. Two columns with the same ID can be said to have the same
// value at runtime.
void QueryImpl::FinalizeColumnIDs(void) const {
  std::vector<VIEW *> views;

  auto next_col_id = 1u;

  ForEachView([&](VIEW *v) {
    unsigned i = 0u;
    for (auto col : v->columns) {
      col->id = next_col_id++;
      col->index = i++;
    }
  });
}

}  // namespace hyde
