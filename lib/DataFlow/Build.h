// Copyright 2020, Trail of Bits. All rights reserved.

#include "Query.h"

namespace hyde {

template <typename... Args>
static void ReplaceInputsWithTuple(QueryImpl *impl, VIEW *user,
                                   Args... input_lists) {
  UseList<COL> *input_col_lists[] = {input_lists...};
  TUPLE * const tuple = impl->tuples.Create();
  unsigned col_index = 0u;
  for (auto input_col_list : input_col_lists) {
    if (input_col_list->Empty()) {
      continue;
    }

    UseList<COL> new_col_list(user);
    for (auto in_col : *input_col_list) {
      assert(in_col->IsConstant());
      auto out_col = tuple->columns.Create(
          in_col->var, tuple, in_col->id, col_index++);
      out_col->CopyConstant(in_col);
      new_col_list.AddUse(out_col);
      tuple->input_columns.AddUse(in_col);
    }

    input_col_list->Swap(new_col_list);
  }
}

}  // namespace hyde
