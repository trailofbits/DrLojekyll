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

  ForEachViewInDepthOrder([&](VIEW *v) {
    auto i = 0u;

    // SELECTs and MERGEs introduce new column IDs.
    if (v->AsMerge() || v->AsSelect()) {
      for (auto col : v->columns) {
        col->id = next_col_id++;
        col->index = i++;
      }

    // Other VIEWs mostly take their column IDs from their inputs.
    } else {
      for (auto col : v->columns) {
        col->id = 0u;
        col->index = i++;
      }
      views.push_back(v);
    }
  });

  auto changed = true;
  auto copy_col_id = [&changed](COL *from_col, COL *to_col) {
    if (from_col->id && from_col->id != to_col->id) {
      changed = true;
      to_col->id = from_col->id;
    }
  };

  for (; changed;) {
    changed = false;
    for (auto v : views) {
      const auto num_cols = v->columns.Size();
      const auto num_input_cols = v->input_columns.Size();

      // TUPLE and INSERTs pass-through their column IDs.
      if (v->AsTuple() || v->AsInsert()) {
        for (auto out_col : v->columns) {
          copy_col_id(v->input_columns[out_col->index], out_col);
        }

      // JOINs pass through the column IDs of non-pivot columns, but then they
      // invent new IDs for the pivot columns.
      } else if (auto join = v->AsJoin(); join) {
        for (const auto &[out_col, in_cols] : join->out_to_in) {
          if (in_cols.Size() == 1) {
            copy_col_id(in_cols[0], out_col);

          } else if (!out_col->id) {  // Assign a new column ID to the pivot.
            out_col->id = next_col_id++;
            changed = true;
          }
        }

      // Maintain IDs of input-to-output columns in MAPs, but make sure the
      // columns associated with `free`-attributed parameters are given fresh
      // IDs.
      } else if (auto map = v->AsMap(); map) {
        auto i = 0u;
        auto arity = map->functor.Arity();
        for (auto j = 0u; i < arity; ++i) {
          const auto out_col = map->columns[i];

          // It's an output column.
          if (map->functor.NthParameter(i).Binding() ==
              ParameterBinding::kFree) {
            if (!out_col->id) {
              out_col->id = next_col_id++;
              changed = true;
            }
            continue;

          // Input column.
          } else {
            copy_col_id(map->input_columns[j++], out_col);
          }
        }
        for (auto in_col : map->attached_columns) {
          copy_col_id(in_col, map->columns[i++]);
        }

      // Comparisons pass-through all column IDs, except in the case of an
      // equality comparison.
      } else if (auto cmp = v->AsConstraint(); cmp) {
        auto i = 0u;
        if (ComparisonOperator::kEqual == cmp->op) {
          if (!cmp->columns[0]->id) {
            cmp->columns[0]->id = next_col_id++;
            changed = true;
          }
          i = 1u;
        } else {
          copy_col_id(cmp->input_columns[0], cmp->columns[0]);
          copy_col_id(cmp->input_columns[1], cmp->columns[1]);
          i = 2u;
        }

        for (auto j = 0u; i < num_cols; ++i, ++j) {
          copy_col_id(cmp->attached_columns[j], cmp->columns[i]);
        }

      // Aggregates pass through group and configuration column IDs, but
      // invent new IDs for the summary columns.
      } else if (auto agg = v->AsAggregate(); agg) {
        auto i = 0u;
        for (auto in_col : agg->group_by_columns) {
          copy_col_id(in_col, agg->columns[i++]);
        }
        for (auto in_col : agg->config_columns) {
          copy_col_id(in_col, agg->columns[i++]);
        }
        for (; i < num_cols; ++i) {
          if (const auto out_col = agg->columns[i]; !out_col->id) {
            out_col->id = next_col_id++;
            changed = true;
          }
        }

      // KVINDEXes pass through the key column IDs, but not the value
      // column IDs (as the values are possibly mutated by the merge functors),
      // and thus might not match the input values.
      } else if (auto kv = v->AsKVIndex(); kv) {
        auto i = 0u;
        for (; i < num_input_cols; ++i) {
          copy_col_id(kv->input_columns[i], kv->columns[i]);
        }

        // Non-key columns.
        for (; i < num_cols; ++i) {
          if (const auto out_col = kv->columns[i]; !out_col->id) {
            out_col->id = next_col_id++;
            changed = true;
          }
        }

      } else {
        assert(false);
      }
    }
  }
}

}  // namespace hyde
