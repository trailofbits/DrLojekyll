// Copyright 2019, Trail of Bits. All rights reserved.

#include "Query.h"

namespace hyde {

// TODO(pag): If a parameter is `mutable`, then the `INSERT` must remain.
// TODO(pag): If there is a `SELECT` on a query, then maybe force it to remain...

void QueryImpl::ConnectInsertsToSelects(void) {
  std::unordered_map<ParsedDeclaration, std::vector<INSERT *>>
      decl_to_inserts;

  std::unordered_map<ParsedDeclaration, std::vector<SELECT *>>
      decl_to_selects;

  auto can_connect = [] (ParsedDeclaration decl) {
    return !decl.IsQuery() &&
           !decl.IsMessage() &&
           !decl.HasDirectGeneratorDependency() &&
           !decl.NumNegatedUses();
  };

  // Go figure out what INSERTs can be connected to SELECTs, and also try
  // to mark some INSERTs as no longer needed.
  for (auto insert : inserts) {
    if (can_connect(insert->decl)) {
      insert->is_used = false;
      decl_to_inserts[insert->decl].push_back(insert);

    } else if (!insert->decl.IsQuery() && !insert->decl.IsMessage()) {
      if (!insert->decl.NumUses()) {
        insert->is_used = false;
      }
    }
  }

  for (auto select : selects) {
    if (auto rel = select->relation.get(); rel) {
      if (can_connect(rel->decl)) {
        decl_to_selects[rel->decl].push_back(select);

      } else if (decl_to_inserts.count(rel->decl)) {
        for (auto insert : decl_to_inserts[rel->decl]) {
          select->inserts.AddUse(insert);
        }
      }
    }
  }

  for (auto &[decl, insert_views] : decl_to_inserts) {
    assert(can_connect(decl));

    const auto merge = merges.Create();
    Node<QueryView> *view = merge;
    for (auto insert : insert_views) {
      const auto ins_tuple = tuples.Create();

      bool is_first_merge = merge->merged_views.Empty();
      for (auto in_col : insert->input_columns) {

        if (is_first_merge) {
          (void) merge->columns.Create(in_col->var, merge, in_col->id);
        }

        (void) ins_tuple->columns.Create(in_col->var, ins_tuple, in_col->id);
        ins_tuple->input_columns.AddUse(in_col);
      }

      merge->merged_views.AddUse(ins_tuple);
    }

    // If the decl has at least one mutable parameter then we need a KV index.
    if (decl.HasMutableParameter()) {
      auto index = kv_indices.Create();

      view = merge->GuardWithTuple(this, true);

      // Create the key columns.
      auto i = 0u;
      for (auto param : decl.Parameters()) {
        const auto merge_col = merge->columns[i++];
        if (param.Binding() != ParameterBinding::kMutable) {
          const auto key_col = index->columns.Create(
              merge_col->var, index, merge_col->id);
          merge_col->ReplaceAllUsesWith(key_col);
        }
      }

      // Create the value columns.
      i = 0u;
      for (auto param : decl.Parameters()) {
        const auto merge_col = merge->columns[i++];
        if (param.Binding() == ParameterBinding::kMutable) {
          const auto val_col = index->columns.Create(
              merge_col->var, index, merge_col->id);
          merge_col->ReplaceAllUsesWith(val_col);
        }
      }

      // Create the inputs.
      i = 0u;
      for (auto param : decl.Parameters()) {
        const auto merge_col = merge->columns[i++];
        if (param.Binding() == ParameterBinding::kMutable) {
          index->merge_functors.push_back(ParsedFunctor::MergeOperatorOf(param));
          index->attached_columns.AddUse(merge_col);
        } else {
          index->input_columns.AddUse(merge_col);
        }
      }
    }

    for (auto select : decl_to_selects[decl]) {

      // Create a TUPLE and MERGE that will read in a tuple of all incoming
      // data to the INSERTs, thus letting us remove the INSERTs.
      const auto sel_tuple = tuples.Create();

      // This TUPLE takes the place of a SELECT, so it should behave the same
      // with respect to preserving the fact that there sometimes need to be
      // distinct flows (e.g. for cross-products, other joins).
      sel_tuple->group_ids = select->group_ids;

      // Connect the MERGE to the TUPLE.
      auto i = 0u;
      for (auto merge_col : view->columns) {
        auto sel_col = select->columns[i++];
        sel_tuple->columns.Create(sel_col->var, sel_tuple, sel_col->id);
        sel_tuple->input_columns.AddUse(merge_col);
      }

      // Replace all uses of the SELECTed columns with the columns from the
      // TUPLE.
      i = 0u;
      for (auto sel_col : select->columns) {
        auto sel_tuple_col = sel_tuple->columns[i++];
        sel_col->ReplaceAllUsesWith(sel_tuple_col);
      }

      // Replace all merges using the SELECT with ones that use the TUPLE.
      select->ReplaceAllUsesWith(sel_tuple);

      select->is_used = false;
    }
  }
}

}  // namespace hyde
