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
    return !decl.HasMutableParameter() &&
           !decl.IsQuery() &&
           !decl.IsMessage() &&
           !decl.HasDirectGeneratorDependency();
  };

  for (auto insert : inserts) {
    if (can_connect(insert->decl)) {
      decl_to_inserts[insert->decl].push_back(insert);
    }
  }

  for (auto select : selects) {
    if (auto rel = select->relation.get()) {
      if (can_connect(rel->decl)) {
        decl_to_selects[rel->decl].push_back(select);
      }
    }
  }

  for (auto &[decl, insert_views] : decl_to_inserts) {
    assert(can_connect(decl));

    for (auto select : decl_to_selects[decl]) {

      // Create a MERGE that will read in a tuple of all incoming data to the
      // INSERTs, thus letting us remove the INSERTs.
      const auto merge = merges.Create();

      // This MERGE takes the place of a SELECT, so it should behave the same
      // with respect to preserving the fact that there sometimes need to be
      // distinct flows (e.g. for cross-products, other joins).
      merge->check_group_ids = true;

      for (auto insert : insert_views) {
        const auto tuple = tuples.Create();
        tuple->check_group_ids = true;

        bool is_first_merge = merge->merged_views.Empty();
        for (auto in_col : insert->input_columns) {

          if (is_first_merge) {
            (void) merge->columns.Create(in_col->var, merge, in_col->id);
          }

          (void) tuple->columns.Create(in_col->var, tuple, in_col->id);
          tuple->input_columns.AddUse(in_col);
        }

        insert->is_used = false;
        merge->merged_views.AddUse(tuple);
      }

      // Replace all uses of the SELECTed columns with the columns from the
      // MERGE.
      auto i = 0u;
      for (auto out_col : select->columns) {
        out_col->ReplaceAllUsesWith(merge->columns[i++]);
      }

      select->is_used = false;
    }
  }
}

}  // namespace hyde
