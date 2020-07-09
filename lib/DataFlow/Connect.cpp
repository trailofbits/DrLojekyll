// Copyright 2019, Trail of Bits. All rights reserved.

#include "Query.h"

namespace hyde {

void QueryImpl::ConnectInsertsToSelects(void) {
  std::unordered_map<ParsedDeclaration, std::vector<INSERT *>>
      decl_to_inserts;

  std::unordered_map<ParsedDeclaration, std::vector<SELECT *>>
      decl_to_selects;

  auto can_connect = [] (ParsedDeclaration decl) {
    return !decl.NumNegatedUses() &&  // Not used in an existence check.
           !decl.NumDeletionClauses() &&  // Not deleted.
           decl.Arity();  // Not a condition.
  };

  auto unlink_insert = [] (INSERT *insert) {
    if (auto stream = insert->stream.get(); stream) {
      if (auto io = stream->AsIO(); io) {
        io->inserts.RemoveIf([=] (VIEW *v) { return v == insert; });
      } else {
        assert(false);
      }
    } else if (auto rel = insert->relation.get(); rel) {
      rel->inserts.RemoveIf([=] (VIEW *v) { return v == insert; });
    }

    insert->is_used = false;
    insert->is_dead = true;
  };

  // Go figure out what INSERTs can be connected to SELECTs, and also try
  // to mark some INSERTs as no longer needed.
  for (auto insert : inserts) {
    if (can_connect(insert->declaration)) {
      unlink_insert(insert);
      decl_to_inserts[insert->declaration].push_back(insert);

    } else if (!insert->declaration.IsQuery() &&
               !insert->declaration.IsMessage()) {
      if (!insert->declaration.NumUses()) {
        unlink_insert(insert);
      }
    }
  }

  for (auto select : selects) {
    if (auto rel = select->relation.get(); rel) {
      if (can_connect(rel->declaration)) {
        decl_to_selects[rel->declaration].push_back(select);

      } else if (decl_to_inserts.count(rel->declaration)) {
        for (auto insert : decl_to_inserts[rel->declaration]) {
          select->inserts.AddUse(insert);
        }
      }
    }
  }

  for (auto &[decl, insert_views] : decl_to_inserts) {
    assert(can_connect(decl));

    const auto merge = merges.Create();
    Node<QueryView> *view = merge;

    for (INSERT *insert : insert_views) {
      const auto ins_tuple = tuples.Create();

#ifndef NDEBUG
      ins_tuple->producer = "INSERT";
#endif

      for (auto cond : insert->positive_conditions) {
        ins_tuple->positive_conditions.AddUse(cond);
      }
      for (auto cond : insert->negative_conditions) {
        ins_tuple->negative_conditions.AddUse(cond);
      }

      bool is_first_merge = merge->merged_views.Empty();
      for (auto in_col : insert->input_columns) {

        if (is_first_merge) {
          (void) merge->columns.Create(in_col->var, merge, in_col->id);
        }

        auto out_col = ins_tuple->columns.Create(in_col->var, ins_tuple, in_col->id);
        ins_tuple->input_columns.AddUse(in_col);
        out_col->CopyConstant(in_col);
      }

      merge->merged_views.AddUse(ins_tuple);
    }

    // If the decl has at least one mutable parameter then we need a KV index.
    if (decl.HasMutableParameter()) {
      auto index = kv_indices.Create();

      view = merge->GuardWithTuple(this, true);

      // Create the key columns.
      auto i = 0u;
      for (ParsedParameter param : decl.Parameters()) {
        const auto merge_col = merge->columns[i++];
        if (param.Binding() != ParameterBinding::kMutable) {
          const auto key_col = index->columns.Create(
              merge_col->var, index, merge_col->id);
          merge_col->ReplaceAllUsesWith(key_col);
        }
      }

      // Create the value columns.
      i = 0u;
      for (ParsedParameter param : decl.Parameters()) {
        const auto merge_col = merge->columns[i++];
        if (param.Binding() == ParameterBinding::kMutable) {
          const auto val_col = index->columns.Create(
              merge_col->var, index, merge_col->id);
          merge_col->ReplaceAllUsesWith(val_col);
        }
      }

      // Create the inputs.
      i = 0u;
      for (ParsedParameter param : decl.Parameters()) {
        const auto merge_col = merge->columns[i++];
        if (param.Binding() == ParameterBinding::kMutable) {
          index->merge_functors.push_back(ParsedFunctor::MergeOperatorOf(param));
          index->attached_columns.AddUse(merge_col);
        } else {
          index->input_columns.AddUse(merge_col);
        }
      }
    }

    // Create a new outgoing INSERT for the query/message by re-purposing an
    // old one.
    //
    // This lets internal uses of the message/query take their values from
    // the MERGE or KVINDEX, but then exposes the results of the query/message
    // back out to the outside world via a single designated INSERT.
    if (!insert_views.empty() && (decl.IsMessage() || decl.IsQuery())) {
      auto new_insert = insert_views.back();
      new_insert->is_used = true;
      new_insert->input_columns.Clear();

      for (auto col : view->columns) {
        new_insert->input_columns.AddUse(col);
      }
    }

    for (SELECT *select : decl_to_selects[decl]) {

      // Create a TUPLE and MERGE that will read in a tuple of all incoming
      // data to the INSERTs, thus letting us remove the INSERTs.
      const auto sel_tuple = tuples.Create();

#ifndef NDEBUG
      sel_tuple->producer = "SELECT";
#endif

      for (auto cond : select->positive_conditions) {
        sel_tuple->positive_conditions.AddUse(cond);
      }
      for (auto cond : select->negative_conditions) {
        sel_tuple->negative_conditions.AddUse(cond);
      }

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
    }
  }
}

}  // namespace hyde
