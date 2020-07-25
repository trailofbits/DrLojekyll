// Copyright 2019, Trail of Bits. All rights reserved.

#include <drlojekyll/Parse/ErrorLog.h>

#include "Query.h"

namespace hyde {

// Connect INSERT nodes to SELECT nodes when the "full state" of the relation
// does not need to be visible for point queries.
bool QueryImpl::ConnectInsertsToSelects(const ErrorLog &log) {
  std::unordered_map<ParsedDeclaration,
                     std::pair<std::vector<INSERT *>, std::vector<SELECT *>>>
      decl_to_views;

  auto can_connect = [](ParsedDeclaration decl) {
    return !decl.NumNegatedUses() &&  // Not used in an existence check.
           !decl.IsMessage() && 0u < decl.Arity();  // Not a condition.
  };

  // Collect a list of declarations mapping to their inserts and selects.
  for (auto rel : relations) {
    auto &views = decl_to_views[rel->declaration];

    for (auto insert : rel->inserts) {
      views.first.push_back(insert->AsInsert());
    }

    for (auto select : rel->selects) {
      views.second.push_back(select->AsSelect());
    }
  }

  // Double check that I/Os are sends or receives, but never both.
  for (auto io : ios) {
    auto error = false;
    if (!io->receives.Empty()) {
      error = !io->sends.Empty();

    } else if (!io->sends.Empty()) {
      error = !io->receives.Empty();
    }

    if (error) {
      log.Append(io->declaration.SpellingRange())
          << "Internal error: cannot have both sends and receives on the "
          << "message '" << io->declaration.Name() << '/'
          << io->declaration.Arity() << "'";
      return false;
    }
  }

  for (const auto &[decl, views] : decl_to_views) {
    const auto can_connect_decl = can_connect(decl);
    const auto &[insert_views, select_views] = views;

    if (insert_views.empty() && select_views.empty()) {
      continue;
    }

    VIEW *view = nullptr;
    INSERT *single_insert = nullptr;

    if (!insert_views.empty() && decl.Arity()) {
      const auto merge = merges.Create();
      view = merge;

      // Create a MERGE that takes in TUPLEs that replace each INSERT, except
      // for the INSERTs representing DELETEs.
      bool is_first_merge = true;
      for (auto insert : insert_views) {
        VIEW *ins_proxy = insert;

        // Only proxy an INSERT if it actually inserts data; otherwise it's a
        // DELETE and we want to maintain that.
        if (insert->is_insert) {
          ins_proxy = tuples.Create();

#ifndef NDEBUG
          ins_proxy->producer = "INSERT";
#endif

          ins_proxy->group_ids.swap(insert->group_ids);
          ins_proxy->sets_condition.Swap(insert->sets_condition);
          insert->CopyTestedConditionsTo(ins_proxy);

          for (auto in_col : insert->input_columns) {
            auto out_col =
                ins_proxy->columns.Create(in_col->var, ins_proxy, in_col->id);
            ins_proxy->input_columns.AddUse(in_col);
            out_col->CopyConstant(in_col);
          }

        // It's a DELETE, so maintain it, which will mean the MERGE will have
        // to handle differential updates.
        } else {
          merge->can_receive_deletions = true;
        }

        for (auto in_col : ins_proxy->input_columns) {
          if (is_first_merge) {
            (void) merge->columns.Create(in_col->var, merge, in_col->id);
          }
        }

        is_first_merge = false;
        merge->merged_views.AddUse(ins_proxy);
      }

      // If the decl has at least one `mutable`-attributed parameter then we
      // need a KVINDEX.
      if (decl.HasMutableParameter()) {
        assert(!merge->columns.Empty());

        // Create a guard tuple that enforces order of columns, so that we can
        // make the columns of the KVINDEX in the order of keys followed by
        // values, which might not match the normal column order.
        view = view->GuardWithTuple(this, true /* force */);

        const auto index = kv_indices.Create();

        // Create the key columns.
        auto i = 0u;
        for (ParsedParameter param : decl.Parameters()) {
          const auto merge_col = merge->columns[i++];
          if (param.Binding() != ParameterBinding::kMutable) {
            const auto key_col =
                index->columns.Create(merge_col->var, index, merge_col->id);
            merge_col->ReplaceAllUsesWith(key_col);
          }
        }

        // Create the value columns.
        i = 0u;
        for (ParsedParameter param : decl.Parameters()) {
          const auto merge_col = merge->columns[i++];
          if (param.Binding() == ParameterBinding::kMutable) {
            const auto val_col =
                index->columns.Create(merge_col->var, index, merge_col->id);
            merge_col->ReplaceAllUsesWith(val_col);
          }
        }

        // Create the inputs.
        i = 0u;
        for (ParsedParameter param : decl.Parameters()) {
          const auto merge_col = merge->columns[i++];
          if (param.Binding() == ParameterBinding::kMutable) {
            index->merge_functors.push_back(
                ParsedFunctor::MergeOperatorOf(param));
            index->attached_columns.AddUse(merge_col);
          } else {
            index->input_columns.AddUse(merge_col);
          }
        }
      }

      // Create a single outgoing INSERT for the relation. This ensures that
      // all clause dataflows send their data through a single place, and it
      // is UNIQUEd ahead-of-time by a UNION or dealt with by a K/V INDEX.
      // Similarly, any DELETEs are preserved and flow into the UNION.
      //
      // In the case of QUERY nodes, we want to MATERIALIZE the data via an
      // INSERT.
      if (view && (!can_connect_decl || decl.IsQuery())) {

        if (auto io_it = decl_to_input.find(decl);
            io_it != decl_to_input.end()) {

          single_insert = inserts.Create(io_it->second, decl);
          io_it->second->sends.AddUse(single_insert);

        } else if (auto rel_it = decl_to_relation.find(decl);
                   rel_it != decl_to_relation.end()) {
          single_insert = inserts.Create(rel_it->second, decl);
          rel_it->second->inserts.AddUse(single_insert);

        } else {
          assert(false);
        }

        for (auto col : view->columns) {
          single_insert->columns.Create(col->var, single_insert, col->id);
          single_insert->input_columns.AddUse(col);
        }
      }

      for (auto insert : insert_views) {
        if (insert->is_insert) {
          insert->PrepareToDelete();
        }
      }

      if (!can_connect_decl) {
        for (auto select : select_views) {
          select->inserts.AddUse(single_insert);
        }
      }
    }

    // Replace all uses of any of the SELECTs with TUPLEs that take the place
    // of the INSERTs.
    if (view && can_connect_decl) {

      REL *rel = nullptr;
      IO *io = nullptr;

      for (auto select : select_views) {
        if (!rel && !io) {
          rel = select->relation.get();
          if (auto stream = select->stream.get(); stream) {
            io = stream->AsIO();
          }
        }

        // Create a TUPLE and MERGE that will read in a tuple of all incoming
        // data to the INSERTs, thus letting us remove the INSERTs.
        const auto sel_tuple = tuples.Create();

#ifndef NDEBUG
        sel_tuple->producer = "SELECT";
#endif

        // Connect the MERGE to the TUPLE.
        auto i = 0u;
        for (auto merge_col : view->columns) {
          const auto sel_col = select->columns[i++];
          sel_tuple->columns.Create(sel_col->var, sel_tuple, sel_col->id);
          sel_tuple->input_columns.AddUse(merge_col);
        }

        select->ReplaceAllUsesWith(sel_tuple);
      }

      if (rel) {
        rel->selects.Clear();
      }
      if (io) {
        io->receives.Clear();
      }
    }
  }

  return true;
}

}  // namespace hyde