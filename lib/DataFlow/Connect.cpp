// Copyright 2019, Trail of Bits. All rights reserved.

#include <drlojekyll/Parse/ErrorLog.h>

#include "Query.h"

namespace hyde {
namespace {

VIEW *CreateProxyOfInserts(QueryImpl *impl, UseList<Node<QueryView>> &inserts) {

  UseList<Node<QueryView>> old_inserts(inserts.Owner());
  old_inserts.Swap(inserts);

  MERGE *merge = nullptr;
  const auto has_one_insert = inserts.Size() == 1u;

  // Create a MERGE that takes in TUPLEs that replace each INSERT, except
  // for the INSERTs representing DELETEs.
  for (VIEW *insert : old_inserts) {
    assert(insert->AsInsert());

    // Only proxy an INSERT if it actually inserts data; otherwise it's a
    // DELETE and we want to maintain that.
    TUPLE *const proxy = impl->tuples.Create();

#ifndef NDEBUG
    proxy->producer = "INSERT";
#endif

    insert->CopyDifferentialAndGroupIdsTo(proxy);
    insert->TransferSetConditionTo(proxy);
    insert->CopyTestedConditionsTo(proxy);

    insert->DropSetConditions();
    insert->DropTestedConditions();

    auto col_index = 0u;
    for (auto in_col : insert->input_columns) {
      COL *const proxy_col = proxy->columns.Create(
          in_col->var, in_col->type, proxy, in_col->id, col_index++);
      proxy->input_columns.AddUse(in_col);
      proxy_col->CopyConstantFrom(in_col);
    }

    insert->PrepareToDelete();

    if (has_one_insert) {
      return proxy;
    }

    if (!merge) {
      merge = impl->merges.Create();
      col_index = 0u;
      for (auto col : proxy->columns) {
        (void) merge->columns.Create(col->var, col->type, merge, col->id,
                                     col_index++);
      }
    }

    merge->merged_views.AddUse(proxy);
  }

  assert(merge != nullptr);
  return merge;
}

static VIEW *CreateProxyForMutableParams(QueryImpl *impl, VIEW *view,
                                         ParsedDeclaration decl) {

  assert(!view->columns.Empty());

  // If the decl has at least one `mutable`-attributed parameter then we
  // need a KVINDEX.
  if (!decl.HasMutableParameter()) {
    return view;
  }

  KVINDEX *const index = impl->kv_indices.Create();
  std::unordered_map<COL *, COL *> col_map;

  // Create the key columns.
  auto i = 0u;
  auto col_index = 0u;
  for (ParsedParameter param : decl.Parameters()) {
    const auto view_col = view->columns[i++];
    if (param.Binding() != ParameterBinding::kMutable) {
      const auto key_col = index->columns.Create(
          view_col->var, view_col->type, index, view_col->id, col_index++);
      col_map.emplace(view_col, key_col);

      index->input_columns.AddUse(view_col);
    }
  }

  // Create the value columns.
  i = 0u;
  for (ParsedParameter param : decl.Parameters()) {
    const auto view_col = view->columns[i++];
    if (param.Binding() == ParameterBinding::kMutable) {
      const auto val_col = index->columns.Create(
          view_col->var, view_col->type, index, view_col->id, col_index++);
      col_map.emplace(view_col, val_col);

      index->merge_functors.push_back(ParsedFunctor::MergeOperatorOf(param));
      index->attached_columns.AddUse(view_col);
    }
  }

  // We need to return the columns in the expected order.
  TUPLE *const proxy = impl->tuples.Create();
  col_index = 0u;
  for (auto col : view->columns) {
    (void) proxy->columns.Create(col->var, col->type, proxy, col->id,
                                 col_index++);
    proxy->input_columns.AddUse(col_map[col]);
  }

  return proxy;
}

static void ProxySelects(QueryImpl *impl, UseList<Node<QueryView>> &selects,
                         VIEW *insert_proxy) {
  UseList<Node<QueryView>> old_selects(selects.Owner());
  old_selects.Swap(selects);

  for (VIEW *select : old_selects) {
    assert(select->AsSelect());

    // Only proxy an INSERT if it actually inserts data; otherwise it's a
    // DELETE and we want to maintain that.
    TUPLE *const proxy = impl->tuples.Create();

#ifndef NDEBUG
    proxy->producer = "SELECT";
#endif

    select->CopyDifferentialAndGroupIdsTo(proxy);
    select->TransferSetConditionTo(proxy);
    select->CopyTestedConditionsTo(proxy);

    select->DropSetConditions();
    select->DropTestedConditions();

    auto col_index = 0u;
    for (auto in_col : insert_proxy->columns) {
      COL *const sel_col = select->columns[col_index];
      COL *const proxy_col = proxy->columns.Create(
          sel_col->var, sel_col->type, proxy, sel_col->id, col_index++);
      proxy->input_columns.AddUse(in_col);
      proxy_col->CopyConstantFrom(in_col);
    }

    select->ReplaceAllUsesWith(proxy);
  }
}

}  // namespace

// Connect INSERT nodes to SELECT nodes when the "full state" of the relation
// does not need to be visible for point queries.
bool QueryImpl::ConnectInsertsToSelects(const ErrorLog &log) {

  // First, deal with all messages.
  for (IO *io : ios) {

    io->transmits.Unique();
    io->receives.Unique();

    // Messages should only ever be sent or received, but not both.
    if (!io->transmits.Empty() && !io->receives.Empty()) {
      log.Append(io->declaration.SpellingRange())
          << "Internal error: cannot have both sends and receives on the "
          << "message '" << io->declaration.Name() << '/'
          << io->declaration.Arity() << "'";
      return false;
    }

    assert(!io->declaration.HasMutableParameter());

    if (auto num_transmits = io->transmits.Size(); num_transmits) {

      if (1u == num_transmits) {
        continue;
      }

      // If a message has more than one transmit, then we want to merge all
      // of those transmits via a single UNION.

      VIEW *const proxy = CreateProxyOfInserts(this, io->transmits);

      INSERT *insert = inserts.Create(io, io->declaration);
      for (auto col : proxy->columns) {
        insert->input_columns.AddUse(col);
      }

      io->transmits.AddUse(insert);

    } else if (auto num_receives = io->receives.Size(); num_receives) {

      if (1u == num_receives) {
        continue;
      }

      SELECT *const prev_sel = io->receives[0]->AsSelect();

      SELECT *select = nullptr;
      if (prev_sel->pred) {
        select = selects.Create(io, *(prev_sel->pred));
      } else {
        select = selects.Create(io, DisplayRange(prev_sel->position, {}));
      }

      auto col_index = 0u;
      for (auto col : io->receives[0]->columns) {
        (void) select->columns.Create(col->var, col->type, select, col->id,
                                      col_index++);
      }

      ProxySelects(this, io->receives, select);
      assert(io->receives.Empty());
      io->receives.AddUse(select);

    } else {
      assert(false);
    }
  }

  // Then, deal with all relations (queries, locals, exports).
  for (REL *rel : relations) {

    rel->inserts.Unique();
    rel->selects.Unique();

    const ParsedDeclaration decl(rel->declaration);

    // WE don't generate a MERGE in the case of a zero-arity predicate, i.e.
    // a CONDition variable, because there might be multiple ways of proving
    // that CONDition that have different arities.
    if (!decl.Arity()) {
      continue;
    }

    VIEW *const insert_proxy = CreateProxyForMutableParams(
        this, CreateProxyOfInserts(this, rel->inserts), rel->declaration);
    rel->inserts.Clear();

    // If there are no SELECTs on this declaration, then any INSERTs are
    // ineffectual. It's possible that those INSERTs are conditional, though,
    // and `proxy` will deal with those conditions being linked. Thus, in this
    // case, we'll just leave `proxy` dangling, to be cleaned up by
    // canonicalization
    if (rel->selects.Empty() && !decl.IsQuery()) {
      continue;
    }

    ProxySelects(this, rel->selects, insert_proxy);
    assert(rel->selects.Empty());

    if (decl.IsQuery()) {
      INSERT *insert = inserts.Create(rel, rel->declaration);
      for (auto col : insert_proxy->columns) {
        insert->input_columns.AddUse(col);
      }

      rel->inserts.AddUse(insert);
    }
  }

  RemoveUnusedViews();
  TrackDifferentialUpdates(log, true);

  return true;
}

}  // namespace hyde
