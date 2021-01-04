// Copyright 2020, Trail of Bits. All rights reserved.

#include "Build.h"

namespace hyde {

// TODO(pag): If we decrement a condition then maybe we shouldn't re-check
//            if stuff exists, but at the same time, condition variables
//            don't fit nicely into our differential model.
//
//            On second thought, they *might* actually fit semi-fine. The
//            trick is that we need to find anything possibly dependent on
//            the truthiness of the condition, mark it as deleted, then
//            and only then decrement the condition. Right now we have
//            some of that backwards (delete happens later). Anyway, I
//            think it's reasonable to wait until this is a problem, then
//            try to solve it.

// Build an eager region for publishing data, or inserting it. This might end
// up passing things through if this isn't actually a message publication.
void BuildEagerInsertRegion(ProgramImpl *impl, QueryView pred_view,
                            QueryInsert insert, Context &context, OP *parent,
                            TABLE *last_table) {
  const auto view = QueryView(insert);
  const auto cols = insert.InputColumns();

  DataModel *const model = impl->view_to_model[view]->FindAs<DataModel>();
  TABLE * const table = model->table;

  if (table) {
    if (table != last_table) {
      const auto table_insert =
          impl->operation_regions.CreateDerived<CHANGESTATE>(
              parent, TupleState::kAbsentOrUnknown, TupleState::kPresent);

      for (auto col : cols) {
        const auto var = parent->VariableFor(impl, col);
        table_insert->col_values.AddUse(var);
      }

      table_insert->table.Emplace(table_insert, table);
      parent->body.Emplace(parent, table_insert);
      parent = table_insert;
      last_table = table;
    }
  }

  // This insert represents a message publication.
  if (insert.IsStream()) {
    assert(!view.SetCondition());  // TODO(pag): Is this possible?
    auto stream = insert.Stream();
    assert(stream.IsIO());
    auto io = QueryIO::From(stream);

    const auto message_publish = impl->operation_regions.CreateDerived<PUBLISH>(
        parent, ParsedMessage::From(io.Declaration()));
    parent->body.Emplace(parent, message_publish);

    for (auto col : cols) {
      const auto var = parent->VariableFor(impl, col);
      message_publish->arg_vars.AddUse(var);
    }

  // Inserting into a relation.
  } else if (insert.IsRelation()) {
    BuildEagerSuccessorRegions(impl, view, context, parent, view.Successors(),
                               last_table);

  } else {
    assert(false);
  }
}

// A bottom-up insert remover is not a DELETE; instead it is that the relation
// that backs this INSERT is somehow subject to differential updates, e.g.
// because it is downstream from an aggregate or kvindex.
void CreateBottomUpInsertRemover(ProgramImpl *impl, Context &context,
                                 QueryView view, PROC *proc,
                                 TABLE *already_checked) {
  const auto insert = QueryInsert::From(view);
  const auto insert_cols = insert.InputColumns();

  const auto model = impl->view_to_model[view]->FindAs<DataModel>();
  REGION *parent = proc;
  UseRef<REGION> *parent_body = &(proc->body);

  // This insert is associated with persistent storage. It could be an insert
  // into a relation or a stream; in the stream case, it just means the insert
  // shares its data model with its predecessor.
  if (model->table) {

    // The caller didn't already do a state transition, so we have to do it.
    if (already_checked != model->table) {
      auto remove = BuildBottomUpTryMarkUnknown(
          impl, model->table, proc, insert_cols,
          [&](PARALLEL *par) {
            const auto let = impl->operation_regions.CreateDerived<LET>(par);
            par->AddRegion(let);
            parent = let;
            parent_body = &(let->body);
          });

      proc->body.Emplace(proc, remove);
      already_checked = model->table;
    }

  // This insert isn't associated with any persistent storage.
  // It must be a stream.
  } else {
    assert(insert.IsStream());
    already_checked = nullptr;
  }

  // Figure out which columns of the predecessor we have.
  const auto predecessors = view.Predecessors();
  assert(predecessors.size() == 1u);
  const QueryView pred_view = predecessors[0];

  std::vector<QueryColumn> available_cols;
  for (auto col : insert_cols) {
    if (QueryView::Containing(col) == pred_view) {
      available_cols.push_back(col);
    }
  }

  // Sort in order of index, and then unique them.
  std::sort(available_cols.begin(), available_cols.end(),
            [] (QueryColumn a, QueryColumn b) {
              return *(a.Index()) < *(b.Index());
            });
  auto it = std::unique(available_cols.begin(), available_cols.end());
  available_cols.erase(it, available_cols.end());

  const auto checker_proc = GetOrCreateTopDownChecker(
      impl, context, pred_view, available_cols, model->table);

  // Now call the checker procedure. Unlike in normal checkers, we're doing
  // a check on `false`.
  const auto check = impl->operation_regions.CreateDerived<CALL>(
      impl->next_id++, parent, checker_proc,
      ProgramOperation::kCallProcedureCheckFalse);
  for (auto col : available_cols) {
    check->arg_vars.AddUse(parent->VariableFor(impl, col));
  }

  COMMENT( check->comment = __FILE__ ": CreateBottomUpInsertRemover"; )

  // Now we're inside of the check, and we know for certain this tuple has
  // been removed because the checker function returned `false`.
  parent_body->Emplace(parent, check);
  parent = check;
  parent_body = &(check->body);

  // If were doing a removal to a stream, then we want to publish the removal.
  if (insert.IsStream()) {
    const auto stream = insert.Stream();
    assert(stream.IsIO());
    auto io = QueryIO::From(stream);

    const auto message_publish = impl->operation_regions.CreateDerived<PUBLISH>(
        parent, ParsedMessage::From(io.Declaration()),
        ProgramOperation::kPublishMessageRemoval);

    for (auto col : insert_cols) {
      const auto var = parent->VariableFor(impl, col);
      message_publish->arg_vars.AddUse(var);
    }

    parent_body->Emplace(parent, message_publish);
    parent_body = nullptr;
    parent = nullptr;

  // Otherwise, call our successor removal functions. In this case, we're trying
  // to call the removers associated with every `QuerySelect` node.
  } else {
    const auto par = impl->parallel_regions.Create(parent);
    parent_body->Emplace(parent, par);
    parent_body = nullptr;
    parent = par;

    for (auto succ_view : view.Successors()) {
      assert(succ_view.IsSelect());

      const auto sel_cols = succ_view.Columns();
      assert(sel_cols.size() == insert_cols.size());

      for (auto sel_succ : succ_view.Successors()) {

        const auto call = impl->operation_regions.CreateDerived<CALL>(
            impl->next_id++, par,
            GetOrCreateBottomUpRemover(impl, context, succ_view, sel_succ,
                                       already_checked));

        for (auto sel_col : sel_cols) {
          const auto var =
              proc->VariableFor(impl, insert_cols[*(sel_col.Index())]);
          assert(var != nullptr);
          call->arg_vars.AddUse(var);
        }

        par->AddRegion(call);
      }
    }
  }
}

// Build a top-down checker for a relational insert.
//
// NOTE(pag): `available_cols` is always some subset of the input columns read
//            by the insert.
void BuildTopDownInsertChecker(ProgramImpl *impl, Context &context, PROC *proc,
                               QueryInsert insert,
                               std::vector<QueryColumn> &view_cols,
                               TABLE *already_checked) {
  const QueryView view(insert);
  const auto pred_views = view.Predecessors();
  assert(!pred_views.empty());
  const QueryView pred_view = pred_views[0];
  const auto model = impl->view_to_model[view]->FindAs<DataModel>();
  const auto pred_model = impl->view_to_model[pred_view]->FindAs<DataModel>();

  // If the predecessor persists the same data then we'll call the
  // predecessor's checker.
  //
  // NOTE(pag): `view_cols` is already expressed in terms of `pred_view`.
  if (already_checked == model->table ||
      model->table == pred_model->table) {
    const auto check = CallTopDownChecker(
        impl, context, proc, pred_view, view_cols, pred_view,
        ProgramOperation::kCallProcedureCheckTrue, already_checked);
    proc->body.Emplace(proc, check);

    COMMENT( check->comment = __FILE__ ": BuildTopDownInsertChecker"; )

    const auto ret_true = BuildStateCheckCaseReturnTrue(impl, check);
    check->body.Emplace(check, ret_true);
    return;
  }

  // The predecessor persists different data, so we'll check in the table,
  // and if it's not present, /then/ we'll call the predecessor handler.
  assert(view_cols.size() == insert.NumInputColumns());

  // This INSERT was persisted, thus we can check it.
  assert(model->table);
  TABLE *table_to_update = model->table;
  already_checked = model->table;

  auto call_pred = [&](REGION *parent) -> REGION * {
    const auto check = ReturnTrueWithUpdateIfPredecessorCallSucceeds(
        impl, context, parent, view, view_cols, table_to_update, pred_view,
        already_checked);
    COMMENT( check->comment = __FILE__ ": BuildTopDownInsertChecker::call_pred"; )
    return check;
  };

  if (view.CanReceiveDeletions()) {
    proc->body.Emplace(proc, BuildTopDownCheckerStateCheck(
        impl, proc, model->table, view_cols,
        BuildStateCheckCaseReturnTrue, BuildStateCheckCaseReturnFalse,
        [&](ProgramImpl *, REGION *parent) -> REGION * {
          return BuildTopDownTryMarkAbsent(
              impl, model->table, parent, view_cols,
              [&](PARALLEL *par) {
                call_pred(par)->ExecuteAlongside(impl, par);
              });
        }));
  } else {
    proc->body.Emplace(proc, BuildTopDownCheckerStateCheck(
        impl, proc, model->table, view_cols,
        BuildStateCheckCaseReturnTrue, BuildStateCheckCaseReturnFalse,
        BuildStateCheckCaseReturnFalse));
  }
}

}  // namespace hyde
