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
    auto io = QueryIO::From(insert.Stream());
    auto message = ParsedMessage::From(io.Declaration());

    // There's an accumulation vector, add it in.
    if (const auto pub_vec = context.publish_vecs[message];
        pub_vec != nullptr) {

      auto append = impl->operation_regions.CreateDerived<VECTORAPPEND>(
          parent, ProgramOperation::kAppendToMessageOutputVector);
      parent->body.Emplace(parent, append);
      append->vector.Emplace(append, pub_vec);

      for (auto col : insert.InputColumns()) {
        append->tuple_vars.AddUse(append->VariableFor(impl, col));
      }

    // No accumulation vector, publish right now.
    } else {
      const auto message_publish = impl->operation_regions.CreateDerived<PUBLISH>(
          parent, message);
      parent->body.Emplace(parent, message_publish);

      for (auto col : cols) {
        const auto var = parent->VariableFor(impl, col);
        message_publish->arg_vars.AddUse(var);
      }
    }

  // Inserting into a relation.
  } else if (insert.IsRelation()) {
    BuildEagerInsertionRegions(impl, view, context, parent, view.Successors(),
                               last_table);

  } else {
    assert(false);
  }
}

// A bottom-up insert remover is not a DELETE; instead it is that the relation
// that backs this INSERT is somehow subject to differential updates, e.g.
// because it is downstream from an aggregate or kvindex.
void CreateBottomUpInsertRemover(ProgramImpl *impl, Context &context,
                                 QueryView view, OP *parent_,
                                 TABLE *already_removed_) {
  auto [parent, table, already_removed] = InTryMarkUnknown(
        impl, view, parent_, already_removed_);

  const auto insert = QueryInsert::From(view);
  const auto insert_cols = insert.InputColumns();
  (void) insert_cols;

  // If were doing a removal to a stream, then we want to defer publication
  // of the removal until later, when we know the thing is truly gone.
  if (insert.IsStream()) {
    auto io = QueryIO::From(insert.Stream());
    auto message = ParsedMessage::From(io.Declaration());

    const auto pub_vec = context.publish_vecs[message];
    assert(pub_vec != nullptr);

    auto append = impl->operation_regions.CreateDerived<VECTORAPPEND>(
        parent, ProgramOperation::kAppendToMessageOutputVector);
    parent->body.Emplace(parent, append);
    append->vector.Emplace(append, pub_vec);

    for (auto col : insert.InputColumns()) {
      append->tuple_vars.AddUse(append->VariableFor(impl, col));
    }

  // Otherwise, call our successor removal functions. In this case, we're trying
  // to call the removers associated with every `QuerySelect` node.
  } else {

    const auto par = impl->parallel_regions.Create(parent);
    parent->body.Emplace(parent, par);

    // Make sure that we've already done the checking for these nodes.
    assert(table != nullptr);
    assert(already_removed == table);

    for (auto succ_view : view.Successors()) {
      assert(succ_view.IsSelect());

      assert(succ_view.Columns().size() == insert_cols.size());

      auto let = impl->operation_regions.CreateDerived<LET>(par);
      par->AddRegion(let);

      BuildEagerRemovalRegions(impl, succ_view, context, let,
                               succ_view.Successors(), already_removed);
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
    const auto [check, check_call] = CallTopDownChecker(
        impl, context, proc, pred_view, view_cols, pred_view, already_checked);
    proc->body.Emplace(proc, check);

    COMMENT( check_call->comment = __FILE__ ": BuildTopDownInsertChecker"; )

    const auto ret_true = BuildStateCheckCaseReturnTrue(impl, check_call);
    check_call->body.Emplace(check_call, ret_true);
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
