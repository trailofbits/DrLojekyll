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
                            TABLE *last_model) {
  const auto view = QueryView(insert);
  const auto cols = insert.InputColumns();

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
    if (const auto table = TABLE::GetOrCreate(impl, view);
        table != last_model) {

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
      last_model = table;
    }

    // If we're setting a condition then we also need to see if any constant
    // tuples depend on that condition.
    if (auto set_cond = view.SetCondition(); set_cond) {
      const auto seq = impl->series_regions.Create(parent);
      parent->body.Emplace(parent, seq);

      // Now that we know that the data has been dealt with, we increment the
      // condition variable.
      const auto set = impl->operation_regions.CreateDerived<ASSERT>(
          seq, ProgramOperation::kIncrementAll);
      set->cond_vars.AddUse(ConditionVariable(impl, *set_cond));
      set->ExecuteAfter(impl, seq);

      // Call the initialization procedure. The initialization procedure is
      // responsible for initializing data flow from constant tuples that
      // may be condition-variable dependent.
      const auto call = impl->operation_regions.CreateDerived<CALL>(
          seq, impl->procedure_regions[0]);
      call->ExecuteAfter(impl, seq);

      // Create a dummy/empty LET binding so that we have an `OP *` as a parent
      // going forward.
      parent = impl->operation_regions.CreateDerived<LET>(seq);
      parent->ExecuteAfter(impl, seq);
    }

    if (const auto succs = view.Successors(); succs.size()) {
      BuildEagerSuccessorRegions(impl, view, context, parent, succs,
                                 last_model);
    }

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
  const auto insert_cols = QueryInsert::From(view).InputColumns();

  const auto model = impl->view_to_model[view]->FindAs<DataModel>();
  PARALLEL *parent = nullptr;

  if (model->table) {

    // We've already transitioned for this table, so our job is just to pass
    // the buck along, and then eventually we'll temrinate recursion.
    if (already_checked == model->table) {

      parent = impl->parallel_regions.Create(proc);
      proc->body.Emplace(proc, parent);

    // The caller didn't already do a state transition, so we cn do it.
    } else {
      auto remove = BuildBottomUpTryMarkUnknown(
          impl, model->table, proc, insert_cols,
          [&] (PARALLEL *par) {
            parent = par;
          });

      proc->body.Emplace(proc, remove);

      already_checked = model->table;
    }

  // This insert isn't associated with any persistent storage. That is unusual.
  } else {
    assert(false);

    already_checked = nullptr;
    parent = impl->parallel_regions.Create(proc);
    proc->body.Emplace(proc, parent);
  }

  for (auto succ_view : view.Successors()) {
    assert(succ_view.IsSelect());

    const auto sel_cols = succ_view.Columns();
    assert(sel_cols.size() == insert_cols.size());

    for (auto sel_succ : succ_view.Successors()) {

      const auto call = impl->operation_regions.CreateDerived<CALL>(
          parent, GetOrCreateBottomUpRemover(impl, context, succ_view, sel_succ,
                                             already_checked));

      for (auto sel_col : sel_cols) {
        const auto var = proc->VariableFor(
            impl, insert_cols[*(sel_col.Index())]);
        assert(var != nullptr);
        call->arg_vars.AddUse(var);
      }

      parent->regions.AddUse(call);
    }
  }

  auto ret = impl->operation_regions.CreateDerived<RETURN>(
      proc, ProgramOperation::kReturnFalseFromProcedure);
  ret->ExecuteAfter(impl, proc);
}

}  // namespace hyde
