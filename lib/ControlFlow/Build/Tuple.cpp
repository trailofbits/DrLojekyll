// Copyright 2020, Trail of Bits. All rights reserved.

#include "Build.h"

namespace hyde {

// Build an eager region for tuple. If the tuple can receive differential
// updates then its data needs to be saved.
void BuildEagerTupleRegion(ProgramImpl *impl, QueryView pred_view,
                           QueryTuple tuple, Context &context, OP *parent,
                           TABLE *last_model) {
  const QueryView view(tuple);

  // If this tuple maintains all of the columns of its predecessor, then we
  // don't need to do anything special for differential updates, because we
  // can call the predecessor top-down checker when we're generating code for
  // differential proving.
  //
  // NOTE(pag): We don't even check if `view` is subject to differential
  //            updates. The only interesting case is the one where we have a
  //            TUPLE that takes its data from a message RECEIVE. In this case,
  //            the message receipt is treated as ephemeral and thus
  //            uncheckable.
  if (MayNeedToBePersisted(pred_view) &&
      view.AllColumnsOfSinglePredecessorAreUsed()) {
    (void) 0;

  // If this tuple may be the base case for a top-down recursive check (that
  // supports differential updates), then we need to make sure that the input
  // data provided to this tuple is persisted. At first glance, one might think
  // that we need to persist the tuple's output data; however, this is not
  // quite right because the tuple might narrow it's input data, keeping only
  // a few columns, or it may widen it, i.e. duplicated some of the columns,
  // or introducing constants. We don't maintain precise enough refcounts to be
  // able to know the number of ways in which a tuple might have produced some
  // data, and so we need to be able to look upon that data at a later time to
  // recover the ways.
  } else if (MayNeedToBePersisted(view)) {

    // NOTE(pag): See comment above, use of `pred_view` in getting the table
    //            is deliberate.
    if (const auto table = TABLE::GetOrCreate(impl, pred_view);
        table != last_model) {
      parent = BuildInsertCheck(impl, pred_view, context, parent, table,
                                true, pred_view.Columns());
      last_model = table;
    }
  }

  BuildEagerSuccessorRegions(impl, view, context, parent, view.Successors(),
                             last_model);
}

// Build a top-down checker on a tuple. This possibly widens the tuple, i.e.
// recovering "lost" columns, and possibly re-orders arguments before calling
// down to the tuple's predecessor's checker.
void BuildTopDownTupleChecker(ProgramImpl *impl, Context &context,
                              PROC *proc, QueryTuple tuple) {
  const QueryView view(tuple);
  const auto pred_view = view.Predecessors()[0];

  // This tuple doesn't throw away any of `pred_view`s columns, so we can call
  // the checker for `pred_view`.
  if (MayNeedToBePersisted(pred_view) &&
      view.AllColumnsOfSinglePredecessorAreUsed()) {

    const auto check = impl->operation_regions.CreateDerived<CALL>(
        proc, GetOrCreateTopDownChecker(impl, context, pred_view),
        ProgramOperation::kCallProcedureCheckFalse);

    const auto inout_cols = GetColumnMap(view, pred_view);
    for (auto [in_col, out_col] : inout_cols) {
      const auto in_var = proc->VariableFor(impl, out_col);
      check->arg_vars.AddUse(in_var);
    }

    UseRef<REGION>(proc, check).Swap(proc->body);

  // We need to do an index scan and try to prove that this tuple is present.
  } else if (MayNeedToBePersisted(view)) {

    std::vector<unsigned> in_col_indices;
    std::vector<bool> indexed_cols(pred_view.Columns().size());
    const auto inout_cols = GetColumnMap(view, pred_view);
    for (auto [in_col, out_col] : inout_cols) {
      const unsigned in_col_index = *(in_col.Index());
      in_col_indices.push_back(in_col_index);
      indexed_cols[in_col_index] = true;

      proc->col_id_to_var.emplace(
          in_col.Id(), proc->VariableFor(impl, out_col));
    }

    // Figure out what columns we're selecting.
    std::vector<QueryColumn> selected_cols;
    for (auto pred_col : pred_view.Columns()) {
      if (!indexed_cols[*(pred_col.Index())]) {
        selected_cols.push_back(pred_col);
      }
    }

    const auto table = TABLE::GetOrCreate(impl, pred_view);
    const auto index = table->GetOrCreateIndex(impl, std::move(in_col_indices));

    const auto seq = impl->series_regions.Create(proc);
    UseRef<REGION>(proc, seq).Swap(proc->body);

    const auto vec = proc->vectors.Create(
        impl->next_id++, VectorKind::kTableScan, selected_cols);

    // Scan an index, using the columns from the tuple to find the columns
    // from the tuple's predecessor.
    const auto scan = impl->operation_regions.CreateDerived<TABLESCAN>(seq);
    scan->ExecuteAfter(impl, seq);
    UseRef<TABLE>(scan, table).Swap(scan->table);
    UseRef<TABLEINDEX>(scan, index).Swap(scan->index);
    UseRef<VECTOR>(scan, vec).Swap(scan->output_vector);

    for (auto [in_col, out_col] : inout_cols) {
      const auto in_var = proc->VariableFor(impl, out_col);
      scan->in_vars.AddUse(in_var);
    }
    for (auto table_col : table->columns) {
      if (indexed_cols[table_col->index]) {
        scan->in_cols.AddUse(table_col);

      } else {
        scan->out_cols.AddUse(table_col);
      }
    }

    // Loop over the results of the table scan.
    const auto loop = impl->operation_regions.CreateDerived<VECTORLOOP>(
        seq, ProgramOperation::kLoopOverScanVector);
    loop->ExecuteAfter(impl, seq);
    UseRef<VECTOR>(loop, vec).Swap(loop->vector);

    for (auto col : selected_cols) {
      const auto var = loop->defined_vars.Create(
          impl->next_id++, VariableRole::kScanOutput);
      var->query_column = col;
      loop->col_id_to_var.emplace(col.Id(), var);
    }

    for (auto pred_col : pred_view.Columns()) {
      if (!indexed_cols[*(pred_col.Index())]) {
        selected_cols.push_back(pred_col);
      }
    }

    // Inside the scan, we'll check if anything that could feasibly be
    // feeding this tuple matches, and if so, `return-true`.
    const auto check = impl->operation_regions.CreateDerived<CALL>(
        loop, GetOrCreateTopDownChecker(impl, context, pred_view),
        ProgramOperation::kCallProcedureCheckTrue);

    for (auto col : pred_view.Columns()) {
      const auto in_var = loop->VariableFor(impl, col);
      check->arg_vars.AddUse(in_var);
    }

    UseRef<REGION>(loop, check).Swap(loop->body);

    const auto sub_seq = impl->series_regions.Create(check);
    UseRef<REGION>(check, sub_seq).Swap(check->body);

    // Clear out the scan vector if we've proven the tuple.
    const auto clear_found = impl->operation_regions.CreateDerived<VECTORCLEAR>(
        sub_seq, ProgramOperation::kClearScanVector);
    UseRef<VECTOR>(clear_found, vec).Swap(clear_found->vector);
    clear_found->ExecuteAfter(impl, sub_seq);

    // Change the tuple's state if we've proven it.
    const auto table_insert =
        impl->operation_regions.CreateDerived<CHANGESTATE>(
            sub_seq, TupleState::kAbsentOrUnknown, TupleState::kPresent);
    for (auto col : tuple.Columns()) {
      const auto var = proc->VariableFor(impl, col);
      table_insert->col_values.AddUse(var);
    }

    UseRef<TABLE>(table_insert, table).Swap(table_insert->table);
    table_insert->ExecuteAfter(impl, sub_seq);

    // Return `true` if we've proven the tuple.
    const auto ret = impl->operation_regions.CreateDerived<RETURN>(
        sub_seq, ProgramOperation::kReturnTrueFromProcedure);
    ret->ExecuteAfter(impl, sub_seq);

    // Clear out the scan vector after the loop. We'll let the caller inject
    // a `return-false`.
    const auto clear_notfound = impl->operation_regions.CreateDerived<VECTORCLEAR>(
        seq, ProgramOperation::kClearScanVector);
    UseRef<VECTOR>(clear_notfound, vec).Swap(clear_notfound->vector);
    clear_notfound->ExecuteAfter(impl, seq);

  // We've gotten down here and need to find the base case of something. We
  // don't really know if this tuple is backed by a table or not. We'll check
  // anyway. This will come up in the case that, for example, we have a JOIN
  // that can receive a differential update from one of the sources, but not
  // *this* particular source.
  //
  // It's possible that there is nothing that inserts into `table`, which may be
  // fine because the return will be `false`.
  //
  // TODO(pag): Possibly more thought needs to go into this.
  } else {
    const auto table = TABLE::GetOrCreate(impl, view);
    const auto check = impl->operation_regions.CreateDerived<CHECKSTATE>(proc);
    for (auto col : view.Columns()) {
      const auto var = proc->VariableFor(impl, col);
      check->col_values.AddUse(var);
    }

    UseRef<TABLE>(check, table).Swap(check->table);
    UseRef<REGION>(proc, check).Swap(proc->body);

    // If the tuple is present, then return `true`.
    const auto present = impl->operation_regions.CreateDerived<RETURN>(
        check, ProgramOperation::kReturnTrueFromProcedure);
    UseRef<REGION>(check, present).Swap(check->OP::body);
  }
}

}  // namespace hyde
