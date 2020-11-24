// Copyright 2020, Trail of Bits. All rights reserved.

#include "Build.h"

namespace hyde {
namespace {

class ContinueJoinWorkItem final : public WorkItem {
 public:
  virtual ~ContinueJoinWorkItem(void) {}

  ContinueJoinWorkItem(QueryView view_)
      : WorkItem(view_.Depth()),
        view(view_) {}

  // Find the common ancestor of all insert regions.
  REGION *FindCommonAncestorOfInsertRegions(void) const;

  void Run(ProgramImpl *program, Context &context) override;

  std::vector<OP *> inserts;

 private:
  QueryView view;
};

// Find the common ancestor of all insert regions.
REGION *ContinueJoinWorkItem::FindCommonAncestorOfInsertRegions(void) const {
  PROC *const proc = inserts[0]->containing_procedure;
  REGION *common_ancestor = nullptr;

  for (const auto insert : inserts) {
    if (!common_ancestor) {
      common_ancestor = insert;
    } else {
      common_ancestor = common_ancestor->FindCommonAncestor(insert);
    }
  }

  assert(common_ancestor != nullptr);
  if (proc == common_ancestor || !common_ancestor) {
    common_ancestor = proc->body.get();
  }

  return common_ancestor->NearestRegionEnclosedByInduction();
}

// Build a join region given a JOIN view and a pivot vector.
static TABLEJOIN *BuildJoin(ProgramImpl *impl, QueryJoin join_view,
                            VECTOR *pivot_vec, SERIES *seq) {

  // We're now either looping over pivots in a pivot vector, or there was only
  // one entrypoint to the `QueryJoin` that was followed pre-work item, and
  // so we're in the body of an `insert`.
  const auto join = impl->operation_regions.CreateDerived<TABLEJOIN>(
      seq, join_view, impl->next_id++);
  join->ExecuteAfter(impl, seq);

  // The JOIN internalizes the loop over its pivot vector. This is so that
  // it can have visibility into the sortedness, and choose what to do based
  // of of runs of sorted elements.
  join->pivot_vec.Emplace(join, pivot_vec);

  // After running the join, clear out the pivot vector.
  const auto clear = impl->operation_regions.CreateDerived<VECTORCLEAR>(
      seq, ProgramOperation::kClearJoinPivotVector);
  clear->vector.Emplace(clear, pivot_vec);
  clear->ExecuteAfter(impl, seq);

  // Fill in the pivot variables/columns.
  for (auto pivot_col : join_view.PivotColumns()) {
    auto var =
        join->pivot_vars.Create(impl->next_id++, VariableRole::kJoinPivot);
    var->query_column = pivot_col;
    if (pivot_col.IsConstantRef()) {
      var->query_const = QueryConstant::From(pivot_col);
    }

    join->col_id_to_var.emplace(pivot_col.Id(), var);
  }

  std::vector<unsigned> pivot_col_indices;
  std::vector<QueryColumn> pivot_cols;
  std::unordered_map<QueryView, unsigned> view_to_index;
  const auto pred_views = join_view.JoinedViews();
  const auto num_pivots = join_view.NumPivotColumns();

  // Add in the pivot columns, the tables from which we're selecting, and
  // the indexes that we're scanning.
  for (auto i = 0u, max_i = pred_views.size(); i < max_i; ++i) {

    pivot_cols.clear();
    const auto pred_view = pred_views[i];
    for (auto j = 0u; j < num_pivots; ++j) {
      for (auto pivot_col : join_view.NthInputPivotSet(j)) {
        assert(!pivot_col.IsConstant());
        if (QueryView::Containing(pivot_col) == pred_view) {
          pivot_cols.push_back(pivot_col);
          pivot_col_indices.push_back(*(pivot_col.Index()));
          break;
        }
      }
    }

    const auto table = TABLE::GetOrCreate(impl, pred_view);
    const auto index =
        table->GetOrCreateIndex(impl, std::move(pivot_col_indices));
    join->tables.AddUse(table);
    join->indices.AddUse(index);

    join->pivot_cols.emplace_back(join);
    join->output_cols.emplace_back(join);
    join->output_vars.emplace_back(join);
    view_to_index.emplace(pred_view, i);

    auto &pivot_table_cols = join->pivot_cols.back();
    for (auto pivot_col : pivot_cols) {
      for (auto indexed_col : index->columns) {
        if (pivot_col.Index() && indexed_col->index == *(pivot_col.Index())) {
          pivot_table_cols.AddUse(indexed_col);
          goto matched_pivot_col;
        }
      }
      assert(false);
    matched_pivot_col:
      continue;
    }
  }

  // Add in the non-pivot columns.
  join_view.ForEachUse([&](QueryColumn in_col, InputColumnRole role,
                           std::optional<QueryColumn> out_col) {
    if (in_col.IsConstantOrConstantRef() && out_col &&
        !out_col->IsConstantOrConstantRef()) {
      auto in_var = join->VariableFor(impl, in_col);
      join->col_id_to_var.emplace(out_col->Id(), in_var);
    }

    if (InputColumnRole::kJoinNonPivot != role || !out_col ||
        in_col.IsConstantOrConstantRef() ||
        out_col->IsConstantOrConstantRef()) {
      return;
    }

    const auto pred_view = QueryView::Containing(in_col);
    const auto pred_view_idx = view_to_index[pred_view];
    const auto table = join->tables[pred_view_idx];
    auto &out_cols = join->output_cols.at(pred_view_idx);
    auto &out_vars = join->output_vars.at(pred_view_idx);

    out_cols.AddUse(table->columns[*(in_col.Index())]);
    auto var = out_vars.Create(impl->next_id++, VariableRole::kJoinNonPivot);
    var->query_column = *out_col;

    join->col_id_to_var.emplace(in_col.Id(), var);
    join->col_id_to_var.emplace(out_col->Id(), var);
  });

  return join;
}

void ContinueJoinWorkItem::Run(ProgramImpl *impl, Context &context) {
  if (inserts.empty()) {
    assert(false);
    return;
  }

  context.view_to_work_item.erase(view);

  const auto join_view = QueryJoin::From(view);
  PROC *const proc = inserts[0]->containing_procedure;

  auto pivot_vec =
      proc->VectorFor(impl, VectorKind::kJoinPivots, join_view.PivotColumns());

  for (auto insert : inserts) {
    const auto append = impl->operation_regions.CreateDerived<VECTORAPPEND>(
        insert, ProgramOperation::kAppendJoinPivotsToVector);

    for (auto col : join_view.PivotColumns()) {
      const auto var = insert->VariableFor(impl, col);
      append->tuple_vars.AddUse(var);
    }

    append->vector.Emplace(append, pivot_vec);
    insert->body.Emplace(insert, append);
  }

  // Find the common ancestor of all of the `kInsertIntoView` associated with
  // the reached `QueryJoin`s that happened before this work item. Everything
  // under this common ancestor must execute before the loop over the join_view
  // pivots.
  const auto ancestor = FindCommonAncestorOfInsertRegions();
  const auto seq = impl->series_regions.Create(ancestor->parent);
  ancestor->ReplaceAllUsesWith(seq);
  ancestor->ExecuteAfter(impl, seq);

  // Sort and unique the pivot vector before looping.
  const auto unique = impl->operation_regions.CreateDerived<VECTORUNIQUE>(
      seq, ProgramOperation::kSortAndUniquePivotVector);
  unique->vector.Emplace(unique, pivot_vec);
  unique->ExecuteAfter(impl, seq);

  OP *parent = BuildJoin(impl, join_view, pivot_vec, seq);

  // If this join can receive deletions, then we need to possibly double check
  // its sources, because indices don't actually maintain states.
  if (view.CanReceiveDeletions()) {

    // We (should) have all columns by this point, so we'll proceed like that.
    std::vector<QueryColumn> view_cols(view.Columns().begin(),
                                       view.Columns().end());

    // Call the predecessors. If any of the predecessors return `false` then that
    // means we have failed.
    for (auto pred_view : view.Predecessors()) {

      // If this predecessor doesn't produce deletions then we don't need to
      // double check its index.
      if (!pred_view.CanProduceDeletions()) {
        continue;
      }

      const auto index_is_good = CallTopDownChecker(
          impl, context, parent, view, view_cols, pred_view,
          ProgramOperation::kCallProcedureCheckTrue, nullptr);

      const auto ret_false =
          BuildStateCheckCaseReturnFalse(impl, index_is_good);
      index_is_good->body.Emplace(index_is_good, ret_false);
      parent->body.Emplace(parent, index_is_good);
      parent = index_is_good;
    }
  }

  BuildEagerSuccessorRegions(impl, view, context, parent, view.Successors(),
                             nullptr);
}

}  // namespace

// Build an eager region for a join.
void BuildEagerJoinRegion(ProgramImpl *impl, QueryView pred_view,
                          QueryJoin view, Context &context, OP *parent,
                          TABLE *last_model) {

  // First, check if we should push this tuple through the JOIN. If it's
  // not resident in the view tagged for the `QueryJoin` then we know it's
  // never been seen before.
  if (const auto table = TABLE::GetOrCreate(impl, pred_view);
      table != last_model) {

    parent = BuildInsertCheck(impl, pred_view, context, parent, table,
                              QueryView(view).CanReceiveDeletions(),
                              pred_view.Columns());
    last_model = table;
  }

  auto &action = context.view_to_work_item[view];
  if (!action) {
    action = new ContinueJoinWorkItem(view);
    context.work_list.emplace_back(action);
  }

  dynamic_cast<ContinueJoinWorkItem *>(action)->inserts.push_back(parent);
}

// Build a top-down checker on a join.
void BuildTopDownJoinChecker(ProgramImpl *impl, Context &context, PROC *proc,
                             QueryJoin join_view,
                             std::vector<QueryColumn> &view_cols,
                             TABLE *already_checked) {

  QueryView view(join_view);
  const auto model = impl->view_to_model[view]->FindAs<DataModel>();

  // Append the pivots to our pivot vector.
  auto pivot_vec =
      proc->VectorFor(impl, VectorKind::kJoinPivots, join_view.PivotColumns());

  SERIES *const seq = impl->series_regions.Create(proc);
  proc->body.Emplace(proc, seq);

  // Append a tuple to a pivot vector.
  auto add_to_pivot_vec = [&](REGION *parent) -> REGION * {
    const auto append = impl->operation_regions.CreateDerived<VECTORAPPEND>(
        parent, ProgramOperation::kAppendJoinPivotsToVector);
    for (auto col : join_view.PivotColumns()) {
      const auto var = parent->VariableFor(impl, col);
      append->tuple_vars.AddUse(var);
    }
    append->vector.Emplace(append, pivot_vec);
    return append;
  };

  // First things first, regardless of whether or not we have a data model,
  // we'll figure out how many of the pivot columns we already have. If we have
  // the pivots then we're in a good place.
  const auto num_pivots = join_view.NumPivotColumns();
  auto num_found_pivots = 0u;
  std::vector<VAR *> pivots(num_pivots);

  // TODO(pag): Nothing really enforces this, but I don't want to deal with it
  //            until it's a problem. We would need to do a full scan of
  //            one of the joined views.
  assert(!view_cols.empty());

  // Figure out all of our input columns. Figure out how many pivot columns we
  // have, and map in the input column variables.
  std::unordered_map<QueryView, std::vector<QueryColumn>> pred_cols;
  join_view.ForEachUse([&](QueryColumn in_col, InputColumnRole role,
                           std::optional<QueryColumn> out_col) {
    if (std::find(view_cols.begin(), view_cols.end(), *out_col) !=
        view_cols.end()) {

      // We found a pivot that we have as an argument.
      if (InputColumnRole::kJoinPivot == role) {
        auto &pivot_var = pivots[*(out_col->Index())];
        if (!pivot_var) {
          pivot_var = proc->VariableFor(impl, *out_col);
          ++num_found_pivots;
        }
      }

      if (!in_col.IsConstant()) {
        const auto param_var = proc->VariableFor(impl, *out_col);
        proc->col_id_to_var.emplace(in_col.Id(), param_var);
        pred_cols[QueryView::Containing(in_col)].push_back(in_col);
      }
    }
  });

  assert(!pred_cols.empty());

  // Best case: we already have all of the pivots, so we'll be able to go
  // directly and do a join.
  if (num_found_pivots == num_pivots) {
    seq->regions.AddUse(add_to_pivot_vec(seq));

  // Second best case: we have a model for this JOIN table, so we can use the model
  // to find all of the pivots so that later we can do a join.
  } else if (model->table) {

    // NOTE(pag): `BuildMaybeScanPartial` will mutate its input column list,
    //            and this would be fine if we did everything in the scope of
    //            the scan; however, we're just collecting pivots in the scan
    //            and want to depend on `view_cols` representing the inputs
    //            to the function later, so we'll make a copy here so that
    //            the original is preserved.
    auto view_cols_copy = view_cols;

    seq->regions.AddUse(BuildMaybeScanPartial(
        impl, view, view_cols_copy, model->table, seq,
        [&](REGION *parent) -> REGION * { return add_to_pivot_vec(parent); }));

  // Worst-case, but really not so bad. The JOIN itself doesn't have a data
  // model. We don't yet have all the pivots. We know, however, that all
  // predecesors of a JOIN have a model, so we can depend upon them.
  } else {

    // Go find the most represented view. We will use that in an index scan.
    auto max_view = pred_cols.begin()->first;
    auto max_score = 0.0;
    std::vector<QueryColumn> *max_view_cols = &(pred_cols.begin()->second);

    for (auto &[pred_view, pred_view_cols] : pred_cols) {
      auto view_score =
          double(pred_view_cols.size()) / double(pred_view.Columns().size());
      if (view_score > max_score) {
        max_score = view_score;
        max_view = pred_view;
        max_view_cols = &pred_view_cols;
      }
    }

    const auto pred_model = impl->view_to_model[max_view]->FindAs<DataModel>();
    assert(pred_model->table != nullptr);

    auto &pred_view_cols = *max_view_cols;
    seq->regions.AddUse(BuildMaybeScanPartial(
        impl, max_view, pred_view_cols, pred_model->table, seq,
        [&](REGION *parent) -> REGION * {
          // Map the `max_view` variables to be named in the same way as `view`s
          // variables so that we can use `add_to_pivot_vec`.
          join_view.ForEachUse([&](QueryColumn in_col, InputColumnRole role,
                                   std::optional<QueryColumn> out_col) {
            if (!in_col.IsConstant() &&
                QueryView::Containing(in_col) == max_view) {
              const auto in_var = parent->VariableFor(impl, in_col);
              parent->col_id_to_var.emplace(out_col->Id(), in_var);
            }
          });

          return add_to_pivot_vec(parent);
        }));
  }

  // By this point, we have all of the pivots, so we can do a join.

  // Sort and unique the pivot vector before doing our JOIN.
  const auto unique = impl->operation_regions.CreateDerived<VECTORUNIQUE>(
      seq, ProgramOperation::kSortAndUniquePivotVector);
  unique->vector.Emplace(unique, pivot_vec);
  seq->regions.AddUse(unique);

  const auto join = BuildJoin(impl, join_view, pivot_vec, seq);

  // Make sure any non-pivot inputs are checked for equality; we don't care
  // about that data.
  const auto check = impl->operation_regions.CreateDerived<TUPLECMP>(
      join, ComparisonOperator::kEqual);
  join->body.Emplace(join, check);
  join_view.ForEachUse([&](QueryColumn in_col, InputColumnRole role,
                           std::optional<QueryColumn> out_col) {
    if (InputColumnRole::kJoinNonPivot == role) {
      auto join_var = join->VariableFor(impl, in_col);
      auto param_var = proc->col_id_to_var[in_col.Id()];
      if (param_var) {
        check->lhs_vars.AddUse(param_var);
        check->rhs_vars.AddUse(join_var);
      }
    }
  });

  // Okay, by this point we know that we're in the right tuple, given the
  // inputs to the function an the results of the join. Now we need to do
  // state checking.
  SERIES *const in_check = impl->series_regions.Create(check);
  check->body.Emplace(check, in_check);

  PARALLEL *par = nullptr;

  auto do_state_transition = false;

  if (model->table && already_checked != model->table) {
    in_check->regions.AddUse(BuildTopDownCheckerStateCheck(
        impl, in_check, model->table, view.Columns(),
        BuildStateCheckCaseReturnTrue, BuildStateCheckCaseNothing,
        [&](ProgramImpl *, REGION *parent) -> REGION * {
          do_state_transition = true;
          return BuildTopDownTryMarkAbsent(
              impl, model->table, parent, view.Columns(),
              [&](PARALLEL *par_node) { par = par_node; });
        }));

  } else {
    par = impl->parallel_regions.Create(in_check);
    in_check->regions.AddUse(par);
  }

  already_checked = model->table;  // May be `nullptr`.

  // We (should) have all columns by this point, so we'll proceed like that.
  view_cols.clear();
  view_cols.insert(view_cols.end(), view.Columns().begin(),
                   view.Columns().end());

  // Call the predecessors. If any of the predecessors return `false` then that
  // means we have failed.
  for (auto pred_view : view.Predecessors()) {

    // If this predecessor doesn't produce deletions then we don't need to
    // double check its index.
    if (!pred_view.CanProduceDeletions()) {
      continue;
    }

    const auto one_is_bad = CallTopDownChecker(
        impl, context, par, view, view_cols, pred_view,
        ProgramOperation::kCallProcedureCheckFalse, already_checked);

    const auto ret_false = BuildStateCheckCaseReturnFalse(impl, one_is_bad);
    one_is_bad->body.Emplace(one_is_bad, ret_false);
    par->regions.AddUse(one_is_bad);
  }

  // If all predecessors return `true`, then we can change this tuple's state
  // if it has a model and the caller isn't doing it for us, and then return
  // true.
  if (do_state_transition) {
    in_check->regions.AddUse(
        BuildChangeState(impl, model->table, in_check, view_cols,
                         TupleState::kAbsentOrUnknown, TupleState::kPresent));
  }

  in_check->regions.AddUse(BuildStateCheckCaseReturnTrue(impl, in_check));
}

// Build a bottom-up join remover.
void CreateBottomUpJoinRemover(ProgramImpl *impl, Context &context,
                               QueryView from_view, QueryJoin join_view,
                               PROC *proc, TABLE *already_checked) {
  assert(join_view.NumPivotColumns());

  const QueryView view(join_view);

  auto parent = impl->series_regions.Create(proc);
  proc->body.Emplace(proc, parent);

  // First, and somewhat unlike other bottom-up removers, we will make sure that
  // the data is gone in the data model associated with this particular
  // predecessor. This is because JOINs require that their predecessors all
  // have backing storage.
  const auto pred_model = impl->view_to_model[from_view]->FindAs<DataModel>();
  assert(pred_model->table != nullptr);
  if (already_checked != pred_model->table) {

    const auto table_remove =
        BuildChangeState(impl, pred_model->table, parent, from_view.Columns(),
                         TupleState::kPresent, TupleState::kUnknown);

    parent->regions.AddUse(table_remove);

    // Make a new series region inside of the state change check.
    parent = impl->series_regions.Create(table_remove);
    table_remove->body.Emplace(table_remove, parent);
  }

  // Okay, now we can proceed with the join, knowing that we've cleared out
  // the base case.

  std::unordered_map<QueryView, std::vector<QueryColumn>> pivot_cols;
  std::unordered_map<QueryView, std::vector<unsigned>> pivot_col_indices;
  std::unordered_map<QueryView, std::vector<QueryColumn>> non_pivot_cols;

  join_view.ForEachUse([&](QueryColumn in_col, InputColumnRole role,
                           std::optional<QueryColumn> out_col) {
    const auto in_view = QueryView::Containing(in_col);
    if (InputColumnRole::kJoinPivot == role) {
      pivot_cols[in_view].push_back(in_col);
      pivot_col_indices[in_view].push_back(*(in_col.Index()));

    } else if (InputColumnRole::kJoinNonPivot == role) {
      assert(out_col);
      if (out_col && !in_col.IsConstantOrConstantRef() &&
          !out_col->IsConstantOrConstantRef()) {
        non_pivot_cols[in_view].push_back(in_col);
      }

    } else {
      assert(false);
    }
  });

  // Make sure that we have variable bindings for all the pivot columns across
  // all joined tables.
  const auto pred_views = view.Predecessors();
  const auto num_pivots = join_view.NumPivotColumns();
  const auto &from_view_pivots = pivot_cols[from_view];
  assert(from_view_pivots.size() == num_pivots);

  for (auto pred_view : pred_views) {
    if (pred_view != from_view) {
      const auto &pred_pivots = pivot_cols[pred_view];
      assert(pred_pivots.size() == num_pivots);

      for (auto i = 0u; i < num_pivots; ++i) {
        const auto param_var = proc->VariableFor(impl, from_view_pivots[i]);
        assert(param_var != nullptr);
        proc->col_id_to_var.emplace(pred_pivots[i].Id(), param_var);
      }
    }
  }

  // Called within the context of a join on an index scan.
  auto with_join = [&](REGION *join) -> REGION * {
    join_view.ForEachUse([&](QueryColumn in_col, InputColumnRole,
                             std::optional<QueryColumn> out_col) {
      if (auto in_var = join->VariableFor(impl, in_col); in_var && out_col) {
        join->col_id_to_var.emplace(out_col->Id(), in_var);
      }
    });

    auto par = impl->parallel_regions.Create(join);
    for (auto succ_view : view.Successors()) {
      const auto call = impl->operation_regions.CreateDerived<CALL>(
          parent,
          GetOrCreateBottomUpRemover(impl, context, view, succ_view, nullptr));

      for (auto col : view.Columns()) {
        const auto var = join->VariableFor(impl, col);
        assert(var != nullptr);
        call->arg_vars.AddUse(var);
      }

      par->regions.AddUse(call);
    }
    return par;
  };

  // If this is more than a two-way join then we're going to make a join region
  // so as not to prescribe a join order/strategy (e.g. nested loop join) onto
  // the code.
  if (2u < pred_views.size()) {

    // Create a pivot vector, which is needed by a join region.
    const auto pivot_vec = proc->vectors.Create(
        impl->next_id++, VectorKind::kJoinPivots, from_view_pivots);

    // Create the region that will add the tuple to-be-removed to the pivot
    // vector.
    const auto add_to_vec = impl->operation_regions.CreateDerived<VECTORAPPEND>(
        parent, ProgramOperation::kAppendJoinPivotsToVector);
    parent->regions.AddUse(add_to_vec);

    add_to_vec->vector.Emplace(add_to_vec, pivot_vec);

    for (auto in_col : from_view_pivots) {
      auto pivot_var = proc->VariableFor(impl, in_col);
      assert(pivot_var != nullptr);
      add_to_vec->tuple_vars.AddUse(pivot_var);
    }

    // NOTE(pag): We don't really need to join against all views, just all
    //            views except `from_view`. Re-using `BuildJoin` keeps things a
    //            simpler, and we may be able to better optimize things in the
    //            future so that the bottom-up removers from all predecessor
    //            nodes can "share" this common JOIN code.
    const auto join = BuildJoin(impl, join_view, pivot_vec, parent);
    join->body.Emplace(join, with_join(join));

  // JOINing two tables; all we can do is an index-scan of the other table; no
  // need for a join region.
  } else if (2u == pred_views.size()) {
    const auto other_view = pred_views[unsigned(pred_views[0] == from_view)];
    const auto other_model =
        impl->view_to_model[other_view]->FindAs<DataModel>();
    assert(other_model->table != nullptr);
    parent->regions.AddUse(
        BuildMaybeScanPartial(impl, other_view, pivot_cols[other_view],
                              other_model->table, parent, with_join));

  } else {
    assert(false);
  }

  auto ret = impl->operation_regions.CreateDerived<RETURN>(
      proc, ProgramOperation::kReturnFalseFromProcedure);
  ret->ExecuteAfter(impl, proc);
}

}  // namespace hyde
