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

  TABLEJOIN *DoJoinWithPivotsAvailable(ProgramImpl *program, Context &context,
                                       SERIES *seq, VECTOR *pivot_vec);

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

// Do the "main" part of the join, which is to actually join all the tables.
// The pre-condition is that the variables associated with the output pivot
// columns are all available. This is used by both bottom-up proving and
// top-down checking.
TABLEJOIN *ContinueJoinWorkItem::DoJoinWithPivotsAvailable(
    ProgramImpl *impl, Context &context, SERIES *seq, VECTOR *pivot_vec) {

  const auto join_view = QueryJoin::From(view);

  // Sort and unique the pivot vector before looping.
  const auto unique = impl->operation_regions.CreateDerived<VECTORUNIQUE>(
      seq, ProgramOperation::kSortAndUniquePivotVector);
  UseRef<VECTOR>(unique, pivot_vec).Swap(unique->vector);
  unique->ExecuteAfter(impl, seq);

  // We're now either looping over pivots in a pivot vector, or there was only
  // one entrypoint to the `QueryJoin` that was followed pre-work item, and
  // so we're in the body of an `insert`.
  const auto join =
      impl->operation_regions.CreateDerived<TABLEJOIN>(seq, join_view);
  join->ExecuteAfter(impl, seq);

  // The JOIN internalizes the loop over its pivot vector. This is so that
  // it can have visibility into the sortedness, and choose what to do based
  // of of runs of sorted elements.
  UseRef<VECTOR>(join, pivot_vec).Swap(join->pivot_vec);

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
    if (!out_col ||
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

// We've collected all the ways we're going to collect (from the input message
// for which we're building a bottom-up prover) that lead into this join, and
// they have all, at their deepest points, pushed their data into the join
// pivot vector. Now it's time to build the join itself, and then push the
// data off to the successors.
void ContinueJoinWorkItem::Run(ProgramImpl *impl, Context &context) {
  if (inserts.empty()) {
    assert(false);
    return;
  }

  context.view_to_work_item.erase(view);

  const auto join_view = QueryJoin::From(view);
  PROC *const proc = inserts[0]->containing_procedure;

  const auto pivot_vec =
      proc->VectorFor(impl, VectorKind::kJoinPivots, join_view.PivotColumns());

  for (auto insert : inserts) {
    const auto append = impl->operation_regions.CreateDerived<VECTORAPPEND>(
        insert, ProgramOperation::kAppendJoinPivotsToVector);

    for (auto col : join_view.PivotColumns()) {
      const auto var = insert->VariableFor(impl, col);
      append->tuple_vars.AddUse(var);
    }

    UseRef<VECTOR>(append, pivot_vec).Swap(append->vector);
    UseRef<REGION>(insert, append).Swap(insert->body);
  }

  // Find the common ancestor of all of the `kInsertIntoView` associated with
  // the reached `QueryJoin`s that happened before this work item. Everything
  // under this common ancestor must execute before the loop over the join_view
  // pivots.
  const auto ancestor = FindCommonAncestorOfInsertRegions();
  const auto seq = impl->series_regions.Create(ancestor->parent);
  ancestor->ReplaceAllUsesWith(seq);
  ancestor->ExecuteAfter(impl, seq);

  auto join = DoJoinWithPivotsAvailable(impl, context, seq, pivot_vec);

  // After running the join, clear out the pivot vector.
  const auto clear = impl->operation_regions.CreateDerived<VECTORCLEAR>(
      seq, ProgramOperation::kClearJoinPivotVector);
  UseRef<VECTOR>(clear, pivot_vec).Swap(clear->vector);
  clear->ExecuteAfter(impl, seq);

  BuildEagerSuccessorRegions(impl, view, context, join, view.Successors(),
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

// Build a top-down checker on an induction. This is overall pretty complicated.
// We need to handle a few cases here, but the basics are this: we want to
// get to a point where we can execute a TABLEJOIN or a TABLEPRODUCT, then
// check the outputs of those. To get to that point, we might need to
// re-construct the join pivots
void BuildTopDownJoinOrProductChecker(
    ProgramImpl *impl, Context &context, PROC *proc,
    QueryJoin join, std::vector<QueryColumn> &view_cols,
    TABLE *already_checked) {

  // TODO(pag): Check if there is actually a `table` for `view`s model.

  const QueryView view(join);
  const auto model = impl->view_to_model[view]->FindAs<DataModel>();

  // Build up a mapping for how input and output columns are connected in this
  // join.
  std::unordered_map<QueryColumn, QueryColumn> in_to_out_cols;
  std::unordered_map<QueryColumn, std::vector<QueryColumn>> out_to_in_cols;

  join.ForEachUse([&] (QueryColumn in_col, InputColumnRole,
                       std::optional<QueryColumn> out_col) {
    if (out_col && !in_col.IsConstant()) {
      in_to_out_cols.emplace(in_col, *out_col);
      out_to_in_cols[*out_col].push_back(in_col);
    }
  });

  // If there is a model for this JOIN's output then we can actually derive
  // the pivots from an index scan. This is much simpler and is our "fast
  // path".
  if (model->table) {
    assert(false && "TODO: Remove this when we have a test that actually hit this condition.");

    // Do a tower of checks against this JOIN's predecessors.
    auto check_preds = [&] (REGION *parent) -> REGION * {
      REGION *first_check = nullptr;
      CALL *parent_check = nullptr;

      std::vector<QueryColumn> call_cols;
      for (auto pred_view : view.Predecessors()) {

        // Figure out the columns to pass to the `pred_view` checker, and
        // make sure all the variables associated with `pred_view`'s columns
        // are mapped.
        call_cols.clear();
        for (auto in_col : pred_view.Columns()) {
          call_cols.push_back(in_col);
          auto out_col = in_to_out_cols.find(in_col)->second;
          parent->col_id_to_var.emplace(
              in_col.Id(), parent->VariableFor(impl, out_col));
        }

        const auto check = CallTopDownChecker(
            impl, context, parent, pred_view, call_cols, pred_view,
            ProgramOperation::kCallProcedureCheckTrue, nullptr);

        if (!first_check) {
          first_check = check;
        } else {
          UseRef<REGION>(parent, check).Swap(parent_check->OP::body);
        }

        parent_check = check;
        parent = check;
      }

      // The caller will do the state change for us.
      if (model->table == already_checked) {
        const auto ret_true = BuildStateCheckCaseReturnTrue(impl, parent_check);
        UseRef<REGION>(parent_check, ret_true).Swap(parent_check->OP::body);

      // We need to do the state change.
      } else {

        call_cols.clear();
        for (auto out_col : view.Columns()) {
          call_cols.push_back(out_col);
        }

        auto change_state = BuildChangeState(
            impl, model->table, parent_check, call_cols,
            TupleState::kAbsentOrUnknown, TupleState::kPresent);
        UseRef<REGION>(parent_check, change_state).Swap(parent_check->body);

        const auto ret_true = BuildStateCheckCaseReturnTrue(
            impl, change_state);
        ret_true->ExecuteAfter(impl, change_state);
      }

      return first_check;
    };

    // The caller has done a state transition on `model->table` and it will
    // do the marking of present.
    if (model->table == already_checked) {
      assert(view_cols.size() == view.Columns().size());


    // The caller didn't check the same model, so we need to do the state
    // checking and transitioning ourselves.
    } else {
      auto do_unknown = [&] (ProgramImpl *, REGION *parent) -> REGION * {
        return BuildTopDownCheckerResetAndProve(
            impl, model->table, parent, view.Columns(),
            [&] (PARALLEL *par) {
              check_preds(par)->ExecuteAlongside(impl, par);
            });
      };

      const auto region = BuildMaybeScanPartial(
          impl, view, view_cols, model->table, proc,
          [&] (REGION *parent) -> REGION * {
            return BuildTopDownCheckerStateCheck(
                impl, parent, model->table, view.Columns(),
                BuildStateCheckCaseReturnTrue,
                BuildStateCheckCaseNothing,
                do_unknown);
          });

      UseRef<REGION>(proc, region).Swap(proc->body);
    }

    return;
  }

  // OKAY, no model is available :-/ We're going to have to do a JOIN or
  // product and really figure things out.

  // Figure out which, if any, of the available `view_cols` are actually
  // pivot columns. If we find all the pivot columns we need, then we'll
  // do a plain old join.
  std::vector<QueryColumn> pivot_cols;
  auto pivot_range = join.PivotColumns();
  std::set_intersection(view_cols.begin(), view_cols.end(),
                        pivot_range.begin(), pivot_range.end(),
                        std::back_inserter(pivot_cols));

  // Use our mapping to figure out what columns we have, and what columns
  // are missing.
  std::unordered_map<QueryView, std::vector<std::pair<QueryColumn, QueryColumn>>>
      present_cols;

  std::unordered_map<QueryView, std::vector<std::pair<QueryColumn, QueryColumn>>>
      all_cols;

  for (auto out_col : view_cols) {
    for (auto in_col : out_to_in_cols[out_col]) {
      present_cols[QueryView::Containing(in_col)].emplace_back(in_col, out_col);
    }
  }

  for (auto out_col : view.Columns()) {
    for (auto in_col : out_to_in_cols[out_col]) {
      all_cols[QueryView::Containing(in_col)].emplace_back(in_col, out_col);
    }
  }

  // Create bindings for output-to-input variables.
  auto let = impl->operation_regions.CreateDerived<LET>(proc);
  UseRef<REGION>(proc, let).Swap(proc->body);
  for (const auto &[view, col_pairs] : present_cols) {
    for (auto [in_col, out_col] : col_pairs) {
      auto out_var = proc->VariableFor(impl, out_col);
      auto in_var = let->defined_vars.Create(
          impl->next_id++, VariableRole::kLetBinding);
      in_var->query_column = in_col;
      let->used_vars.AddUse(out_var);
      let->col_id_to_var.emplace(in_col.Id(), in_var);
    }
  }

  ContinueJoinWorkItem action(join);

  auto seq = impl->series_regions.Create(let);
  UseRef<REGION>(let, seq).Swap(let->body);

  VECTOR *pivot_vec = nullptr;
  if (join.NumPivotColumns()) {
    pivot_vec =
        proc->VectorFor(impl, VectorKind::kJoinPivots, join.PivotColumns());
  }

  // Append to the pivot vector.
  auto make_vec_append = [&] (REGION *parent) -> VECTORAPPEND * {
    const auto pivot_append =
        impl->operation_regions.CreateDerived<VECTORAPPEND>(
            parent, ProgramOperation::kAppendJoinPivotsToVector);

    for (auto col : join.PivotColumns()) {
      auto col_var = parent->VariableFor(impl, col);
      pivot_append->tuple_vars.AddUse(col_var);
    }

    UseRef<VECTOR>(pivot_append, pivot_vec).Swap(pivot_append->vector);

    return pivot_append;
  };

  OP *join_or_product = nullptr;

  // This is the ideal case, we have all of the pivot columns available, so
  // we can re-implement the join in the same way as bottom-up execution.
  if (!pivot_cols.empty() && pivot_cols.size() == join.NumPivotColumns()) {

    // TODO(pag): There are likely optimization opportunities where we can
    //            identify if we just need data from a subset of the tables,
    //            and if so, then go and scan that data into some vectors.
    //
    //            If it's a single table case then we can handle it with a
    //            vector-loop; if it's a multi-table case then we may want
    //            to introduce a vector-product region, or just fall back to
    //            nested loops :-/

    const auto pivot_append = make_vec_append(seq);
    pivot_append->ExecuteAfter(impl, seq);

    join_or_product = action.DoJoinWithPivotsAvailable(
        impl, context, seq, pivot_vec);

  // We may or may not have some pivot columns; we definitely have at least
  // one column, and this is definitely a normal JOIN case.
  } else if (join.NumPivotColumns()) {

    // Figure out which input view is the "most represented" by the data
    // available in `view_cols`. We'll scan that view when searching for pivots.
    // If two views are equally represented then we perfer the view with less
    // columns, on the assumption that each tuple isn't as wide.
    double max_percent = 0.0;
    unsigned num_cols = ~0u;
    std::optional<QueryView> max_view;

    for (const auto &[view, col_pairs] : present_cols) {
      const auto percent = double(col_pairs.size()) /
                           double(view.Columns().size());
      const auto num_cols_in_view = view.Columns().size();
      if (percent > max_percent ||
          (percent == max_percent && num_cols_in_view < num_cols)) {
        max_view = view;
        max_percent = percent;
        num_cols = num_cols_in_view;
      }
    }

    // TODO(pag): Shouldn't be possible.
    if (!max_view) {
      assert(false);
      return;
    }

    const auto max_view_table = TABLE::GetOrCreate(impl, *max_view);
    std::vector<QueryColumn> max_view_cols;
    for (auto [in_col, out_col] : present_cols[*max_view]) {
      max_view_cols.push_back(in_col);
    }

    const auto region = BuildMaybeScanPartial(
        impl, *max_view, max_view_cols, max_view_table, seq,
        [&] (REGION *parent) -> REGION * {

          // Create bindings for input-to-output variables.
          auto scan_let = impl->operation_regions.CreateDerived<LET>(parent);

          for (auto [in_col, out_col] : all_cols[*max_view]) {
            auto in_var = parent->VariableFor(impl, in_col);
            auto out_var = scan_let->defined_vars.Create(
                impl->next_id++, VariableRole::kLetBinding);
            out_var->query_column = out_col;
            scan_let->used_vars.AddUse(in_var);
            scan_let->col_id_to_var.emplace(out_col.Id(), out_var);
          }

          const auto pivot_append = make_vec_append(scan_let);
          UseRef<REGION>(scan_let, pivot_append).Swap(scan_let->body);

          return scan_let;
        });

    region->ExecuteAfter(impl, seq);

    // Optimization: If the only columns that are actually missing are pivot
    // columns then we don't /need/ to do a JOIN; we just need to
    // loop over the pivots!!
    if (pivot_cols.empty() && view_cols.size() == join.NumMergedColumns()) {
      const auto pivot_loop = impl->operation_regions.CreateDerived<VECTORLOOP>(
          seq, ProgramOperation::kLoopOverJoinPivotVector);
      pivot_loop->ExecuteAfter(impl, seq);

      UseRef<VECTOR>(pivot_loop, pivot_vec).Swap(pivot_loop->vector);
      for (auto out_col : join.PivotColumns()) {
        const auto pivot_var = pivot_loop->defined_vars.Create(
            impl->next_id++, VariableRole::kJoinPivot);
        pivot_var->query_column = out_col;
        pivot_loop->col_id_to_var.emplace(out_col.Id(), pivot_var);
        for (auto in_col : out_to_in_cols[out_col]) {
          pivot_loop->col_id_to_var.emplace(in_col.Id(), pivot_var);
        }
      }

      join_or_product = pivot_loop;

    // Can't avoid doing a JOIN.
    //
    // TODO(pag): Maybe think about whether or not TABLEJOIN regions can have
    //            more expressive "where" clauses where we can constrain them
    //            with the data we know.
    } else {
      join_or_product = action.DoJoinWithPivotsAvailable(
          impl, context, seq, pivot_vec);
    }

  } else {

  }

  // If we did a table join or product, then we need to check each of the
  // producers, and if all are present, then we have succeeded. We also need
  // to make sure that we check that all the output columns that we got match
  // with what we expected them to be in terms of the input columns to this
  // function.
  if (join_or_product) {
    OP *parent = join_or_product;

    std::vector<QueryColumn> pred_columns;

    // First, do the column value comparisons.
    for (auto pred_view : view.Predecessors()) {
      const auto check = impl->operation_regions.CreateDerived<TUPLECMP>(
          parent, ComparisonOperator::kEqual);

      for (auto [in_col, out_col] : present_cols[pred_view]) {
        auto proc_input_var = proc->VariableFor(impl, out_col);
        auto join_output_var = join_or_product->VariableFor(impl, in_col);
        check->lhs_vars.AddUse(proc_input_var);
        check->rhs_vars.AddUse(join_output_var);
      }

      UseRef<REGION>(parent, check).Swap(parent->body);
      parent = check;
    }

    // Second, go and do recursive checks.
    for (auto pred_view : view.Predecessors()) {
      pred_columns.clear();
      for (auto col : pred_view.Columns()) {
        pred_columns.push_back(col);
      }

      const auto check = CallTopDownChecker(
          impl, context, parent, pred_view, pred_columns, pred_view,
          ProgramOperation::kCallProcedureCheckTrue, nullptr);

      UseRef<REGION>(parent, check).Swap(parent->body);
      parent = check;
    }

    const auto ret_true = BuildStateCheckCaseReturnTrue(impl, parent);
    UseRef<REGION>(parent, ret_true).Swap(parent->body);
  }

  // After running the join, clear out the pivot vector.
  if (pivot_vec) {
    const auto clear = impl->operation_regions.CreateDerived<VECTORCLEAR>(
        seq, ProgramOperation::kClearJoinPivotVector);
    UseRef<VECTOR>(clear, pivot_vec).Swap(clear->vector);
    clear->ExecuteAfter(impl, seq);
  }
}

}  // namespace hyde
