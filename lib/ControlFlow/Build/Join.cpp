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

  // After running the join, clear out the pivot vector.
  const auto clear = impl->operation_regions.CreateDerived<VECTORCLEAR>(
      seq, ProgramOperation::kClearJoinPivotVector);
  UseRef<VECTOR>(clear, pivot_vec).Swap(clear->vector);
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

    const auto insert =
        impl->operation_regions.CreateDerived<TABLEINSERT>(parent);

    for (auto col : pred_view.Columns()) {
      const auto var = parent->VariableFor(impl, col);
      insert->col_values.AddUse(var);
    }

    UseRef<TABLE>(insert, table).Swap(insert->table);
    UseRef<REGION>(parent, insert).Swap(parent->body);
    parent = insert;
  }

  auto &action = context.view_to_work_item[view];
  if (!action) {
    action = new ContinueJoinWorkItem(view);
    context.work_list.emplace_back(action);
  }

  dynamic_cast<ContinueJoinWorkItem *>(action)->inserts.push_back(parent);
}

}  // namespace hyde
