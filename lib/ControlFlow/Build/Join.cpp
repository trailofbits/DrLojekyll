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
  PROC * const proc = inserts[0]->containing_procedure;
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
  OP *parent = inserts[0];
  PROC * const proc = parent->containing_procedure;
  SERIES *seq = nullptr;
  VECTOR *pivot_vec = nullptr;

  // If there are more than one sources leading into this JOIN then we want
  // to have each append to a vector, then we'll loop over the vector of
  // pivots.
  if (1u < inserts.size()) {
    pivot_vec = proc->VectorFor(
        impl, VectorKind::kJoinPivots, join_view.PivotColumns());

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
    seq = impl->series_regions.Create(ancestor->parent);
    ancestor->ReplaceAllUsesWith(seq);
    ancestor->ExecuteAfter(impl, seq);

    // Sort and unique the pivot vector before looping.
    const auto unique = impl->operation_regions.CreateDerived<VECTORUNIQUE>(
        seq, ProgramOperation::kSortAndUniquePivotVector);
    UseRef<VECTOR>(unique, pivot_vec).Swap(unique->vector);
    unique->ExecuteAfter(impl, seq);

    // Loop over the pivot vector.
    const auto loop = impl->operation_regions.CreateDerived<VECTORLOOP>(
        seq, ProgramOperation::kLoopOverJoinPivots);

    for (auto col : join_view.PivotColumns()) {
      const auto var = loop->defined_vars.Create(
          impl->next_id++, VariableRole::kVectorVariable);
      var->query_column = col;
      loop->col_id_to_var.emplace(col.Id(), var);
    }

    UseRef<VECTOR>(loop, pivot_vec).Swap(loop->vector);

    loop->ExecuteAfter(impl, seq);

    parent = loop;
  }

  // We're now either looping over pivots in a pivot vector, or there was only
  // one entrypoint to the `QueryJoin` that was followed pre-work item, and
  // so we're in the body of an `insert`.
  const auto join = impl->operation_regions.CreateDerived<TABLEJOIN>(
      parent, join_view);
  UseRef<REGION>(parent, join).Swap(parent->body);

  // If this join executes inside of a vector loop, then we want to clear out
  // the vector after executing the join.
  if (seq) {

    // We prefer to pass down the lexically needed variables.
    for (auto col : join_view.PivotColumns()) {
      join->col_id_to_var.emplace(col.Id(), parent->VariableFor(impl, col));
    }

    auto clear = impl->operation_regions.CreateDerived<VECTORCLEAR>(
        seq, ProgramOperation::kClearJoinPivotVector);
    UseRef<VECTOR>(clear, pivot_vec).Swap(clear->vector);
    clear->ExecuteAfter(impl, seq);
  }

  std::vector<unsigned> pivot_cols;
  for (auto pred_view : view.Predecessors()) {

    auto &pivot_vars = join->pivot_vars.emplace_back(join);
    join_view.ForEachUse([&] (QueryColumn in_col, InputColumnRole role,
                              std::optional<QueryColumn> out_col) {
      if (out_col && QueryView::Containing(in_col) == pred_view &&
          InputColumnRole::kJoinPivot == role) {
        assert(!in_col.IsConstant());
        pivot_cols.push_back(*(in_col.Index()));
        pivot_vars.AddUse(parent->VariableFor(impl, *out_col));
      }
    });

    auto i = 0u;
    auto &out_vars = join->output_vars.emplace_back(join);

    for (auto col : pred_view.Columns()) {

      // Ugly way of figuring out if this was a pivot column.
      auto role = VariableRole::kJoinNonPivot;
      if (i < pivot_cols.size() && col.Index() == pivot_cols[i]) {
        role = VariableRole::kJoinPivot;
        ++i;
      }

      // Make a variable for each column of the input table, tagged as either
      // a pivot or non-pivot.
      const auto var = out_vars.Create(impl->next_id++, role);
      var->query_column = col;
      join->col_id_to_var.emplace(col.Id(), var);
    }

    const auto table = TABLE::GetOrCreate(impl, pred_view);
    const auto index = table->GetOrCreateIndex(impl, std::move(pivot_cols));
    join->tables.AddUse(table);
    join->indices.AddUse(index);
  }

  join_view.ForEachUse([&] (QueryColumn in_col, InputColumnRole role,
                            std::optional<QueryColumn> out_col) {
    if (out_col) {
      const auto in_var = join->col_id_to_var[in_col.Id()];
      assert(in_var != nullptr);
      join->col_id_to_var.emplace(out_col->Id(), in_var);
    }
  });

  BuildEagerSuccessorRegions(
      impl, view, context, join, view.Successors(), nullptr);
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
