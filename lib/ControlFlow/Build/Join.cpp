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
  const auto join = impl->operation_regions.CreateDerived<DATAVIEWJOIN>(
      parent, join_view);
  UseRef<REGION>(parent, join).Swap(parent->body);

  // If this join executes inside of a vector loop, then we want to clear out
  // the vector after executing the join.
  if (seq) {
    auto clear = impl->operation_regions.CreateDerived<VECTORCLEAR>(
        seq, ProgramOperation::kClearJoinPivotVector);
    UseRef<VECTOR>(clear, pivot_vec).Swap(clear->vector);
    clear->ExecuteAfter(impl, seq);
  }

  std::vector<QueryColumn> pivot_cols;

  for (auto pred_view : view.Predecessors()) {
    pivot_cols.clear();

    auto &out_vars = join->output_vars.emplace_back(join);

    join_view.ForEachUse([&] (QueryColumn in_col, InputColumnRole role,
                              std::optional<QueryColumn> out_col) {
      if (out_col && QueryView::Containing(in_col) == pred_view) {
        if (InputColumnRole::kJoinPivot == role) {
          pivot_cols.push_back(in_col);

        } else if (InputColumnRole::kJoinNonPivot == role) {
          auto var = out_vars.Create(
              out_col->Id(), VariableRole::kJoinNonPivot);
          var->query_column = *out_col;
          join->col_id_to_var.emplace(out_col->Id(), var);

        } else {
          assert(false);
        }
      }
    });

    const auto table = TABLE::GetOrCreate(impl, pred_view.Columns(), view);
    const auto index = table->GetOrCreateIndex(pivot_cols);
    join->views.AddUse(table);
    join->indices.AddUse(index);
  }

  BuildEagerSuccessorRegions(impl, view, context, join, view.Successors());
}

}  // namespace

// Build an eager region for a join.
void BuildEagerJoinRegion(ProgramImpl *impl, QueryView pred_view,
                          QueryJoin view, Context &context, OP *parent) {

  // First, check if we should push this tuple through the JOIN. If it's
  // not resident in the view tagged for the `QueryJoin` then we know it's
  // never been seen before.
  const auto insert = impl->operation_regions.CreateDerived<DATAVIEWINSERT>(parent);
  for (auto col : pred_view.Columns()) {
    const auto var = parent->VariableFor(impl, col);
    insert->col_values.AddUse(var);
    insert->col_ids.push_back(col.Id());
  }

  const auto table_view = TABLE::GetOrCreate(impl, pred_view.Columns(), view);
  UseRef<DATAVIEW>(insert, table_view).Swap(insert->view);
  UseRef<REGION>(parent, insert).Swap(parent->body);

  auto &action = context.view_to_work_item[view];
  if (!action) {
    action = new ContinueJoinWorkItem(view);
    context.work_list.emplace_back(action);
  }

  dynamic_cast<ContinueJoinWorkItem *>(action)->inserts.push_back(insert);
}

}  // namespace hyde
