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

  return common_ancestor;
}

void ContinueJoinWorkItem::Run(ProgramImpl *impl, Context &context) {
  if (inserts.empty()) {
    assert(false);
    return;
  }

  const auto join_view = QueryJoin::From(view);
  OP *parent = inserts[0];
  PROC * const proc = parent->containing_procedure;

  // If there are more than one sources leading into this JOIN then we want
  // to have each append to a vector, then we'll loop over the vector of
  // pivots.
  if (1u < inserts.size()) {
    TABLE * const pivot_vec = proc->VectorFor(view.Columns());

    for (auto insert : inserts) {
      OP * const append = impl->operation_regions.CreateDerived<VECTORAPPEND>(
          insert, ProgramOperation::kAppendJoinPivotsToVector);

      for (auto col : join_view.PivotColumns()) {
        const auto var = proc->VariableFor(col);
        append->variables.AddUse(var);
      }
      append->variables.Unique();
      append->tables.AddUse(pivot_vec);

      UseRef<REGION>(insert, append).Swap(insert->body);
    }

    // Find the common ancestor of all of the `kInsertIntoView` associated with
    // the reached `QueryJoin`s that happened before this work item. Everything
    // under this common ancestor must execute before the loop over the join_view
    // pivots.
    auto ancestor = FindCommonAncestorOfInsertRegions();
    auto seq = impl->series_regions.Create(ancestor->parent);
    ancestor->ReplaceAllUsesWith(seq);
    ancestor->ExecuteAfter(impl, seq);

    parent = impl->operation_regions.CreateDerived<VECTORLOOP>(
        seq, ProgramOperation::kLoopOverJoinPivots);
    for (auto col : join_view.PivotColumns()) {
      const auto var = proc->VariableFor(col);
      parent->variables.AddUse(var);
    }
    parent->variables.Unique();
    parent->tables.AddUse(pivot_vec);

    parent->ExecuteAfter(impl, seq);
  }

  // We're now either looping over pivots in a pivot vector, or there was only
  // one entrypoint to the `QueryJoin` that was followed pre-work item, and
  // so we're in the body of an `insert`.
  OP * const join = impl->operation_regions.CreateDerived<VIEWJOIN>(parent);

  std::vector<QueryColumn> cols;
  for (auto pred_view : view.Predecessors()) {
    cols.clear();
    join_view.ForEachUse([&] (QueryColumn in_col, InputColumnRole role,
                              std::optional<QueryColumn>) {
      if (InputColumnRole::kJoinPivot == role &&
          QueryView::Containing(in_col) == pred_view) {
        cols.push_back(in_col);
      }
    });

    const auto table = TABLE::GetOrCreate(impl, pred_view.Columns(), view);
    const auto index = table->GetOrCreateIndex(cols);
    join->views.AddUse(table);
    join->indices.AddUse(index);
    join->join = join_view;
  }

  UseRef<REGION>(parent, join).Swap(parent->body);
  BuildEagerSuccessorRegions(impl, view, context, join, view.Successors());
}

}  // namespace

// Build an eager region for a join.
void BuildEagerJoinRegion(ProgramImpl *impl, QueryView pred_view,
                          QueryJoin view, Context &context, OP *parent) {
  PROC * const proc = parent->containing_procedure;

  // First, check if we should push this tuple through the JOIN. If it's
  // not resident in the view tagged for the `QueryJoin` then we know it's
  // never been seen before.
  OP * const insert = impl->operation_regions.CreateDerived<VIEWINSERT>(parent);
  for (auto col : view.Columns()) {
    const auto var = proc->VariableFor(col);
    insert->variables.AddUse(var);
  }
  insert->views.AddUse(
      TABLE::GetOrCreate(impl, pred_view.Columns(), view));
  insert->variables.Unique();
  UseRef<REGION>(parent, insert).Swap(parent->body);

  auto &action = context.view_to_work_item[view];
  if (!action) {
    action = new ContinueJoinWorkItem(view);
    context.work_list.emplace_back(action);
  }

  dynamic_cast<ContinueJoinWorkItem *>(action)->inserts.push_back(insert);
}

}  // namespace hyde
