// Copyright 2020, Trail of Bits. All rights reserved.

#include "Build.h"

namespace hyde {
namespace {

class ContinueProductWorkItem final : public WorkItem {
 public:
  virtual ~ContinueProductWorkItem(void) {}

  ContinueProductWorkItem(QueryView view_)
      : WorkItem(view_.Depth()),
        view(view_) {}

  // Find the common ancestor of all insert regions.
  REGION *FindCommonAncestorOfAppendRegions(void) const;

  void Run(ProgramImpl *program, Context &context) override;

  std::unordered_set<VECTOR *> vectors;
  std::vector<OP *> appends;

 private:
  QueryView view;
};

// Find the common ancestor of all insert regions.
REGION *ContinueProductWorkItem::FindCommonAncestorOfAppendRegions(void) const {
  PROC *const proc = appends[0]->containing_procedure;
  REGION *common_ancestor = nullptr;

  for (const auto append : appends) {
    if (!common_ancestor) {
      common_ancestor = append;
    } else {
      common_ancestor = common_ancestor->FindCommonAncestor(append);
    }
  }

  assert(common_ancestor != nullptr);
  if (proc == common_ancestor || !common_ancestor) {
    common_ancestor = proc->body.get();
  }

  return common_ancestor->NearestRegionEnclosedByInduction();
}

void ContinueProductWorkItem::Run(ProgramImpl *impl, Context &context) {
  if (appends.empty()) {
    assert(false);
    return;
  }

  context.view_to_work_item.erase(view);

  const auto join_view = QueryJoin::From(view);
  PROC *const proc = appends[0]->containing_procedure;

  // Find the common ancestor of all of the appends associated with whatever
  // flows we saw into the PRODUCT node. We want to execute the ancestor
  // logically after those execute, so we'll re-base that ancestor into
  // a sequence.
  const auto ancestor = FindCommonAncestorOfAppendRegions();
  const auto seq = impl->series_regions.Create(ancestor->parent);
  ancestor->ReplaceAllUsesWith(seq);
  ancestor->ExecuteAfter(impl, seq);

  // Sort and unique the product input vectors that might actually have data.
  for (auto vec : vectors) {
    const auto unique = impl->operation_regions.CreateDerived<VECTORUNIQUE>(
        seq, ProgramOperation::kSortAndUniqueProductInputVector);
    UseRef<VECTOR>(unique, vec).Swap(unique->vector);
    unique->ExecuteAfter(impl, seq);
  }

  // We're now either looping over pivots in a pivot vector, or there was only
  // one entrypoint to the `QueryJoin` that was followed pre-work item, and
  // so we're in the body of an `insert`.
  const auto product =
      impl->operation_regions.CreateDerived<TABLEPRODUCT>(seq, join_view);
  product->ExecuteAfter(impl, seq);

  // Clear out the input vectors that might have been filled up before the
  // cross-product.
  for (auto vec : vectors) {
    auto clear = impl->operation_regions.CreateDerived<VECTORCLEAR>(
        seq, ProgramOperation::kClearProductInputVector);
    UseRef<VECTOR>(clear, vec).Swap(clear->vector);
    clear->ExecuteAfter(impl, seq);
  }

  for (auto pred_view : view.Predecessors()) {

    const auto table = TABLE::GetOrCreate(impl, pred_view);
    auto &vec = context.product_vector[table];
    if (!vec) {
      vec =
          proc->VectorFor(impl, VectorKind::kProductInput, pred_view.Columns());
    }

    product->tables.AddUse(table);
    product->input_vectors.AddUse(vec);

    // Make a variable for each column of the input table.
    auto &out_vars = product->output_vars.emplace_back(product);
    for (auto col : pred_view.Columns()) {
      const auto var =
          out_vars.Create(impl->next_id++, VariableRole::kProductOutput);
      var->query_column = col;
      product->col_id_to_var.emplace(col.Id(), var);
    }
  }

  // Map the output column IDs of the product based on the input column IDs.
  join_view.ForEachUse([&](QueryColumn in_col, InputColumnRole,
                           std::optional<QueryColumn> out_col) {
    if (out_col) {
      const auto in_var = product->col_id_to_var[in_col.Id()];
      assert(in_var != nullptr);
      product->col_id_to_var.emplace(out_col->Id(), in_var);
    }
  });

  BuildEagerSuccessorRegions(impl, view, context, product, view.Successors(),
                             nullptr);
}

}  // namespace

// Build an eager cross-product for a join.
void BuildEagerProductRegion(ProgramImpl *impl, QueryView pred_view,
                             QueryJoin view, Context &context, OP *parent,
                             TABLE *last_model) {

  // First, check if we should push this tuple through the JOIN. If it's
  // not resident in the view tagged for the `QueryJoin` then we know it's
  // never been seen before.
  const auto table = TABLE::GetOrCreate(impl, pred_view);
  if (table != last_model) {

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

  auto &vec = context.product_vector[table];
  if (!vec) {
    const auto proc = parent->containing_procedure;
    vec = proc->VectorFor(impl, VectorKind::kProductInput, pred_view.Columns());
  }

  // Append this tuple to the product input vector.
  const auto append = impl->operation_regions.CreateDerived<VECTORAPPEND>(
      parent, ProgramOperation::kAppendToProductInputVector);

  for (auto col : pred_view.Columns()) {
    const auto var = parent->VariableFor(impl, col);
    append->tuple_vars.AddUse(var);
  }

  UseRef<VECTOR>(append, vec).Swap(append->vector);
  UseRef<REGION>(parent, append).Swap(parent->body);

  auto &action = context.view_to_work_item[view];
  if (!action) {
    action = new ContinueProductWorkItem(view);
    context.work_list.emplace_back(action);
  }

  dynamic_cast<ContinueProductWorkItem *>(action)->vectors.insert(vec);
  dynamic_cast<ContinueProductWorkItem *>(action)->appends.push_back(append);
}

}  // namespace hyde
