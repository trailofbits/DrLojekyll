// Copyright 2020, Trail of Bits. All rights reserved.

#include "Build.h"

namespace hyde {
namespace {

class ContinueProductWorkItem final : public WorkItem {
 public:
  virtual ~ContinueProductWorkItem(void) {}

  ContinueProductWorkItem(Context &context, QueryView view_)
      : WorkItem(context, view_.Depth()),
        view(view_) {}

  // Find the common ancestor of all insert regions.
  REGION *FindCommonAncestorOfAppendRegions(void) const;

  void Run(ProgramImpl *program, Context &context) override;

  std::vector<VECTOR *> vectors;
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

  const auto join_view = QueryJoin::From(view);
  PROC *const proc = appends[0]->containing_procedure;

  context.view_to_work_item.erase({proc, view.UniqueId()});

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
    unique->vector.Emplace(unique, vec);
    unique->ExecuteAfter(impl, seq);
  }

  // We're now either looping over pivots in a pivot vector, or there was only
  // one entrypoint to the `QueryJoin` that was followed pre-work item, and
  // so we're in the body of an `insert`.
  const auto product =
      impl->operation_regions.CreateDerived<TABLEPRODUCT>(
          seq, join_view, impl->next_id++);
  product->ExecuteAfter(impl, seq);

  // Clear out the input vectors that might have been filled up before the
  // cross-product.
  for (auto vec : vectors) {
    auto clear = impl->operation_regions.CreateDerived<VECTORCLEAR>(
        seq, ProgramOperation::kClearProductInputVector);
    clear->vector.Emplace(clear, vec);
    clear->ExecuteAfter(impl, seq);
  }

  for (auto pred_view : view.Predecessors()) {
    DataModel * const pred_model = \
        impl->view_to_model[pred_view]->FindAs<DataModel>();
    TABLE * const pred_table = pred_model->table;

    auto &vec = context.product_vector[{proc, pred_table}];
    if (!vec) {
      vec =
          proc->VectorFor(impl, VectorKind::kProductInput, pred_view.Columns());
    }

    product->tables.AddUse(pred_table);
    product->input_vecs.AddUse(vec);

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
      const auto in_var = product->VariableFor(impl, in_col);
      assert(in_var != nullptr);
      product->col_id_to_var[out_col->Id()] = in_var;
    }
  });

  OP *parent = product;

  // If this product can receive deletions, then we need to possibly double
  // check its sources, because indices don't actually maintain states.
  if (view.CanReceiveDeletions()) {

    // We (should) have all columns by this point, so we'll proceed like that.
    std::vector<QueryColumn> view_cols(view.Columns().begin(),
                                       view.Columns().end());

    // Call the predecessors. If any of the predecessors return `false` then
    // that means we have failed.
    for (auto pred_view : view.Predecessors()) {
      const auto index_is_good = CallTopDownChecker(
          impl, context, parent, view, view_cols, pred_view, nullptr);

      COMMENT( index_is_good->comment = __FILE__ ": ContinueJoinWorkItem::Run"; )

      parent->body.Emplace(parent, index_is_good);
      parent = index_is_good;
    }
  }

  BuildEagerInsertionRegions(impl, view, context, parent, view.Successors(),
                             nullptr);
}

}  // namespace

// Build an eager cross-product for a join.
void BuildEagerProductRegion(ProgramImpl *impl, QueryView pred_view,
                             QueryJoin product_view, Context &context,
                             OP *parent, TABLE *last_table) {
  const QueryView view(product_view);

  // First, check if we should push this tuple through the PRODUCT. If it's
  // not resident in the view tagged for the `QueryJoin` then we know it's
  // never been seen before.
  DataModel * const pred_model = \
      impl->view_to_model[pred_view]->FindAs<DataModel>();
  TABLE * const pred_table = pred_model->table;
  if (pred_table != last_table) {
    parent =
        BuildInsertCheck(impl, pred_view, context, parent, pred_table,
                         pred_view.CanProduceDeletions(), pred_view.Columns());
    last_table = pred_table;
  }

  // Nothing really to do, this cross-product just needs to pass its data
  // through. This is some kind of weird degenerate case that might happen
  // due to a failure in optimization.
  if (view.Predecessors().size() == 1u) {
    product_view.ForEachUse([&](QueryColumn in_col, InputColumnRole,
                                std::optional<QueryColumn> out_col) {
      if (out_col) {
        const auto in_var = parent->VariableFor(impl, in_col);
        assert(in_var != nullptr);
        parent->col_id_to_var[out_col->Id()] = in_var;
      }
    });

    BuildEagerInsertionRegions(impl, view, context, parent, view.Successors(),
                               last_table);
    return;
  }

  const auto proc = parent->containing_procedure;
  auto &vec = context.product_vector[{proc, pred_table}];
  bool is_new_vec = false;
  if (!vec) {
    is_new_vec = true;
    vec = proc->VectorFor(impl, VectorKind::kProductInput, pred_view.Columns());
  }

  // Append this tuple to the product input vector.
  const auto append = impl->operation_regions.CreateDerived<VECTORAPPEND>(
      parent, ProgramOperation::kAppendToProductInputVector);

  for (auto col : pred_view.Columns()) {
    const auto var = parent->VariableFor(impl, col);
    append->tuple_vars.AddUse(var);
  }

  append->vector.Emplace(append, vec);
  parent->body.Emplace(parent, append);

  auto &action = context.view_to_work_item[{proc, product_view.UniqueId()}];
  if (!action) {
    action = new ContinueProductWorkItem(context, product_view);
    context.work_list.emplace_back(action);
  }

  if (is_new_vec) {
    dynamic_cast<ContinueProductWorkItem *>(action)->vectors.push_back(vec);
  }
  dynamic_cast<ContinueProductWorkItem *>(action)->appends.push_back(append);
}

}  // namespace hyde
