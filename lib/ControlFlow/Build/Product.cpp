// Copyright 2020, Trail of Bits. All rights reserved.

#include "Induction.h"

namespace hyde {
namespace {

static unsigned ContinueProductOrder(QueryView view) {
  unsigned depth = view.Depth();
  unsigned order = 0;

  // We're doing a kind of priority inversion here. We are saying that there
  // is a JOIN, and this JOIN leads into an induction, and that induction
  // cycles back to the JOIN. But, we may not yet be inside of that induction,
  // or we're blocked on it, so what we're going to do is invert the ordering
  // of the JOIN and the INDUCTION work items, so that the continuation of
  // the JOIN is ordered to happen /after/ the continuation of the INDUCTION.
  //
  //                  .---------.
  //                UNION       |
  //               /   |        B
  //            ...   PRODUCT   |
  //                  /  '------'
  //                 A
  //
  // Basically, we might come in via `A`, and we know that the JOIN will lead to
  // the UNION, and when we continue the UNION, we will eventually find our
  // way back to the JOIN via `B`, so we will treat the initial appends to the
  // JOIN's pivot vector from A as an inductive input vector to the UNION.
  if (auto ind_depth = view.InductionDepth(); ind_depth.has_value()) {
    order = WorkItem::kContinueInductionOrder;
    assert(0u < depth);  // Achieves priority inversion w.r.t. induction.
    depth += 1u + ind_depth.value();

  } else {
    order = WorkItem::kContinueJoinOrder;
  }

  return depth | order;
}

}  // namespace

ContinueProductWorkItem::ContinueProductWorkItem(Context &context,
                                                 QueryView view_,
                                                 INDUCTION *induction_)
    : WorkItem(context, ContinueProductOrder(view_)),
      view(view_),
      induction(induction_) {}

// Find the common ancestor of all insert regions.
REGION *ContinueProductWorkItem::FindCommonAncestorOfAppendRegions(void) const {

  // This is quite subtle and there is a ton of collusion with induction
  // creation going on here. Basically, if we have a PRODUCT that "straddles"
  // an inductive back-edge, i.e. some of its predecessors are on that back-
  // edge, but others are more like inputs to the induction, then the induction
  // is in charge of the appends, pivot vectors, etc. To some extent, this is
  // a "cost-saving" measure: we avoid having the same logical JOIN execute
  // both outside and inside of the INDUCTION, and it also means we get to have
  // "inductive joins" have a more uniform concurrency story, by only sharding
  // induction vectors across workers. The big trick, though, is that the
  // induction code doesn't know what the variables being output by the join
  // will be until the PRODUCT itself is created. And so, it fakes this by going
  // and making a `LET` with some defined variables, but deferring their
  // assignment to the PRODUCT.
  if (NeedsInductionCycleVector(view)) {
    assert(induction != nullptr);
    PARALLEL *const par = induction->fixpoint_add_cycles[view];
    LET *const let = par->parent->AsOperation()->AsLetBinding();
    assert(let != nullptr);

    // This is the trick!
    assert(!let->defined_vars.Empty());
    assert(let->used_vars.Empty());
    return let;

  } else {
    assert(!appends.empty());
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

    // NOTE(pag): We *CAN'T* go any higher than `common_ancestor`, because then
    //            we might accidentally "capture" the vector appends for an
    //            unrelated induction, thereby introducing super weird ordering
    //            problems where an induction A is contained in the init region
    //            of an induction B, and B's fixpoint cycle region appends to
    //            A's induction vector.
    return common_ancestor;
  }
}

void ContinueProductWorkItem::Run(ProgramImpl *impl, Context &context) {

  // There should be at least one vector append, even in the inductive case,
  // such that the APPEND relates to the non-inductive predecessor.
  if (appends.empty()) {
    assert(false);
    return;
  }

  const auto join_view = QueryJoin::From(view);
  const auto needs_inductive_cycle_vec = NeedsInductionCycleVector(view);
  const auto needs_inductive_output_vec = NeedsInductionOutputVector(view);
  PROC *const proc = appends[0]->containing_procedure;

  context.view_to_product_action.erase(view);

  // Find the common ancestor of all of the appends associated with whatever
  // flows we saw into the PRODUCT node. We want to execute the ancestor
  // logically after those execute, so we'll re-base that ancestor into
  // a sequence.
  const auto ancestor = FindCommonAncestorOfAppendRegions();
  const auto seq = impl->series_regions.Create(ancestor->parent);
  ancestor->ReplaceAllUsesWith(seq);

  if (!needs_inductive_cycle_vec) {
    ancestor->parent = seq;
    seq->AddRegion(ancestor);
  }

  // Sort and unique the product input vectors that might actually have data.
  for (auto vec : vectors) {
    const auto unique = impl->operation_regions.CreateDerived<VECTORUNIQUE>(
        seq, ProgramOperation::kSortAndUniqueProductInputVector);
    unique->vector.Emplace(unique, vec);
    seq->AddRegion(unique);
  }

  // We're now either looping over pivots in a pivot vector, or there was only
  // one entrypoint to the `QueryJoin` that was followed pre-work item, and
  // so we're in the body of an `insert`.
  const auto product = impl->operation_regions.CreateDerived<TABLEPRODUCT>(
      seq, join_view, impl->next_id++);
  seq->AddRegion(product);

  // Clear out the input vectors that might have been filled up before the
  // cross-product.
  for (auto vec : vectors) {
    auto clear = impl->operation_regions.CreateDerived<VECTORCLEAR>(
        seq, ProgramOperation::kClearProductInputVector);
    clear->vector.Emplace(clear, vec);
    seq->AddRegion(clear);
  }

  for (auto pred_view : view.Predecessors()) {
    DataModel *const pred_model =
        impl->view_to_model[pred_view]->FindAs<DataModel>();
    TABLE *const pred_table = pred_model->table;

    auto &vec = product_vector[pred_table];
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
      const auto [index_is_good, index_is_good_call] = CallTopDownChecker(
          impl, context, parent, view, view_cols, pred_view, nullptr);

      COMMENT(index_is_good_call->comment =
                  __FILE__ ": ContinueProductWorkItem::Run";)

      parent->body.Emplace(parent, index_is_good);
      parent = index_is_good_call;
    }
  }

  // Add a tuple to the output vector. We don't need to compute a worker ID
  // because we know we're dealing with only worker-specific data in this
  // cycle.
  if (needs_inductive_output_vec) {
    PARALLEL *par = impl->parallel_regions.Create(parent);
    parent->body.Emplace(parent, par);
    par->AddRegion(
        AppendToInductionOutputVectors(impl, view, context, induction, par));

    parent = impl->operation_regions.CreateDerived<LET>(par);
    par->AddRegion(parent);
  }

  auto [insert_parent, table, last_table] =
      InTryInsert(impl, context, view, parent, nullptr);
  parent = insert_parent;

  // Collusion with inductions!!!! The `BuildFixpointLoop` function in
  // `Induction.cpp` sets up our ancestor to be this `LET`, and the induction
  // will manually handle calling `BuildEagerInsertionRegions` from inside
  // this `LET`. It does this *before* this function runs, though, so it has
  // to stub out the output variables of the JOIN, so that we can fill them
  // in here.
  if (needs_inductive_cycle_vec) {
    assert(induction != nullptr);
    LET *let_in_fixpoint_region = ancestor->AsOperation()->AsLetBinding();
    let_in_fixpoint_region->parent = parent;
    parent->body.Emplace(parent, let_in_fixpoint_region);

    // Fill in the assignments!
    assert(let_in_fixpoint_region->defined_vars.Size() ==
           view.Columns().size());
    assert(let_in_fixpoint_region->used_vars.Empty());
    for (auto col : view.Columns()) {
      let_in_fixpoint_region->used_vars.AddUse(parent->VariableFor(impl, col));
    }
    assert(!let_in_fixpoint_region->used_vars.Empty());

  } else {
    BuildEagerInsertionRegions(impl, view, context, parent, view.Successors(),
                               last_table);
  }
}

// Build an eager cross-product for a join.
void BuildEagerProductRegion(ProgramImpl *impl, QueryView pred_view,
                             QueryJoin product_view, Context &context, OP *root,
                             TABLE *last_table_) {
  const QueryView view(product_view);

  // First, check if we should push this tuple through the PRODUCT. If it's
  // not resident in the view tagged for the `QueryJoin` then we know it's
  // never been seen before.
  //
  // NOTE(pag): What's interesting about JOINs is that we force the data of
  //            our *predecessors* into tables, so that we can always complete
  //            the JOINs later and see "the other sides."
  const auto [parent_, pred_table_, last_table] =
      InTryInsert(impl, context, pred_view, root, last_table_);

  OP *parent = parent_;
  TABLE *const pred_table = pred_table_;
  INDUCTION *induction = nullptr;
  if (view.InductionGroupId().has_value()) {
    induction = GetOrInitInduction(impl, view, context, parent);
  }

  // Nothing really to do, this cross-product just needs to pass its data
  // through. This is some kind of weird degenerate case that might happen
  // due to a failure in optimization.
  if (view.Predecessors().size() == 1u) {
    const auto captured_parent = parent;
    product_view.ForEachUse([=](QueryColumn in_col, InputColumnRole,
                                std::optional<QueryColumn> out_col) {
      if (out_col) {
        const auto in_var = captured_parent->VariableFor(impl, in_col);
        assert(in_var != nullptr);
        captured_parent->col_id_to_var[out_col->Id()] = in_var;
      }
    });

    BuildEagerInsertionRegions(impl, view, context, parent, view.Successors(),
                               last_table);
    return;
  }

  auto &product_action = context.view_to_product_action[view];

  auto make_append = [&](void) {
    PROC *const proc = parent->containing_procedure;
    VECTOR *&vec = product_action->product_vector[pred_table];
    bool is_new_vec = false;
    if (!vec) {
      is_new_vec = true;
      vec =
          proc->VectorFor(impl, VectorKind::kProductInput, pred_view.Columns());
    }

    // Append this tuple to the product input vector.
    const auto append = impl->operation_regions.CreateDerived<VECTORAPPEND>(
        parent, ProgramOperation::kAppendToProductInputVector);

    if (induction) {
      append->worker_id.Emplace(append, impl->zero);
    }

    for (auto col : pred_view.Columns()) {
      const auto var = parent->VariableFor(impl, col);
      append->tuple_vars.AddUse(var);
    }

    append->vector.Emplace(append, vec);
    parent->body.Emplace(parent, append);

    if (is_new_vec) {
      product_action->vectors.push_back(vec);
    }
    product_action->appends.push_back(append);
  };

  // If this PRODUCT is on the edge of an induction, i.e. one or more of the
  // PRODUCT's input views is a back-edge from and induction, and one or more of
  // the input views is an input source to the induction., then we need to
  // collude with an INDUCTION to make this work. In practice, this turns out
  // to get really crazy.
  if (NeedsInductionCycleVector(view)) {
    VECTOR *const inductive_vec = induction->view_to_add_vec[pred_view];
    VECTOR *const swap_vec = induction->view_to_swap_vec[pred_view];

    // `pred_view` is a non-inductive predecessor of this PRODUCT.
    if (inductive_vec == swap_vec) {
      assert(product_action != nullptr);
      product_action->product_vector.emplace(pred_table, inductive_vec);
      make_append();

    // `pred_view` is an inductive predecessor of this PRODUCT.
    } else {
      AppendToInductionInputVectors(impl, pred_view, view, context, parent,
                                    induction, true);
    }

  // This is a "simple" PRODUCT, i.e. all predecessor views are either all
  // all inside or all outside of an inductive region.
  } else {
    if (!product_action) {

      // A weird infinite loop situation for inductive PRODUCTs, where a flow
      // is reaching back to itself not through a MERGE.
      if (induction) {
        assert(false);
        return;
      }

      product_action = new ContinueProductWorkItem(context, view, induction);
      context.work_list.emplace_back(product_action);
    }
    make_append();
  }
}

}  // namespace hyde
