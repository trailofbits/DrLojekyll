// Copyright 2020, Trail of Bits. All rights reserved.

#include "Induction.h"

namespace hyde {
namespace {

static unsigned ContinueJoinOrder(QueryView view) {
  unsigned depth = view.Depth();
  unsigned order = 0;

  // We're doing a kind of priority inversion here. We are saying that there
  // is a JOIN, and this JOIN leads into an induction, and that induction
  // cycles back to the JOIN. But, we may not yet be inside of that induction,
  // or we're blocked on it, so what we're going to do is invert the ordering
  // of the JOIN and the INDUCTION work items, so that the continuation of
  // the JOIN is ordered to happen /after/ the continuation of the INDUCTION.
  //
  //                  .------.
  //                UNION    |
  //               /   |     B
  //            ...   JOIN   |
  //                  /  '---'
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

class ContinueJoinWorkItem final : public WorkItem {
 public:
  virtual ~ContinueJoinWorkItem(void) {}

  ContinueJoinWorkItem(Context &context, QueryView view_,
                       VECTOR *input_pivot_vec_, VECTOR *swap_pivot_vec_,
                       INDUCTION *induction_)
      : WorkItem(context, ContinueJoinOrder(view_)),
        view(view_),
        input_pivot_vec(input_pivot_vec_),
        swap_pivot_vec(swap_pivot_vec_),
        induction(induction_) {}

  // Find the common ancestor of all insert regions.
  REGION *FindCommonAncestorOfInsertRegions(void) const;

  void Run(ProgramImpl *program, Context &context) override;

  std::vector<OP *> inserts;

 private:
  const QueryView view;
  VECTOR * const input_pivot_vec;
  VECTOR * const swap_pivot_vec;
  INDUCTION * const induction;
};

// Find the common ancestor of all insert regions.
REGION *ContinueJoinWorkItem::FindCommonAncestorOfInsertRegions(void) const {

  // This is quite subtle and there is a ton of collusion with induction
  // creation going on here. Basically, if we have a JOIN that "straddles"
  // an inductive back-edge, i.e. some of its predecessors are on that back-
  // edge, but others are more like inputs to the induction, then the induction
  // is in charge of the appends, pivot vectors, etc. To some extent, this is
  // a "cost-saving" measure: we avoid having the same logical JOIN execute
  // both outside and inside of the INDUCTION, and it also means we get to have
  // "inductive joins" have a more uniform concurrency story, by only sharding
  // induction vectors across workers. The big trick, though, is that the
  // induction code doesn't know what the variables being output by the join
  // will be until the JOIN itself is created. And so, it fakes this by going
  // and making a `LET` with some defined variables, but deferring their
  // assignment to the JOIN.
  if (induction) {
    assert(inserts.empty());
    PARALLEL * const par = induction->fixpoint_add_cycles[view];
    LET * const let = par->parent->AsOperation()->AsLetBinding();
    assert(let != nullptr);

    // This is the trick!
    assert(!let->defined_vars.Empty());
    assert(let->used_vars.Empty());
    return let;

  } else {
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

    // NOTE(pag): We *CAN'T* go any higher than `common_ancestor`, because then
    //            we might accidentally "capture" the vector appends for an
    //            unrelated induction, thereby introducing super weird ordering
    //            problems where an induction A is contained in the init region
    //            of an induction B, and B's fixpoint cycle region appends to
    //            A's induction vector.
    return common_ancestor;
  }
}

namespace {

// Build a join region given a JOIN view and a pivot vector.
static TABLEJOIN *BuildJoin(ProgramImpl *impl, QueryJoin join_view,
                            VECTOR *pivot_vec, SERIES *seq) {

  // We're now either looping over pivots in a pivot vector, or there was only
  // one entrypoint to the `QueryJoin` that was followed pre-work item, and
  // so we're in the body of an `insert`.
  const auto join = impl->operation_regions.CreateDerived<TABLEJOIN>(
      seq, join_view, impl->next_id++);
  seq->AddRegion(join);

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

    join->col_id_to_var[pivot_col.Id()] = var;
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

    DataModel * const pred_model = \
        impl->view_to_model[pred_view]->FindAs<DataModel>();
    TABLE * const pred_table = pred_model->table;
    TABLEINDEX * const pred_index =
        pred_table->GetOrCreateIndex(impl, std::move(pivot_col_indices));
    join->tables.AddUse(pred_table);
    join->indices.AddUse(pred_index);

    join->pivot_cols.emplace_back(join);
    join->output_cols.emplace_back(join);
    join->output_vars.emplace_back(join);
    view_to_index.emplace(pred_view, i);

    auto &pivot_table_cols = join->pivot_cols.back();
    for (auto pivot_col : pivot_cols) {
      for (auto indexed_col : pred_index->columns) {
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
    assert(out_col);
    if (!out_col) {
      return;
    }

    if (out_col->IsConstantOrConstantRef()) {
      (void) join->VariableFor(impl, *out_col);
      return;

    } else if (in_col.IsConstantOrConstantRef()) {
      const auto in_var = join->VariableFor(impl, in_col);
      join->col_id_to_var[out_col->Id()] = in_var;
      return;

    } else if (InputColumnRole::kJoinNonPivot != role) {
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

    join->col_id_to_var[in_col.Id()] = var;
    join->col_id_to_var[out_col->Id()] = var;
  });

  return join;
}

}  // namespace

void ContinueJoinWorkItem::Run(ProgramImpl *impl, Context &context) {
  const auto join_view = QueryJoin::From(view);

  context.view_to_join_action.erase(view);

  for (auto insert : inserts) {
    assert(!induction);

    const auto append = impl->operation_regions.CreateDerived<VECTORAPPEND>(
        insert, ProgramOperation::kAppendJoinPivotsToVector);
    insert->body.Emplace(insert, append);

    for (auto col : join_view.PivotColumns()) {
      const auto var = insert->VariableFor(impl, col);
      append->tuple_vars.AddUse(var);
    }

    append->vector.Emplace(append, input_pivot_vec);
  }

  // Find the common ancestor of all of the `kInsertIntoView` associated with
  // the reached `QueryJoin`s that happened before this work item. Everything
  // under this common ancestor must execute before the loop over the join_view
  // pivots.
  const auto ancestor = FindCommonAncestorOfInsertRegions();
  const auto seq = impl->series_regions.Create(ancestor->parent);
  ancestor->ReplaceAllUsesWith(seq);

  // Sort and unique the pivot vector before looping.
  if (!induction) {
    ancestor->parent = seq;
    seq->AddRegion(ancestor);

    const auto unique = impl->operation_regions.CreateDerived<VECTORUNIQUE>(
        seq, ProgramOperation::kSortAndUniquePivotVector);
    unique->vector.Emplace(unique, swap_pivot_vec);
    seq->AddRegion(unique);
  }

  OP *parent = BuildJoin(impl, join_view, swap_pivot_vec, seq);

  // If this join can receive deletions, then we need to possibly double check
  // its sources, because indices don't actually maintain states.
  if (view.CanReceiveDeletions()) {

    // We (should) have all columns by this point, so we'll proceed like that.
    std::vector<QueryColumn> view_cols(view.Columns().begin(),
                                       view.Columns().end());

    // Map the JOIN's output variables to its inputs so that we can do the state
    // checks below.
    view.ForEachUse([&](QueryColumn in_col, InputColumnRole role,
                        std::optional<QueryColumn> out_col) {
      if (out_col) {
        parent->col_id_to_var[in_col.Id()] =
            parent->VariableFor(impl, *out_col);
      }
    });

    // Call the predecessors. If any of the predecessors return `false` then
    // that means we have failed.
    for (auto pred_view : view.Predecessors()) {

      if (!pred_view.CanProduceDeletions()) {
        continue;
      }

      // NOTE(pag): All views leading into a JOIN are always backed by a table.
      DataModel * const pred_model =
          impl->view_to_model[pred_view]->FindAs<DataModel>();
      TABLE * const pred_table = pred_model->table;
      assert(pred_table != nullptr);

      // Check to see if the data is present. If it's not (either absent or
      // unknown), then our assumption is that we are in some kind of inductive
      // loop and it will eventually be proven in the forward direction.
//      const auto [check, parent_out] = CallTopDownChecker(
//          impl, context, parent, view, view_cols, pred_view, nullptr);

      OP *parent_out = nullptr;
      CHECKSTATE * const check = BuildTopDownCheckerStateCheck(
          impl, parent, pred_table, pred_view.Columns(),
          [&parent_out] (ProgramImpl *impl_, REGION *in_check) {
            parent_out = impl_->operation_regions.CreateDerived<LET>(in_check);
            return parent_out;
          },
          BuildStateCheckCaseNothing,
          BuildStateCheckCaseNothing);

      parent->body.Emplace(parent, check);
      parent = parent_out;

//      const auto [index_is_good, index_is_good_call] = CallTopDownChecker(
//          impl, context, parent, view, view_cols, pred_view, nullptr);
//
//      COMMENT( index_is_good_call->comment =
//          __FILE__ ": ContinueJoinWorkItem::Run"; )
//
//      parent->body.Emplace(parent, index_is_good);
//      parent = index_is_good_call;
    }
  }

  // Add a tuple to the output vector. We don't need to compute a worker ID
  // because we know we're dealing with only worker-specific data in this
  // cycle.
  if (NeedsInductionOutputVector(view)) {
    PARALLEL *par = impl->parallel_regions.Create(parent);
    parent->body.Emplace(parent, par);

    par->AddRegion(AppendToInductionOutputVectors(
        impl, view, context, induction, par));

    parent = impl->operation_regions.CreateDerived<LET>(par);
    par->AddRegion(parent);
  }

  // Collusion with inductions!!!! The `BuildFixpointLoop` function in
  // `Induction.cpp` sets up our ancestor to be this `LET`, and the induction
  // will manually handle calling `BuildEagerInsertionRegions` from inside
  // this `LET`. It does this *before* this function runs, though, so it has
  // to stub out the output variables of the JOIN, so that we can fill them
  // in here.
  if (induction) {
    LET *let_in_fixpoint_region = ancestor->AsOperation()->AsLetBinding();
    let_in_fixpoint_region->parent = parent;
    parent->body.Emplace(parent, let_in_fixpoint_region);

    // Fill in the assignments!
    assert(let_in_fixpoint_region->defined_vars.Size() == view.Columns().size());
    assert(let_in_fixpoint_region->used_vars.Empty());
    for (auto col : view.Columns()) {
      let_in_fixpoint_region->used_vars.AddUse(parent->VariableFor(impl, col));
    }

  } else {
    BuildEagerInsertionRegions(impl, view, context, parent, view.Successors(),
                               nullptr);
  }
}

// Build an eager region for a join.
void BuildEagerJoinRegion(ProgramImpl *impl, QueryView pred_view,
                          QueryJoin join, Context &context, OP *parent_,
                          TABLE *last_table_) {
  const QueryView view(join);

  // NOTE(pag): What's interesting about JOINs is that we force the data of
  //            our *predecessors* into tables, so that we can always complete
  //            the JOINs later and see "the other sides."
  auto [parent, pred_table, last_table] =
      InTryInsert(impl, context, pred_view, parent_, last_table_);

  auto &join_action = context.view_to_join_action[view];

  // If this join is on the edge of an induction, i.e. one or more of the
  // JOIN's input views is a back-edge from and induction, and one or more of
  // the input views is an input source to the induction., then we need to
  // collude with an INDUCTION to make this work. In practice, this turns out
  // to get really crazy.
  if (NeedsInductionCycleVector(view)) {
    INDUCTION * const induction = GetOrInitInduction(
        impl, view, context, parent);
    VECTOR * const pivot_vec = induction->view_to_add_vec[view];
    VECTOR * const swap_vec = induction->view_to_swap_vec[view];
    assert(pivot_vec && swap_vec);

    if (!join_action) {
      join_action = new ContinueJoinWorkItem(context, view, pivot_vec,
                                             swap_vec, induction);
      context.work_list.emplace_back(join_action);
    }

    AppendToInductionInputVectors(impl, view, context, parent, induction, true);

  // Yay, it's just a "simple" induction, i.e. it's entirely contained outside
  // of an inductive region, or it's entirely contained inside of an inductive
  // region.
  } else {
    PROC * const proc = parent_->containing_procedure;
    VECTOR * const pivot_vec = proc->VectorFor(
        impl, VectorKind::kJoinPivots, join.PivotColumns());

    if (!join_action) {
      join_action = new ContinueJoinWorkItem(context, view, pivot_vec,
                                             pivot_vec, nullptr);
      context.work_list.emplace_back(join_action);
    }

    join_action->inserts.push_back(parent);
  }
}
//
//namespace {
//
//static void CreateBottomUpInductiveJoinRemover(
//    ProgramImpl *impl, Context &context, QueryView from_view,
//    QueryView to_view, OP *parent) {
//
//
//  INDUCTION * const induction = GetOrInitInduction(
//      impl, to_view, context, parent);
//
//  // This is a bit annoying but we need to go and make a custom worker-id
//  // hash, instead of using `AppendToInductionInputVectors`, because we
//  // need to hash only the columns that are pivots, and not all of the
//  // columns.
//
//  // NOTE(pag): We are colluding with how `GetOrInitInduction` sets up
//  //            induction vectors for the predecessors of JOINs that can
//  //            send through deletions. The data flow `QueryImpl::Link`
//  //            ensures that all JOINs, NEGATEs, and UNIONs are fed by
//  //            TUPLEs.
//  assert(!from_view.IsJoin());
//  assert(!from_view.IsNegate());
//  assert(!from_view.IsMerge());
//  VECTOR * const vec = induction->view_to_add_vec[from_view];
//  assert(vec != nullptr);
//
//  // Hash the variables together to form a worker ID. We only want to hash the
//  // columns that will end up as pivots for `to_view`.
//  WORKERID * const hash =
//      impl->operation_regions.CreateDerived<WORKERID>(parent);
//  VAR * const worker_id = new VAR(impl->next_id++, VariableRole::kWorkerId);
//  hash->worker_id.reset(worker_id);
//  parent->body.Emplace(parent, hash);
//
//  // Find the pivots, and map the output pivots to the input pivots.
//  std::unordered_map<QueryColumn, QueryColumn> out_to_in;
//  const QueryJoin join = QueryJoin::From(to_view);
//  join.ForEachUse([&](QueryColumn in_col, InputColumnRole role,
//                      std::optional<QueryColumn> out_col) {
//    if (InputColumnRole::kJoinPivot == role && out_col &&
//        QueryView::Containing(in_col) == from_view) {
//      out_to_in.emplace(*out_col, in_col);
//    }
//  });
//
//  // Add the pivots in order.
//  for (auto col : join.PivotColumns()) {
//    QueryColumn in_col = out_to_in.find(col)->second;
//    hash->hashed_vars.AddUse(hash->VariableFor(impl, in_col));
//  }
//
//  const auto par = impl->parallel_regions.Create(hash);
//  hash->body.Emplace(parent, par);
//
//  // Add a tuple to the removal vector.
//  const auto append_to_vec =
//      impl->operation_regions.CreateDerived<VECTORAPPEND>(
//          par, ProgramOperation::kAppendToInductionVector);
//  append_to_vec->vector.Emplace(append_to_vec, vec);
//  append_to_vec->worker_id.Emplace(append_to_vec, worker_id);
//
//  // Add all input `from_view` columns to the versioned vector.
//  for (auto col : from_view.Columns()) {
//    const auto var = par->VariableFor(impl, col);
//    append_to_vec->tuple_vars.AddUse(var);
//  }
//
//  par->AddRegion(append_to_vec);
//
//  switch (induction->state) {
//    case INDUCTION::kAccumulatingInputRegions:
//      induction->init_appends_remove.push_back(append_to_vec);
//      break;
//
//    case INDUCTION::kAccumulatingCycleRegions:
//      induction->cycle_appends.push_back(append_to_vec);
//      break;
//
//    default:
////      view.SetTableId(99999);
////      break;
//      assert(false); break;
//  }
//}
//
//}  // namespace

// Build a bottom-up join remover.
void CreateBottomUpJoinRemover(ProgramImpl *impl, Context &context,
                               QueryView from_view, QueryJoin join_view,
                               OP *root, TABLE *already_checked_) {
  assert(join_view.NumPivotColumns());

  const QueryView view(join_view);

  // First, and somewhat unlike other bottom-up removers, we will make sure that
  // the data is gone in the data model associated with this particular
  // predecessor. This is because JOINs require that their predecessors all
  // have backing storage.
  auto [marked, pred_table, last_table] =
      InTryMarkUnknown(impl, context, from_view, root, already_checked_);

  // This is an inductive JOIN, where some of the predecessors are in the
  // induction, and some are out of the induction.
  if (view.InductionGroupId().has_value()) {
    (void) GetOrInitInduction(impl, view, context, marked);
    //CreateBottomUpInductiveJoinRemover(impl, context, from_view, view, marked);
    //return;
  }

  auto parent = impl->series_regions.Create(marked);
  marked->body.Emplace(marked, parent);

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
        const auto param_var = root->VariableFor(impl, from_view_pivots[i]);
        assert(param_var != nullptr);
        root->col_id_to_var[pred_pivots[i].Id()] = param_var;
      }
    }
  }

  // Called within the context of a join on an index scan.
  auto with_join = [&](REGION *join, bool) -> REGION * {
    join_view.ForEachUse([&](QueryColumn in_col, InputColumnRole,
                             std::optional<QueryColumn> out_col) {
      if (auto in_var = join->VariableFor(impl, in_col); in_var && out_col) {
        join->col_id_to_var[out_col->Id()] = in_var;
      }
    });

    const auto let = impl->operation_regions.CreateDerived<LET>(join);
    BuildEagerRemovalRegions(impl, view, context, let, view.Successors(),
                             nullptr);
    return let;
  };

  // If this is more than a two-way join then we're going to make a join region
  // so as not to prescribe a join order/strategy (e.g. nested loop join) onto
  // the code.
  if (2u < pred_views.size()) {

    // Create a pivot vector, which is needed by a join region.
    const auto pivot_vec = parent->containing_procedure->vectors.Create(
        impl->next_id++, VectorKind::kJoinPivots, from_view_pivots);

    // Create the region that will add the tuple to-be-removed to the pivot
    // vector.
    const auto add_to_vec = impl->operation_regions.CreateDerived<VECTORAPPEND>(
        parent, ProgramOperation::kAppendJoinPivotsToVector);
    parent->AddRegion(add_to_vec);

    add_to_vec->vector.Emplace(add_to_vec, pivot_vec);

    for (auto in_col : from_view_pivots) {
      auto pivot_var = root->VariableFor(impl, in_col);
      assert(pivot_var != nullptr);
      add_to_vec->tuple_vars.AddUse(pivot_var);
    }

    // NOTE(pag): We don't really need to join against all views, just all
    //            views except `from_view`. Re-using `BuildJoin` keeps things a
    //            simpler, and we may be able to better optimize things in the
    //            future so that the bottom-up removers from all predecessor
    //            nodes can "share" this common JOIN code.
    const auto join = BuildJoin(impl, join_view, pivot_vec, parent);
    join->body.Emplace(join, with_join(join, true));

  // JOINing two tables; all we can do is an index-scan of the other table; no
  // need for a join region.
  } else if (2u == pred_views.size()) {
    const auto other_view = pred_views[unsigned(pred_views[0] == from_view)];
    const auto other_model =
        impl->view_to_model[other_view]->FindAs<DataModel>();
    assert(other_model->table != nullptr);
    (void) BuildMaybeScanPartial(impl, other_view, pivot_cols[other_view],
                                 other_model->table, parent, with_join);

  } else {
    assert(false);
  }
}

// Build a top-down checker on a join.
REGION *BuildTopDownJoinChecker(
    ProgramImpl *impl, Context &context, REGION *proc, QueryJoin join_view,
    std::vector<QueryColumn> &view_cols, TABLE *already_checked) {

  QueryView view(join_view);

  std::vector<VAR *> pivot_vars(join_view.NumPivotColumns());
  std::vector<VAR *> out_vars(view.Columns().size());
  auto num_found_pivots = 0u;
  auto num_found_cols = 0u;

  // Figure out out how `view_cols` relates to our pivot columns, as well as
  // how it relates to the input columns flowing into the join.
  std::unordered_map<QueryView, std::vector<std::pair<QueryColumn, VAR *>>>
      pred_col_vars;

  std::unordered_map<QueryView, std::vector<std::pair<QueryColumn, QueryColumn>>>
      pivot_map;

  join_view.ForEachUse([&](QueryColumn in_col, InputColumnRole role,
                           std::optional<QueryColumn> out_col) {
    assert(out_col.has_value());
    assert(!in_col.IsConstant());

    const auto pred_view = QueryView::Containing(in_col);

    // Build up a mapping of pivot columns.
    if (InputColumnRole::kJoinPivot == role) {
      pivot_map[pred_view].emplace_back(*out_col, in_col);
    }

    // Look to see if we know about the column.
    if (std::find(view_cols.begin(), view_cols.end(), *out_col) ==
        view_cols.end()) {
      return;
    }

    const auto out_col_var = proc->VariableFor(impl, *out_col);
    const auto out_col_index = *(out_col->Index());
    auto &out_var = out_vars[out_col_index];
    if (!out_var) {
      out_var = out_col_var;
      ++num_found_cols;
    }

    // We found a pivot that we have as an argument.
    if (InputColumnRole::kJoinPivot == role) {

      auto &pivot_var = pivot_vars[out_col_index];
      if (!pivot_var) {
        pivot_var = out_col_var;
        ++num_found_pivots;
      }
    } else {
      assert(InputColumnRole::kJoinNonPivot == role);
    }

    auto &pred_vars = pred_col_vars[pred_view];
    pred_vars.emplace_back(in_col, out_col_var);
  });

  SERIES * const seq = impl->series_regions.Create(proc);

  // Map in the variables.
  for (const auto &[pred_view, col_vars] : pred_col_vars) {
    for (auto [pred_col, join_var] : col_vars) {
      seq->col_id_to_var[pred_col.Id()] = join_var;
    }
  }

  // The base case is that we have variables for every column we need. In this
  // case, what we can do is call down to each of our predecessors, and if any
  // of them return false, then we bail out, otherwise we return true.
  if (num_found_cols == out_vars.size()) {
    PARALLEL * const par = impl->parallel_regions.Create(seq);
    seq->AddRegion(par);

    // Call each predecessor in parallel. If any of them return `false`, then
    // return false.
    for (auto pred_view : join_view.JoinedViews()) {
      par->AddRegion(CallTopDownChecker(
          impl, context, par, view, view_cols, pred_view, already_checked,
          [] (REGION *) -> REGION * { return nullptr; },
          [=] (REGION *parent_if_false) -> REGION * {
            return BuildStateCheckCaseReturnFalse(impl, parent_if_false);
          }));
    }

    // If we fall through to here, then return true.
    seq->AddRegion(BuildStateCheckCaseReturnTrue(impl, seq));

    return seq;
  }

  // We're going to replay this join top-down. That means building up a
  // pivot vector.
  VECTOR * const pivot_vec = proc->containing_procedure->VectorFor(
      impl, VectorKind::kJoinPivots, join_view.PivotColumns());

  // Make sure all the pivots in our pivot map are sorted in terms of the
  // pivot ordering in `join_view`, and not in terms of `pred_view` or
  // whatever the order is that we get from `ForEachUse` above.
  for (auto &[pred_view, pivot_out_in] : pivot_map) {
    std::sort(pivot_out_in.begin(), pivot_out_in.end(),
              [] (std::pair<QueryColumn, QueryColumn> a,
                  std::pair<QueryColumn, QueryColumn> b) {
                return *(a.first.Index()) < *(b.first.Index());
              });
  }

  // In the best case, we have all of our pivot_vars; that's a very nice
  // situation to be in.
  if (num_found_pivots == join_view.NumPivotColumns()) {

    const auto append = impl->operation_regions.CreateDerived<VECTORAPPEND>(
        seq, ProgramOperation::kAppendJoinPivotsToVector);
    seq->AddRegion(append);

    for (auto var : pivot_vars) {
      append->tuple_vars.AddUse(var);
    }
    append->vector.Emplace(append, pivot_vec);

  // We don't have all of our pivot columns, so we'll work to recover them.
  // This means doing an index scan on one of the predecessor tables. We'll
  // try to be "smart" about this, but really, this is just a random heuristic
  // and who knows what's best -- we have no concept of the distribution of
  // tuples, e.g. we might only be missing one column in one table, and five
  // in another, but there could be way more things to read in for that one
  // column case than in that five column case.
  } else {
    std::vector<std::pair<double, QueryView>> pred_view_scores;

    // Calculate a "coverage" score for each predecessor view, and collect
    // all of the scored views in `pred_view_scores`.
    for (const auto &[pred_view, col_vars] : pred_col_vars) {
      const auto num_vars_available = double(col_vars.size());
      const auto num_needed_vars = double(pred_view.Columns().size());
      const auto score = num_vars_available / num_needed_vars;
      pred_view_scores.emplace_back(score, pred_view);
    }

    // Sort the scores so that it's easy to pull out the best scoring view.
    std::sort(pred_view_scores.begin(), pred_view_scores.end(),
              [] (std::pair<double, QueryView> a,
                  std::pair<double, QueryView> b) {
                return a.first < b.first;
              });

    // Make sure we event have a best scoring view.
    assert(!pred_view_scores.empty());
    assert(0.0 < pred_view_scores.back().first);

    const QueryView best_pred_view = pred_view_scores.back().second;
    const auto &pivot_out_in = pivot_map[best_pred_view];
    auto pred_model = impl->view_to_model[best_pred_view]->FindAs<DataModel>();
    auto pred_table = pred_model->table;
    assert(pred_table != nullptr);

    std::vector<QueryColumn> pred_cols;
    for (auto [pred_col, var] : pred_col_vars[best_pred_view]) {
      pred_cols.push_back(pred_col);
    }

    // Scan for the missing columns, and bring in the pivots.
    const auto built_scan = BuildMaybeScanPartial(
        impl, best_pred_view, pred_cols, pred_table, seq,
        [&](REGION *parent, bool) -> REGION * {

          const auto append =
              impl->operation_regions.CreateDerived<VECTORAPPEND>(
                  parent, ProgramOperation::kAppendJoinPivotsToVector);

          for (auto [out_col, in_col] : pivot_out_in) {
            append->tuple_vars.AddUse(parent->VariableFor(impl, in_col));
          }

          append->vector.Emplace(append, pivot_vec);
          return append;
        });

    assert(built_scan);
    (void) built_scan;
  }

  // By now we have stuff in the pivot vector, so lets go and do our
  // join.

  // Sort and unique the pivot vector before doing our JOIN.
  VECTORUNIQUE * const unique =
      impl->operation_regions.CreateDerived<VECTORUNIQUE>(
          seq, ProgramOperation::kSortAndUniquePivotVector);
  seq->AddRegion(unique);
  unique->vector.Emplace(unique, pivot_vec);

  // TODO(pag): Only do the join if we *don't* have all columns available.
  //            Otherwise we can just loop over the vector.
  TABLEJOIN * const join = BuildJoin(impl, join_view, pivot_vec, seq);

  // Make sure all inputs are checked for equality. This is basically to
  // make sure that we're in the right tuple.
  TUPLECMP * const cmp = impl->operation_regions.CreateDerived<TUPLECMP>(
      join, ComparisonOperator::kEqual);
  join->body.Emplace(join, cmp);
  for (const auto &[pred_view, col_vars] : pred_col_vars) {
    for (auto [pred_col, join_var] : col_vars) {
      cmp->lhs_vars.AddUse(join_var);
      cmp->rhs_vars.AddUse(join->VariableFor(impl, pred_col));
    }
  }

  // Okay, we're in the right tuple, now call ourselves recursively with
  // every column available. That function will call down to our children.
  std::vector<QueryColumn> all_cols;
  for (auto col : view.Columns()) {
    all_cols.push_back(col);
  }

  // If the recursive call returns true, then return true, otherwise, go to
  // the next iteration of the join.
  cmp->body.Emplace(cmp, CallTopDownChecker(
      impl, context, cmp, view, all_cols, view, already_checked,
      [=] (REGION *parent_if_true) -> REGION * {
        return BuildStateCheckCaseReturnTrue(impl, parent_if_true);
      },
      [] (REGION *parent_if_false) -> REGION * { return nullptr; }));


  // If we fell through to the end, then none of the iterations of the join
  // succeeded and we failed to find the tuple.
  seq->AddRegion(BuildStateCheckCaseReturnFalse(impl, seq));

  return seq;
}

}  // namespace hyde
