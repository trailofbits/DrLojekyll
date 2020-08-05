// Copyright 2020, Trail of Bits. All rights reserved.

#include <algorithm>
#include <cassert>
#include <set>
#include <unordered_map>
#include <utility>
#include <vector>

#include "Program.h"

#include <drlojekyll/Util/DisjointSet.h>

// IDEAS
//
//    - born_on_same_day:
//        -   We want both sides to contribute their data to the JOIN, but the
//            actual execution of the JOIN should be deferred as a pipeline
//            blocker at a higher level. So what will happen is each side
//            contributes and records the JOIN pivots into a vector, which is
//            later sorted and uniqued.
//
//        - sort child nodes of a parallel by a hash, but where the hash is
//          not based on children and only on the superficial aspects of the
//          region itself, so that we can look for opportunities to join
//          region bodies
//
//    - Make sure all SELECTs reachable from a RECV are entrypoints, put
//      the INSERTs for them into vectors.
//
//    - Right now, no guarantee that the arity of two identical sets of column
//      IDs will match, and so ARITY is a key issue. Need to add a notion of
//      provenance or selectors to a table. For example:
//
//                 .--- COMPARE --.
//          RECV --+              +-- JOIN
//                 '--- TUPLE ----'
//
//      In this case, the COMPARE and TUPLE might have identical columns, and
//      thus appear equivalent, but we should really have an additional field
//      per row that tells us if the row is present for both the COMPARE and the
//      TUPLE.
//
//    - Think about if the TABLEs entering a JOIN should be based on all data
//      in the predecessor, or just the data used by the JOIN. Right now it's
//      the former.
namespace hyde {
namespace {
//
//struct ViewRegion : public DisjointSet {
//  explicit ViewRegion(QueryView view_)
//      : DisjointSet(view_.Depth()),
//        view(view_) {}
//
//  const QueryView view;
//  REGION *region{nullptr};
//};

using WorkItem = std::tuple<PROC *, QueryView, REGION *>;

struct Context {
  std::vector<WorkItem> work_list;
  std::vector<WorkItem> next_work_list;

  std::set<QueryView> eager;
  std::set<QueryView> lazy;

  // Boolean guard variable used to determine if we should try to execute
  // a lazy PRODUCT in eager code.
  std::unordered_map<QueryJoin, VAR *> product_guard_var;

  // A vector of tuples produced for a PRODUCT.
  std::unordered_map<QueryJoin, TABLE *> product_vector;

//  std::unordered_map<QueryView, std::set<QueryView>> used_by;
//  std::unordered_map<QueryView, std::set<QueryView>> fed_by;


//  std::vector<std::unique_ptr<ViewRegion>> view_regions;
//  std::unordered_map<QueryView, ViewRegion *> view_to_region;
//
//  ViewRegion *RegionFor(QueryView view) {
//    auto &region = view_to_region[view];
//    if (!region) {
//      region = new ViewRegion(view);
//      view_regions.emplace_back(region);
//    }
//    return region;
//  }
};

// Return the set of all views that contribute data to `view`. This includes
// things like conditions.
std::set<QueryView> TransitivePredecessorsOf(QueryView output) {
  std::set<QueryView> dependencies;
  std::vector<QueryView> frontier;
  frontier.push_back(output);

  while (!frontier.empty()) {
    const auto view = frontier.back();
    frontier.pop_back();
    for (auto pred_view : view.Predecessors()) {
      if (!dependencies.count(pred_view)) {
        dependencies.insert(pred_view);
        frontier.push_back(pred_view);
      }
    }
  }

  return dependencies;
}

// Return the set of all views that are transitively derived from `input`.
std::set<QueryView> TransitiveSuccessorsOf(QueryView input) {
  std::set<QueryView> dependents;
  std::vector<QueryView> frontier;
  frontier.push_back(input);

  while (!frontier.empty()) {
    const auto view = frontier.back();
    frontier.pop_back();
    for (auto succ_view : view.Successors()) {
      if (!dependents.count(succ_view)) {
        dependents.insert(succ_view);
        frontier.push_back(succ_view);
      }
    }
  }

  return dependents;
}

// We want to break up all code in terms of which sets of RECV I/Os potentially
// feed data to that node. If data froms from VIEW `a` to VIEW `b`, and
// `usage.fed_by[a] != usage.fed_by[b]` then we'll treat `b` as being a new
// procedure entrypoint.
static bool ViewIsAlsoFedBy(QueryView view, Context &usage,
                            const std::set<QueryView> &covered_set) {
  const auto fed_by_it = usage.fed_by.find(view);
  if (fed_by_it == usage.fed_by.end()) {
    assert(false);
    return false;
  }

  return fed_by_it->second == covered_set;
}

static REGION *BuildEagerRegion(ProgramImpl *impl, QueryView pred_view,
                                QueryView view, Context &context, REGION *parent);

// Add in the regions for the successors of a node.
static REGION *BuildEagerSuccessorRegions(ProgramImpl *impl, QueryView view,
                                          Context &context, REGION *parent) {

  REGION *child_region = nullptr;

  // Add in the successor views.
  for (auto succ_view : view.Successors()) {
    auto child = BuildEagerRegion(impl, view, succ_view, context, parent);
    if (!child) {
      continue;  // Lazily executed, we chose not to do anything with it.
    }

    if (!child_region) {
      child_region = child;

    } else if (auto par = child_region->AsParallel(); par) {
      child_region->ExecuteAlongside(impl, par);

    } else {
      auto par = impl->parallel_regions.Create(parent);
      child_region->ExecuteAlongside(impl, par);
      child->ExecuteAlongside(impl, par);
      child_region = par;
      parent = par;
    }
  }

  return child_region;
}


// Create an eager PRODUCT region for a cross-product. An eager PRODUCT region is
// responsible for handling the evaluation of PRODUCTs, and doing so for eagerly
// executed joins, as well as for lazily executed PRODUCTs whose results have
// previously been evaluated, and thus require eager updates.
static REGION *BuildEagerProductRegion(
    ProgramImpl *impl, QueryView pred_view, QueryJoin view, Context &context,
    REGION *parent) {

#ifndef NDEBUG
  view.ForEachUse([&](QueryColumn in_col, InputColumnRole role,
                      std::optional<QueryColumn> out_col) {
    if (out_col && QueryView::Containing(in_col) == pred_view) {
      assert(in_col.Id() == out_col->Id());
    }
  });
#endif

  const auto proc = parent->containing_procedure;
  const auto is_eager = context.eager.count(view);
  const auto is_on_eagers_edge = context.eager.count(pred_view);

  // NOTE(pag): We parameterize by `view_tag` to distinguish eager and lazy
  //            variants, where the eager variant can share the view tag of
  //            the predecessor, but the lazy variant can use the view tag
  //            of `view`.
  const QueryView view_tag = is_eager ? pred_view : view;

  auto make_insert = [=](REGION *parent_of_insert) -> OP * {
    const auto insert = impl->operation_regions.Create(
        parent_of_insert, ProgramOperation::kInsertIntoView);

    // Insert the variables from predecessor into the table. The semantics of
    // the insert is that is conditionally executes it's `body` if the insert
    // adds new data.
    insert->views.AddUse(TABLE::GetOrCreate(impl, pred_view.Columns(),
                                            view_tag));
    for (auto col : pred_view.Columns()) {
      insert->variables.AddUse(proc->GetOrCreateLocal(col));
    }

    insert->variables.Unique();
    return insert;
  };

  OP *ret_region = nullptr;
  OP *env = nullptr;

  // This PRODUCT needs to always be eagerly executed.
  if (is_eager) {
    ret_region = make_insert(parent);
    env = ret_region;

  } else {

    auto &guard_var = context.product_guard_var[view];
    if (!guard_var) {
      guard_var = impl->global_vars.Create(
          ~0u - impl->global_vars.Size(), VariableRole::kGlobalBoolean);
    }

    // We're on the edge of egerness and laziness. Materialize this side of the
    // product, and then enter into the the product if we've ever entered it
    // before.
    if (is_on_eagers_edge) {
      ret_region = make_insert(parent);

      env = impl->operation_regions.Create(
          env, ProgramOperation::kTestGlobalVariableIsTrue);

      env->variables.AddUse(guard_var);

    // This PRODUCT is fully lazy, i.e. it should only execute if the product
    // has executed before.
    } else {
      ret_region = impl->operation_regions.Create(
          parent, ProgramOperation::kTestGlobalVariableIsTrue);

      ret_region->variables.AddUse(guard_var);
      env = make_insert(ret_region);
    }

    UseRef<REGION>(ret_region, env).Swap(ret_region->body);
  }

  assert(env != nullptr);

  // Outputs of the PRODUCT are dumped into this view.
  auto &output_vec = context.product_vector[view];
  if (!output_vec) {
    output_vec = TABLE::Create(proc, view.Columns());
  }

  const auto input_vec = TABLE::Create(proc, pred_view.Columns());
  const auto append_region = impl->operation_regions.Create(
      env, ProgramOperation::kAppendProductInputToVector);

  append_region->tables.AddUse(input_vec);
  for (auto pred_col : pred_view.Columns()) {
    append_region->variables.AddUse(proc->GetOrCreateLocal(pred_col));
  }
  append_region->variables.Unique();

  UseRef<REGION>(env, append_region).Swap(env->body);

  auto &region = proc->view_to_region[view];

  // Make the main loop for this predecessor. We loop over the vector of new
  // predecessor tuple entries, and for each one, loop over all other tables
  // being joined through the PRODUCT.
  //
  // This loop is added to a parallel region, so that each time we reach a
  // PRODUCT, we add in a new side of the cross-product.
  auto make_loop = [&](PARALLEL *par_region) -> OP * {
    auto loop = impl->operation_regions.Create(
        par_region, ProgramOperation::kLoopOverProductInputVector);
    loop->tables.AddUse(input_vec);

    for (auto col : pred_view.Columns()) {
      loop->variables.AddUse(proc->GetOrCreateLocal(col));
    }
    loop->variables.Unique();

    auto outer_loop = loop;

    // Make a loop nest iterating over all views other than `pred_view`. In
    // place of `pred_view`, we have the above loop over the product tuple
    // vector associated with `pred_view`.
    for (auto joined_view : view.JoinedViews()) {
      if (joined_view == pred_view) {
        continue;
      }

      const auto inner_loop = impl->operation_regions.Create(
          outer_loop, ProgramOperation::kLoopOverView);

      // Insert the variables from predecessor into the table. The semantics of
      // the insert is that is conditionally executes it's `body` if the insert
      // adds new data.
      const QueryView view_tag = is_eager ? joined_view : view;
      inner_loop->views.AddUse(TABLE::GetOrCreate(impl, joined_view.Columns(),
                                                  view_tag));
      for (auto col : joined_view.Columns()) {
        inner_loop->variables.AddUse(proc->GetOrCreateLocal(col));
      }
      inner_loop->variables.Unique();

      UseRef<REGION>(outer_loop, inner_loop).Swap(outer_loop->body);
      outer_loop = inner_loop;
    }

    // TODO(pag): Add in an append to the output vector.

    par_region->regions.AddUse(loop);
    return loop;
  };

  // This is our first time seeing this PRODUCT, so we're going to create a
  // SERIES region that's first going to iterate over all the input vectors
  // that need to be scanned, then dump results into a common vector (in their
  // innermost loops), then the second thing in the series will be a loop
  // over those results, which are the PRODUCT outputs. The series (or a
  // conditioned version thereof) will be put onto a worklist for placement
  // into the procedure.
  if (!region) {
    SERIES *series_region = nullptr;

    // If it's eager, then we'll defer to the top-level builder to place
    // the parallel region.
    if (is_eager) {
      series_region = impl->series_regions.Create(proc);
      context.work_list.emplace_back(proc, view, series_region);

    // If it's lazy, then only push forward if we've done so before.
    } else {
      auto test = impl->operation_regions.Create(
          proc, ProgramOperation::kTestGlobalVariableIsTrue);
      test->variables.AddUse(context.product_guard_var[view]);

      series_region = impl->series_regions.Create(test);
      UseRef<REGION>(test, series_region).Swap(test->body);

      context.work_list.emplace_back(proc, view, test);
    }

    const auto par_region = impl->parallel_regions.Create(series_region);
    series_region->regions.AddUse(par_region);

    make_loop(par_region);
    region = par_region;

    // TODO(pag): Add in a loop over the output vector.

  // This is the Nth time seeing this PRODUCT, add a loop into the parallel
  // region.
  } else if (auto par_region = region->AsParallel(); par_region) {
    make_loop(par_region);

  } else {
    assert(false);
    return nullptr;
  }

  return ret_region;
}

// Create an eager JOIN region. An eager JOIN region is responsible for handling
// the evaluation of JOINs, and doing so for eagerly executed joins, as well as
// for lazily executed JOINs whose results have previously been evaluated, and
// thus require eager updates.
static REGION *BuildEagerJoinRegion(
    ProgramImpl *impl, QueryView pred_view, QueryJoin view, Context &context,
    REGION *parent) {

  assert(0u < view.NumPivotColumns());

  const auto proc = parent->containing_procedure;
  const auto let_binding = impl->operation_regions.Create(
      parent, ProgramOperation::kLetBinding);

  // Map input to output variables where the column IDs differ.
  view.ForEachUse([&](QueryColumn in_col, InputColumnRole role,
                      std::optional<QueryColumn> out_col) {
    if (out_col && QueryView::Containing(in_col) == pred_view &&
        in_col.Id() == out_col->Id()) {
      let_binding->variables.AddUse(proc->GetOrCreateLocal(*out_col));
      let_binding->variables.AddUse(proc->GetOrCreateLocal(in_col));
    }
  });

  // Pivot output columns have different IDs than their input columns.
  assert(!let_binding->variables.Empty());

  auto make_insert = [=](OP *parent_of_insert) -> OP * {
    const auto insert = impl->operation_regions.Create(
        parent_of_insert, ProgramOperation::kInsertIntoView);

    // Insert the variables from predecessor into the table. The semantics of
    // the insert is that is conditionally executes it's `body` if the insert
    // adds new data.
    //
    // TODO(pag): This adds all columns to the table, not just the ones needed
    //            by the JOIN. Think about if we want to change this, and if
    //            it is an optimization or not.
    insert->views.AddUse(TABLE::GetOrCreate(impl, pred_view.Columns(), view));
    for (auto pred_col : pred_view.Columns()) {
      insert->variables.AddUse(proc->GetOrCreateLocal(pred_col));
    }
    insert->variables.Unique();

    UseRef<REGION>(parent_of_insert, insert).Swap(parent_of_insert->body);
    return insert;
  };

  auto env = let_binding;
  const auto num_pivots = view.NumPivotColumns();
  const auto is_eager = context.eager.count(view);
  const auto is_on_eagers_edge = context.eager.count(pred_view);

  // This JOIN needs to always be eagerly executed.
  if (is_eager) {
    env = make_insert(env);

  // This JOIN is lazy, so we only want to execute it if we have previously
  // computed some results for some of the pivots.
  } else {

    // We're on the edge of egerness and laziness; we need to materialize the
    // tables that the lazy code might need to use for building the join.
    if (is_on_eagers_edge) {
      env = make_insert(env);
    }

    auto pivot_table = TABLE::GetOrCreate(impl, view.PivotColumns(), view);
    const auto check = impl->operation_regions.Create(
        env, ProgramOperation::kCheckTupleIsPresentInView);
    check->views.AddUse(pivot_table);
    for (auto col : view.PivotColumns()) {
      check->variables.AddUse(proc->GetOrCreateLocal(col));
    }
    check->variables.Unique();

    UseRef<REGION>(env, check).Swap(env->body);
    env = check;

    // We're in a 100% lazy JOIN (i.e. predecessor is lazy and this join is
    // lazy). If any part of this JOIN has been executed before, i.e. because
    // lazy code has asked for some specific query result, then we want to
    // keep all of those results updated so that if they ask again, they will
    // see updated results.
    if (!is_on_eagers_edge) {
      env = make_insert(env);
    }
  }

  assert(env != nullptr);

  auto &region = proc->view_to_region[view];

  // This is the first time we're seeing this JOIN node, go create a JOIN region
  // for it.
  if (!region) {
    auto join_region = impl->operation_regions.Create(
        env, ProgramOperation::kJoinTables);

    // Attach the users of the JOIN in.
    auto users_of_join = BuildEagerSuccessorRegions(
        impl, view, context, join_region);
    UseRef<REGION>(join_region, users_of_join).Swap(join_region->body);

    // We'll assign the JOIN region to `region` so that if we enter into this
    // join again through another path then we can synthesize a vector to hold
    // the pivots, and move the JOIN out to after an ancestor node.
    region = join_region;

    // Add the tables to the JOIN. The same table might appear multiple times,
    // e.g. if there is a RECV going to two TUPLEs going into a JOIN, then the
    // column IDs of the two TUPLEs might match, resulting in the same table
    // appearing twice.
    std::vector<QueryColumn> indexed_cols;
    for (auto i = 0u; i < num_pivots; ++i) {
      indexed_cols.clear();
      for (auto col : view.NthInputPivotSet(i)) {
        indexed_cols.push_back(col);
      }

      auto joined_table = TABLE::GetOrCreate(
          impl, QueryView::Containing(indexed_cols[0]).Columns(), view);

      join_region->views.AddUse(joined_table);
      join_region->indices.AddUse(
          joined_table->GetOrCreateIndex(indexed_cols));
    }

    UseRef<REGION>(env, join_region).Swap(env->body);

  // This is the 2nd or Nth time seeing this JOIN.
  } else if (auto op_region = region->AsOperation(); op_region) {

    // This is the 2nd time seeing this join. We need to create a vector,
    // move the JOIN region out to a common ancestor, and swap it with an
    // INSERT into the vector, and then have the region associated with this
    // second viewing of the JOIN be an INSERT into the vector.
    if (ProgramOperation::kJoinTables == op_region->op) {

      auto vec = TABLE::Create(proc, view.PivotColumns());

      auto prev_add_to_vec = impl->operation_regions.Create(
          op_region->parent, ProgramOperation::kAppendJoinPivotsToVector);

      auto curr_add_to_vec = impl->operation_regions.Create(
          parent, ProgramOperation::kAppendJoinPivotsToVector);

      // NOTE(pag): We don't yet know where we will place this LOOP, so we'll
      //            set the parent as the procedure itself.
      auto loop = impl->operation_regions.Create(
          proc, ProgramOperation::kLoopOverJoinPivots);

      prev_add_to_vec->tables.AddUse(vec);
      curr_add_to_vec->tables.AddUse(vec);

      for (auto col : view.PivotColumns()) {
        const auto var = proc->GetOrCreateLocal(col);
        prev_add_to_vec->variables.AddUse(var);
        curr_add_to_vec->variables.AddUse(var);
        loop->variables.AddUse(var);
      }
      prev_add_to_vec->variables.Unique();
      curr_add_to_vec->variables.Unique();
      loop->variables.Unique();

      loop->tables.AddUse(vec);

      // `op_region` is a JOIN, so replace all of its uses, for which there
      // should only be one, with the add to a vector operation. If the
      // procedure we're building has only a single way into the JOIN, then we
      // want to process that JOIN only in that place. However, if it has more
      // than one ways in, then we want all ways in to push the pivots of
      // interest into a shared vector, and process all of those all at once.
      // Thus, we replace the join handling with a vector push, pull out the
      // join handling into an explicit loop over that vector, and then we'll
      // add the view to a work list to be processed later.
      region->ReplaceAllUsesWith(prev_add_to_vec);
      region = loop;

      // Replace it for the Nth time we see it and add us to the work list.
      context.work_list.emplace_back(proc, view, loop);

      UseRef<REGION>(env, curr_add_to_vec).Swap(env->body);

    // This is the Nth time seeing this join. We just need to add to the same
    // vector as prior iterations.
    } else if (ProgramOperation::kLoopOverJoinPivots == op_region->op) {

      auto curr_add_to_vec = impl->operation_regions.Create(
          parent, ProgramOperation::kAppendJoinPivotsToVector);

      curr_add_to_vec->tables.AddUse(op_region->tables[0]);
      for (auto pivot_var : op_region->variables) {
        curr_add_to_vec->variables.AddUse(pivot_var);
      }

      curr_add_to_vec->variables.Unique();

      UseRef<REGION>(env, curr_add_to_vec).Swap(env->body);

    } else {
      assert(false);
      return nullptr;
    }
  } else {
    assert(false);
    return nullptr;
  }

  return let_binding;
}

static REGION *BuildEagerInsertRegion(
    ProgramImpl *impl, QueryView pred_view, QueryInsert view, Context &context,
    REGION *parent) {

  const auto proc = parent->containing_procedure;
  const auto insert = impl->operation_regions.Create(
      parent, ProgramOperation::kInsertIntoView);

  insert->tables.AddUse(TABLE::GetOrCreate(impl, view.InputColumns(), view));
  for (auto pred_col : pred_view.Columns()) {
    insert->variables.AddUse(proc->GetOrCreateLocal(pred_col));
  };

  insert->variables.Unique();

  auto &region = proc->view_to_region[view];

  // This is the first time we're seeing this INSERT node, go create all the
  // SELECTs.
  if (!region) {

  } else {

  }
}

// Build an eager region where this eager region is being conditionally
// executed, i.e. executing inside the scope of the conditions being tested
// by `view.PositiveConditions()` and `view.NegativeConditions()`.
static REGION *BuildConditionalEagerRegion(
    ProgramImpl *impl, QueryView pred_view, QueryView view, Context &context,
    REGION *parent) {

  if (view.IsJoin()) {
    const auto join = QueryJoin::From(view);
    if (join.NumPivotColumns()) {
      return BuildEagerJoinRegion(impl, pred_view, join, context, parent);
    } else {
      return BuildEagerProductRegion(impl, pred_view, join, context, parent);
    }
  } else if (view.IsMerge()) {

  } else if (view.IsAggregate()) {

  } else if (view.IsKVIndex()) {

  } else if (view.IsMap()) {

  } else if (view.IsCompare()) {

  } else if (view.IsTuple()) {

  } else if (view.IsSelect()) {

  } else if (view.IsInsert()) {
    return BuildEagerInsertRegion(impl, pred_view, QueryInsert::From(view),
                                  context, parent);

  } else {
    assert(false);
    return nullptr;
  }

//
//  if (series->regions.Size() == 1u) {
//    auto only_child = series->regions[0];
//    only_child->parent = parent;
//    series->regions.Clear();
//    return only_child;
//
//  } else {
//    return series;
//  }
}

REGION *BuildEagerRegion(ProgramImpl *impl, QueryView pred_view,
                         QueryView view, Context &usage, REGION *parent) {
  const auto pos_conds = view.PositiveConditions();
  const auto neg_conds = view.NegativeConditions();

  // If this view is conditional
  if (!pos_conds.empty() || !neg_conds.empty()) {
    const auto cond = impl->operation_regions.Create(
        parent, ProgramOperation::kTestConditions);
    cond->positive_conditions.insert(
        cond->positive_conditions.end(), pos_conds.begin(), pos_conds.end());
    cond->negative_conditions.insert(
        cond->negative_conditions.end(), neg_conds.begin(), neg_conds.end());

    auto child_region = BuildConditionalEagerRegion(
       impl, pred_view, view, usage, cond);

    UseRef<REGION>(cond, child_region).Swap(cond->body);

    return cond;

  } else {
    return BuildConditionalEagerRegion(
         impl, pred_view, view, usage, parent);
  }
}

static PROC *DeclareEagerProcedure(ProgramImpl *impl, QueryView view) {
  auto &proc = impl->procedures[view];
  if (!proc) {
    proc = impl->procedure_regions.Create(view);
  }
  return proc;
}

// Create a procedure for a view.
static void BuildEagerProcedure(ProgramImpl *impl, QueryView view,
                                Context &usage) {
  const auto covered_set_it = usage.fed_by.find(view);
  assert(covered_set_it != usage.fed_by.end());
  const auto &covered_set = covered_set_it->second;

  const auto proc = DeclareEagerProcedure(impl, view);
  if (auto proc_body = proc->body->AsOperation();
      proc_body &&
      ProgramOperation::kLoopOverImplicitInputVector == proc_body->op) {

    REGION *region = BuildEagerRegion(impl, view, view, usage, proc_body);
    assert(!proc_body->body.get());
    UseRef<REGION>(proc_body, region).Swap(proc_body->body);

  } else {
    assert(false);
  }
}

}  // namespace

// Build a program from a query.
std::optional<Program> Program::Build(const Query &query, const ErrorLog &) {
  auto impl = std::make_shared<ProgramImpl>();

  Context context;

  // Conditions need to be eagerly updated. Transmits and queries may need to
  // depend on them so they must be up-to-date.
  for (auto cond : query.Conditions()) {
    for (auto setter : cond.Setters()) {
      auto deps = TransitivePredecessorsOf(setter);
      context.eager.insert(deps.begin(), deps.end());
    }
  }

  // Transmits are messages that we send out "ASAP," i.e. when new data is
  // injested by a receive, we want to compute stuff and send out messages
  // (e.g. for making orchestration decisions).
  for (auto io : query.IOs()) {
    for (auto transmit : io.Transmits()) {
      auto deps = TransitivePredecessorsOf(transmit);
      context.eager.insert(deps.begin(), deps.end());
    }
  }

  const auto program = impl.get();

  for (auto io : query.IOs()) {
    for (auto receive : io.Receives()) {
      (void) BuildEagerProcedure(program, receive, context);
    }
  }

  std::vector<REGION *> sub_regions;
  auto process_sub_regions = [=, &sub_regions](void) {
    if (sub_regions.empty()) {
      return;
    }

    auto proc = sub_regions.back()->containing_procedure;
    auto par = program->parallel_regions.Create(proc);
    par->ExecuteAfter(program, proc);
    for (auto region : sub_regions) {
      region->ExecuteAlongside(program, par);
    }
    sub_regions.clear();
  };

  while (!context.work_list.empty()) {

    // Sort pending regions by procedure, and by depth within the procedure.
    std::stable_sort(
        context.work_list.begin(), context.work_list.end(),
        [](WorkItem a, WorkItem b) {
          return std::make_pair(std::get<0>(a), std::get<1>(a).Depth()) <
                 std::make_pair(std::get<0>(b), std::get<1>(b).Depth());
        });

    context.next_work_list.swap(context.work_list);
    context.work_list.clear();
    sub_regions.clear();

    // Process all sub-regions at the same depth for the same procedure.
    auto prev_depth = ~0u;
    for (auto [proc, view, region] : context.next_work_list) {
      if (sub_regions.empty()) {
        sub_regions.push_back(region);
        prev_depth = view.Depth();
      } else if (proc == sub_regions.back()->containing_procedure) {
        if (prev_depth == view.Depth()) {
          sub_regions.push_back(region);
        } else {
          process_sub_regions();
          sub_regions.push_back(region);
          prev_depth = view.Depth();
        }
      } else {
        process_sub_regions();
        sub_regions.push_back(region);
        prev_depth = view.Depth();
      }
    }

    process_sub_regions();
  }

  return impl;
}

//// Conditions need to be eagerly updated. Transmits and queries may need to
//// depend on them so they must be up-to-date.
//for (auto cond : query.Conditions()) {
//  for (auto setter : cond.Setters()) {
//    auto deps = TransitivePredecessorsOf(setter);
//    usage.eager.insert(deps.begin(), deps.end());
//    for (auto dep : deps) {
//      usage.used_by[dep].insert(cond);
//    }
//  }
//}
//
//// Transmits are messages that we send out "ASAP," i.e. when new data is
//// injested by a receive, we want to compute stuff and send out messages
//// (e.g. for making orchestration decisions).
//for (auto io : query.IOs()) {
//  for (auto transmit : io.Transmits()) {
//    auto deps = TransitivePredecessorsOf(transmit);
//    usage.eager.insert(deps.begin(), deps.end());
//    for (auto dep : deps) {
//      usage.used_by[dep].insert(io);
//    }
//  }
//}

//  query.ForEachView([&](QueryView view) {
//    auto view_region = new ViewRegion(view);
//    usage.view_regions.emplace_back(view_region);
//    usage.view_to_region.emplace(view, view_region);
//  });

//  // Inserts on relations actually correspond to `#query`ies in the Datalog
//  // code. These are
//  for (auto insert : query.Inserts()) {
//    if (insert.IsRelation()) {
//      auto deps = TransitivePredecessorsOf(insert);
//      deps.erase(usage.eager.begin(), usage.eager.end());
//      usage.lazy.insert(deps.begin(), deps.end());
//      for (auto dep : deps) {
//        usage.used_by[dep].insert(insert);
//      }
//    }
//  }
//
//  // Figure out what views are reachable from each RECV I/O.
//  std::vector<QueryView> frontier;
//  for (auto io : query.IOs()) {
//    for (auto receive : io.Receives()) {
//      frontier.clear();
//      frontier.push_back(receive);
//      while (!frontier.empty()) {
//        auto reached_view = frontier.back();
//        frontier.pop_back();
//        auto &reached_set = usage.fed_by[reached_view];
//        const auto reached_set_size = reached_set.size();
//        reached_set.insert(receive);
//        if (reached_set.size() == reached_set_size) {
//          continue;
//        }
//
//        reached_view.ForEachUser([&](QueryView frontier_view) {
//          frontier.push_back(frontier_view);
//        });
//      }
//    }
//  }

}  // namespace hyde
