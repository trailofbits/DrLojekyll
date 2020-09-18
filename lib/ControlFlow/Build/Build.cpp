// Copyright 2020, Trail of Bits. All rights reserved.

#include "Build.h"

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

// Return the set of all views that contribute data to `view`. This includes
// things like conditions.
static std::set<QueryView> TransitivePredecessorsOf(QueryView output) {
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
static std::set<QueryView> TransitiveSuccessorsOf(QueryView input) {
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

// Build an eager region where this eager region is being unconditionally
// executed, i.e. ignoring whether or not `view.PositiveConditions()` or
// `view.NegativeConditions()` have elements.
static void BuildUnconditionalEagerRegion(ProgramImpl *impl,
                                          QueryView pred_view, QueryView view,
                                          Context &context, OP *parent,
                                          TABLE *last_model) {
  if (view.IsJoin()) {
    const auto join = QueryJoin::From(view);
    if (join.NumPivotColumns()) {
      BuildEagerJoinRegion(
          impl, pred_view, join, context, parent, last_model);
    } else {
      BuildEagerProductRegion(
          impl, pred_view, join, context, parent, last_model);
    }

  } else if (view.IsMerge()) {
    const auto merge = QueryMerge::From(view);
    if (context.inductive_successors.count(view)) {
      BuildEagerInductiveRegion(impl, pred_view, merge,
                                context, parent, last_model);
    } else {
      BuildEagerUnionRegion(impl, pred_view, merge,
                            context, parent, last_model);
    }

  } else if (view.IsAggregate()) {

  } else if (view.IsKVIndex()) {

  } else if (view.IsMap()) {

  } else if (view.IsCompare()) {
    BuildEagerCompareRegions(impl, QueryCompare::From(view), context, parent);

  } else if (view.IsSelect() || view.IsTuple()) {
    BuildEagerSuccessorRegions(impl, view, context, parent,
                               view.Successors(), last_model);

  } else if (view.IsInsert()) {
    BuildEagerInsertRegion(impl, pred_view, QueryInsert::From(view),
                           context, parent, last_model);

  } else {
    assert(false);
  }
}

// Returns a global reference count variable associated with a query condition.
static VAR *ConditionVariable(ProgramImpl *impl, QueryCondition cond) {
  auto &cond_var = impl->cond_ref_counts[cond];
  if (!cond_var) {
    cond_var = impl->global_vars.Create(
        impl->next_id++,
        VariableRole::kConditionRefCount);
    cond_var->query_cond = cond;
  }
  return cond_var;
}

// Map all variables to their defining regions.
static void MapVariables(REGION *region) {
  if (!region) {
    return;

  } else if (auto op = region->AsOperation(); op) {
    if (auto let = op->AsLetBinding(); let) {
      for (auto var : let->defined_vars) {
        var->defining_region = region;
      }
    } else if (auto loop = op->AsVectorLoop(); loop) {
      for (auto var : loop->defined_vars) {
        var->defining_region = region;
      }
    } else if (auto join = op->AsTableJoin(); join) {
      for (const auto &var_list : join->output_vars) {
        for (auto var : var_list) {
          var->defining_region = region;
        }
      }
    }

    MapVariables(op->body.get());

  } else if (auto induction = region->AsInduction(); induction) {
    MapVariables(induction->init_region.get());
    MapVariables(induction->cyclic_region.get());
    MapVariables(induction->output_region.get());

  } else if (auto par = region->AsParallel(); par) {
    for (auto sub_region : par->regions) {
      MapVariables(sub_region);
    }
  } else if (auto series = region->AsSeries(); series) {
    for (auto sub_region : series->regions) {
      MapVariables(sub_region);
    }
  }
}

// Create a procedure for a view.
static void BuildEagerProcedure(ProgramImpl *impl, QueryIO io,
                                Context &context) {
  const auto receives = io.Receives();
  if (receives.empty()) {
    return;
  }

  const auto proc = impl->procedure_regions.CreateDerived<PROC>(impl->next_id++);
  proc->io = io;

  const auto vec = proc->VectorFor(
      impl, VectorKind::kInput, receives[0].Columns());
  const auto loop = impl->operation_regions.CreateDerived<VECTORLOOP>(
      proc, ProgramOperation::kLoopOverInputVector);
  auto par = impl->parallel_regions.Create(loop);

  for (auto col : receives[0].Columns()) {
    const auto var = loop->defined_vars.Create(
        impl->next_id++, VariableRole::kVectorVariable);
    var->query_column = col;
    loop->col_id_to_var.emplace(col.Id(), var);
  }

  UseRef<REGION>(loop, par).Swap(loop->body);
  UseRef<VECTOR>(loop, vec).Swap(loop->vector);
  UseRef<REGION>(proc, loop).Swap(proc->body);

  context.view_to_induction.clear();
  context.work_list.clear();
  context.view_to_work_item.clear();
  context.product_vector.clear();

  for (auto receive : io.Receives()) {
    auto let = impl->operation_regions.CreateDerived<LET>(par);
    let->ExecuteAlongside(impl, par);

    auto i = 0u;
    for (auto col : receive.Columns()) {
      auto first_col = receives[0].Columns()[i++];
      if (col.Id() != first_col.Id()) {
        let->used_vars.AddUse(par->VariableFor(impl, first_col));
        const auto var = let->defined_vars.Create(
            impl->next_id++, VariableRole::kLetBinding);
        var->query_column = col;
        loop->col_id_to_var.emplace(col.Id(), var);
      }
    }

    BuildEagerSuccessorRegions(
        impl, receive, context, let,
        receive.Successors(), nullptr);
  }

  while (!context.work_list.empty()) {
//    context.view_to_work_item.clear();
    std::stable_sort(context.work_list.begin(),
                     context.work_list.end(),
                     [](const WorkItemPtr &a, const WorkItemPtr &b) {
                       return a->order > b->order;
                     });

    WorkItemPtr action = std::move(context.work_list.back());
    context.work_list.pop_back();
    action->Run(impl, context);
  }
}

// Analyze the MERGE/UNION nodes and figure out which ones are inductive.
static void DiscoverInductions(const Query &query, Context &context) {
  unsigned merge_id = 0u;
  for (auto view : query.Merges()) {
    context.merge_sets.emplace(view, merge_id++);
    auto preds = TransitivePredecessorsOf(view);

    // This is not an inductive merge.
    if (!preds.count(view)) {
      continue;
    }

    for (auto succ_view : QueryView(view).Successors()) {
      if (preds.count(succ_view)) {
        context.inductive_successors[view].insert(succ_view);
      } else {
        context.noninductive_successors[view].insert(succ_view);
      }
    }

    auto succs = TransitiveSuccessorsOf(view);
    for (auto pred_view : QueryView(view).Predecessors()) {
      if (succs.count(pred_view)) {
        context.inductive_predecessors[view].insert(pred_view);
      } else {
        context.noninductive_predecessors[view].insert(pred_view);
      }
    }
  }

  // Now group together the merges into co-inductive sets, i.e. when one
  // induction is tied with another induction.
  std::set<QueryView> seen;
  std::vector<QueryView> frontier;
  std::set<std::pair<QueryView, QueryView>> disallowed_edges;

  for (auto &[view, noninductive_predecessors] : context.noninductive_predecessors) {
    for (QueryView pred_view : noninductive_predecessors) {
      disallowed_edges.emplace(pred_view, view);
    }
  }

  for (const auto &[view, inductive_successors] : context.inductive_successors) {
    if (inductive_successors.empty()) {
      continue;
    }

    frontier.clear();
    seen.clear();

    for (QueryView succ_view : inductive_successors) {
      frontier.push_back(succ_view);
    }

    while (!frontier.empty()) {
      const auto view = frontier.back();
      frontier.pop_back();
      for (auto succ_view : view.Successors()) {
        if (!seen.count(succ_view) &&
            !disallowed_edges.count({view, succ_view})) {
          seen.insert(succ_view);
          frontier.push_back(succ_view);
        }
      }
    }

    DisjointSet &base_set = context.merge_sets[view];
    for (auto reached_view : seen) {
      if (reached_view.IsMerge()) {
        QueryMerge reached_merge = QueryMerge::From(reached_view);
        if (context.inductive_successors.count(reached_merge)) {
          auto &reached_set = context.merge_sets[reached_merge];
          DisjointSet::Union(&base_set, &reached_set);
        }
      }
    }
  }

  for (auto &[merge, merge_set] : context.merge_sets) {
    merge_set.FindAs<InductionSet>()->merges.push_back(merge);
  }
}

// Building the data model means figuring out which `QueryView`s can share the
// same backing storage. This doesn't mean that all views will be backed by
// such storage, but when we need backing storage, we can maximally share it
// among other places where it might be needed.
static void BuildDataModel(const Query &query, ProgramImpl *program) {
  query.ForEachView([=](QueryView view) {
    auto model = new DataModel;
    program->models.emplace_back(model);
    program->view_to_model.emplace(view, model);
  });

  auto all_cols_match = [] (auto cols, auto pred_cols) {
    const auto num_cols = cols.size();
    if (num_cols != pred_cols.size()) {
      return false;
    }

    for (auto i = 0u; i < num_cols; ++i) {
      if (cols[i].Index() != pred_cols[i].Index()) {
        return false;
      }
    }

    return true;
  };

  // If the view tests any conditions then it can't share a data model
  // with its predecessor.
  //
  // NOTE(pag): Conditions are a tire fire.
  auto is_conditional = [] (QueryView view) {
    return !view.NegativeConditions().empty() ||
           !view.PositiveConditions().empty() ||
           view.IsCompare() || view.IsMap();
  };

  query.ForEachView([=](QueryView view) {
    if (is_conditional(view)) {
      return;
    }

    const auto model = program->view_to_model[view];
    const auto preds = view.Predecessors();

    // UNIONs can share the data of any of their predecessors so long as
    // those predecessors don't themselves have other successors, i.e. they
    // only lead into the UNION.
    if (view.IsMerge()) {
      for (auto pred : preds) {
        if (pred.Successors().size() == 1u) {
          const auto pred_model = program->view_to_model[pred];
          DisjointSet::Union(model, pred_model);
        }
      }

    // If a TUPLE "perfectly" passes through its data, then it shares the
    // same data model as its predecessor.
    } else if (view.IsTuple()) {
      if (preds.size() == 1u) {
        const auto pred = preds[0];
        if (all_cols_match(view.Columns(), pred.Columns())) {
          const auto pred_model = program->view_to_model[pred];
          DisjointSet::Union(model, pred_model);
        }
      }

    // INSERTs have no output columns. If an insert has only a single
    // predecessor (it should), and if all columns of the predecessor are
    // used and in the same order, then this INSERT and its predecessor
    // share the same data model.
    } else if (view.IsInsert()) {
      if (preds.size() == 1u) {
        const auto insert = QueryInsert::From(view);
        const auto cols = insert.InputColumns();
        const auto pred_cols = preds[0].Columns();
        if (all_cols_match(cols, pred_cols)) {
          const auto pred_model = program->view_to_model[preds[0]];
          DisjointSet::Union(model, pred_model);
        }
      }

    // Select predecessors are INSERTs, which don't have output columns.
    // In theory, there could be more than one INSERT. Selects always share
    // the data model with their corresponding INSERTs.
    //
    // TODO(pag): This more about the interplay with conditional inserts.
    } else if (view.IsSelect()) {
      for (auto pred : preds) {
        if (pred.IsInsert()) {
          const auto pred_model = program->view_to_model[pred];
          DisjointSet::Union(model, pred_model);
        }
      }
    }
  });
}

}  // namespace

// Build an eager region. This guards the execution of the region in
// conditionals if the view itself is conditional.
void BuildEagerRegion(ProgramImpl *impl, QueryView pred_view,
                      QueryView view, Context &usage, OP *parent,
                      TABLE *last_model) {
  const auto pos_conds = view.PositiveConditions();
  const auto neg_conds = view.NegativeConditions();

  // Innermost test for negative conditions.
  if (!neg_conds.empty()) {
    auto test = impl->operation_regions.CreateDerived<EXISTS>(
        parent, ProgramOperation::kTestAllZero);

    for (auto cond : neg_conds) {
      test->cond_vars.AddUse(ConditionVariable(impl, cond));
    }

    UseRef<REGION>(parent, test).Swap(parent->body);
    parent = test;
    last_model = nullptr;
  }

  // Outermost test for positive conditions.
  if (!pos_conds.empty()) {
    auto test = impl->operation_regions.CreateDerived<EXISTS>(
        parent, ProgramOperation::kTestAllNonZero);

    for (auto cond : pos_conds) {
      test->cond_vars.AddUse(ConditionVariable(impl, cond));
    }

    UseRef<REGION>(parent, test).Swap(parent->body);
    parent = test;
    last_model = nullptr;
  }

  BuildUnconditionalEagerRegion(
      impl, pred_view, view, usage, parent, last_model);
}

WorkItem::~WorkItem(void) {}

// Build a program from a query.
std::optional<Program> Program::Build(const Query &query, const ErrorLog &) {
  auto impl = std::make_shared<ProgramImpl>(query);
  const auto program = impl.get();

  BuildDataModel(query, program);

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

  // Create constant variables.
  for (auto const_val : query.Constants()) {
    auto var = impl->const_vars.Create(
        impl->next_id++,
        VariableRole::kConstant);
    var->query_const = const_val;
    impl->const_to_var.emplace(const_val, var);
  }

  // Go figure out which merges are inductive, and then classify their
  // predecessors and successors in terms of which ones are inductive and
  // which aren't.
  DiscoverInductions(query, context);

  for (auto io : query.IOs()) {
    BuildEagerProcedure(program, io, context);
  }

  impl->Optimize();

  // Assign defining regions to each variable.
  for (auto proc : impl->procedure_regions) {
    MapVariables(proc->body.get());
  }

  return Program(std::move(impl));
}

}  // namespace hyde
