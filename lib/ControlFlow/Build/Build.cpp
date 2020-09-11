// Copyright 2020, Trail of Bits. All rights reserved.

#include "Build.h"

// IDEAS
//
//    - TODO: Find all constants used in the data flow and assign them to
//            variables up-front in the function.
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
                                          Context &context, OP *parent) {
  if (view.IsJoin()) {
    const auto join = QueryJoin::From(view);
    if (join.NumPivotColumns()) {
      BuildEagerJoinRegion(impl, pred_view, join, context, parent);
    } else {
      BuildEagerProductRegion(impl, pred_view, join, context, parent);
    }
  } else if (view.IsMerge()) {
    BuildEagerMergeRegion(
        impl, pred_view, QueryMerge::From(view), context, parent);

  } else if (view.IsAggregate()) {

  } else if (view.IsKVIndex()) {

  } else if (view.IsMap()) {

  } else if (view.IsCompare()) {

  } else if (view.IsSelect() || view.IsTuple()) {
    BuildEagerSuccessorRegions(impl, view, context, parent, view.Successors());

  } else if (view.IsInsert()) {
    BuildEagerInsertRegion(impl, pred_view, QueryInsert::From(view),
                                  context, parent);

  } else {
    assert(false);
  }
}

// Returns a global reference count variable associated with a query condition.
static VAR *ConditionVariable(ProgramImpl *impl, QueryCondition cond) {
  auto &cond_var = impl->cond_ref_counts[cond];
  if (!cond_var) {
    cond_var = impl->global_vars.Create(
        --impl->next_global_var_id,
         VariableRole::kGlobalBoolean);
  }
  return cond_var;
}

// Build an eager region. This guards the execution of the region in
// conditionals if the view itself is conditional.
static void BuildEagerRegion(ProgramImpl *impl, QueryView pred_view,
                             QueryView view, Context &usage, OP *parent) {
  const auto pos_conds = view.PositiveConditions();
  const auto neg_conds = view.NegativeConditions();

  // Innermost test for negative conditions.
  if (!neg_conds.empty()) {
    auto test = impl->operation_regions.Create(
        parent, ProgramOperation::kTestAllZero);

    for (auto cond : neg_conds) {
      test->variables.AddUse(ConditionVariable(impl, cond));
    }

    UseRef<REGION>(parent, test).Swap(parent->body);
    parent = test;
  }

  // Outermost test for positive conditions.
  if (!pos_conds.empty()) {
    auto test = impl->operation_regions.Create(
        parent, ProgramOperation::kTestAllNonZero);

    for (auto cond : pos_conds) {
      test->variables.AddUse(ConditionVariable(impl, cond));
    }

    UseRef<REGION>(parent, test).Swap(parent->body);
    parent = test;
  }

  BuildUnconditionalEagerRegion(
      impl, pred_view, view, usage, parent);
}

}  // namespace

void WorkItem::Anchor(void) {}

// Create a procedure for a view.
static void BuildEagerProcedure(ProgramImpl *impl, QueryView view,
                                Context &context) {

  const auto proc = impl->procedure_regions.Create(view, impl);
  impl->procedures.emplace(view, proc);

  OP * const loop = impl->operation_regions.Create(
      proc, ProgramOperation::kLoopOverInputVector);
  for (auto col : view.Columns()) {
    loop->variables.AddUse(proc->VariableFor(col));
  }

  // They should all be unique anyway.
  loop->variables.Unique();
  assert(loop->variables.Size() == view.Columns().size());

  UseRef<REGION>(proc, loop).Swap(proc->body);

  BuildEagerSuccessorRegions(impl, view, context, loop, view.Successors());
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
      auto succs_of_succ = TransitiveSuccessorsOf(succ_view);
      if (succs_of_succ.count(view)) {
        context.inductive_successors[view].push_back(succ_view);
      } else {
        context.noninductive_successors[view].push_back(succ_view);
      }
    }

    for (auto pred_view : QueryView(view).Predecessors()) {
      auto preds_of_pred = TransitivePredecessorsOf(pred_view);
      if (preds_of_pred.count(view)) {
        context.inductive_predecessors[view].insert(pred_view);
      } else {
        context.noninductive_predecessors[view].insert(pred_view);
      }
    }
  }

  // Now group together the merges into co-inductive sets, i.e. when one
  // induction is tied with another induction.
  for (const auto &[view, inductive_successors] : context.inductive_successors) {
    DisjointSet &base_set = context.merge_sets[view];
    for (QueryView succ_view : inductive_successors) {
      for (auto reached_view : TransitiveSuccessorsOf(succ_view)) {
        if (reached_view.IsMerge()) {
          QueryMerge reached_merge = QueryMerge::From(reached_view);
          if (context.inductive_successors.count(reached_merge)) {
            auto &reached_set = context.merge_sets[reached_merge];
            DisjointSet::Union(&base_set, &reached_set);
          }
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
void BuildDataModel(const Query &query, ProgramImpl *program) {
  query.ForEachView([=](QueryView view) {
    auto model = new DataModel;
    program->models.emplace_back(model);
    program->view_to_model.emplace(view, model);
  });

  query.ForEachView([=](QueryView view) {
    const auto model = program->view_to_model[view];
    if (auto preds = view.Predecessors(); preds.size() == 1) {
      const auto pred_model = program->view_to_model[preds[0]];
      if (view.IsCompare() || view.IsTuple() || view.IsInsert() ||
          view.IsSelect()) {
        DisjointSet::Union(model, pred_model);

      // This is really the only interesting case, and is the motivator for
      // including `range` specifiers on functor declarations in the language.
      // If a functor does not amplify the number of tuples, i.e. filters some
      // out, or passes them through, perhaps adding in additional data, then
      // we can have the `MapView` share the same backing storage as its only
      // predecessor view.
      } else if (view.IsMap()) {
        const auto range = QueryMap::From(view).Functor().Range();
        if (FunctorRange::kZeroOrOne == range ||
            FunctorRange::kOneToOne == range) {
          DisjointSet::Union(model, pred_model);
        }
      }
    }
  });
}

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

  // Go figure out which merges are inductive, and then classify their
  // predecessors and successors in terms of which ones are inductive and
  // which aren't.
  DiscoverInductions(query, context);

  // Work list of tasks to process.
  using WorkItemPtr = std::unique_ptr<WorkItem>;
  std::vector<WorkItemPtr> prev_work_list;

  for (auto io : query.IOs()) {
    for (auto receive : io.Receives()) {
      context.view_to_induction.clear();
      context.product_guard_var.clear();
      context.product_vector.clear();
      context.work_list.clear();
      context.view_to_work_item.clear();
      (void) BuildEagerProcedure(program, receive, context);

      while (!context.work_list.empty()) {
        prev_work_list.swap(context.work_list);
        context.view_to_work_item.clear();
        std::stable_sort(prev_work_list.begin(), prev_work_list.end(),
                         [](const WorkItemPtr &a, const WorkItemPtr &b) {
                           return a->view.Depth() < b->view.Depth();
                         });
        for (const auto &item : prev_work_list) {
          item->Run(program, context);
        }
        prev_work_list.clear();
      }
    }
  }

  return Program(std::move(impl));
}

}  // namespace hyde
