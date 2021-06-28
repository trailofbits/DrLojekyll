// Copyright 2020, Trail of Bits. All rights reserved.

#include "Build.h"

#include <drlojekyll/Parse/ErrorLog.h>

#include <algorithm>
#include <sstream>

namespace hyde {
namespace {

// Figure out what data definitely must be stored persistently. We need to do
// this ahead-of-time, as opposed to just-in-time, because otherwise we run
// into situations where a node N will have two successors S1 and S2, e.g.
// two identically-shaped TUPLEs, and those tuples will feed into JOINs.
// Those tuples will both need persistent storage (because they feed JOINs),
// but we'll only observe this when we are generating code for the JOINs, and
// thus when we generate the state changes for the join, we'll generate two
// identical state changes, where one will make the other one unsatisfiable.
// For example:
//
//      join-tables
//        vector-loop ...
//        select ...
//        select ...
//          par
//            if-transition-state {@A:29} in %table:43[...] from ...
//              ...
//            if-transition-state {@A:29} in %table:43[...] from ...
//              ...
static void FillDataModel(const Query &query, ProgramImpl *impl,
                          Context &context) {

  query.ForEachView([&](QueryView view) {
    if (view.CanReceiveDeletions()) {
      for (auto pred : view.Predecessors()) {
        if (pred.CanProduceDeletions()) {
          if (view.InductionGroupId().has_value()) {
            (void) TABLE::GetOrCreate(impl, context, pred);
          }
        } else {
          (void) TABLE::GetOrCreate(impl, context, pred);
        }
      }
      for (auto pred : view.NonInductivePredecessors()) {
        (void) TABLE::GetOrCreate(impl, context, pred);
      }
    }

    if (view.SetCondition() || !view.PositiveConditions().empty() ||
        !view.NegativeConditions().empty()) {
      (void) TABLE::GetOrCreate(impl, context, view);
    }
  });

  for (auto view : query.Selects()) {
    (void) TABLE::GetOrCreate(impl, context, view);
  }

  for (auto view : query.Inserts()) {
    auto insert = QueryInsert::From(view);
    if (insert.IsRelation()) {
      (void) TABLE::GetOrCreate(impl, context, view);
    }
  }

  for (auto view : query.Merges()) {
    if (NeedsInductionCycleVector(view) || NeedsInductionOutputVector(view)) {
      (void) TABLE::GetOrCreate(impl, context, view);
    }
  }

  for (auto join : query.Joins()) {

    QueryView view(join);
    if (!view.CanReceiveDeletions()) {
      auto num_constant = 0u;
      auto num_variable = 0u;
      for (auto pred : join.JoinedViews()) {
        if (pred.IsConstantAfterInitialization()) {
          (void) TABLE::GetOrCreate(impl, context, pred);
          ++num_constant;
        } else {
          ++num_variable;
        }
      }
      if (num_constant && 1u == num_variable) {
        // TODO(pag): Issue #240.
      }
    }

    for (auto pred : join.JoinedViews()) {
      (void) TABLE::GetOrCreate(impl, context, pred);
    }

    // A top-down checker looking at a join can hit terrible performance issues
    // if they need to inspect the JOIN's outputs and if the pivot columns
    // aren't used.
    if (view.CanReceiveDeletions()) {

      //      // Easier to just avoid any possible performance issues; storage is
      //      // cheap... right? :-P
      //      (void) TABLE::GetOrCreate(impl, context, view);

      auto num_pivots = join.NumPivotColumns();
      for (auto succ_view : view.Successors()) {
        std::vector<bool> used_pivots(num_pivots);
        auto num_used_pivots = 0u;
        succ_view.ForEachUse([&](QueryColumn in_col, InputColumnRole role,
                                 std::optional<QueryColumn> out_col) {
          if (!in_col.IsConstant() && QueryView::Containing(in_col) == view) {
            if (auto index = *(in_col.Index());
                index < num_pivots && !used_pivots[index]) {
              used_pivots[index] = true;
              ++num_used_pivots;
            }
          }
        });
        if (num_used_pivots < num_pivots) {
          (void) TABLE::GetOrCreate(impl, context, view);
          break;
        }
      }
    }
  }

  for (auto negate : query.Negations()) {
    const QueryView view(negate);
    if (!view.CanReceiveDeletions()) {
      if (negate.NegatedView().IsConstantAfterInitialization()) {
        // TODO(pag): Issue #242.
      }
    }
    (void) TABLE::GetOrCreate(impl, context, negate.NegatedView());
    (void) TABLE::GetOrCreate(impl, context, view.Predecessors()[0]);
  }

  for (auto map : query.Maps()) {
    QueryView view(map);
    if (view.CanProduceDeletions()) {
      (void) TABLE::GetOrCreate(impl, context, view);
    }
  }

  for (auto map : query.Compares()) {
    QueryView view(map);
    if (view.CanProduceDeletions()) {
      (void) TABLE::GetOrCreate(impl, context, view);
    }
  }
}

// Building the data model means figuring out which `QueryView`s can share the
// same backing storage. This doesn't mean that all views will be backed by
// such storage, but when we need backing storage, we can maximally share it
// among other places where it might be needed.
static void BuildDataModel(const Query &query, ProgramImpl *program) {
  std::unordered_map<unsigned, DataModel *> eq_classes;

  query.ForEachView([&](QueryView view) {
    auto model = new DataModel;
    program->models.emplace_back(model);
    program->view_to_model.emplace(view, model);
    eq_classes.emplace(view.EquivalenceSetId(), model);
  });

  query.ForEachView([&](QueryView view) {
    auto curr_model = program->view_to_model[view]->FindAs<DataModel>();
    auto dest_model = eq_classes[view.EquivalenceSetId()];
    DisjointSet::Union(curr_model, dest_model);
  });
}

// Build out all the bottom-up (negative) provers that are used to mark tuples
// as being in an unknown state. We want to do this after building all
// (positive) bottom-up provers so that we can know which views correspond
// with which tables.
static bool BuildBottomUpRemovalProvers(ProgramImpl *impl, Context &context) {
  auto changed = false;
  while (!context.bottom_up_removers_work_list.empty()) {
    auto [from_view, to_view, proc, already_checked] =
        context.bottom_up_removers_work_list.back();
    context.bottom_up_removers_work_list.pop_back();

    changed = true;

    assert(context.work_list.empty());
    context.work_list.clear();

    auto let = impl->operation_regions.CreateDerived<LET>(proc);
    proc->body.Emplace(proc, let);

    if (to_view.IsTuple()) {
      CreateBottomUpTupleRemover(impl, context, to_view, let, already_checked);

    } else if (to_view.IsCompare()) {
      CreateBottomUpCompareRemover(impl, context, to_view, let,
                                   already_checked);

    } else if (to_view.IsInsert()) {
      CreateBottomUpInsertRemover(impl, context, to_view, let, already_checked);

    } else if (to_view.IsMerge()) {
      if (to_view.InductionGroupId().has_value()) {
        CreateBottomUpInductionRemover(impl, context, to_view, let,
                                       already_checked);
      } else {
        CreateBottomUpUnionRemover(impl, context, to_view, let,
                                   already_checked);
      }

    } else if (to_view.IsJoin()) {
      auto join = QueryJoin::From(to_view);
      if (join.NumPivotColumns()) {
        CreateBottomUpJoinRemover(impl, context, from_view, join, let,
                                  already_checked);
      } else {
        assert(false && "TODO: Cross-products!");
      }
    } else if (to_view.IsAggregate()) {
      assert(false && "TODO Aggregates!");

    } else if (to_view.IsKVIndex()) {
      assert(false && "TODO Key Values!");

    } else if (to_view.IsMap()) {
      auto map = QueryMap::From(to_view);
      auto functor = map.Functor();
      if (functor.IsPure()) {
        CreateBottomUpGenerateRemover(impl, context, map, functor, let,
                                      already_checked);

      } else {
        assert(false && "TODO Impure Functors!");
      }

    } else if (to_view.IsNegate()) {
      CreateBottomUpNegationRemover(impl, context, to_view, let,
                                    already_checked);

    // NOTE(pag): This shouldn't be reachable, as the bottom-up INSERT
    //            removers jump past SELECTs.
    } else if (to_view.IsSelect()) {
      assert(false);

    } else {
      assert(false);
    }

    CompleteProcedure(impl, proc, context);
  }

  return changed;
}


// Will return `nullptr` if no conditional tests need to be done.
static CALL *InCondionalTests(ProgramImpl *impl, QueryView view,
                              Context &context, REGION *parent) {

  const auto pos_conds = view.PositiveConditions();
  const auto neg_conds = view.NegativeConditions();

  if (pos_conds.empty() && neg_conds.empty()) {
    return nullptr;
  }

  // Letting this view be used by a negation would complicate things when
  // doing the return-true path in top-down checking.
  assert(!view.IsUsedByNegation());

  auto &checker_proc = context.cond_checker_procs[view];
  if (!checker_proc) {
    checker_proc = impl->procedure_regions.Create(
        impl->next_id++, ProcedureKind::kConditionTester);

    REGION *parent = checker_proc;
    auto parent_body = &(checker_proc->body);

    // Outermost test for negative conditions.
    if (!neg_conds.empty()) {
      TUPLECMP *const test = impl->operation_regions.CreateDerived<TUPLECMP>(
          parent, ComparisonOperator::kEqual);

      for (auto cond : neg_conds) {
        test->lhs_vars.AddUse(ConditionVariable(impl, cond));
        test->rhs_vars.AddUse(impl->zero);
      }

      test->false_body.Emplace(test,
                               BuildStateCheckCaseReturnFalse(impl, test));

      parent_body->Emplace(parent, test);
      parent_body = &(test->body);
      parent = test;
    }

    // Innermost test for positive conditions.
    if (!pos_conds.empty()) {
      TUPLECMP *const test = impl->operation_regions.CreateDerived<TUPLECMP>(
          parent, ComparisonOperator::kEqual);

      for (auto cond : pos_conds) {
        test->lhs_vars.AddUse(ConditionVariable(impl, cond));
        test->rhs_vars.AddUse(impl->zero);
      }

      test->body.Emplace(test, BuildStateCheckCaseReturnFalse(impl, test));

      parent_body->Emplace(parent, test);
      parent_body = &(test->false_body);
      parent = test;
    }

    // If we made it down here, then return true.
    parent_body->Emplace(parent, BuildStateCheckCaseReturnTrue(impl, parent));
  }

  CALL *const call = impl->operation_regions.CreateDerived<CALL>(
      impl->next_id++, parent, checker_proc);

  return call;
}

// Starting from a negated view, work up to the negation, then down to the
// source view for the negation, doing a scan over the source view columns.
// Invoke `cb` within the context of that scan.
template <typename CB>
static REGION *PivotAroundNegation(ProgramImpl *impl, Context &context,
                                   QueryView view, QueryNegate negate,
                                   REGION *parent, CB cb_present) {
  SERIES *const seq = impl->series_regions.Create(parent);

  // Map from negated view columns to the negate.
  negate.ForEachUse([&](QueryColumn in_col, InputColumnRole role,
                        std::optional<QueryColumn> out_col) {
    if (role == InputColumnRole::kNegated) {
      assert(out_col.has_value());
      assert(QueryView::Containing(in_col) == view);
      VAR *const in_var = seq->VariableFor(impl, in_col);
      assert(in_var != nullptr);
      assert(out_col->Type() == in_var->Type());
      seq->col_id_to_var[out_col->Id()] = in_var;
    }
  });

  std::vector<QueryColumn> source_view_cols;
  const QueryView source_view = QueryView(negate).Predecessors()[0];

  // Now map from negate to source view columns.
  negate.ForEachUse([&](QueryColumn in_col, InputColumnRole role,
                        std::optional<QueryColumn> out_col) {
    if (role == InputColumnRole::kCopied && out_col.has_value() &&
        QueryView::Containing(in_col) == source_view) {
      if (auto out_var_it = seq->col_id_to_var.find(out_col->Id());
          out_var_it != seq->col_id_to_var.end()) {
        VAR *const out_var = out_var_it->second;
        assert(in_col.Type() == out_var->Type());
        seq->col_id_to_var[in_col.Id()] = out_var;
        source_view_cols.push_back(in_col);
      }
    }
  });

  DataModel *const source_view_model =
      impl->view_to_model[source_view]->FindAs<DataModel>();
  TABLE *const source_view_table = source_view_model->table;
  assert(source_view_table != nullptr);

  // We know we have a source view data model and table, so do a scan over the
  // source view.
  BuildMaybeScanPartial(
      impl, source_view, source_view_cols, source_view_table, seq,
      [&](REGION *in_scan, bool in_loop) -> REGION * {
        // Make sure to make the variables for the negation's output columns
        // available to our recursive call.
        negate.ForEachUse([&](QueryColumn in_col, InputColumnRole role,
                              std::optional<QueryColumn> out_col) {
          if (out_col && InputColumnRole::kCopied == role) {
            VAR *const in_var = in_scan->VariableFor(impl, in_col);
            in_scan->col_id_to_var[out_col->Id()] = in_var;
          }
        });

        // Recursively call the checker for the source view of the negation.
        // If the data is available in the source view, then we'll do something.
        const auto [rec_check, rec_check_call] =
            CallTopDownChecker(impl, context, in_scan, source_view,
                               source_view_cols, source_view, nullptr);

        cb_present(rec_check_call);

        return rec_check;
      });

  return seq;
}

static REGION *MaybeRemoveFromNegatedView(ProgramImpl *impl, Context &context,
                                          QueryView view, QueryNegate negate,
                                          REGION *parent) {

  // We know we have a table for the predecessor of the negation, and we may or
  // may not have a table for the negation. Even with a table for the negation,
  // that table might not use all of the predecessor's columns, so we're best
  // off just scanning the negation's predecessor.
  const QueryView source_view = QueryView(negate).Predecessors()[0];
  DataModel *const source_view_model =
      impl->view_to_model[source_view]->FindAs<DataModel>();
  TABLE *const source_view_table = source_view_model->table;
  assert(source_view_table != nullptr);

  return PivotAroundNegation(
      impl, context, view, negate, parent, [&](OP *if_present_in_source) {
        BuildEagerRemovalRegion(impl, source_view, negate, context,
                                if_present_in_source, source_view_table);
      });
}

static REGION *MaybeReAddToNegatedView(ProgramImpl *impl, Context &context,
                                       QueryView view, QueryNegate negate,
                                       REGION *parent) {

  const QueryView source_view = QueryView(negate).Predecessors()[0];
  DataModel *const source_view_model =
      impl->view_to_model[source_view]->FindAs<DataModel>();
  TABLE *const source_view_table = source_view_model->table;
  assert(source_view_table != nullptr);

  // If we don't have a table for the negation, then we need to pivot from the
  // negated view, up to the negation, and down to the source view of the
  // negation.
  return PivotAroundNegation(
      impl, context, view, negate, parent, [&](OP *if_present_in_source) {
        BuildEagerRegion(impl, source_view, negate, context,
                         if_present_in_source, source_view_table);
      });
}

static void MaybeRemoveFromNegatedView(ProgramImpl *impl, Context &context,
                                       QueryView view, PARALLEL *par) {
  view.ForEachNegation([&](QueryNegate negate) {
    if (!negate.HasNeverHint()) {
      par->AddRegion(
          MaybeRemoveFromNegatedView(impl, context, view, negate, par));
    }
  });
}

static void MaybeReAddToNegatedView(ProgramImpl *impl, Context &context,
                                       QueryView view, PARALLEL *par) {
  view.ForEachNegation([&](QueryNegate negate) {
    assert(!negate.HasNeverHint());
    par->AddRegion(MaybeReAddToNegatedView(impl, context, view, negate, par));
  });
}

static void BuildTopDownChecker(ProgramImpl *impl, Context &context,
                                QueryView view,
                                std::vector<QueryColumn> &view_cols, PROC *proc,
                                TABLE *already_checked) {

  // If this view is conditional, then the /last/ thing that we do, i.e. before
  // returning `true`, is check if the conditions are actually satisfied.
  auto ret_true = [&](ProgramImpl *, REGION *parent) -> REGION * {
    CALL *const call = InCondionalTests(impl, view, context, parent);
    if (!call) {
      return BuildStateCheckCaseReturnTrue(impl, parent);
    }
    call->body.Emplace(call, BuildStateCheckCaseReturnTrue(impl, call));
    call->false_body.Emplace(call, BuildStateCheckCaseReturnFalse(impl, call));
    return call;
  };

  // If we have a table, and if we don't have all of the columns, then go
  // and get all of the columns via a recursive check and a scan over the
  // backing store. The fall-through at the end is to return false if none
  // of the recursive calls returns true.
  const auto model = impl->view_to_model[view]->FindAs<DataModel>();
  if (const auto table = model->table; table) {

    // If we have a table, and our caller has a table, and they don't match
    // then clear out `already_checked`, as it's unrelated to our table.
    if (already_checked != table) {
      already_checked = nullptr;
    }

    // Recursively call the top-down checker with all columns available.
    //
    // Key dependencies: `already_checked`, `view`, `view_cols`.
    auto call_self = [&](REGION *parent) -> CALL * {
      const auto available_cols = ComputeAvailableColumns(view, view_cols);
      const auto checker_proc = GetOrCreateTopDownChecker(
          impl, context, view, available_cols, already_checked);

      const auto check = impl->operation_regions.CreateDerived<CALL>(
          impl->next_id++, parent, checker_proc);

      // Pass in the arguments.
      auto i = 0u;
      for (auto [col, avail_col] : available_cols) {
        const auto var = parent->VariableFor(impl, avail_col);
        assert(var != nullptr);
        check->arg_vars.AddUse(var);

        const auto param = checker_proc->input_vars[i++];
        assert(var->Type() == param->Type());
        (void) param;
      }

      return check;
    };

    SERIES *seq = impl->series_regions.Create(proc);

    // Try to build a scan. If we can't then return `nullptr`, and `did_scan`
    // will be `false`. If we do build a scan, then do a recursive call in the
    // scan, where we'll have all columns available. If the recursive call
    // returns `true` then it means we've proven the tuple exists, and we can
    // return true.
    const auto did_scan = BuildMaybeScanPartial(
        impl, view, view_cols, model->table, seq,
        [&](REGION *parent, bool in_loop) -> REGION * {
          if (!in_loop) {
            return nullptr;
          }

          // If `already_checked` was non-null, then all of `view_cols` should
          // have been provided, thus not requiring a partial scan.
          assert(!already_checked);

          const auto check = call_self(parent);

          // If the call succeeds, then `return-true`. If it fails, then do
          // nothing, i.e. continue in our partial scan to the next tuple
          // from our index.
          check->body.Emplace(check, ret_true(impl, check));

          return check;
        });

    // If we did a scan, and if execution falls through post-scan, then it means
    // that none of the recursive calls to finders succeeded, so we must return
    // false because we failed to prove the tuple.
    if (did_scan) {
      assert(!already_checked);

      proc->body.Emplace(proc, seq);
      seq->AddRegion(BuildStateCheckCaseReturnFalse(impl, seq));
      return;

    // If this view can't produce deletions, and if we have a table for it, then
    // all we need to do is check the state.
    } else if (!view.CanProduceDeletions()) {
      assert(view.PositiveConditions().empty());
      assert(view.NegativeConditions().empty());

      seq->parent = nullptr;

      // If our caller did a check and we got to here, and we can't produce
      // deletions, then this is really weird, but also just means the data
      // wasn't found and all we can do is return false.
      if (already_checked == model->table) {
        proc->body.Emplace(proc, BuildStateCheckCaseReturnFalse(impl, proc));

      // Our caller didn't do a check, so we'll do it, and we'll just return
      // whatever the check tells us. In practice, the `if-unknown` case should
      // never execute.
      //
      // TODO(pag): Would be good to have an "abort" block.
      } else {
        proc->body.Emplace(proc, BuildTopDownCheckerStateCheck(
                                     impl, proc, model->table, view_cols,
                                     ret_true, BuildStateCheckCaseReturnFalse,
                                     BuildStateCheckCaseReturnFalse));
      }
      return;

    // This node can produce differential updates, it has a model, and we
    // haven't yet checked the state of the tuple. We're down here so we know
    // the tuple once existed, because we found it in an index. We haven't
    // actually checked the tuple's state, though, so now we'll check it, and
    // if it's unknown then we'll recursively call ourselves and try to prove
    // it in its own absence.
    } else if (already_checked == nullptr) {

      seq->parent = nullptr;

      // It's possible that we'll need to re-try this (or a nearly similar)
      // call due to a race condition. In that event, we'll make a tail-call.
      SERIES *retry_seq = impl->series_regions.Create(proc);
      proc->body.Emplace(proc, retry_seq);

      // These will be executed if the recursive call returns `true` or
      // `false`, respectively.
      SERIES *true_seq = nullptr;
      SERIES *false_seq = nullptr;

      retry_seq->AddRegion(BuildTopDownCheckerStateCheck(
          impl, retry_seq, model->table, view.Columns(), ret_true,
          BuildStateCheckCaseReturnFalse,
          [&](ProgramImpl *, REGION *parent) -> REGION * {
            // Change the tuple's state to mark it as absent so that we can't
            // use it as its own base case.
            const auto table_remove =
                BuildChangeState(impl, table, parent, view_cols,
                                 TupleState::kUnknown, TupleState::kAbsent);

            already_checked = model->table;
            const auto recursive_call_if_changed = call_self(table_remove);
            table_remove->body.Emplace(table_remove, recursive_call_if_changed);

            // If we're proven the tuple, then try to mark it as present.
            const auto table_add = BuildChangeState(
                impl, table, recursive_call_if_changed, view_cols,
                TupleState::kAbsent, TupleState::kPresent);

            recursive_call_if_changed->body.Emplace(recursive_call_if_changed,
                                                    table_add);

            // If updating the state succeeded, then we'll go down the `true`
            // path.
            true_seq = impl->series_regions.Create(table_add);
            table_add->body.Emplace(table_add, true_seq);

            // If the recursive call failed, and we were the ones to change
            // the tuple's state to absent, then we'll go down the `false`
            // path.
            false_seq = impl->series_regions.Create(recursive_call_if_changed);
            recursive_call_if_changed->false_body.Emplace(
                recursive_call_if_changed, false_seq);

            return table_remove;
          }));

      // TODO(pag): If `view` is used by a negation, then we should have seen
      //            the right things happen in the bottom-up insertion/removal
      //            paths.

      //      // If this view is used by a negation, then on the true or false paths
      //      // we may need to adjust things.
      //      if (view.IsUsedByNegation()) {
      //        true_seq->AddRegion(
      //            MaybeRemoveFromNegatedView(impl, context, view, true_seq));
      //        false_seq->AddRegion(
      //            MaybeReAddToNegatedView(impl, context, view, false_seq));
      //      }

      // Make sure that we `return-true` and `return-false` to our callers.
      true_seq->AddRegion(ret_true(impl, true_seq));
      false_seq->AddRegion(BuildStateCheckCaseReturnFalse(impl, false_seq));

      // If we fall through to the end, then a race condition has occurred
      // during one of the above state transitions. To recover, call ourselves
      // recursively and return the result.
      //
      // NOTE(pag): We set `already_checked = nullptr;`, which is visible to
      //            `call_self`.
      //
      // NOTE(pag): The recursive call will end doing the condition test, so
      //            we don't double-check that here (via `ret_true`).
      already_checked = nullptr;
      const auto recursive_call_if_race = call_self(retry_seq);
      retry_seq->AddRegion(recursive_call_if_race);

      recursive_call_if_race->body.Emplace(
          recursive_call_if_race,
          BuildStateCheckCaseReturnTrue(impl, recursive_call_if_race));

      recursive_call_if_race->false_body.Emplace(
          recursive_call_if_race,
          BuildStateCheckCaseReturnTrue(impl, recursive_call_if_race));

      return;

    // Mark the series as "dead". Fall through to the "actual" child calls.
    } else {
      seq->parent = nullptr;
    }

  // No table associated with this view.
  } else {
    assert(view.PositiveConditions().empty());
    assert(view.NegativeConditions().empty());
    assert(!view.IsUsedByNegation());
    assert(!view.IsUsedByJoin());
    already_checked = nullptr;

    // TODO(pag): Consider returning false?
  }

  // If we're down here, then it means one of the following:
  //
  //    1)  `view` doesn't have a table, and so we didn't do a scan.
  //    2)  `view_cols` is "complete" for `view`, and so we didn't do a scan.
  //    3)  we've been recursively called in the context of a scan.

  REGION *child = nullptr;
  REGION *parent = proc;
  UseRef<REGION> *parent_body = &(proc->body);

  // Before proceeding, we must ensure that if any constants flowed up through
  // this node, then on our way down, we check that the columns we have match
  // the constants that flowed up.

  std::vector<std::pair<QueryColumn, QueryColumn>> constants_to_check;
  view.ForEachUse([&](QueryColumn in_col, InputColumnRole role,
                      std::optional<QueryColumn> out_col) {
    if (out_col && in_col.IsConstantOrConstantRef() &&
        std::find(view_cols.begin(), view_cols.end(), *out_col) !=
            view_cols.end()) {

      // We don't need to compare against this constant because the tuple is
      // "inventing" it, i.e. appending it in.
      //
      // TODO(pag): Check that this isn't an all-constant tuple?? Think about
      //            this if we hit the below assertion.
      if (in_col.IsConstant() && view.IsTuple()) {
        assert(0 < view.Predecessors().size());
        return;
      }

      switch (role) {
        case InputColumnRole::kIndexValue:
        case InputColumnRole::kAggregatedColumn:
        case InputColumnRole::kMergedColumn: return;
        default: constants_to_check.emplace_back(*out_col, in_col); break;
      }
    }
  });

  // We need to compare some of the arguments against constants. This may
  // be tricky because some of the argument columns may be marked as
  // constant refs, so we can't trust `VariableFor`; we need to find the
  // arguments "by index."
  if (!constants_to_check.empty()) {
    const auto cmp = impl->operation_regions.CreateDerived<TUPLECMP>(
        parent, ComparisonOperator::kEqual);
    parent_body->Emplace(parent, cmp);

    for (auto [out_col, in_col] : constants_to_check) {
      assert(in_col.IsConstantOrConstantRef());
      cmp->lhs_vars.AddUse(proc->input_vars[*(out_col.Index())]);
      cmp->rhs_vars.AddUse(proc->VariableFor(impl, in_col));
    }

    // NOTE(pag): We *don't* do conditional constant propagation (via injecting
    //            stuff into `col_id_to_var`) here because `view` might be a
    //            comparison, and so there could be repeats of `out_col` in
    //            `constants_to_check` that would screw up our checks going
    //            down.

    // If the comparison failed then return false.
    cmp->false_body.Emplace(cmp, BuildStateCheckCaseReturnFalse(impl, cmp));

    // Everything else will nest inside of the comparison.
    parent = cmp;
    parent_body = &(cmp->body);
  }

  // Alright, now it's finally time to call the view-specific checkers.

  // If we have a table, then being down here implies we've already checked
  // the state and transitioned it to absent. Or, we don't have a table.
  assert(!model->table || already_checked);

  if (view.IsJoin()) {
    const auto join = QueryJoin::From(view);
    if (join.NumPivotColumns()) {
      child = BuildTopDownJoinChecker(impl, context, parent, join, view_cols,
                                      already_checked);
    } else {
      assert(false && "TODO: Checker for cross-product.");
    }

  } else if (view.IsMerge()) {
    const auto merge = QueryMerge::From(view);
    if (view.InductionGroupId().has_value()) {
      child = BuildTopDownInductionChecker(impl, context, parent, merge,
                                           view_cols, already_checked);

    } else {
      child = BuildTopDownUnionChecker(impl, context, parent, merge, view_cols,
                                       already_checked);
    }

  } else if (view.IsAggregate()) {
    assert(false && "TODO: Checker for aggregates.");

  } else if (view.IsKVIndex()) {
    assert(false && "TODO: Checker for k/v indices.");

  } else if (view.IsMap()) {
    const auto map = QueryMap::From(view);
    child = BuildTopDownGeneratorChecker(impl, context, parent, map, view_cols,
                                         already_checked);

  } else if (view.IsCompare()) {
    const auto cmp = QueryCompare::From(view);
    child = BuildTopDownCompareChecker(impl, context, parent, cmp, view_cols,
                                       already_checked);

  } else if (view.IsSelect()) {
    const auto select = QuerySelect::From(view);
    child = BuildTopDownSelectChecker(impl, context, parent, select, view_cols,
                                      already_checked);

  } else if (view.IsTuple()) {
    const auto tuple = QueryTuple::From(view);
    child = BuildTopDownTupleChecker(impl, context, parent, tuple, view_cols,
                                     already_checked);

  // The only way, from the top-down, to reach an INSERT is via a SELECT, but
  // the top-down SELECT checker skips over the INSERTs and jumps into the
  // TUPLEs that precede them.
  } else if (view.IsInsert()) {
    assert(false);

  } else if (view.IsNegate()) {
    const auto negate = QueryNegate::From(view);
    child = BuildTopDownNegationChecker(impl, context, parent, negate,
                                        view_cols, already_checked);

  // Not possible?
  } else {
    assert(false);
  }

  if (child) {
    assert(child->parent == parent);
    parent_body->Emplace(parent, child);
  }

  CompleteProcedure(impl, proc, context);

  // This view is conditional, wrap whatever we had generated in a big
  // if statement.
  const auto pos_conds = view.PositiveConditions();
  const auto neg_conds = view.NegativeConditions();
  const auto proc_body = proc->body.get();

  // Innermost test for negative conditions.
  if (!neg_conds.empty()) {
    auto test = impl->operation_regions.CreateDerived<TUPLECMP>(
        proc, ComparisonOperator::kEqual);

    for (auto cond : neg_conds) {
      test->lhs_vars.AddUse(ConditionVariable(impl, cond));
      test->rhs_vars.AddUse(impl->zero);
    }

    proc->body.Emplace(proc, test);
    proc_body->parent = test;
    test->body.Emplace(test, proc_body);
  }

  // Outermost test for positive conditions.
  if (!pos_conds.empty()) {
    auto test = impl->operation_regions.CreateDerived<TUPLECMP>(
        proc, ComparisonOperator::kEqual);

    for (auto cond : pos_conds) {
      test->lhs_vars.AddUse(ConditionVariable(impl, cond));
      test->rhs_vars.AddUse(impl->zero);
    }

    proc->body.Emplace(proc, test);
    proc_body->parent = test;
    test->false_body.Emplace(test, proc_body);
  }

  if (!EndsWithReturn(proc)) {
    const auto ret = impl->operation_regions.CreateDerived<RETURN>(
        proc, ProgramOperation::kReturnFalseFromProcedure);
    ret->ExecuteAfter(impl, proc);
  }
}

// Build out all the top-down checkers. We want to do this after building all
// bottom-up provers so that we can know which views correspond with which
// tables.
static bool BuildTopDownCheckers(ProgramImpl *impl, Context &context) {
  auto changed = false;

  while (!context.top_down_checker_work_list.empty()) {
    auto [view, view_cols, proc, already_checked] =
        context.top_down_checker_work_list.back();
    context.top_down_checker_work_list.pop_back();
    changed = true;

    assert(context.work_list.empty());
    context.work_list.clear();

    BuildTopDownChecker(impl, context, view, view_cols, proc, already_checked);
  }
  return changed;
}

// Add entry point records for each query of the program.
static void BuildQueryEntryPointImpl(ProgramImpl *impl, Context &context,
                                     ParsedDeclaration decl,
                                     QueryInsert insert) {

  const QueryView view(insert);
  const auto query = ParsedQuery::From(decl);
  const auto model = impl->view_to_model[view]->FindAs<DataModel>();
  assert(model->table != nullptr);

  std::vector<std::pair<QueryColumn, QueryColumn>> available_cols;
  std::vector<unsigned> col_indices;
  for (auto param : decl.Parameters()) {
    if (param.Binding() == ParameterBinding::kBound) {
      col_indices.push_back(param.Index());
    }

    const auto in_col = insert.NthInputColumn(param.Index());
    available_cols.emplace_back(in_col, in_col);
  }

  const DataTable table(model->table);
  std::optional<ProgramProcedure> checker_proc;
  std::optional<ProgramProcedure> forcer_proc;
  std::optional<DataIndex> scanned_index;

  if (!col_indices.empty()) {
    if (const auto index = model->table->GetOrCreateIndex(impl, col_indices)) {
      scanned_index.emplace(DataIndex(index));
    }
  }

  if (view.CanReceiveDeletions()) {
    auto pred_view = view.Predecessors()[0];
    assert(pred_view.IsTuple());

    const auto checker = GetOrCreateTopDownChecker(impl, context, pred_view,
                                                   available_cols, nullptr);
    impl->query_checkers.AddUse(checker);
    checker_proc.emplace(ProgramProcedure(checker));
    checker->has_raw_use = true;
  }

  impl->queries.emplace_back(query, table, scanned_index, checker_proc,
                             forcer_proc);
}


// Add entry point records for each query to the program.
static void BuildQueryEntryPoint(ProgramImpl *impl, Context &context,
                                 ParsedDeclaration decl, QueryInsert insert) {
  std::unordered_set<std::string> seen_variants;

  for (auto redecl : decl.Redeclarations()) {

    // We may have duplicate redeclarations, so don't repeat any.
    std::string binding(redecl.BindingPattern());
    if (seen_variants.count(binding)) {
      continue;
    }
    seen_variants.insert(std::move(binding));
    BuildQueryEntryPointImpl(impl, context, redecl, insert);
  }
}

static bool
CanImplementTopDownChecker(ProgramImpl *impl, QueryView view,
                           const std::vector<QueryColumn> &available_cols) {

  if (view.IsSelect() && QuerySelect::From(view).IsStream()) {
    return true;  // The top-down checker will return false;

  // Join checkers are based off of their predecessors, which are guaranteed
  // to have models.
  } else if (view.IsJoin()) {
    return true;

  } else if (view.IsInsert()) {
    return false;
  }

  // We have a model, so worst case, we can do a full table scan.
  const auto model = impl->view_to_model[view]->FindAs<DataModel>();
  if (model->table) {
    return true;
  }

  // We need some columns.
  return !available_cols.empty();
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
      for (auto var : join->pivot_vars) {
        var->defining_region = region;
      }
      for (const auto &var_list : join->output_vars) {
        for (auto var : var_list) {
          var->defining_region = region;
        }
      }
    } else if (auto product = op->AsTableProduct(); product) {
      for (const auto &var_list : product->output_vars) {
        for (auto var : var_list) {
          var->defining_region = region;
        }
      }
    } else if (auto gen = op->AsGenerate(); gen) {
      for (auto var : gen->defined_vars) {
        var->defining_region = region;
      }

      MapVariables(gen->empty_body.get());

    } else if (auto call = op->AsCall(); call) {
      MapVariables(call->false_body.get());

    } else if (auto scan = op->AsTableScan(); scan) {
      for (auto var : scan->out_vars) {
        var->defining_region = region;
      }
    } else if (auto update = op->AsTransitionState(); update) {
      MapVariables(update->failed_body.get());

    } else if (auto check = op->AsCheckState(); check) {
      MapVariables(check->absent_body.get());
      MapVariables(check->unknown_body.get());

    } else if (auto cmp = op->AsTupleCompare(); cmp) {
      MapVariables(cmp->false_body.get());
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
  } else if (auto proc = region->AsProcedure(); proc) {
    for (auto var : proc->input_vars) {
      var->defining_region = proc;
    }
    MapVariables(proc->body.get());
  }
}

}  // namespace

// Returns a global reference count variable associated with a query condition.
VAR *ConditionVariable(ProgramImpl *impl, QueryCondition cond) {
  auto &cond_var = impl->cond_ref_counts[cond];
  if (!cond_var) {
    cond_var = impl->global_vars.Create(impl->next_id++,
                                        VariableRole::kConditionRefCount);
    cond_var->query_cond = cond;
  }
  return cond_var;
}

OP *BuildStateCheckCaseReturnFalse(ProgramImpl *impl, REGION *parent) {
  return impl->operation_regions.CreateDerived<RETURN>(
      parent, ProgramOperation::kReturnFalseFromProcedure);
}

OP *BuildStateCheckCaseReturnTrue(ProgramImpl *impl, REGION *parent) {
  return impl->operation_regions.CreateDerived<RETURN>(
      parent, ProgramOperation::kReturnTrueFromProcedure);
}

OP *BuildStateCheckCaseNothing(ProgramImpl *, REGION *) {
  return nullptr;
}

// Expand the set of available columns.
void ExpandAvailableColumns(
    QueryView view,
    std::unordered_map<unsigned, QueryColumn> &wanted_to_avail) {

  // Now, map outputs to inputs.
  view.ForEachUse([&](QueryColumn in_col, InputColumnRole role,
                      std::optional<QueryColumn> out_col) {
    if (out_col && InputColumnRole::kIndexValue != role &&
        InputColumnRole::kAggregatedColumn != role) {

      if (auto it = wanted_to_avail.find(out_col->Id());
          it != wanted_to_avail.end()) {
        wanted_to_avail.emplace(in_col.Id(), it->second);
      }
    }
  });

  // The same input column may be used multiple times, and so if we have one
  // of the outputs, then we can find the other output via the inputs.
  auto pivot_ins_to_outs = [&](void) {
    view.ForEachUse([&](QueryColumn in_col, InputColumnRole role,
                        std::optional<QueryColumn> out_col) {
      if (out_col && InputColumnRole::kIndexValue != role &&
          InputColumnRole::kAggregatedColumn != role) {

        if (auto it = wanted_to_avail.find(in_col.Id());
            it != wanted_to_avail.end()) {
          wanted_to_avail.emplace(out_col->Id(), it->second);
        }
      }
    });
  };

  pivot_ins_to_outs();

//  // Finally, some of the inputs may be constants. We have to do constants
//  // last because something in `available_cols` might be a "variable" that
//  // takes on a different value than a constant, and thus needs to be checked
//  // against that constant.
//  view.ForEachUse([&](QueryColumn in_col, InputColumnRole role,
//                      std::optional<QueryColumn> out_col) {
//    if (out_col && InputColumnRole::kIndexValue != role &&
//        InputColumnRole::kAggregatedColumn != role &&
//        in_col.IsConstantOrConstantRef()) {
//      wanted_to_avail.emplace(out_col->Id(), in_col);
//    }
//  });
//
//  pivot_ins_to_outs();
}

// Filter out only the available columns that are part of the view we care
// about.
std::vector<std::pair<QueryColumn, QueryColumn>> FilterAvailableColumns(
    QueryView view,
    const std::unordered_map<unsigned, QueryColumn> &wanted_to_avail) {
  std::vector<std::pair<QueryColumn, QueryColumn>> ret;
  for (auto col : view.Columns()) {
    if (auto it = wanted_to_avail.find(col.Id()); it != wanted_to_avail.end()) {
      ret.emplace_back(col, it->second);
    }
//    else if (col.IsConstantOrConstantRef()) {
//      ret.emplace_back(col, *(col.AsConstantColumn()));
//    }
  }
  return ret;
}

// Gets or creates a top down checker function.
PROC *GetOrCreateTopDownChecker(
    ProgramImpl *impl, Context &context, QueryView view,
    const std::vector<std::pair<QueryColumn, QueryColumn>> &available_cols,
    TABLE *already_checked) {

  // There is a pretty evil situation we might encounter:
  //
  //            ...   .----.
  //              \  /      \        .
  //              UNION     |
  //             /    \     |
  //          TUPLE    .....'
  //           /       /   \         .
  //         ...
  //
  // In this situation, the data model of the TUPLE and the inductive UNION
  // are identical. If we're calling the top-down checker for the TUPLE, and
  // we have `already_checked == nullptr`, then it's the tuple's responsibility
  // to assert the absence of the row/tuple in the table, then call the
  // predecessors to try to prove the presence of that row/tuple in its own
  // absence. The tricky bit here, though, is that this is strictly downward-
  // facing: the TUPLE will ask its predecessors, and totally ignore the
  // inductive UNION which it feeds. Thus, it's preventing that UNION and its
  // predecessors (which includes the TUPLE) from even participating in this
  // decision. This is bad because the TUPLE and the UNION share their data
  // models!
  if (const auto model = impl->view_to_model[view]->FindAs<DataModel>();
      !already_checked && model->table && model->table->views[0].IsMerge() &&
      model->table->views[0] != view) {

    std::vector<QueryColumn> available_cols_in_merge;

    auto top_merge = model->table->views[0];
    auto top_merge_cols = top_merge.Columns();

    for (auto [col, avail_col] : available_cols) {
      available_cols_in_merge.push_back(top_merge_cols[*(col.Index())]);
    }

    auto sub_wanted_to_avail =
        ComputeAvailableColumns(top_merge, available_cols_in_merge);

    return GetOrCreateTopDownChecker(impl, context, top_merge,
                                     sub_wanted_to_avail,
                                     already_checked /* nullptr */);
  }

  //  assert(CanImplementTopDownChecker(impl, view, available_cols));
  (void) CanImplementTopDownChecker;

  // Make up a string that captures what we have available.
  std::stringstream ss;
  ss << view.KindName() << ':' << view.UniqueId();
  for (auto [view_col, avail_col] : available_cols) {
    ss << ',' << view_col.Id() << '/'
       << static_cast<uint32_t>(view_col.Type().Kind());
  }
  if (already_checked) {
    ss << ':' << already_checked->id;
  }

  auto &proc = context.view_to_top_down_checker[ss.str()];

  // We haven't made this procedure before; so we'll declare it now and
  // enqueue it for building. We enqueue all such procedures for building so
  // that we can build top-down checkers after all bottom-up provers have been
  // created. Doing so lets us determine which views, and thus data models,
  // are backed by what tables.
  if (!proc) {
    proc = impl->procedure_regions.Create(impl->next_id++,
                                          ProcedureKind::kTupleFinder);

    std::vector<QueryColumn> view_cols;
    std::unordered_map<unsigned, QueryColumn> wanted_to_avail;

    for (auto [view_col, avail_col] : available_cols) {
      const auto var =
          proc->input_vars.Create(impl->next_id++, VariableRole::kParameter);
      var->query_column = view_col;
      proc->col_id_to_var[view_col.Id()] = var;
      wanted_to_avail.emplace(view_col.Id(), view_col);
      view_cols.push_back(view_col);
    }

    // Now, map outputs to inputs.
    ExpandAvailableColumns(view, wanted_to_avail);
    for (auto [col_id, avail_col] : wanted_to_avail) {
      proc->col_id_to_var.emplace(col_id, proc->VariableFor(impl, avail_col));
    }

    context.top_down_checker_work_list.emplace_back(view, view_cols, proc,
                                                    already_checked);
  }

  return proc;
}

// We want to call the checker for `view`, but we only have the columns
// `succ_cols` available for use.
//
// Return value is either `{call, call}` or `{cmp, call}` where the `cmp`
// contains the `call`.
std::pair<OP *, CALL *>
CallTopDownChecker(ProgramImpl *impl, Context &context, REGION *parent,
                   QueryView succ_view,
                   const std::vector<QueryColumn> &succ_cols, QueryView view,
                   TABLE *already_checked) {

  assert(!succ_view.IsInsert());
  assert(!view.IsInsert());

  std::unordered_map<unsigned, QueryColumn> wanted_to_avail;
  for (auto succ_col : succ_cols) {
    wanted_to_avail.emplace(succ_col.Id(), succ_col);
  }

  ExpandAvailableColumns(succ_view, wanted_to_avail);
  if (succ_view != view) {
    ExpandAvailableColumns(view, wanted_to_avail);
  }
  const auto available_cols = FilterAvailableColumns(view, wanted_to_avail);

  // Map the variables for the call.
  const auto let = impl->operation_regions.CreateDerived<LET>(parent);
  OP *call_parent = let;
  for (auto [wanted_col, avail_col] : available_cols) {
    let->col_id_to_var[wanted_col.Id()] = parent->VariableFor(impl, avail_col);
  }

  // Also map in the available columns, but don't override anything that's
  // there (hence use of `emplace`).
  for (auto [wanted_col, avail_col] : available_cols) {
    let->col_id_to_var.emplace(avail_col.Id(),
                               parent->VariableFor(impl, avail_col));
  }

  // We need to create a mapping of input-to-output columns in `succ_view`.
  // For example, we might have the following case:
  //
  //           +-------+---+---+
  //           |       | A | B |
  //           | TUPLE +---+---+    succ_view
  //           |       | C | C |
  //           +-------+-+-+-+-+
  //                     |  /
  //                     | /
  //                     |/
  //           +-------+-+-+
  //           |       | C |
  //           | TUPLE +---+        view
  //           |       | . |
  //           +-------+---+
  //
  // Here, column `C` from `view` is projected into columns `A` and `B` of
  // succ_view. In a top-down checker, this turns into a requirement to
  // only call `view` if `A == B`.
  std::unordered_map<QueryColumn, std::vector<QueryColumn>> in_to_out;
  succ_view.ForEachUse([&](QueryColumn in_col, InputColumnRole role,
                           std::optional<QueryColumn> succ_view_col) {
    // The inputs that align with the outputs don't necessarily relate in
    // a straightforward way, due to going through merge/aggregate functors.
    //
    // For inequality comparisons, the comparison node will do the checking. For
    // equality comparisons, two inputs are merged into one output, so there is
    // no burden of comparison.
    if (InputColumnRole::kIndexValue == role ||
        InputColumnRole::kAggregatedColumn == role ||
        InputColumnRole::kCompareLHS == role ||
        InputColumnRole::kCompareRHS == role) {
      return;
    }

    if (!succ_view_col) {
      return;
    }

    in_to_out[in_col].push_back(*succ_view_col);
  });

  const auto proc = GetOrCreateTopDownChecker(impl, context, view,
                                              available_cols, already_checked);

  std::vector<std::pair<QueryColumn, QueryColumn>> must_be_equal;
  for (auto [wanted_col, avail_col] : available_cols) {
    const std::vector<QueryColumn> &cols = in_to_out[wanted_col];
    for (auto i = 1u; i < cols.size(); ++i) {
      must_be_equal.emplace_back(cols[i - 1u], cols[i]);
    }
  }

  TUPLECMP *cmp = nullptr;
  if (!must_be_equal.empty()) {
    cmp = impl->operation_regions.CreateDerived<TUPLECMP>(
        let, ComparisonOperator::kEqual);
    let->body.Emplace(let, cmp);
    COMMENT(cmp->comment = "Ensuring downward equality of projection";)

    // NOTE(pag): We're looking up the variables in `parent` and *not* in `let`
    //            because in the above code, when finding the `available_cols`,
    //            we forced a bunch of variable bindings in the `let` for the
    //            convenience of the `call`, and those bindings would work
    //            against this comparison, making it looking like it was
    //            comparing variables against themselves, thereby resulting in
    //            the comparison eventually being optimized away.
    for (auto [lhs_col, rhs_col] : must_be_equal) {
      const auto lhs_var = parent->VariableFor(impl, lhs_col);
      const auto rhs_var = parent->VariableFor(impl, rhs_col);
      cmp->lhs_vars.AddUse(lhs_var);
      cmp->rhs_vars.AddUse(rhs_var);
    }

    call_parent = cmp;
  }

  // Now call the checker procedure.
  const auto check = impl->operation_regions.CreateDerived<CALL>(
      impl->next_id++, call_parent, proc);

  call_parent->body.Emplace(call_parent, check);

  auto i = 0u;
  for (auto [wanted_col, avail_col] : available_cols) {
    VAR *const var = call_parent->VariableFor(impl, wanted_col);
    assert(var != nullptr);
    check->arg_vars.AddUse(var);
    const auto param = proc->input_vars[i++];
    assert(var->Type() == param->Type());
    (void) param;
  }

  assert(check->arg_vars.Size() == proc->input_vars.Size());
  return {let, check};
}

// Call the predecessor view's checker function, and if it succeeds, return
// `true`. If we have a persistent table then update the tuple's state in that
// table.
OP *ReturnTrueWithUpdateIfPredecessorCallSucceeds(
    ProgramImpl *impl, Context &context, REGION *parent, QueryView view,
    const std::vector<QueryColumn> &view_cols, TABLE *table,
    QueryView pred_view, TABLE *already_checked) {

  const auto [check, check_call] = CallTopDownChecker(
      impl, context, parent, view, view_cols, pred_view, already_checked);

  const auto ret_true = BuildStateCheckCaseReturnTrue(impl, check_call);
  check_call->body.Emplace(check_call, ret_true);

  return check;
}

// Possibly add a check to into `parent` to transition the tuple with the table
// associated with `view` to be in an present state. Returns the table of `view`
// and the updated `already_removed`.
//
// NOTE(pag): If the table associated with `view` is also associated with an
//            induction, then we defer insertion until we get into that
//            induction.
std::tuple<OP *, TABLE *, TABLE *>
InTryInsert(ProgramImpl *impl, Context &context, QueryView view, OP *parent,
            TABLE *already_added) {

  const auto model = impl->view_to_model[view]->FindAs<DataModel>();
  TABLE *const table = model->table;
  if (table) {
    if (already_added != table) {

      // Figure out what columns to pass in for marking.
      std::vector<QueryColumn> cols;
      if (view.IsInsert()) {
        auto insert = QueryInsert::From(view);
        cols.insert(cols.end(), insert.InputColumns().begin(),
                    insert.InputColumns().end());
      } else {
        cols.insert(cols.end(), view.Columns().begin(), view.Columns().end());
      }

      assert(!cols.empty());

      TupleState from_state = TupleState::kAbsent;
      if (view.CanProduceDeletions()) {
        from_state = TupleState::kAbsentOrUnknown;
      }

      // Do the marking.
      const auto table_remove = BuildChangeState(
          impl, table, parent, cols, from_state, TupleState::kPresent);

      parent->body.Emplace(parent, table_remove);
      parent = table_remove;
      already_added = table;
    }
  } else {
    already_added = nullptr;
  }

  return {parent, table, already_added};
}

// Possibly add a check to into `parent` to transition the tuple with the table
// associated with `view` to be in an unknown state. Returns the table of `view`
// and the updated `already_removed`.
//
// NOTE(pag): If the table associated with `view` is also associated with an
//            induction, then we defer insertion until we get into that
//            induction.
std::tuple<OP *, TABLE *, TABLE *>
InTryMarkUnknown(ProgramImpl *impl, Context &context, QueryView view,
                 OP *parent, TABLE *already_removed) {

  const auto model = impl->view_to_model[view]->FindAs<DataModel>();
  TABLE *const table = model->table;
  if (table) {
    if (already_removed != table) {

      // Figure out what columns to pass in for marking.
      std::vector<QueryColumn> cols;
      if (view.IsInsert()) {
        auto insert = QueryInsert::From(view);
        cols.insert(cols.end(), insert.InputColumns().begin(),
                    insert.InputColumns().end());
      } else {
        cols.insert(cols.end(), view.Columns().begin(), view.Columns().end());
      }

      assert(!cols.empty());

      // Do the marking.
      const auto table_remove =
          BuildChangeState(impl, table, parent, cols, TupleState::kPresent,
                           TupleState::kUnknown);

      parent->body.Emplace(parent, table_remove);
      parent = table_remove;
      already_removed = table;
    }
  } else {
    already_removed = nullptr;
  }

  return {parent, table, already_removed};
}

// If we've just updated a condition, then we might need to notify all
// users of that condition.
template <typename T>
static void EvaluateConditionAndNotify(ProgramImpl *impl, QueryView view,
                                       Context &context, PARALLEL *parent,
                                       bool for_add, T with_tuple) {

  // Make a call to test that the conditions of `pos_view` are satisfied.
  CALL *const cond_call = InCondionalTests(impl, view, context, parent);
  if (!cond_call) {
    assert(false);
    return;
  }

  parent->AddRegion(cond_call);

  // If all of `pos_view`s conditions are satisfied, then we can push through
  // any data from its table. We'll do the table scan, and
  auto seq = impl->series_regions.Create(cond_call);

  if (for_add) {
    cond_call->body.Emplace(cond_call, seq);
  } else {
    cond_call->false_body.Emplace(cond_call, seq);
  }

  const auto model = impl->view_to_model[view]->FindAs<DataModel>();
  const auto table = model->table;
  assert(table != nullptr);

  std::vector<QueryColumn> selected_cols;
  for (auto col : view.Columns()) {
    selected_cols.push_back(col);
  }

  TABLESCAN *const scan = impl->operation_regions.CreateDerived<TABLESCAN>(
      impl->next_id++, seq);
  seq->AddRegion(scan);


  std::vector<unsigned> cols_indexes;
  for (auto col : table->columns) {
    cols_indexes.push_back(col->index);
    scan->out_cols.AddUse(col);
  }

  scan->table.Emplace(scan, table);
  scan->index.Emplace(
      scan, table->GetOrCreateIndex(impl, std::move(cols_indexes)));

  for (auto col : selected_cols) {
    VAR * const var =
        scan->out_vars.Create(impl->next_id++, VariableRole::kScanOutput);
    var->query_column = col;
    scan->col_id_to_var[col.Id()] = var;
  }

  OP *succ_parent = scan;

  // We might need to double check that the tuple is actually present.
  if (view.CanReceiveDeletions()) {
    const auto [call_parent, check_call] = CallTopDownChecker(
        impl, context, succ_parent, view, selected_cols, view, nullptr);

    succ_parent->body.Emplace(succ_parent, call_parent);
    succ_parent = check_call;
  }

  with_tuple(succ_parent, table);
}

// If we've transitioned a condition from `0 -> 1`, i.e. done the first enable
// of the condition, so we need to allow data through, or rip back data that
// got through.
static void BuildEagerUpdateCondAndNotify(ProgramImpl *impl, Context &context,
                                          QueryCondition cond, PARALLEL *parent,
                                          bool for_add) {

  // Now that we know that the data has been dealt with, we increment or
  // decrement the condition variable.
  TESTANDSET *const set = impl->operation_regions.CreateDerived<TESTANDSET>(
      parent, (for_add ? ProgramOperation::kTestAndAdd
                       : ProgramOperation::kTestAndSub));
  parent->AddRegion(set);

  set->accumulator.Emplace(set, ConditionVariable(impl, cond));
  set->displacement.Emplace(set, impl->one);
  set->comparator.Emplace(set, for_add ? impl->one : impl->zero);

  parent = impl->parallel_regions.Create(set);
  set->body.Emplace(set, parent);

  // If we transitioned from zero-to-one, then we can possibly "unleash"
  // positive users.
  for (auto view : cond.PositiveUsers()) {
    EvaluateConditionAndNotify(
        impl, view, context, parent, for_add /* for_add */,
        [&](OP *in_loop, TABLE *table) {
          if (for_add) {
            BuildEagerInsertionRegions(impl, view, context, in_loop,
                                       view.Successors(), table);
          } else {
            BuildEagerRemovalRegions(impl, view, context, in_loop,
                                     view.Successors(), table);
          }
        });
  }

  // If we transitioned from zero-to-one, then assuming the conditions
  // previously passed (they might not have), then we want to push through
  // deletions.
  for (auto view : cond.NegativeUsers()) {
    EvaluateConditionAndNotify(
        impl, view, context, parent, !for_add /* for_add */,
        [&](OP *in_loop, TABLE *table) {
          if (!for_add) {
            BuildEagerInsertionRegions(impl, view, context, in_loop,
                                       view.Successors(), table);
          } else {
            BuildEagerRemovalRegions(impl, view, context, in_loop,
                                     view.Successors(), table);
          }
        });
  }
}

// Build and dispatch to the bottom-up remover regions for `view`. The idea
// is that we've just removed data from `view`, and now want to tell the
// successors of this.
void BuildEagerRemovalRegionsImpl(ProgramImpl *impl, QueryView view,
                                  Context &context, OP *parent_,
                                  const std::vector<QueryView> &successors,
                                  TABLE *already_removed_) {

  // The caller didn't already do a state transition, so we can do it. We might
  // have a situation like this:
  //
  //               JOIN
  //              /    \         .
  //        TUPLE1      TUPLE2
  //              \    /
  //              TUPLE3
  //
  // Where TUPLE1 and TUPLE2 take their data from the TUPLE3, then feed into
  // the JOIN. In this case, the JOIN requires that TUPLE1 and TUPLE2 be
  // persisted. If they use all of the columns of TUPLE3, then TUPLE1 and TUPLE2
  // will share the same model as TUPLE3. When we remove from TUPLE3, we don't
  // want to do separate `TryMarkUnknown` steps for each of TUPLE1 and TUPLE2
  // because otherwise whichever executed first would prevent the other from
  // actually doing the marking.
  auto [parent, table, already_removed] =
      InTryMarkUnknown(impl, context, view, parent_, already_removed_);

  // At this point, we know that if `view`s data needed to be marked as
  // unknown then it has been.
  // persisting its data, so that if the state of the conditions changes, then
  // we can send through the data that wasn't sent through (if the condition
  // wasn't previously satisfied), or delete the data that no longer satisfies
  // the condition.
  if (!view.PositiveConditions().empty() ||
      !view.NegativeConditions().empty() || view.SetCondition().has_value()) {
    assert(view.IsTuple());  // Only tuples should have conditions.
    assert(table && table == already_removed);  // The data should be persisted.

    std::vector<QueryColumn> view_cols;
    for (auto col : view.Columns()) {
      view_cols.push_back(col);
    }

    // The top-down checker will evaluate the conditions. If this truly got
    // removed and the conditions still pass, then sent it through.
    const auto [check, check_call] = CallTopDownChecker(
        impl, context, parent, view, view_cols, view, already_removed);

    const auto let = impl->operation_regions.CreateDerived<LET>(check_call);
    check_call->false_body.Emplace(check_call, let);

    parent->body.Emplace(parent, check);
    parent = let;
  }

  // All successors execute in a PARALLEL region, even if there are zero or
  // one successors. Empty and trivial PARALLEL regions are optimized out later.
  //
  // A key benefit of PARALLEL regions is that within them, CSE can be performed
  // to identify and eliminate repeated branches.
  PARALLEL *par = impl->parallel_regions.Create(parent);
  parent->body.Emplace(parent, par);

  // Proving this `view` might set a condition. If we set a condition, then
  // we need to make sure than a CHANGESTATE actually happened. That could
  // mean re-parenting all successors within a CHANGESTATE.
  //
  // NOTE(pag): Above we made certain to call the top-down checker to make
  //            sure the data is actually gone.
  if (auto set_cond = view.SetCondition(); set_cond) {
    assert(table != nullptr);
    assert(already_removed != nullptr);
    BuildEagerUpdateCondAndNotify(impl, context, *set_cond, par,
                                  false /* for_add */);
  }

  if (view.IsUsedByNegation()) {
    MaybeReAddToNegatedView(impl, context, view, par);
  }

  for (auto succ_view : successors) {

    // New style: use iterative method for removal.
    const auto let = impl->operation_regions.CreateDerived<LET>(par);
    par->AddRegion(let);

    BuildEagerRemovalRegion(impl, view, succ_view, context, let,
                            already_removed);
  }
}

// Add in all of the successors of a view inside of `parent`, which is
// usually some kind of loop. The successors execute in parallel.
void BuildEagerInsertionRegionsImpl(ProgramImpl *impl, QueryView view,
                                    Context &context, OP *parent_,
                                    const std::vector<QueryView> &successors,
                                    TABLE *last_table_) {

  // Make sure all output columns are available.
  for (auto col : view.Columns()) {
    (void) parent_->VariableFor(impl, col);
  }

  auto [parent, table, last_table] =
      InTryInsert(impl, context, view, parent_, last_table_);

  // At this point, we know that if `view`s data needed to be persisted then
  // it has been. If `view` tests any conditions, then we evaluate those *after*
  // persisting its data, so that if the state of the conditions changes, then
  // we can send through the data that wasn't sent through (if the condition
  // wasn't previously satisfied), or delete the data that no longer satisfies
  // the condition.
  if (auto cond_call = InCondionalTests(impl, view, context, parent)) {
    assert(table && table == last_table);  // The data should be persisted.
    parent->body.Emplace(parent, cond_call);
    parent = cond_call;
    last_table = nullptr;
  }

  // All successors execute in a PARALLEL region, even if there are zero or
  // one successors. Empty and trivial PARALLEL regions are optimized out later.
  //
  // A key benefit of PARALLEL regions is that within them, CSE can be performed
  // to identify and eliminate repeated branches.
  PARALLEL *par = impl->parallel_regions.Create(parent);
  parent->body.Emplace(parent, par);

  // Proving this `view` might set a condition. If we set a condition, then
  // we need to make sure than a CHANGESTATE actually happened. That could
  // mean re-parenting all successors within a CHANGESTATE.
  if (auto set_cond = view.SetCondition(); set_cond) {
    assert(table != nullptr);
    BuildEagerUpdateCondAndNotify(impl, context, *set_cond, par,
                                  true /* for_add */);
  }

  // If this view is used by a negation, and if we just proved this view, then
  // we need to go and make sure that the corresponding data gets removed from
  // the negation.
  if (view.IsUsedByNegation()) {
    MaybeRemoveFromNegatedView(impl, context, view, par);
  }

  //  std::unordered_map<DataModel *, std::vector<QueryView>> grouped_successors;
  //  for (auto succ_view : successors) {
  //    const auto succ_view_model =
  //        impl->view_to_model[succ_view]->FindAs<DataModel>();
  //    grouped_successors[succ_view_model].push_back(succ_view);
  //  }
  //
  //  for (const auto &[succ_model, succ_views] : grouped_successors) {
  //    const auto let = impl->operation_regions.CreateDerived<LET>(par);
  //    par->AddRegion(let);
  //
  //    const auto num_succ_views = succ_views.size();
  //
  //    if (1u == num_succ_views) {
  //      BuildEagerRegion(impl, view, succ_views[0], context, let, last_table);
  //
  //    // We may have multiple successors sharing a data model. If this happens,
  //    // then the code for one successor might
  //    } else {
  //
  //      OP *succ_parent = let;
  //      TABLE *succ_table = table;
  //      TABLE *succ_last_table = last_table;
  //
  //      // Only try to do an insert for the group of table-sharing successors
  //      // if their table is different than the table of `view`, into which we
  //      // have just inserted (initial `InTryInsert` in this function).
  //      //
  //      //
  //      if (table != succ_model->table &&
  //          (succ_views[0].IsTuple() || succ_views[0].IsMerge())) {
  //        auto combined_succ_insert = InTryInsert(
  //            impl, context, succ_views[0], let, last_table);
  //        succ_parent = std::get<0>(combined_succ_insert);
  //        succ_table = std::get<1>(combined_succ_insert);
  //        succ_last_table = std::get<2>(combined_succ_insert);
  //      }
  //
  //      PARALLEL * const succ_par = impl->parallel_regions.Create(succ_parent);
  //      succ_parent->body.Emplace(succ_parent, succ_par);
  //
  ////      const auto first_cols = succ_views[0].Columns();
  ////      for (auto i = 1ull; i < num_succ_views; ++i) {
  ////        auto j = 0u;
  ////        for (auto cols : succ_views[i].Columns()) {
  ////
  ////        }
  ////      }
  //
  //      for (auto succ_view : succ_views) {
  //        LET * const succ_let =
  //            impl->operation_regions.CreateDerived<LET>(succ_par);
  //        succ_par->AddRegion(succ_let);
  //        BuildEagerRegion(impl, view, succ_view, context, succ_let, succ_last_table);
  //      }
  //    }
  //  }

  for (QueryView succ_view : successors) {
    const auto let = impl->operation_regions.CreateDerived<LET>(par);
    par->AddRegion(let);
    BuildEagerRegion(impl, view, succ_view, context, let, last_table);
  }
}

// Returns `true` if `view` might need to have its data persisted for the
// sake of supporting differential updates / verification.
bool MayNeedToBePersistedDifferential(QueryView view) {
  if (MayNeedToBePersisted(view)) {
    return true;
  }

  if (view.CanReceiveDeletions() || view.CanProduceDeletions()) {
    return true;
  }

  // If any successor of `view` can receive a deletion, then we may need to
  // support re-proving of a tuple in `succ`, but to do so, we may need to
  // also do a top-down execution into `view`, and have `view` provide the
  // base case.
  for (auto succ : view.Successors()) {
    if (succ.CanReceiveDeletions()) {
      return true;
    }
  }

  return false;
}

// Returns `true` if `view` might need to have its data persisted.
bool MayNeedToBePersisted(QueryView view) {

  // If this view sets a condition then its data must be tracked; if it
  // tests a condition, then we might jump back in at some future point if
  // things transition states.
  return view.SetCondition() || !view.PositiveConditions().empty() ||
         !view.NegativeConditions().empty() || view.IsUsedByNegation();
}

// Complete a procedure by exhausting the work list.
void CompleteProcedure(ProgramImpl *impl, PROC *proc, Context &context,
                       bool add_return) {
  while (!context.work_list.empty()) {

    std::stable_sort(context.work_list.begin(), context.work_list.end(),
                     [](const WorkItemPtr &a, const WorkItemPtr &b) {
                       return a->order > b->order;
                     });

    WorkItemPtr action = std::move(context.work_list.back());
    context.work_list.pop_back();
    action->Run(impl, context);
  }

  // Add a default `return false` at the end of normal procedures.
  if (add_return && !EndsWithReturn(proc)) {
    const auto ret = impl->operation_regions.CreateDerived<RETURN>(
        proc, ProgramOperation::kReturnFalseFromProcedure);
    ret->ExecuteAfter(impl, proc);
  }
}

static void MapVariablesInEagerRegion(ProgramImpl *impl, QueryView pred_view,
                                      QueryView view, OP *parent) {
  view.ForEachUse([=](QueryColumn in_col, InputColumnRole role,
                      std::optional<QueryColumn> out_col) {
    if (!out_col) {
      return;
    }

    assert(in_col.Id() != out_col->Id());
    assert(QueryView::Containing(*out_col) == view);

    // Comparisons merge two inputs into a single output.
    if ((InputColumnRole::kCompareLHS == role ||
         InputColumnRole::kCompareRHS == role) &&
        (ComparisonOperator::kEqual == QueryCompare::From(view).Operator())) {
      return;

    // Index values are merged with prior values to form the output. We don't
    // overwrite join outputs otherwise they don't necessarily get assigned to
    // the right selection variables.
    } else if (InputColumnRole::kIndexValue == role) {
      return;

    } else if (QueryView::Containing(in_col) == pred_view) {
      const auto src_var = parent->VariableFor(impl, in_col);
      assert(src_var != nullptr);
      parent->col_id_to_var[out_col->Id()] = src_var;


    } else if (in_col.IsConstantRef()) {
      const auto src_var = parent->VariableFor(impl, in_col);
      assert(src_var != nullptr);
      parent->col_id_to_var.emplace(out_col->Id(), src_var);

    // NOTE(pag): This is subtle. We use `emplace` here instead of `[...] =`
    //            to give preference to the constant matching the incoming view.
    //            The key issue here is when we have a column of a MERGE node
    //            taking in a lot constants, we can't be certain which constant
    //            we're getting.
    } else if (in_col.IsConstant() && InputColumnRole::kMergedColumn != role) {
      const auto src_var = parent->VariableFor(impl, in_col);
      assert(src_var != nullptr);
      parent->col_id_to_var.emplace(out_col->Id(), src_var);
    }
  });
}

// Build an eager region for removing data.
void BuildEagerRemovalRegion(ProgramImpl *impl, QueryView from_view,
                             QueryView to_view, Context &context, OP *parent,
                             TABLE *already_checked) {

  MapVariablesInEagerRegion(impl, from_view, to_view, parent);

  if (to_view.IsTuple()) {
    CreateBottomUpTupleRemover(impl, context, to_view, parent, already_checked);

  } else if (to_view.IsCompare()) {
    CreateBottomUpCompareRemover(impl, context, to_view, parent,
                                 already_checked);

  } else if (to_view.IsInsert()) {
    CreateBottomUpInsertRemover(impl, context, to_view, parent,
                                already_checked);

  } else if (to_view.IsMerge()) {
    if (to_view.InductionGroupId().has_value()) {
      CreateBottomUpInductionRemover(impl, context, to_view, parent,
                                     already_checked);
    } else {
      CreateBottomUpUnionRemover(impl, context, to_view, parent,
                                 already_checked);
    }

  } else if (to_view.IsJoin()) {
    auto join = QueryJoin::From(to_view);
    if (join.NumPivotColumns()) {
      CreateBottomUpJoinRemover(impl, context, from_view, join, parent,
                                already_checked);
    } else {
      assert(false && "TODO: Cross-products!");
    }
  } else if (to_view.IsAggregate()) {
    assert(false && "TODO Aggregates!");

  } else if (to_view.IsKVIndex()) {
    assert(false && "TODO Key Values!");

  } else if (to_view.IsMap()) {
    auto map = QueryMap::From(to_view);
    auto functor = map.Functor();
    if (functor.IsPure()) {
      CreateBottomUpGenerateRemover(impl, context, map, functor, parent,
                                    already_checked);

    } else {
      assert(false && "TODO Impure Functors!");
    }

  } else if (to_view.IsNegate()) {
    CreateBottomUpNegationRemover(impl, context, to_view, parent,
                                  already_checked);

  // NOTE(pag): This shouldn't be reachable, as the bottom-up INSERT
  //            removers jump past SELECTs.
  } else if (to_view.IsSelect()) {
    assert(false);

  } else {
    assert(false);
  }
}

// Build an eager region. This guards the execution of the region in
// conditionals if the view itself is conditional.
void BuildEagerRegion(ProgramImpl *impl, QueryView pred_view, QueryView view,
                      Context &context, OP *parent, TABLE *last_table) {

  MapVariablesInEagerRegion(impl, pred_view, view, parent);

  if (view.IsJoin()) {
    const auto join = QueryJoin::From(view);
    if (join.NumPivotColumns()) {
      BuildEagerJoinRegion(impl, pred_view, join, context, parent, last_table);
    } else {
      BuildEagerProductRegion(impl, pred_view, join, context, parent,
                              last_table);
    }

  } else if (view.IsMerge()) {
    const auto merge = QueryMerge::From(view);
    if (view.InductionGroupId().has_value()) {
      BuildEagerInductiveRegion(impl, pred_view, merge, context, parent,
                                last_table);
    } else {
      BuildEagerUnionRegion(impl, pred_view, merge, context, parent,
                            last_table);
    }

  } else if (view.IsAggregate()) {
    assert(false && "TODO(pag): Aggregates");

  } else if (view.IsKVIndex()) {
    assert(false && "TODO(pag): KV Indices.");

  } else if (view.IsMap()) {
    auto map = QueryMap::From(view);
    if (map.Functor().IsPure()) {
      BuildEagerGenerateRegion(impl, pred_view, map, context, parent,
                               last_table);

    } else {
      assert(false && "TODO(pag): Impure functors");
    }

  } else if (view.IsCompare()) {
    BuildEagerCompareRegions(impl, QueryCompare::From(view), context, parent);

  } else if (view.IsSelect()) {
    BuildEagerInsertionRegions(impl, view, context, parent, view.Successors(),
                               last_table);

  } else if (view.IsTuple()) {
    BuildEagerTupleRegion(impl, pred_view, QueryTuple::From(view), context,
                          parent, last_table);

  } else if (view.IsInsert()) {
    const auto insert = QueryInsert::From(view);
    BuildEagerInsertRegion(impl, pred_view, insert, context, parent,
                           last_table);

  } else if (view.IsNegate()) {
    const auto negate = QueryNegate::From(view);
    BuildEagerNegateRegion(impl, pred_view, negate, context, parent,
                           last_table);

  } else {
    assert(false);
  }
}

WorkItem::WorkItem(Context &context, unsigned order_) : order(order_) {}

WorkItem::~WorkItem(void) {}

// Build a program from a query.
std::optional<Program> Program::Build(const ::hyde::Query &query) {
  auto impl = std::make_shared<ProgramImpl>(query);
  const auto program = impl.get();

  Context context;
  context.init_proc = impl->procedure_regions.Create(
      impl->next_id++, ProcedureKind::kInitializer);

  BuildDataModel(query, program);

  // Create constant variables.
  for (auto const_val : query.Constants()) {
    const auto var =
        impl->const_vars.Create(impl->next_id++, VariableRole::kConstant);
    var->query_const = const_val;
    impl->const_to_var.emplace(const_val, var);
  }

  // Create tag variables.
  for (auto const_val : query.Tags()) {
    const auto var =
        impl->const_vars.Create(impl->next_id++, VariableRole::kConstantTag);
    var->query_const = QueryConstant(const_val);
    impl->const_to_var.emplace(const_val, var);
  }

  //  // Go figure out which merges are inductive, and then classify their
  //  // predecessors and successors in terms of which ones are inductive and
  //  // which aren't.
  //  DiscoverInductions(query, context, log);
  //  if (!log.IsEmpty()) {
  //    return std::nullopt;
  //  }

  // Now that we've identified our inductions, we can fill our data model,
  // i.e. assign persistent tables to each disjoint set of views.
  FillDataModel(query, program, context);

  // Build bottom-up procedures starting from message receives.
  PROC *const entry_proc = BuildEntryProcedure(program, context, query);

  for (auto io : query.IOs()) {
    BuildIOProcedure(impl.get(), query, io, context, entry_proc);
  }

  // Build the initialization procedure, needed to start data flows from
  // things like constant tuples.
  BuildInitProcedure(program, context, query);

  for (auto insert : query.Inserts()) {
    if (insert.IsRelation()) {
      auto decl = insert.Relation().Declaration();
      if (decl.IsQuery()) {
        BuildQueryEntryPoint(program, context, decl, insert);
      }
    }
  }

  // Build top-down provers and the bottom-up removers (bottom-up removers are
  // separate procedures when using the `IRFormat::kRecursive`; when using
  // `IRFormat::kIterative`, the bottom-up removers are iterative and in-line
  // with the inserters.
  while (BuildTopDownCheckers(program, context) ||
         BuildBottomUpRemovalProvers(program, context)) {
  }

  for (auto proc : impl->procedure_regions) {
    if (!EndsWithReturn(proc)) {
      BuildStateCheckCaseReturnFalse(impl.get(), proc)
          ->ExecuteAfter(impl.get(), proc);
    }
  }

  impl->Optimize();

  // Assign defining regions to each variable.
  //
  // NOTE(pag): We don't really want to map variables throughout the building
  //            process because otherwise every time we replaced all uses of
  //            one region with another, we'd screw up the mapping.
  for (auto proc : impl->procedure_regions) {
    MapVariables(proc);
  }

  impl->Analyze();

//  ExtractPrimaryProcedure(impl.get(), entry_proc, context);
//
//  impl->Optimize();
//
//  // Assign defining regions to each variable.
//  //
//  // NOTE(pag): We don't really want to map variables throughout the building
//  //            process because otherwise every time we replaced all uses of
//  //            one region with another, we'd screw up the mapping.
//  for (auto proc : impl->procedure_regions) {
//    MapVariables(proc);
//  }

  //impl->Analyze();

//  // Finally, go through our tables. Any table with no indices is given a
//  // full table index, on the assumption that it is used for things like state
//  // checking.
//  for (TABLE *table : impl->tables) {
//    if (table->indices.Empty()) {
//      std::vector<unsigned> offsets;
//      for (auto col : table->columns) {
//        offsets.push_back(col->index);
//      }
//      (void) table->GetOrCreateIndex(impl.get(), std::move(offsets));
//    }
//  }

  return Program(std::move(impl));
}

}  // namespace hyde
