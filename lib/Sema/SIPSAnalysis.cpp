// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include <drlojekyll/Sema/SIPSAnalysis.h>

#include <algorithm>
#include <cassert>
#include <optional>
#include <unordered_map>
#include <vector>

#include <drlojekyll/Util/Compiler.h>
#include <drlojekyll/Util/DisjointSet.h>

#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Lex/Format.h>
#include <drlojekyll/Parse/Format.h>

namespace hyde {

extern OutputStream *gOut;

namespace {

static bool OrderPredicates(std::pair<unsigned, ParsedPredicate> a,
                            std::pair<unsigned, ParsedPredicate> b) noexcept {
  return a.first < b.first;
}

}  // namespace

SIPSVisitor::~SIPSVisitor(void) {}
void SIPSVisitor::Begin(ParsedPredicate) {}
void SIPSVisitor::Begin(ParsedClause) {}
void SIPSVisitor::DeclareParameter(const Column &) {}
void SIPSVisitor::DeclareVariable(ParsedVariable, unsigned) {}
void SIPSVisitor::DeclareConstant(ParsedLiteral, unsigned) {}
void SIPSVisitor::AssertEqual(ParsedVariable, unsigned, ParsedVariable, unsigned) {}
void SIPSVisitor::AssertNotEqual(ParsedVariable, unsigned, ParsedVariable, unsigned) {}
void SIPSVisitor::AssertLessThan(ParsedVariable, unsigned, ParsedVariable, unsigned) {}
void SIPSVisitor::AssertGreaterThan(ParsedVariable, unsigned, ParsedVariable, unsigned) {}
void SIPSVisitor::AssertPresent(
    ParsedDeclaration, ParsedPredicate, const Column *, const Column *) {}
void SIPSVisitor::AssertAbsent(
    ParsedDeclaration, ParsedPredicate, const Column *, const Column *) {}
void SIPSVisitor::Insert(
    ParsedDeclaration, const Column *, const Column *) {}
void SIPSVisitor::EnterFromWhereSelect(
    ParsedPredicate, ParsedDeclaration, const Column *, const Column *,
    const Column *, const Column *) {}
void SIPSVisitor::EnterSelectFromSummary(
    ParsedPredicate functor, ParsedDeclaration decl,
    const Column *, const Column *,
    const Column *, const Column *,
    const Column *, const Column *,
    const Column *, const Column *) {}
void SIPSVisitor::EnterFromSelect(
    ParsedPredicate, ParsedDeclaration, const Column *, const Column *) {}
void SIPSVisitor::EnterAggregation(
    ParsedPredicate, ParsedDeclaration,
    const Column *, const Column *,
    const Column *, const Column *,
    const Column *, const Column *,
    ParsedPredicate, ParsedDeclaration,
    const Column *, const Column *,
    const Column *, const Column *,
    const Column *, const Column *) {}
void SIPSVisitor::Collect(
    ParsedPredicate, ParsedDeclaration,
    const Column *, const Column *,
    const Column *, const Column *,
    const Column *, const Column *) {}
void SIPSVisitor::ExitSelect(ParsedPredicate, ParsedDeclaration) {}
void SIPSVisitor::Commit(ParsedPredicate) {}
void SIPSVisitor::Commit(ParsedClause) {}
void SIPSVisitor::CancelComparison(ParsedComparison, unsigned, unsigned) {}
void SIPSVisitor::CancelRangeRestriction(ParsedComparison, ParsedVariable) {}
void SIPSVisitor::CancelRangeRestriction(ParsedClause, ParsedVariable) {}
void SIPSVisitor::CancelPredicate(const FailedBinding *, FailedBinding *) {}
void SIPSVisitor::CancelMessage(const ParsedPredicate) {}
SIPSVisitor::AdvanceType SIPSVisitor::Advance(void) { return kTryNextPermutation; }

class SIPSGenerator::Impl {
 public:
  Impl(ParsedPredicate assumption_);
  Impl(ParsedClause clause_);

  // Visit an ordering of the predicates.
  bool Visit(SIPSVisitor &visitor);

  // Try to advance the generator.
  bool Advance(void);

  // Get a fresh variable.
  unsigned GetFreshVarId(void);

  // Try to apply the negated predicates as eagerly as possible.
  void VisitNegatedPredicates(SIPSVisitor &visitor);

  // Visit the end of a clause body.
  bool VisitEndOfClause(SIPSVisitor &visitor);

  // Bind the free parameters used in selection predicate.
  void BindFreeParams(SIPSVisitor &visitor, bool defer_asserts=false);

  // Apply any deferred assertions.
  void ApplyDeferredAsserts(SIPSVisitor &visitor);

  // Visit a predicate. Returns `true` if it was processed, and `false`
  // if we cancelled things.
  bool VisitPredicate(SIPSVisitor &visitor, unsigned index);

  // Find a redeclaration of `decl`, such that all binding constraints of the
  // redeclaration are satisfied by the available bindings in `equalities`.
  bool FindRedeclMatchingBindingConstraints(
      SIPSVisitor &visitor, ParsedPredicate predicate,
      ParsedDeclaration &decl);

  // Collect the variables that are bounds and will be passed into `predicate`
  // into `bound_params`, and the free variables which will be defined by
  // `predicate` into `free_params`.
  void CollectPredicateBoundAndFreeVars(
      SIPSVisitor &visitor,
      ParsedDeclaration decl,
      ParsedPredicate predicate);

  // Visit an assignment. Returns `true` if it was processed, and `false`
  // if we cancelled things.
  bool VisitAssign(SIPSVisitor &visitor, ParsedAssignment assignment);

  // Visit a comparison. Returns `true` if it was processed. Returns `false`
  // if this comparison was not processed. If it was not processed, then
  // it is possible that it was cancelled, in which case `cancelled` may be
  // set to `true`.
  bool VisitCompare(SIPSVisitor &visitor, ParsedComparison comparison);

  // Try to visit all of the remaining comparisons. Returns `false` if
  // this permutation is cancelled.
  bool VisitCompares(SIPSVisitor &visitor);

  // Visit a comparison. Returns `true` if it was processed. Returns `false`
  // if this comparison was not processed. If it was not processed, then
  // it is possible that it was cancelled, in which case `cancelled` may be
  // set to `true`.
  bool VisitAggregate(SIPSVisitor &visitor, ParsedAggregate aggregate,
                      unsigned p, bool &ret);

  // Try to visit all of the remaining aggregates. Returns `false` if
  // this permutation is cancelled.
  bool VisitAggregates(SIPSVisitor &visitor, unsigned p, bool &ret);

  // The clause being analyzed.
  const ParsedClause clause;

  // The predicate which we are assuming has been provided and comes with
  // concrete data.
  const std::optional<ParsedPredicate> assumption;

  // The vector of positive predicates which we will evaluate.
  std::vector<std::pair<unsigned, ParsedPredicate>> positive_predicates;

  // The vector of negative predicates, which we will evaluate if their
  // parameters are all bound.
  std::vector<ParsedPredicate> negative_predicates;
  std::vector<ParsedPredicate> next_negative_predicates;

  // Does the visitor want to advance?
  SIPSVisitor::AdvanceType advance_type{SIPSVisitor::kTryNextPermutation};

  bool cancelled{false};
  bool started{false};
  unsigned next_var_id{0};

  std::vector<std::unique_ptr<DisjointSet>> vars;
  std::unordered_map<ParsedVariable, DisjointSet *> equalities;
  std::unordered_map<ParsedVariable, DisjointSet *> prev_equalities;

  std::vector<ParsedComparison> comparisons;
  std::vector<ParsedComparison> next_comparisons;

  std::vector<bool> aggregates_processed;
  std::vector<ParsedAggregate> aggregates;

  std::vector<SIPSVisitor::FailedBinding> failed_bindings;

  std::vector<SIPSVisitor::Column> bound_params;
  std::vector<SIPSVisitor::Column> free_params;
  std::vector<std::tuple<ParsedVariable, unsigned, ParsedVariable, unsigned>> deferred_asserts
  ;

  // The `bound`-marked parameters to the aggregating functor.
  std::vector<SIPSVisitor::Column> aggregate_input_params;

  // The `summarize`d-marked parameters to the aggregating functor.
  std::vector<SIPSVisitor::Column> aggregate_collection_params;

  // The incoming bound parameters to the predicate being summarized. These
  // get partitioned into `inner_group_by_params` and `outer_group_by_params`.
  std::vector<SIPSVisitor::Column> summarized_bound_params;

  // The incoming bound parameters to the predicate being summarized which
  // also correspond to `bound` parameters of the aggregating functor. These
  // are like "inner group by" columns.
  std::vector<SIPSVisitor::Column> inner_group_by_params;

  // The incoming bound parameters to the predicate being summarized which
  // do not correspond with `bound` parameters of the aggregating functor.
  // These are like "outer group by" columns.
  std::vector<SIPSVisitor::Column> outer_group_by_params;

  // The free parameters produced by the predicate being summarized.
  std::vector<SIPSVisitor::Column> summarized_free_params;
};

SIPSGenerator::Impl::Impl(ParsedClause clause_)
    : clause(clause_) {
  auto i = 0u;
  for (auto pred : clause.PositivePredicates()) {
    assert(pred.IsPositive());
    positive_predicates.emplace_back(i++, pred);
  }
}

SIPSGenerator::Impl::Impl(ParsedPredicate assumption_)
    : clause(ParsedClause::Containing(assumption_)),
      assumption(assumption_) {

  // The `assumption` predicate is kind of like a left corner in a parse. We
  // are assuming we have it and that all of its variables are bound, because
  // with the stepper, we're simulating a bottom-up execution. Thus we want
  // to exclude the assumption from the list of tested predicates that will
  // influence its SIP score and binding environment.
  //
  // NOTE(pag): This initializes `positive_predicates` in sorted order (via `i`)
  //            so it's already in the right place for use with permutation.
  auto i = 0u;
  for (auto pred : clause.PositivePredicates()) {
    assert(pred.IsPositive());
    if (pred != assumption_) {
      positive_predicates.emplace_back(i++, pred);
    }
  }
}

// Get a fresh variable.
unsigned SIPSGenerator::Impl::GetFreshVarId(void) {
  auto next_id = next_var_id++;
  auto next_var = new DisjointSet(next_id);
  vars.emplace_back(next_var);
  return next_id;
}

// Try to apply the negated predicates as eagerly as possible.
void SIPSGenerator::Impl::VisitNegatedPredicates(SIPSVisitor &visitor) {
  next_negative_predicates.clear();
  for (auto predicate : negative_predicates) {
    for (auto pred_arg : predicate.Arguments()) {
      if (!equalities.count(pred_arg)) {
        next_negative_predicates.push_back(predicate);
        goto next_negated_predicate;
      }
    }

    do {
      bound_params.clear();
      auto i = 0u;
      auto decl = ParsedDeclaration::Of(predicate);

      if (!FindRedeclMatchingBindingConstraints(visitor, predicate, decl)) {
        assert(!failed_bindings.empty());
        visitor.CancelPredicate(
            &(failed_bindings.front()),
            &((&(failed_bindings.back()))[1]));
        cancelled = true;
        return;
      }

      for (auto pred_arg : predicate.Arguments()) {
        bound_params.emplace_back(
            decl.NthParameter(i),
            pred_arg,
            i,
            equalities[pred_arg]->Find()->id);
        ++i;
      }

      visitor.AssertAbsent(
          decl, predicate,
          &(bound_params.front()),
          &((&(bound_params.back()))[1]));

    } while (false);

  next_negated_predicate:
    continue;
  }
  negative_predicates.swap(next_negative_predicates);
}

// Visit the end of a clause body.
bool SIPSGenerator::Impl::VisitEndOfClause(SIPSVisitor &visitor) {
  failed_bindings.clear();

  // Make sure all compared variables are range-restricted.
  for (auto comparison : comparisons) {
    // (*gOut) << "cancelled, remaining compares\n";
    cancelled = true;

    if (!equalities.count(comparison.LHS())) {
      visitor.CancelRangeRestriction(comparison, comparison.LHS());

    } else if (!equalities.count(comparison.RHS())) {
      visitor.CancelRangeRestriction(comparison, comparison.RHS());

    } else {
      assert(false);  // Shouldn't be possible.
      visitor.CancelComparison(
          comparison,
          equalities[comparison.LHS()]->Find()->id,
          equalities[comparison.RHS()]->Find()->id);
    }
  }

  // There remain one or more negated predicates that have not yet
  // been invoked; report on the cancellation reason.
  for (auto predicate : negative_predicates) {
    // (*gOut) << "cancelled, remaining negative preds\n";
    cancelled = true;

    const auto negated_decl = ParsedDeclaration::Of(predicate);
    auto i = 0u;
    for (auto pred_arg : predicate.Arguments()) {
      const auto arg_i = i++;

      if (!equalities.count(pred_arg)) {
        failed_bindings.emplace_back(
            negated_decl, predicate, pred_arg,
            negated_decl.NthParameter(arg_i));
      }
    }

    visitor.CancelPredicate(
        &(failed_bindings.front()),
        &((&(failed_bindings.back()))[1]));
  }

  // There remain one or more aggregates that haven't successfully been
  // applied.
  for (auto i = 0u; i < aggregates.size(); ++i) {
    if (aggregates_processed[i]) {
      continue;
    }

    // (*gOut) << "cancelled, remaining aggregates\n";
    cancelled = true;
    auto aggregate = aggregates[i];
    auto predicate = aggregate.Predicate();
    auto decl = ParsedDeclaration::Of(predicate);
    if (!FindRedeclMatchingBindingConstraints(visitor, predicate, decl)) {
      assert(!failed_bindings.empty());
      visitor.CancelPredicate(
          &(failed_bindings.front()),
          &((&(failed_bindings.back()))[1]));
    }
  }

  // Build up the parameter
  bound_params.clear();
  auto i = 0u;
  const auto decl = ParsedDeclaration::Of(clause);
  for (auto clause_arg : clause.Parameters()) {
    if (!equalities.count(clause_arg)) {
      visitor.CancelRangeRestriction(clause, clause_arg);
      cancelled = true;
      return false;

    } else {
      bound_params.emplace_back(
          decl.NthParameter(i),
          clause_arg,
          i,
          equalities[clause_arg]->Find()->id);
    }
    ++i;
  }

  if (cancelled) {
    return false;
  }

  visitor.Insert(
      decl, &(bound_params.front()), &((&(bound_params.back()))[1]));
  return true;
}

// Bind the free parameters used in selection predicate.
void SIPSGenerator::Impl::BindFreeParams(SIPSVisitor &visitor,
                                         bool defer_asserts) {
  deferred_asserts.clear();

  for (const auto &free_param : free_params) {
    auto &goal_set = equalities[free_param.var];
    if (!goal_set) {
      goal_set = vars[free_param.id].get();

    // Something like `foo(A, A)`, where `A` is free. This turns into
    // `foo(A, A1), A=A1`.
    } else {
      goal_set = goal_set->Find();
      const auto free_set = vars[free_param.id]->Find();
      if (defer_asserts) {
        deferred_asserts.emplace_back(
            free_param.var, goal_set->id, free_param.var, free_set->id);
      } else {
        visitor.AssertEqual(free_param.var, goal_set->id,
                            free_param.var, free_set->id);
      }
      goal_set = DisjointSet::Union(goal_set, free_set);
    }
  }
}

void SIPSGenerator::Impl::ApplyDeferredAsserts(SIPSVisitor &visitor) {
  for (auto &deferred_assert : deferred_asserts) {
    visitor.AssertEqual(
        std::get<0>(deferred_assert), std::get<1>(deferred_assert),
        std::get<2>(deferred_assert), std::get<3>(deferred_assert));
  }
  deferred_asserts.clear();
}

// Find a redeclaration of `decl`, such that all binding constraints of the
// redeclaration are satisfied by the available bindings in `equalities`.
bool SIPSGenerator::Impl::FindRedeclMatchingBindingConstraints(
    SIPSVisitor &visitor, ParsedPredicate predicate,
    ParsedDeclaration &decl) {

  const auto arity = predicate.Arity();
  for (auto redecl : decl.Redeclarations()) {
    for (auto i = 0U; i < arity; ++i) {
      const auto redecl_param = redecl.NthParameter(i);
      const auto redecl_param_binding = redecl_param.Binding();
      const auto pred_arg = predicate.NthArgument(i);
      const auto pred_arg_is_bound = equalities.count(pred_arg);

      // Make sure we meet the binding constraint for at least one of
      // the redeclarations of this functor or query. Functors, especially
      // aggregate ones, that introduce things like summary values, can be
      // used to force certain execution orders of the conjuncts of a clause
      // because of these constraints.
      if ((!pred_arg_is_bound &&
          (ParameterBinding::kBound == redecl_param_binding ||
           ParameterBinding::kAggregate == redecl_param_binding)) ||
          (pred_arg_is_bound &&
           (ParameterBinding::kFree == redecl_param_binding ||
            ParameterBinding::kSummary == redecl_param_binding))) {
        failed_bindings.emplace_back(
            redecl, predicate, pred_arg, redecl_param);
        goto try_next_redecl;
      }
    }

    decl = redecl;
    return true;

  try_next_redecl:
    continue;
  }

  return false;
}

// Collect the variables that are bound and will be passed into `predicate`
// into `bound_params`, and the free variables which will be defined by
// `predicate` into `free_params`.
void SIPSGenerator::Impl::CollectPredicateBoundAndFreeVars(
    SIPSVisitor &visitor,
    ParsedDeclaration decl,
    ParsedPredicate predicate) {

  const auto arity = decl.Arity();
  bound_params.clear();
  free_params.clear();

  // Go through the parameters to the predicate, and separate out the bound
  // ones from the free ones. For free ones, we create fresh variable
  // declarations, however, we delay adding them to `equalities` until after
  // telling the visitor about the predicate and its arguments. This is
  // because we may have something like `foo(A, A)` where `A` is unbound, and
  // so we have to treat it like `foo(A1, A2), A=A1, A=A2`.
  for (auto i = 0U; i < arity; ++i) {
    const auto var = predicate.NthArgument(i);
    const auto var_is_bound = equalities.count(var);
    const auto param = decl.NthParameter(i);

    auto do_free = [=] (SIPSVisitor &visitor) {
      assert(!var_is_bound);
      free_params.emplace_back(param, var, i, GetFreshVarId());
      visitor.DeclareVariable(var, free_params.back().id);
    };

    auto do_bound = [=] (void) {
      assert(var_is_bound);
      bound_params.emplace_back(
          param, var, i, equalities[var]->Find()->id);
    };

    switch (param.Binding()) {
      case ParameterBinding::kImplicit:
        if (var_is_bound) {
          do_bound();
        } else {
          do_free(visitor);
        }
        break;

      case ParameterBinding::kMutable:
      case ParameterBinding::kFree:
      case ParameterBinding::kSummary:
        do_free(visitor);
        break;

      case ParameterBinding::kBound:
      case ParameterBinding::kAggregate:
        do_bound();
        break;
    }
  }
}

// Visit a predicate. Returns `true` if it was processed, and `false`
// if we cancelled things.
bool SIPSGenerator::Impl::VisitPredicate(
    SIPSVisitor &visitor, unsigned p) {

  // Perform as many comparisons as possible as early as possible, and prefer
  // comparisons to negated predicates as they don't need to touch memory or
  // the database.
  if (!VisitCompares(visitor)) {
    return false;
  }

  // Try to negate things as early as possible, i.e. once everything is bound.
  VisitNegatedPredicates(visitor);

  // Now if we can, collect on aggregates, do so. This returns `true` if any
  // aggregate was applied, and visiting an aggregate descends into the `p`th
  // predicate.
  bool ret = false;
  if (VisitAggregates(visitor, p, ret)) {
    return ret;
  }

  // We've bottomed out; we're done visiting this clause body.
  if (p >= positive_predicates.size()) {
    return VisitEndOfClause(visitor);
  }

  const auto predicate = positive_predicates[p].second;
  assert(predicate.IsPositive());

  auto decl = ParsedDeclaration::Of(predicate);

  // The only place where a message is an acceptable dependency is as an
  // assumption.
  if (decl.IsMessage()) {
    if (assumption || 0 != p) {
      visitor.CancelMessage(predicate);
      cancelled = true;
      return false;
    }
  }

  // For functors and queries, we care about the precise choice in order
  // to meet a binding constraint of one of the redeclarations. For
  // functors, we need to exactly match the usage specification. For
  // queries, we need to match the `bound` arguments, and the `free`
  // ones "cost" us in implicit later binding.
  failed_bindings.clear();
  if (!FindRedeclMatchingBindingConstraints(visitor, predicate, decl)) {
    visitor.CancelPredicate(
        &(failed_bindings.front()),
        &((&(failed_bindings.back()))[1]));
    cancelled = true;
    return false;
  }

  CollectPredicateBoundAndFreeVars(visitor, decl, predicate);

  if (bound_params.empty() && free_params.empty()) {
    assert(false);  // Not possible.
    return VisitPredicate(visitor, p + 1);

  // We only have bound parameters. This is equivalent to asking if a tuple
  // is present, i.e. if a certain row exists in a table.
  } else if (free_params.empty()) {
    assert(!bound_params.empty());
    visitor.AssertPresent(
        decl, predicate, &(bound_params.front()),
        &((&(bound_params.back()))[1]));

    return VisitPredicate(visitor, p + 1);

  // We only have free parameters. This is equivalent to a full table scan.
  } else if (bound_params.empty()) {
    assert(!free_params.empty());

    visitor.EnterFromSelect(
        predicate, decl, &(free_params.front()),
        &((&(free_params.back()))[1]));

    BindFreeParams(visitor);

    if (!VisitPredicate(visitor, p + 1)) {
      return false;
    }

    visitor.ExitSelect(predicate, decl);

  // We have some bound parameters, and some free parameters. This is equivalent
  // to a `SELECT ... FROM table WHERE ...`, where we're selecting the free
  // parameters, but constraining them in terms of rows containing the bound
  // parameters.
  } else {
    assert(!free_params.empty());
    assert(!bound_params.empty());
    visitor.EnterFromWhereSelect(
        predicate, decl,
        &(bound_params.front()), &((&(bound_params.back()))[1]),
        &(free_params.front()), &((&(free_params.back()))[1]));

    BindFreeParams(visitor);

    if (!VisitPredicate(visitor, p + 1)) {
      return false;
    }

    visitor.ExitSelect(predicate, decl);
  }

  return true;
}

// Visit an assignment. Returns `true` if it was processed, and `false`
// if we cancelled things.
//
// TODO(pag): Add tracking to identify assignments to multiple constants?
bool SIPSGenerator::Impl::VisitAssign(
    SIPSVisitor &visitor, ParsedAssignment comparison) {
  auto const_id = GetFreshVarId();

  visitor.DeclareConstant(comparison.RHS(), const_id);

  const auto var = comparison.LHS();
  auto &var_set = equalities[var];
  if (var_set) {
    visitor.AssertEqual(var, var_set->Find()->id, var, const_id);
    var_set = DisjointSet::Union(var_set, vars[const_id].get());
  } else {
    var_set = vars[const_id].get();
  }

  return true;
}

// Visit a comparison. Returns `true` if it was processed. Returns `false`
// if this comparison was not processed. If it was not processed, then
// it is possible that it was cancelled, in which case `cancelled` may be
// set to `true`.
//
// TODO(pag): Try to maintain some kind of lattice to fallacies in the
//            comparisons?
bool SIPSGenerator::Impl::VisitCompare(
    SIPSVisitor &visitor, ParsedComparison comparison) {
  const auto lhs_var = comparison.LHS();
  const auto rhs_var = comparison.RHS();
  const auto lhs_is_bound = equalities.count(lhs_var);
  const auto rhs_is_bound = equalities.count(rhs_var);
  if (!lhs_is_bound && !rhs_is_bound) {
    return false;

  } else if (lhs_is_bound && rhs_is_bound) {
    auto &lhs_set = equalities[lhs_var];
    auto &rhs_set = equalities[rhs_var];

    lhs_set = lhs_set->Find();
    rhs_set = rhs_set->Find();

    switch (comparison.Operator()) {
      case ComparisonOperator::kEqual:
        if (lhs_set != rhs_set) {
          visitor.AssertEqual(lhs_var, lhs_set->id, rhs_var, rhs_set->id);
          lhs_set = DisjointSet::Union(lhs_set, rhs_set);
          rhs_set = lhs_set;
        }
        return true;

      case ComparisonOperator::kNotEqual:
        if (lhs_set != rhs_set) {
          visitor.AssertNotEqual(lhs_var, lhs_set->id, rhs_var, rhs_set->id);
          return true;

        } else {
          visitor.CancelComparison(comparison, lhs_set->id, rhs_set->id);
          cancelled = true;
          return false;
        }

      case ComparisonOperator::kLessThan:
        if (lhs_set != rhs_set) {
          visitor.AssertLessThan(lhs_var, lhs_set->id, rhs_var, rhs_set->id);
          return true;

        } else {
          visitor.CancelComparison(comparison, lhs_set->id, rhs_set->id);
          cancelled = true;
          return false;
        }

      case ComparisonOperator::kGreaterThan:
        if (lhs_set != rhs_set) {
          visitor.AssertGreaterThan(lhs_var, lhs_set->id, rhs_var, rhs_set->id);
          return true;

        } else {
          visitor.CancelComparison(comparison, lhs_set->id, rhs_set->id);
          cancelled = true;
          return false;
        }
    }

  } else if (ComparisonOperator::kEqual == comparison.Operator()) {
    if (lhs_is_bound) {
      equalities[rhs_var] = equalities[lhs_var];
      return true;
//      auto lhs_set = equalities[lhs_var];
//      auto var_id = GetFreshVarId();
//      auto rhs_set = vars[var_id].get();
//      visitor.DeclareVariable(rhs_var, var_id);
//      visitor.AssertEqual(lhs_var, lhs_set->id, rhs_var, rhs_set->id);
//      equalities.emplace(rhs_var, rhs_set);
//      DisjointSet::UnionInto(rhs_set, lhs_set);
//      return true;

    } else {
      equalities[lhs_var] = equalities[rhs_var];
      return true;
//      auto rhs_set = equalities[rhs_var];
//      auto var_id = GetFreshVarId();
//      auto lhs_set = vars[var_id].get();
//      visitor.DeclareVariable(lhs_var, var_id);
//      visitor.AssertEqual(lhs_var, lhs_set->id, rhs_var, rhs_set->id);
//      equalities.emplace(lhs_var, lhs_set);
//      DisjointSet::UnionInto(lhs_set, rhs_set);
//      return true;
    }

  } else {
    return false;
  }
}

// Try to visit all of the remaining comparisons. Returns `false` if
// this permutation is cancelled.
bool SIPSGenerator::Impl::VisitCompares(SIPSVisitor &visitor) {
  next_comparisons.clear();
  for (auto comparison : comparisons) {
    if (!VisitCompare(visitor, comparison)) {
      if (cancelled) {
        return false;
      } else {
        next_comparisons.push_back(comparison);
      }
    }
  }
  comparisons.swap(next_comparisons);
  return true;
}

// Visit an aggregate. Returns `true` if it was processed. Returns `false`
// if this aggregate was not processed. If it was not processed, then
// it is possible that it was cancelled, in which case `cancelled` may be
// set to `true`.
//
// The logic of visiting an aggregate is that we treat it kind of like
// visiting what is aggregated first, letting that establish certain variables,
// then going and trying to apply it to the aggregating functor.
bool SIPSGenerator::Impl::VisitAggregate(
    SIPSVisitor &visitor, ParsedAggregate aggregate, unsigned p, bool &ret) {

  ret = false;

  const auto summarized_predicate = aggregate.Predicate();
  auto summarized_decl = ParsedDeclaration::Of(summarized_predicate);
  failed_bindings.clear();
  if (!FindRedeclMatchingBindingConstraints(
      visitor, summarized_predicate, summarized_decl)) {
    return false;
  }

  // Create variable assignments.
  //
  // NOTE(pag): This doesn't modify `equalities`.
  CollectPredicateBoundAndFreeVars(
      visitor, summarized_decl, summarized_predicate);

  // Go and bind the free parameters. This in turn will affect the selection
  // of the aggregating functor.
  //
  // NOTE(pag): If we have `agg(A, A) over pred(A)` then we need to defer the
  //            assertions (`visitor.AssertEq`) until we're inside of the actual
  //            collection loop. Otherwise stuff doesn't make sense. This call
  //            will clear and fill `this->deferred_asserts`.
  //
  // NOTE(pag): The aggregating functor might take bound parameters from the
  //            outside, and it might take bound parameters from the summarized
  //            decl. If they are `bound` and from from the outside, they are
  //            "outer group by". If they are `bound` and from the inside, then
  //            they are "inner group by". The distinction is like: we group
  //            tuples by outer group by first, then make as many aggregator
  //            objects for the inner group by variables, which "configure" the
  //            aggregator objects, then for each summarized tuple, we collect
  //            the tuple, feeding in the `aggregate`-marked variables, and
  //            after we extract out the `summary`-marked variables from the
  //            aggregating functor.
  prev_equalities = equalities;
  BindFreeParams(visitor, true  /* defer_asserts */);
  summarized_bound_params.swap(bound_params);
  summarized_free_params.swap(free_params);

  // Go and find the redecl of the aggregating functor that matches the bound
  // parameters.
  const auto aggregate_functor = aggregate.Functor();
  const auto functor_arity = aggregate_functor.Arity();
  auto functor_decl = ParsedDeclaration::Of(aggregate_functor);
  failed_bindings.clear();
  if (!FindRedeclMatchingBindingConstraints(
      visitor, aggregate_functor, functor_decl)) {
    equalities.swap(prev_equalities);
    deferred_asserts.clear();
    return false;
  }

  CollectPredicateBoundAndFreeVars(visitor, functor_decl, aggregate_functor);

  // Make sure that every free parameter from the predicate being
  // summarized corresponds with an `aggregate`-attributed parameter to the
  // functor, and isn't "leaking" out to the rest of the world.
  //
  // While we're at it, we'll build up the set of collection parameters to
  // send into the aggregate for each iteration.
  aggregate_collection_params.clear();
  uint64_t seen = 0;
  assert(functor_arity < 64);
  for (const auto &fp : summarized_free_params) {
    for (auto i = 0u; i < functor_arity; ++i) {

      // Don't re-visit the same argument twice.
      if ((seen >> i) & 1) {
        continue;
      }

      const auto functor_arg = aggregate_functor.NthArgument(i);
      if (functor_arg == fp.var) {
        seen |= uint64_t(1) << i;

        const auto functor_param = functor_decl.NthParameter(i);
        const auto binding = functor_param.Binding();
        if (binding == ParameterBinding::kAggregate) {
          const auto functor_set = equalities[functor_arg];
          assert(functor_set != nullptr);
          aggregate_collection_params.emplace_back(
              functor_param, functor_arg, i, functor_set->Find()->id);
          goto check_next;

        // The free parameter does not correspond with an aggregate.
        //
        // TODO(pag): How to communicate this issue to a higher level? Is there
        //            ever a case where this condition is satisfiable in one
        //            permutation and not another?
        } else {
          equalities.swap(prev_equalities);
          deferred_asserts.clear();
          return false;
        }
      }
    }

    // The free parameter is not tied to anything in the functor, which means
    // it's unbounded. This corresponds with `A` in the following:
    //
    //    count_grouped_As(A, NumXs)
    //      : count(X, NumXs) over (@i32 A, @i32 X) { blah(X). }.
    //
    // This pattern is feasibly like a "group by A". Another way of thinking of
    // it is that we want to have a distinct aggregation object per unique A.
    //
    // Given that the semantics are more closely tied to a group by where each
    // grouped aggregator is distinct, it doesn't make sense to have `A` free,
    // thus requiring some unknown number of groups. Thus, we require that `A`
    // in this case be bound, rather than free.
    //
    // The best we can do is return `false` and hope that applying some other
    // predicate in the permutation will bind the needed variables.
    equalities.swap(prev_equalities);
    deferred_asserts.clear();
    return false;

  check_next:
    continue;
  }

  // Go collect `bound`-attributed parameters to the aggregating functor. These
  // can be thought of as configuration arguments. That is, for each unique
  // set of these `bound`-attributed parameters, we have a distinct aggregation
  // object.
  aggregate_input_params.clear();
  size_t num_aggregate_params = 0;
  size_t num_summary_params = 0;
  seen = 0;
  for (auto i = 0u; i < functor_arity; ++i) {
    auto functor_arg = aggregate_functor.NthArgument(i);
    switch (functor_decl.NthParameter(i).Binding()) {
      case ParameterBinding::kBound: {
        const SIPSVisitor::Column *col = nullptr;
        for (auto &bp : bound_params) {
          if (bp.n == i) {
            assert(!((seen >> i) & 1));
            seen |= uint64_t(i) << i;
            col = &bp;
            break;
          }
        }
        assert(col != nullptr);
        aggregate_input_params.emplace_back(*col);
      }

      case ParameterBinding::kAggregate:
        ++num_aggregate_params;
        break;

      case ParameterBinding::kSummary:
        ++num_summary_params;
        if (equalities.count(functor_arg)) {
          equalities.swap(prev_equalities);
          deferred_asserts.clear();
          return false;  // There should not be a binding for the summary yet.
        }
        break;

      case ParameterBinding::kMutable:
      case ParameterBinding::kFree:
      case ParameterBinding::kImplicit:
        assert(false);
        equalities.swap(prev_equalities);
        deferred_asserts.clear();
        return false;
    }
  }

  // There are more `aggregate`-attributed parameters than there are free
  // parameters flowing out of the predicate being summarized.
  //
  // TODO(pag): How to report this to a higher level?
  if (summarized_free_params.size() < aggregate_collection_params.size()) {
    equalities.swap(prev_equalities);
    deferred_asserts.clear();
    return false;
  }

  // The number of `summary`-attributed parameters of the functor does not
  // correspond with the number of free parameters that are coming out of the
  // summarizing functor application.
  if (num_summary_params != free_params.size()) {
    equalities.swap(prev_equalities);
    deferred_asserts.clear();
    return false;
  }

  // The `aggregate_collection_params`, which are the `aggregate`-attributed
  // parameters, and the `aggregate_input_params`, which are the `bound`-
  // attributed parameters, should fully cover the set of all bound parameters
  // to the aggregating functor.
  assert((aggregate_collection_params.size() + aggregate_input_params.size()) ==
         bound_params.size());

  // NOTE(pag): The number of parameters that the summary is sending to the
  //            aggregate doesn't need match what the aggregate needs. This is
  //            the case where we want more than one aggregation objects, one
  //            instance per grouping of these bound params.
  assert(num_aggregate_params >= aggregate_collection_params.size());
  if (aggregate_collection_params.empty()) {
    equalities.swap(prev_equalities);
    deferred_asserts.clear();
    return false;
  }

  inner_group_by_params.clear();
  outer_group_by_params.clear();

  // Now partition the incoming bound parameters to the predicate being
  // summarized into those which contribute to the functor's `bound` parameters,
  // (inner group by) and those which flowing in from the outside for the
  // sake of restricting the scope of the aggregation (outer group by) to some
  // subset of the tuples present in the summarized predicate's relation.
  //
  // TODO(pag): Think about a case like:
  //
  //    foo(A)
  //      : node(A)
  //      , count(A, B, Count) over node(B).
  for (const auto &col : summarized_bound_params) {
    bool is_inner = false;
    for (const auto &agg_bound_col : aggregate_input_params) {
      if (col.var == agg_bound_col.var) {
        is_inner = true;
        break;
      }
    }

    if (is_inner) {
      inner_group_by_params.push_back(col);
    } else {
      outer_group_by_params.push_back(col);
    }
  }

  SIPSVisitor::Column *outer_group_begin = nullptr;
  SIPSVisitor::Column *outer_group_end = nullptr;

  SIPSVisitor::Column *inner_group_begin = nullptr;
  SIPSVisitor::Column *inner_group_end = nullptr;

  SIPSVisitor::Column *aggregate_begin = nullptr;
  SIPSVisitor::Column *aggregate_end = nullptr;

  SIPSVisitor::Column *config_begin = nullptr;
  SIPSVisitor::Column *config_end = nullptr;

  SIPSVisitor::Column *collect_begin = nullptr;
  SIPSVisitor::Column *collect_end = nullptr;

  SIPSVisitor::Column *summary_begin = nullptr;
  SIPSVisitor::Column *summary_end = nullptr;

  // `bound`-attributed arguments to the aggregating functor.
  if (!aggregate_input_params.empty()) {
    config_begin = &(aggregate_input_params.front());
    config_end = &((&(aggregate_input_params.back()))[1]);
  }

  // `aggregate`-attributed arguments to the aggregating functor.
  if (!aggregate_collection_params.empty()) {
    collect_begin = &(aggregate_collection_params.front());
    collect_end = &((&(aggregate_collection_params.back()))[1]);
  }

  // `summary`-attributed arguments to the aggregating functor.
  if (!free_params.empty()) {
    summary_begin = &(free_params.front());
    summary_end = &((&(free_params.back()))[1]);
  }

  // Bound parameters to the predicate being summarized that do not correspond
  // with `bound`-attributed parameters to the summarizing functor. These are
  // coming from the clause "outside" of the aggregation.
  if (!outer_group_by_params.empty()) {
    outer_group_begin = &(outer_group_by_params.front());
    outer_group_end = &((&(outer_group_by_params.back()))[1]);
  }

  // Bound parameters to the predicate being summarized that correspond with
  // `bound`-attributed parameters to the summarizing functor. These are
  // coming from the clause "outside" of the aggregation.
  if (!inner_group_by_params.empty()) {
    inner_group_begin = &(inner_group_by_params.front());
    inner_group_end = &((&(inner_group_by_params.back()))[1]);
  }

  // Free parameters produced by the predicate being summarized, and which
  // will feed into the `aggregate`-attribued parameters of the functor.
  if (!summarized_free_params.empty()) {
    aggregate_begin = &(summarized_free_params.front());
    aggregate_end = &((&(summarized_free_params.back()))[1]);
  }

  // Tell the visitor that we're going to enter an aggregation, and pass it the
  // bound (not aggregate) inputs, if any, as well as the bound parameters that
  // get passed along to the inner predicate.
  visitor.EnterAggregation(
      aggregate_functor, functor_decl,
      config_begin, config_end,
      collect_begin, collect_end,
      summary_begin, summary_end,
      summarized_predicate, summarized_decl,
      outer_group_begin, outer_group_end,
      inner_group_begin, inner_group_end,
      aggregate_begin, aggregate_end);

  if (summarized_bound_params.empty() && summarized_free_params.empty()) {
    assert(false);  // Not possible.
    equalities.swap(prev_equalities);
    deferred_asserts.clear();
    return false;

  // We only have bound parameters. This is equivalent to asking if a tuple
  // is present, i.e. if a certain row exists in a table.
  } else if (summarized_free_params.empty()) {
    assert(!summarized_bound_params.empty());

    visitor.AssertPresent(
        summarized_decl, summarized_predicate,
        &(summarized_bound_params.front()),
        &((&(summarized_bound_params.back()))[1]));

    ApplyDeferredAsserts(visitor);

    visitor.Collect(
        aggregate_functor, functor_decl,
        outer_group_begin, outer_group_end,
        inner_group_begin, inner_group_end,
        aggregate_begin, aggregate_end);

  // We only have free parameters. This is equivalent to a full table scan.
  } else if (summarized_bound_params.empty()) {
    assert(!summarized_free_params.empty());

    visitor.EnterFromSelect(
        summarized_predicate, summarized_decl,
        aggregate_begin, aggregate_end);

    ApplyDeferredAsserts(visitor);

    visitor.Collect(
        aggregate_functor, functor_decl,
        outer_group_begin, outer_group_end,
        inner_group_begin, inner_group_end,
        aggregate_begin, aggregate_end);

    visitor.ExitSelect(summarized_predicate, summarized_decl);

  // We have some bound parameters, and some free parameters. This is equivalent
  // to a `SELECT ... FROM table WHERE ...`, where we're selecting the free
  // parameters, but constraining them in terms of rows containing the bound
  // parameters.
  } else {
    assert(!summarized_bound_params.empty());
    assert(!summarized_free_params.empty());
    visitor.EnterFromWhereSelect(
        summarized_predicate, summarized_decl,
        &(summarized_bound_params.front()),
        &((&(summarized_bound_params.back()))[1]),
        aggregate_begin, aggregate_end);

    ApplyDeferredAsserts(visitor);

    visitor.Collect(
        aggregate_functor, functor_decl,
        outer_group_begin, outer_group_end,
        inner_group_begin, inner_group_end,
        aggregate_begin, aggregate_end);

    visitor.ExitSelect(summarized_predicate, summarized_decl);
  }

  // Finally, undefine the free parameters that were related to what we
  // summarized as they are no longer "in scope", and should not be visible
  // to anything else.
  for (const auto &param : summarized_free_params) {
    equalities.erase(param.var);
  }

  visitor.EnterSelectFromSummary(
      aggregate_functor, functor_decl,
      outer_group_begin, outer_group_end,
      config_begin, config_end,
      collect_begin, collect_end,
      summary_begin, summary_end);

  // Bind the free parameters of the functor. These are the summarized
  // variables.
  BindFreeParams(visitor);
  if (VisitPredicate(visitor, p)) {
    if (!cancelled) {
      visitor.ExitSelect(aggregate_functor, functor_decl);
      ret = true;
    }
  }

  return true;
}

// Try to visit all of the remaining aggregates. Returns `false` if
// this permutation is cancelled.
//
// This is a bit of a funny function. It returns `true` if any aggregate was
// successfully applied, which means that a call to `VisitAggregate` is now
// "taking over" the call to "VisitPredicate" which originally invoked it. We
// need to track which predicates have been and have not been applied, which we
// do with `aggregates_processed`.
bool SIPSGenerator::Impl::VisitAggregates(SIPSVisitor &visitor,
                                          unsigned p, bool &ret) {
  for (auto i = 0u; i < aggregates.size(); ++i) {
    if (aggregates_processed[i]) {
      continue;
    }

    aggregates_processed[i] = true;  // Assume it is.
    // (*gOut) << "p=" << p << " trying aggregate: " << aggregates[i] << '\n';
    if (VisitAggregate(visitor, aggregates[i], p, ret)) {
      return true;
    }
    // (*gOut) << "p=" << p << " aggregate failed: " << aggregates[i] << '\n';
    aggregates_processed[i] = false;
  }

  return false;
}

bool SIPSGenerator::Impl::Visit(hyde::SIPSVisitor &visitor) {
  if (cancelled) {
    return false;
  }

  next_var_id = 0;
  vars.clear();
  equalities.clear();
  comparisons.clear();
  aggregates.clear();

  if (assumption) {
    const auto initial_pred = *assumption;
    visitor.Begin(initial_pred);

    // Create input variables for the initial declaration.
    const auto decl = ParsedDeclaration::Of(initial_pred);
    for (auto param : decl.Parameters()) {
      const auto var_id = GetFreshVarId();
      SIPSVisitor::Column col(
          param, initial_pred.NthArgument(var_id), var_id, var_id);
      visitor.DeclareParameter(col);
    }

    // Now bind the arguments to the initial parameters.
    for (auto i = 0u; i < initial_pred.Arity(); ++i) {
      const auto var = initial_pred.NthArgument(i);
      const auto param_set = vars[i]->Find();
      auto &goal_set = equalities[var];

      if (!goal_set) {
        goal_set = param_set;

      // We are assuming something like `pred(A, A)`. We create parameters like
      // `pred(P0, P1)`, and then assign `P0=A`, and now discover that `P1=A` as
      // well.
      } else {
        visitor.AssertEqual(var, goal_set->Find()->id, var, param_set->id);
        goal_set = DisjointSet::Union(goal_set, param_set);
      }
    }

  } else {
    visitor.Begin(clause);

    const auto decl = ParsedDeclaration::Of(clause);

    // If this clause has bound parameters, then declare them.
    for (auto param : decl.Parameters()) {
      if (param.Binding() == ParameterBinding::kBound) {
        const auto var_id = GetFreshVarId();
        const auto var = clause.NthParameter(param.Index());
        SIPSVisitor::Column col(param, var, param.Index(), var_id);
        visitor.DeclareParameter(col);

        const auto param_set = vars[var_id]->Find();
        auto &goal_set = equalities[var];
        if (!goal_set) {
          goal_set = param_set;
        } else {
          visitor.AssertEqual(var, goal_set->Find()->id, var, param_set->id);
          goal_set = DisjointSet::Union(goal_set, param_set);
        }
      }
    }
  }

  // Bind all assigned variables to their respective constants.
  for (auto assignment : clause.Assignments()) {
    if (!VisitAssign(visitor, assignment)) {
      advance_type = visitor.Advance();
      return false;
    }
  }

  for (auto comparison : clause.Comparisons()) {
    comparisons.push_back(comparison);
  }

  for (auto aggregate : clause.Aggregates()) {
    aggregates.push_back(aggregate);
  }

  aggregates_processed.clear();
  aggregates_processed.resize(aggregates.size(), false);

  negative_predicates.clear();
  for (auto pred : clause.NegatedPredicates()) {
    negative_predicates.push_back(pred);
  }

  // NOTE(pag): This will visit comparisons, negations, and aggregates.
  if (!VisitPredicate(visitor, 0)) {
    advance_type = visitor.Advance();
    return false;
  }

  if (!cancelled) {
    if (assumption) {
      visitor.Commit(*assumption);
    } else {
      visitor.Commit(clause);
    }
  }

  advance_type = visitor.Advance();
  return !cancelled;
}

bool SIPSGenerator::Impl::Advance(void) {
  if (positive_predicates.empty()) {
    cancelled = true;
    return false;
  }

  switch (advance_type) {
    case SIPSVisitor::kTryNextPermutation:
      cancelled = !std::next_permutation(
          positive_predicates.begin(), positive_predicates.end(),
          OrderPredicates);
      return !cancelled;

    case SIPSVisitor::kRetryCurrentPermutation:
      return true;

    case SIPSVisitor::kStop:
      cancelled = true;
      // (*gOut) << "stopped\n";
      return false;
  }

  return !cancelled;
}

SIPSGenerator::~SIPSGenerator(void) {}

SIPSGenerator::SIPSGenerator(ParsedPredicate assumption_)
    : impl(std::make_unique<Impl>(assumption_)) {}

// Visit a clause without any assumptions.
SIPSGenerator::SIPSGenerator(ParsedClause clause)
    : impl(std::make_unique<Impl>(clause)) {}

// Visit the current ordering. Returns `true` if the `visitor.Commit`
// was invoked, and `false` if `visitor.Cancel` was invoked.
bool SIPSGenerator::Visit(SIPSVisitor &visitor) const {
  impl->started = true;
  return impl->Visit(visitor);
}

// Tries to advance to the next possible ordering. Returns `false` if
// we could not advance to the next ordering.
bool SIPSGenerator::Advance(void) const {
  impl->started = true;
  return impl->Advance();
}

// Reset the generator to be beginning.
void SIPSGenerator::Rewind(void) {
  if (impl->started) {
    if (impl->assumption) {
      impl.reset(new Impl(*(impl->assumption)));
    } else {
      impl.reset(new Impl(impl->clause));
    }
  }
}

}  // namespace hyde
