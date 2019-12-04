/*
 * Copyright (c) 2019 Trail of Bits, Inc.
 */

#include <drlojekyll/Sema/SIPSAnalysis.h>

#include <algorithm>
#include <cassert>
#include <unordered_map>
#include <vector>

namespace hyde {
namespace {

static bool OrderPredicates(ParsedPredicate a, ParsedPredicate b) noexcept {
  return a.UniqueId() < b.UniqueId();
}

// Implements union-find for
class DisjointSet {
 public:
  DisjointSet(unsigned id_)
      : parent(this),
        id(id_) {}

  DisjointSet *Find(void) {
    if (parent == this) {
      return this;
    } else {
      parent = parent->Find();
      return parent;
    }
  }

  static DisjointSet *Union(DisjointSet *lhs, DisjointSet *rhs) {
    lhs = lhs->Find();
    rhs = rhs->Find();
    if (lhs == rhs) {
      return lhs;

    } else if (lhs->id > rhs->id) {
      lhs->parent = rhs;
      return rhs;

    } else {
      rhs->parent = lhs;
      return lhs;
    }
  }

  DisjointSet *parent;
  const unsigned id;
};

// A simple, tentative visitor that exists to detect if visiting was cancelled.
class TentativeSIPSVisitor final : public SIPSVisitor {
 public:
  virtual ~TentativeSIPSVisitor(void) {}

  void CancelComparison(ParsedComparison, unsigned, unsigned) override {
    cancelled = true;
  }

  void CancelRangeRestriction(ParsedComparison, ParsedVariable) override {
    cancelled = true;
  }

  void CancelPredicate(const FailedBinding *, FailedBinding *) override {
    cancelled = true;
  }

  bool WasCancelled(void) const {
    return cancelled;
  }

 private:
  bool cancelled{false};
};

}  // namespace

SIPSVisitor::~SIPSVisitor(void) {}
void SIPSVisitor::Begin(ParsedPredicate) {}
void SIPSVisitor::DeclareParameter(ParsedParameter, ParsedVariable, unsigned) {}
void SIPSVisitor::DeclareVariable(ParsedVariable, unsigned) {}
void SIPSVisitor::DeclareConstant(ParsedLiteral, unsigned) {}
void SIPSVisitor::AssertEqual(unsigned, unsigned) {}
void SIPSVisitor::AssertNotEqual(unsigned, unsigned) {}
void SIPSVisitor::AssertLessThan(unsigned, unsigned) {}
void SIPSVisitor::AssertGreaterThan(unsigned, unsigned) {}
void SIPSVisitor::Assign(unsigned, unsigned) {}
void SIPSVisitor::Commit(void) {}
void SIPSVisitor::CancelComparison(ParsedComparison, unsigned, unsigned) {}
void SIPSVisitor::CancelRangeRestriction(ParsedComparison, ParsedVariable) {}
void SIPSVisitor::CancelPredicate(const FailedBinding *, FailedBinding *) {}

class SIPSGenerator::Impl {
 public:
  Impl(ParsedClause clause_, ParsedPredicate assumption_);

  // Visit an ordering of the predicates.
  bool Visit(SIPSVisitor &visitor);

  // Try to advance the generator.
  bool Advance(void);

 private:

  // Try to apply the negated predicates as eagerly as possible.
  void VisitNegatedPredicates(SIPSVisitor &visitor);

  // Visit the end of a clause body.
  bool VisitEndOfClause(SIPSVisitor &visitor);

  // Visit a predicate. Returns `true` if it was processed, and `false`
  // if we cancelled things.
  bool VisitPredicate(SIPSVisitor &visitor, unsigned index);

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
  bool VisitAggregate(SIPSVisitor &visitor, ParsedAggregate aggregate);

  // Try to visit all of the remaining aggregates. Returns `false` if
  // this permutation is cancelled.
  bool VisitAggregates(SIPSVisitor &visitor);

  // The clause being analyzed.
  const ParsedClause clause;

  // The predicate which we are assuming has been provided and comes with
  // concrete data.
  const ParsedPredicate assumption;

  // The vector of positive predicates which we will evaluate.
  std::vector<ParsedPredicate> positive_predicates;

  // The vector of negative predicates, which we will evaluate if their
  // parameters are all bound.
  std::vector<ParsedPredicate> negative_predicates;
  std::vector<ParsedPredicate> next_negative_predicates;

  // Does the visitor want to advance?
  bool try_advance{true};

  bool cancelled{false};
  unsigned next_var_id{0};

  std::vector<std::unique_ptr<DisjointSet>> vars;
  std::unordered_map<ParsedVariable, DisjointSet *> equalities;

  std::vector<ParsedComparison> comparisons;
  std::vector<ParsedComparison> next_comparisons;

  std::vector<ParsedAggregate> aggregates;
  std::vector<ParsedAggregate> next_aggregates;

  std::vector<SIPSVisitor::FailedBinding> failed_bindings;

  std::vector<unsigned> bound_params;
  std::vector<unsigned> free_params;
  std::vector<unsigned> aggregate_params;
  std::vector<unsigned> summary_params;
};

SIPSGenerator::Impl::Impl(ParsedClause clause_, ParsedPredicate assumption_)
    : clause(clause_),
      assumption(assumption_) {
  assert(clause == ParsedClause::Containing(assumption));

  // The `assumption` predicate is kind of like a left corner in a parse. We
  // are assuming we have it and that all of its variables are bound, because
  // with the stepper, we're simulating a bottom-up execution. Thus we want
  // to exclude the assumption from the list of tested predicates that will
  // influence its SIP score and binding environment.
  for (auto pred : clause.PositivePredicates()) {
    assert(pred.IsPositive());
    if (pred != assumption) {
      positive_predicates.push_back(pred);
    }
  }

  std::sort(positive_predicates.begin(), positive_predicates.end(),
            OrderPredicates);
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

    bound_params.clear();
    for (auto pred_arg : predicate.Arguments()) {
      bound_params.push_back(equalities[pred_arg]->Find()->id);
    }

    visitor.AssertAbsent(
        predicate,
        &(bound_params.front()),
        &((&(bound_params.back()))[1]));

  next_negated_predicate:
    continue;
  }
  negative_predicates.swap(next_negative_predicates);
}

// Visit the end of a clause body.
bool SIPSGenerator::Impl::VisitEndOfClause(SIPSVisitor &visitor) {

  bound_params.clear();
  failed_bindings.clear();

  for (auto clause_arg : clause.Parameters()) {
    if (!equalities.count(clause_arg)) {
      visitor.CancelRangeRestriction(clause, clause_arg);
      cancelled = true;
      return false;

    } else {
      bound_params.push_back(equalities[clause_arg]->Find()->id);
    }
  }

  // There remain one or more negated predicates that have not yet
  // been invoked; report on the cancellation reason.
  for (auto predicate : negative_predicates) {
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

  if (!cancelled) {

  }

  return !cancelled;
}

// Visit a predicate. Returns `true` if it was processed, and `false`
// if we cancelled things.
bool SIPSGenerator::Impl::VisitPredicate(
    SIPSVisitor &visitor, unsigned p) {

  if (!VisitCompares(visitor)) {
    return false;
  }

  VisitNegatedPredicates(visitor);

  if (!VisitAggregates(visitor)) {
    return false;
  }

  // We've bottomed out; we're done visiting this clause body.
  if (p >= positive_predicates.size()) {
    return VisitEndOfClause(visitor);
  }

  const auto predicate = positive_predicates[p];
  assert(predicate != assumption);
  assert(predicate.IsPositive());

  const auto arity = predicate.Arity();
  auto decl = ParsedDeclaration::Of(predicate);
  assert(!decl.IsMessage());

  // For functors and queries, we care about the precise choice in order
  // to meet a binding constraint of one of the redeclarations. For
  // functors, we need to exactly match the usage specification. For
  // queries, we need to match the `bound` arguments, and the `free`
  // ones "cost" us in implicit later binding.
  if (decl.IsFunctor() || decl.IsQuery()) {
    failed_bindings.clear();

    for (auto redecl : decl.Redeclarations()) {
      for (auto i = 0U; i < arity; ++i) {
        const auto redecl_param = redecl.NthParameter(i);
        const auto redecl_param_binding = redecl_param.Binding();
        const auto pred_arg = predicate.NthArgument(i);
        const auto pred_arg_is_bound = equalities.count(pred_arg);

        // Make sure we meet the binding constraint for at least one of
        // the redeclarations of this functor or query.
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
      goto found_acceptable_decl;

    try_next_redecl:
      continue;
    }

    visitor.CancelPredicate(
        &(failed_bindings.front()),
        &((&(failed_bindings.back()))[1]));
    cancelled = true;
    return false;
  }

found_acceptable_decl:

  bound_params.clear();
  free_params.clear();
  aggregate_params.clear();
  summary_params.clear();

  for (auto i = 0U; i < arity; ++i) {
    const auto var = predicate.NthArgument(i);
    const auto var_is_bound = equalities.count(var);

    switch (decl.NthParameter(i).Binding()) {
      case ParameterBinding::kImplicit:
        if (var_is_bound) {
          bound_params.push_back(i);
        } else {
          free_params.push_back(i);
        }
        break;

      case ParameterBinding::kFree:
        assert(!var_is_bound);
        free_params.push_back(i);
        break;

      case ParameterBinding::kBound:
        assert(var_is_bound);
        bound_params.push_back(i);
        break;

      case ParameterBinding::kAggregate:
        assert(false);  // Shouldn't be possible.
        assert(var_is_bound);
        aggregate_params.push_back(i);
        break;

      case ParameterBinding::kSummary:
        assert(false);  // Shouldn't be possible.
        assert(!var_is_bound);
        summary_params.push_back(i);
        break;
    }
  }

  if (!VisitPredicate(visitor, p + 1)) {
    return false;
  }

  return true;
}

// Visit an assignment. Returns `true` if it was processed, and `false`
// if we cancelled things.
//
// TODO(pag): Add tracking to identify assignments to multiple constants?
bool SIPSGenerator::Impl::VisitAssign(
    SIPSVisitor &visitor, ParsedAssignment comparison) {
  auto const_id = next_var_id++;
  auto const_set = new DisjointSet(const_id);
  vars.emplace_back(const_set);

  visitor.DeclareConstant(comparison.RHS(), const_id);

  const auto var = comparison.LHS();
  auto &var_set = equalities[var];
  if (var_set) {
    visitor.AssertEqual(var_set->Find()->id, const_id);
    var_set = DisjointSet::Union(var_set, const_set);
  } else {
    var_set = const_set;
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
  auto lhs_is_bound = equalities.count(comparison.LHS());
  auto rhs_is_bound = equalities.count(comparison.RHS());
  if (!lhs_is_bound && !rhs_is_bound) {
    return false;

  } else if (lhs_is_bound && rhs_is_bound) {
    auto &lhs_set = equalities[comparison.LHS()];
    auto &rhs_set = equalities[comparison.RHS()];

    lhs_set = lhs_set->Find();
    rhs_set = rhs_set->Find();

    switch (comparison.Operator()) {
      case ComparisonOperator::kEqual:
        if (lhs_set != rhs_set) {
          visitor.AssertEqual(lhs_set->id, rhs_set->id);
          lhs_set = DisjointSet::Union(lhs_set, rhs_set);
          rhs_set = lhs_set;
        }
        return true;

      case ComparisonOperator::kNotEqual:
        if (lhs_set != rhs_set) {
          visitor.AssertNotEqual(lhs_set->id, rhs_set->id);
          return true;

        } else {
          visitor.CancelComparison(comparison, lhs_set->id, rhs_set->id);
          cancelled = true;
          return false;
        }

      case ComparisonOperator::kLessThan:
        if (lhs_set != rhs_set) {
          visitor.AssertLessThan(lhs_set->id, rhs_set->id);
          return true;

        } else {
          visitor.CancelComparison(comparison, lhs_set->id, rhs_set->id);
          cancelled = true;
          return false;
        }

      case ComparisonOperator::kGreaterThan:
        if (lhs_set != rhs_set) {
          visitor.AssertGreaterThan(lhs_set->id, rhs_set->id);
          return true;

        } else {
          visitor.CancelComparison(comparison, lhs_set->id, rhs_set->id);
          cancelled = true;
          return false;
        }
    }

  } else if (ComparisonOperator::kEqual == comparison.Operator()) {
    if (lhs_is_bound) {
      equalities[comparison.RHS()] = equalities[comparison.LHS()];
      return true;

    } else {
      equalities[comparison.LHS()] = equalities[comparison.RHS()];
      return true;
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

// Visit a comparison. Returns `true` if it was processed. Returns `false`
// if this comparison was not processed. If it was not processed, then
// it is possible that it was cancelled, in which case `cancelled` may be
// set to `true`.
bool SIPSGenerator::Impl::VisitAggregate(
    SIPSVisitor &visitor, ParsedAggregate aggregate) {

  return true;
}

// Try to visit all of the remaining aggregates. Returns `false` if
// this permutation is cancelled.
bool SIPSGenerator::Impl::VisitAggregates(SIPSVisitor &visitor) {
  next_aggregates.clear();
  for (auto aggregate : aggregates) {
    if (!VisitAggregate(visitor, aggregate)) {
      if (cancelled) {
        return false;
      } else {
        next_aggregates.push_back(aggregate);
      }
    }
  }
  aggregates.swap(next_aggregates);
  return true;
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

  visitor.Begin(assumption);

  // Create input variables for the initial declaration.
  const auto decl = ParsedDeclaration::Of(assumption);
  for (auto param : decl.Parameters()) {
    auto var_id = next_var_id++;
    visitor.DeclareParameter(param, assumption.NthArgument(var_id), var_id);
    vars.emplace_back(new DisjointSet(var_id));
  }

  // Now bind the arguments to the initial parameters.
  for (auto i = 0u; i < assumption.Arity(); ++i) {
    const auto var = assumption.NthArgument(i);
    const auto param_set = vars[i]->Find();
    auto &goal_set = equalities[var];

    if (!goal_set) {
      goal_set = param_set;

    // We are assuming something like `pred(A, A)`. We create parameters like
    // `pred(P0, P1)`, and then assign `P0=A`, and now discover that `P1=A` as
    // well.
    } else {
      visitor.AssertEqual(goal_set->Find()->id, param_set->id);
      goal_set = DisjointSet::Union(goal_set, param_set);
    }
  }

  // Bind all assigned variables to their respective constants.
  for (auto assignment : clause.Assignments()) {
    if (!VisitAssign(visitor, assignment)) {
      try_advance = visitor.Advance();
      return false;
    }
  }

  for (auto comparison : clause.Comparisons()) {
    comparisons.push_back(comparison);
  }

  for (auto aggregate : clause.Aggregates()) {
    aggregates.push_back(aggregate);
  }

  negative_predicates.clear();
  for (auto pred : clause.NegatedPredicates()) {
    negative_predicates.push_back(pred);
  }

  // NOTE(pag): This will visit comparisons.
  if (VisitPredicate(visitor, 0)) {

    // Make sure all compared variables are range-restricted.
    if (!comparisons.empty()) {
      cancelled = true;

      for (auto comparison : comparisons) {
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
    }

    if (!aggregates.empty()) {
      cancelled = true;
    }
  }

  try_advance = visitor.Advance();
  return !cancelled;
}

bool SIPSGenerator::Impl::Advance(void) {
  if (!try_advance || positive_predicates.empty()) {
    cancelled = true;
    return false;
  }

  cancelled = !std::next_permutation(
      positive_predicates.begin(), positive_predicates.end(),
      OrderPredicates);

  return !cancelled;
}

SIPSGenerator::SIPSGenerator(ParsedClause clause_, ParsedPredicate assumption_)
    : impl(std::make_unique<Impl>(clause_, assumption_)) {}

// Visit the current ordering. Returns `true` if the `visitor.Commit`
// was invoked, and `false` if `visitor.Cancel` was invoked.
bool SIPSGenerator::Visit(SIPSVisitor &visitor) const {
  return impl->Visit(visitor);
}

// Tries to advance to the next possible ordering. Returns `false` if
// we could not advance to the next ordering.
bool SIPSGenerator::Advance(void) const {
  return impl->Advance();
}

}  // namespace hyde
