// Copyright 2019, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/Parse/Parse.h>

#include <memory>

namespace hyde {

// Visits a clause body in left-to-right order.
class SIPSVisitor {
 public:

  enum AdvanceType {
    kTryNextPermutation,
    kRetryCurrentPermutation,
    kStop
  };

  struct FailedBinding {
    inline FailedBinding(ParsedDeclaration declaration_,
                         ParsedPredicate predicate_,
                         ParsedVariable unbound_var_,
                         ParsedParameter bound_parameter_)
        : declaration(declaration_),
          predicate(predicate_),
          unbound_var(unbound_var_),
          bound_parameter(bound_parameter_) {}

    const ParsedDeclaration declaration;
    const ParsedPredicate predicate;
    const ParsedVariable unbound_var;
    const ParsedParameter bound_parameter;
  };

  struct ParamColumn {
    inline ParamColumn(ParsedParameter param_, ParsedVariable var_,
                       unsigned n_, unsigned id_)
        : param(param_),
          var(var_),
          n(n_),
          id(id_) {}

    const ParsedParameter param;
    const ParsedVariable var;
    const unsigned n;  // This is the `n`th parameter.
    const unsigned id;
  };

  struct VarColumn {
    inline VarColumn(ParsedVariable var_, unsigned id_)
        : var(var_),
          id(id_) {}

    const ParsedVariable var;
    const unsigned id;
  };

  virtual ~SIPSVisitor(void);

  // Notify the visitor that we're about to begin visiting a clause body, with
  // `assumption` being taken as present/true.
  virtual void Begin(ParsedPredicate assumption);

  // Notify the visitor that we're abbout to begin visiting a clause body, with
  // no assumptions holding.
  virtual void Begin(ParsedClause clause);

  // Declares a concrete parameter identified by `id`.
  virtual void DeclareParameter(const ParamColumn &col);

  // Declares a variable identified by `id`.
  virtual void DeclareVariable(ParsedVariable var, unsigned id);

  // Declares a constant identified by `id`.
  virtual void DeclareConstant(ParsedLiteral val, unsigned id);

  // Asserts that a zero-arity exported predicate must be assumed `true` in
  // this clause.
  virtual void AssertTrue(ParsedPredicate pred, ParsedExport cond_var);

  // Asserts that a zero-arity exported predicate must be assumed `false`
  // (missing) in this clause.
  virtual void AssertFalse(ParsedPredicate pred, ParsedExport cond_var);

  // Asserts that the value of the variable identified by `lhs_id` must match
  // the value of the variable identified by `rhs_id`.
  virtual void AssertEqual(ParsedVariable lhs_var, unsigned lhs_id,
                           ParsedVariable rhs_var, unsigned rhs_id);

  // Asserts that the value of the variable identified by `lhs_id` must NOT
  // match the value of the variable identified by `rhs_id`.
  virtual void AssertNotEqual(ParsedVariable lhs_var, unsigned lhs_id,
                              ParsedVariable rhs_var, unsigned rhs_id);

  // Asserts that the value of the variable identified by `lhs_id` must be less
  // than the value of the variable identified by `rhs_id`.
  virtual void AssertLessThan(ParsedVariable lhs_var, unsigned lhs_id,
                              ParsedVariable rhs_var, unsigned rhs_id);

  // Asserts that the value of the variable identified by `lhs_id` must be
  // greater than the value of the variable identified by `rhs_id`.
  virtual void AssertGreaterThan(ParsedVariable lhs_var, unsigned lhs_id,
                                 ParsedVariable rhs_var, unsigned rhs_id);

  // Asserts the presence of some tuple. This is for positive predicates. Uses
  // the variable ids in the range `[begin_id, end_id)`.
  virtual void AssertPresent(
      ParsedDeclaration decl, ParsedPredicate pred, const ParamColumn *begin,
      const ParamColumn *end);

  // Asserts the absence of some tuple. This is for negative predicates. Uses
  // the variable ids in the range `[begin_id, end_id)`.
  virtual void AssertAbsent(
      ParsedDeclaration decl, ParsedPredicate pred, const ParamColumn *begin,
      const ParamColumn *end);

  // Tell the visitor that we're going to insert into a table.
  virtual void Insert(
      ParsedClause clause,
      ParsedDeclaration decl,
      const ParamColumn *begin,
      const ParamColumn *end,
      const VarColumn *bound_begin,
      const VarColumn *bound_end);

  // Selects some columns from a predicate where some of the column values are
  // fixed.
  virtual void EnterFromWhereSelect(
      ParsedPredicate pred, ParsedDeclaration from,
      const ParamColumn *where_begin, const ParamColumn *where_end,
      const ParamColumn *select_begin, const ParamColumn *select_end);

  // Selects some columns from a predicate where some of the column values are
  // fixed.
  virtual void EnterFromSelect(
      ParsedPredicate pred, ParsedDeclaration from,
      const ParamColumn *select_begin, const ParamColumn *select_end);

  // Exits the a selection.
  virtual void ExitSelect(ParsedPredicate pred, ParsedDeclaration from);

  // Enter into the aggregate collection phase. Pass in the bound arguments
  // that are not themselves being summarized or aggregates.
  virtual void EnterAggregation(
      ParsedPredicate functor, ParsedDeclaration functor_decl,
      const ParamColumn *bound_begin, const ParamColumn *bound_end,
      const ParamColumn *aggregate_begin, const ParamColumn *aggregate_end,
      const ParamColumn *summary_begin, const ParamColumn *summary_end,
      ParsedPredicate predicate, ParsedDeclaration predicate_decl,
      const ParamColumn *outer_group_begin, const ParamColumn *outer_group_end,
      const ParamColumn *inner_group_begin, const ParamColumn *inner_group_end,
      const ParamColumn *free_begin, const ParamColumn *free_end);

  // Tell the visitor that we're going to insert into an aggregation.
  virtual void Collect(
      ParsedPredicate functor, ParsedDeclaration functor_decl,
      const ParamColumn *bound_begin, const ParamColumn *bound_end,
      const ParamColumn *aggregate_begin, const ParamColumn *aggregate_end,
      const ParamColumn *summary_begin, const ParamColumn *summary_end);

  // Tell the visitor that is can finish summarizing, and prepare to select
  // the summaries.
  virtual void EnterSelectFromSummary(
      ParsedPredicate functor, ParsedDeclaration decl,
      const ParamColumn *group_begin, const ParamColumn *group_end,
      const ParamColumn *bound_begin, const ParamColumn *bound_end,
      const ParamColumn *aggregate_begin, const ParamColumn *aggregate_end,
      const ParamColumn *summary_begin, const ParamColumn *summary_end);

  // Notify the visitor that we were successful in visiting the clause body,
  // starting from the assumption `assumption`.
  virtual void Commit(ParsedPredicate assumption);

  // Notify the visitor that we were successful in visiting the clause body,
  // starting from no assumptions.
  virtual void Commit(ParsedClause clause);

  // If we have someting like: `head(...) : ..., blah, ..., !blah.` where
  // `blah` is a zero-arity predicate (i.e. a boolean value).
  virtual void CancelContradiction(ParsedPredicate true_pred,
                                   ParsedPredicate false_pred);

  // Notify the visitor that visiting cannot complete/continue due to an
  // invalid comparison `compare` that relates the variable identified by
  // `lhs_id` to the variable identified by `rhs_id`.
  virtual void CancelComparison(
      ParsedComparison compare, unsigned lhs_id, unsigned rhs_id);

  // Notify the visitor that visiting cannot complete due to the variable `var`
  // used in the comparison `compare` not being range-restricted.
  virtual void CancelRangeRestriction(ParsedComparison compare,
                                      ParsedVariable var);

  // Notify the visitor that visiting cannot complete due to the variable `var`
  // used in the head of the clause `clause` not being range-restricted, i.e.
  // having a definite value by the end of the clause body.
  virtual void CancelRangeRestriction(ParsedClause clause,
                                      ParsedVariable var);

  // Notify the visitor that visiting cannot complete due to binding
  // restrictions on a particular predicate. A range `[begin, end)` of
  // failed bindings is provided.
  virtual void CancelPredicate(const FailedBinding *begin,
                               const FailedBinding *end);

  // Cancel due to their being a message on which we must depend.
  virtual void CancelMessage(const ParsedPredicate predicate);

  // The SIPS generator will ask the visitor if it wants to advance
  // before trying to advance.
  virtual AdvanceType Advance(void);
};

// Allows a SIPVisitor to visit the different orderings of the predicates that
// result in different binding patterns. The idea is that for a given clause,
// and assuming all variables to one of its predicates are bound, we would like
// to evaluate all permutations of predicates in that clause, e.g. to detect
// range restriction errors, to score them for their ability to take advantage
// of sideways information passing style (SIPS), or to emit code.
class SIPSGenerator final {
 public:
  class Impl;

  ~SIPSGenerator(void);

  // Visit the clause containing `assumption`.
  explicit SIPSGenerator(ParsedPredicate assumption);

  // Visit a clause without any assumptions.
  explicit SIPSGenerator(ParsedClause clause);

  // Visit the current ordering. Returns `true` if the `visitor.Commit`
  // was invoked, and `false` if `visitor.Cancel` was invoked.
  bool Visit(SIPSVisitor &visitor) const;

  // Tries to advance to the next possible ordering. Returns `false` if
  // we could not advance to the next ordering.
  bool Advance(void) const;

  // Reset the generator to be beginning.
  void Rewind(void);

 private:
  std::unique_ptr<Impl> impl;
};

}  // namespace hyde
