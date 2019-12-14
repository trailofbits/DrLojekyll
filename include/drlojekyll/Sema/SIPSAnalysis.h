/*
 * Copyright (c) 2019 Trail of Bits, Inc.
 */

#pragma once

#include <drlojekyll/Parse/Parse.h>

#include <memory>

namespace hyde {

// Visits a clause body in left-to-right order.
class SIPSVisitor {
 public:
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

  struct Column {
    inline Column(ParsedParameter param_, ParsedVariable var_,
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

  virtual ~SIPSVisitor(void);

  // Notify the visitor that we're about to begin visiting a clause body.
  virtual void Begin(ParsedPredicate assumption);

  // Declares a concrete parameter identified by `id`.
  virtual void DeclareParameter(const Column &col);

  // Declares a variable identified by `id`.
  virtual void DeclareVariable(ParsedVariable var, unsigned id);

  // Declares a constant identified by `id`.
  virtual void DeclareConstant(ParsedLiteral val, unsigned id);

  // Asserts that the value of the variable identified by `lhs_id` must match
  // the value of the variable identified by `rhs_id`.
  virtual void AssertEqual(unsigned lhs_id, unsigned rhs_id);

  // Asserts that the value of the variable identified by `lhs_id` must NOT
  // match the value of the variable identified by `rhs_id`.
  virtual void AssertNotEqual(unsigned lhs_id, unsigned rhs_id);

  // Asserts that the value of the variable identified by `lhs_id` must be less
  // than the value of the variable identified by `rhs_id`.
  virtual void AssertLessThan(unsigned lhs_id, unsigned rhs_id);

  // Asserts that the value of the variable identified by `lhs_id` must be
  // greater than the value of the variable identified by `rhs_id`.
  virtual void AssertGreaterThan(unsigned lhs_id, unsigned rhs_id);

  // Asserts the presence of some tuple. This is for negative predicates. Uses
  // the variable ids in the range `[begin_id, end_id)`.
  virtual void AssertPresent(
      ParsedPredicate pred, const Column *begin,
      const Column *end);

  // Asserts the absence of some tuple. This is for negative predicates. Uses
  // the variable ids in the range `[begin_id, end_id)`.
  virtual void AssertAbsent(
      ParsedPredicate pred, const Column *begin,
      const Column *end);

  // Tell the visitor that we're going to insert into a table.
  virtual void Insert(
      ParsedDeclaration decl, const Column *begin,
      const Column *end);

  // Selects some columns from a predicate where some of the column values are
  // fixed.
  virtual void EnterFromWhereSelect(
      ParsedPredicate pred, ParsedDeclaration from,
      const Column *where_begin, const Column *where_end,
      const Column *select_begin, const Column *select_end);

  // Selects some columns from a predicate where some of the column values are
  // fixed.
  virtual void EnterFromSelect(
      ParsedPredicate pred, ParsedDeclaration from,
      const Column *select_begin, const Column *select_end);

  // Exits the a selection.
  virtual void ExitSelect(ParsedPredicate pred, ParsedDeclaration from);

  // Enter into the aggregate collection phase. Pass in the bound arguments
  // that are not themselves being summarized or aggregates.
  virtual void EnterAggregation(
      ParsedPredicate functor, ParsedDeclaration decl,
      const Column *bound_begin, const Column *bound_end);

  // Tell the visitor that we're going to insert into an aggregation.
  virtual void Collect(
      ParsedPredicate agg_pred, ParsedDeclaration agg_decl, const Column *begin,
      const Column *end);

  // Tell the visitor that is can finish summarizing, and prepare to select
  // the summaries.
  virtual void Summarize(ParsedPredicate functor, ParsedDeclaration decl);

  // Notify the visitor that we were successful in visiting the clause body,
  // starting from the assumption `assumption`.
  virtual void Commit(ParsedPredicate assumption);

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
  virtual void CancelPredicate(const FailedBinding *begin, FailedBinding *end);

  // Cancel due to their being a message on which we must depend.
  virtual void CancelMessage(const ParsedPredicate predicate);

  // The SIPS generator will ask the visitor if it wants to advance
  // before trying to advance.
  virtual bool Advance(void);

 private:
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

  explicit SIPSGenerator(ParsedPredicate assumption);

  // Visit the current ordering. Returns `true` if the `visitor.Commit`
  // was invoked, and `false` if `visitor.Cancel` was invoked.
  bool Visit(SIPSVisitor &visitor) const;

  // Tries to advance to the next possible ordering. Returns `false` if
  // we could not advance to the next ordering.
  bool Advance(void) const;

 private:
  std::unique_ptr<Impl> impl;
};

}  // namespace hyde
