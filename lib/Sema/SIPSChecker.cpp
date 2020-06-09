// Copyright 2020, Trail of Bits. All rights reserved.

#include <unordered_map>

#include <drlojekyll/Display/DisplayManager.h>
#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Parse/ErrorLog.h>
#include <drlojekyll/Parse/Format.h>
#include <drlojekyll/Parse/Parse.h>
#include <drlojekyll/Sema/ModuleIterator.h>
#include <drlojekyll/Sema/SIPSAnalysis.h>
#include <drlojekyll/Sema/SIPSChecker.h>

namespace hyde {
namespace {

// Ensures that all variables used in the heads of clauses are used in their
// bodies.
class RangeRestrictionReporter : public SIPSVisitor {
 public:
  explicit RangeRestrictionReporter(const DisplayManager &dm_,
                                    const ErrorLog &log_)
      : dm(dm_),
        log(log_) {}

  virtual ~RangeRestrictionReporter(void) = default;

  // Notify the visitor that visiting cannot complete due to the variable `var`
  // used in the comparison `compare` not being range-restricted.
  void CancelRangeRestriction(ParsedComparison compare,
                              ParsedVariable var) override {
    Error err(dm, compare.SpellingRange(), var.SpellingRange());
    err << "Variable '" << var.SpellingRange()
        << "' is not range-restricted in this comparison";
    log.Append(std::move(err));
    reported = true;
  }

  // Notify the visitor that visiting cannot complete due to the variable `var`
  // used in the head of the clause `clause` not being range-restricted, i.e.
  // having a definite value by the end of the clause body.
  void CancelRangeRestriction(ParsedClause clause,
                              ParsedVariable var) override {
    Error err(dm, clause.SpellingRange(), var.SpellingRange());
    err << "Variable '" << var.SpellingRange()
        << "' is not range-restricted in this clause";
    log.Append(std::move(err));
    reported = true;
  }

 public:
  const DisplayManager dm;
  const ErrorLog log;
  bool reported{false};
};

// Applies after all permutations of a clause body fail to pass the SIPS visitor.
class AllFailedVisitor : public SIPSVisitor {
 public:
  explicit AllFailedVisitor(const DisplayManager &dm_, const ErrorLog &log_)
      : dm(dm_),
        log(log_) {}

  virtual ~AllFailedVisitor(void) = default;

  // There was a trivial contradiction, e.g. `foo, !foo`.
  void CancelContradiction(ParsedPredicate true_pred,
                           ParsedPredicate false_pred) override {
    auto true_clause = ParsedClause::Containing(true_pred);
    auto pred_name = ParsedDeclaration::Of(true_pred).Name();
    Error err(dm, true_clause.SpellingRange(), true_pred.SpellingRange());
    err << "Used of predicate '" << pred_name
        << "/0' contradicts another negated use";

    auto false_clause = ParsedClause::Containing(false_pred);
    auto note = err.Note(dm, false_clause.SpellingRange(),
                         false_pred.SpellingRange());
    note << "Negated use of '" << pred_name << "/0' is here";
    log.Append(std::move(err));
  }

  // Notify the visitor that visiting cannot complete/continue due to an
  // invalid comparison `compare` that relates the variable identified by
  // `lhs_id` to the variable identified by `rhs_id`.
  void CancelComparison(
      ParsedComparison compare, unsigned, unsigned) override {

    auto clause = ParsedClause::Containing(compare);
    Error err(dm, clause.SpellingRange(), compare.SpellingRange());
    err << "Unsatisfiable in/equality between " << compare.LHS().SpellingRange()
        << " and " << compare.RHS().SpellingRange();
    log.Append(std::move(err));
  }

  // Notify the visitor that visiting cannot complete due to binding
  // restrictions on a particular predicate. A range `[begin, end)` of
  // failed bindings is provided.
  void CancelPredicate(const FailedBinding *begin,
                       const FailedBinding *end) override {
    assert(begin < end);

    auto clause = ParsedClause::Containing(begin->predicate);
    Error err(dm, clause.SpellingRange(), begin->predicate.SpellingRange());
    err << "Unable to find binding of variables for parameters of '"
        << begin->declaration.Name() << '/' << begin->declaration.Arity()
        << '/';

    for (auto redecl : begin->declaration.Redeclarations()) {
      auto note = err.Note(dm, redecl.SpellingRange());
      note << "Declaration of '" << redecl.Name() << '/' << redecl.Arity()
           << "' is here";
    }

    log.Append(std::move(err));
  }

 private:
  const DisplayManager dm;
  const ErrorLog log;
};


}  // namespace

// Ensures that all variables used in the heads of clauses are used in their
// bodies.
void CheckForErrors(const DisplayManager &dm, const ParsedModule &root_module,
                    const ErrorLog &log) {

  auto do_clause = [&] (ParsedClause clause) {
    RangeRestrictionReporter rr_visitor(dm, log);
    SIPSGenerator rr_generator(clause);
    auto any_passed = false;
    do {
      if (rr_generator.Visit(rr_visitor)) {
        any_passed = true;
      } else if (rr_visitor.reported) {
        break;
      }
    } while (rr_generator.Advance());

    if (!any_passed) {
      AllFailedVisitor af_visitor(dm, log);
      SIPSGenerator af_generator(clause);
      af_generator.Visit(af_visitor);
    }
  };

  for (auto module : ParsedModuleIterator(root_module)) {
    for (auto clause : module.Clauses()) {
      do_clause(clause);
    }
    for (auto clause : module.DeletionClauses()) {
      do_clause(clause);
    }
  }
}

}  // namespace hyde