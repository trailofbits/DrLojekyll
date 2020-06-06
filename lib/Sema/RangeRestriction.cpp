// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/Sema/RangeRestriction.h>

#include <drlojekyll/Display/DisplayManager.h>
#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Parse/ErrorLog.h>
#include <drlojekyll/Parse/Format.h>
#include <drlojekyll/Parse/Parse.h>
#include <drlojekyll/Sema/ModuleIterator.h>
#include <drlojekyll/Sema/SIPSAnalysis.h>

namespace hyde {
namespace {

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
  }

 private:
  const DisplayManager dm;
  const ErrorLog log;
};

}  // namespace

// Ensures that all variables used in the heads of clauses are used in their
// bodies.
void CheckForRangeRestrictionErrors(const DisplayManager &dm,
                                    const ParsedModule &root_module,
                                    const ErrorLog &log) {

  auto do_clause = [&] (ParsedClause clause) {
    RangeRestrictionReporter visitor(dm, log);
    SIPSGenerator generator(clause);
    generator.Visit(visitor);
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
