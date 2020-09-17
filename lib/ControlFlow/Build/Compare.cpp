// Copyright 2020, Trail of Bits. All rights reserved.

#include "Build.h"

namespace hyde {

// Build an eager region for performing a comparison.
void BuildEagerCompareRegions(ProgramImpl *impl, QueryCompare view,
                              Context &context, OP *parent) {
  auto cmp = impl->operation_regions.CreateDerived<TUPLECMP>(
      parent, view.Operator());

  auto lhs_var = parent->VariableFor(impl, view.InputLHS());
  auto rhs_var = parent->VariableFor(impl, view.InputRHS());

  const auto is_eq = view.Operator() == ComparisonOperator::kEqual;
  if (is_eq && lhs_var->id > rhs_var->id) {
    std::swap(lhs_var, rhs_var);
  }

  cmp->lhs_vars.AddUse(lhs_var);
  cmp->rhs_vars.AddUse(rhs_var);

  if (is_eq) {
    cmp->col_id_to_var.emplace(view.InputLHS().Id(), lhs_var);
    cmp->col_id_to_var.emplace(view.InputRHS().Id(), lhs_var);
    cmp->col_id_to_var.emplace(view.LHS().Id(), lhs_var);

  } else {
    cmp->col_id_to_var.emplace(view.LHS().Id(), lhs_var);
    cmp->col_id_to_var.emplace(view.RHS().Id(), rhs_var);
  }

  UseRef<REGION>(parent, cmp).Swap(parent->body);
  BuildEagerSuccessorRegions(impl, view, context, cmp,
                             QueryView(view).Successors(), nullptr);
}

}  // namespace hyde
