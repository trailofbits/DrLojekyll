// Copyright 2020, Trail of Bits. All rights reserved.

#include "Build.h"

namespace hyde {
namespace {

// Build an eager region for performing a comparison.
TUPLECMP *CreateCompareRegion(ProgramImpl *impl, QueryCompare view,
                              Context &context, REGION *parent) {
  auto cmp =
      impl->operation_regions.CreateDerived<TUPLECMP>(parent, view.Operator());

  auto lhs_var = parent->VariableFor(impl, view.InputLHS());
  auto rhs_var = parent->VariableFor(impl, view.InputRHS());

  const auto is_eq = view.Operator() == ComparisonOperator::kEqual;
  if (is_eq && lhs_var->id > rhs_var->id) {
    std::swap(lhs_var, rhs_var);
  }

  cmp->lhs_vars.AddUse(lhs_var);
  cmp->rhs_vars.AddUse(rhs_var);

  if (is_eq) {
    cmp->col_id_to_var[view.InputLHS().Id()] = lhs_var;
    cmp->col_id_to_var[view.InputRHS().Id()] = lhs_var;
    cmp->col_id_to_var[view.LHS().Id()] = lhs_var;

  } else {
    cmp->col_id_to_var[view.LHS().Id()] = lhs_var;
    cmp->col_id_to_var[view.RHS().Id()] = rhs_var;
  }

  return cmp;
}

}  // namespace

// Build an eager region for performing a comparison.
void BuildEagerCompareRegions(ProgramImpl *impl, QueryCompare cmp,
                              Context &context, OP *parent) {
  const QueryView view(cmp);
  const auto check = CreateCompareRegion(impl, cmp, context, parent);
  parent->body.Emplace(parent, check);

  // NOTE(pag): A compare will never share the data model of its predecessor,
  //            otherwise it would be too accepting.
  BuildEagerInsertionRegions(impl, view, context, check, view.Successors(),
                             nullptr);
}

// Build a top-down checker on a compare.
REGION *BuildTopDownCompareChecker(
    ProgramImpl *impl, Context &context, REGION *parent, QueryCompare cmp,
    std::vector<QueryColumn> &view_cols, TABLE *already_checked) {

  const QueryView view(cmp);

  TUPLECMP * check = impl->operation_regions.CreateDerived<TUPLECMP>(
      parent, cmp.Operator());
  REGION * const ret = check;

  // If the comparison failed then return false.
  check->false_body.Emplace(check, BuildStateCheckCaseReturnFalse(impl, check));

  // It could be that we're comparing two constants, and that this comparison
  // is impossible. Similarly, it could be that what is flowing down is not
  // one of the things being compared!
  if (cmp.Operator() == ComparisonOperator::kEqual) {

    if (cmp.InputLHS().IsConstantOrConstantRef() &&
        cmp.InputRHS().IsConstantOrConstantRef()) {
      check->lhs_vars.AddUse(parent->VariableFor(impl, cmp.InputLHS()));
      check->rhs_vars.AddUse(parent->VariableFor(impl, cmp.InputRHS()));
    }

  // Make sure the two values coming down are actually different.
  } else {
    check->lhs_vars.AddUse(parent->VariableFor(impl, cmp.LHS()));
    check->rhs_vars.AddUse(parent->VariableFor(impl, cmp.RHS()));
  }

  // If either of the inputs to the comparison are constant, then it's possible
  // that it's an impossible comparison, but we need to make sure that not
  // only are we flowing down corrected, but that we're flowing up correctly.
  if (cmp.InputLHS().IsConstantOrConstantRef() ||
      cmp.InputRHS().IsConstantOrConstantRef()) {

    TUPLECMP * const inner_check =
        impl->operation_regions.CreateDerived<TUPLECMP>(
            check, ComparisonOperator::kEqual);

    check->body.Emplace(check, inner_check);

    // If the comparison failed then return false.
    inner_check->false_body.Emplace(
        inner_check, BuildStateCheckCaseReturnFalse(impl, inner_check));

    if (cmp.InputLHS().IsConstantOrConstantRef()) {
      inner_check->lhs_vars.AddUse(parent->VariableFor(impl, cmp.LHS()));
      inner_check->rhs_vars.AddUse(parent->VariableFor(impl, cmp.InputLHS()));
    }

    if (cmp.InputRHS().IsConstantOrConstantRef()) {
      inner_check->lhs_vars.AddUse(parent->VariableFor(impl, cmp.RHS()));
      inner_check->rhs_vars.AddUse(parent->VariableFor(impl, cmp.InputRHS()));
    }

    check = inner_check;
  }

  // Okay, by this point we have a plausible tuple. Our baseline comparison
  // passes, and its time to call our predecessor.
  const QueryView pred_view = view.Predecessors()[0];
  check->body.Emplace(
      check,
      CallTopDownChecker(
          impl, context, check, view, view_cols, pred_view, already_checked,
          [=] (REGION *parent_if_true) -> REGION * {
            return BuildStateCheckCaseReturnTrue(impl, parent_if_true);
          },
          [=] (REGION *parent_if_false) -> REGION * {
            return BuildStateCheckCaseReturnFalse(impl, parent_if_false);
          }));

  return ret;
}

void CreateBottomUpCompareRemover(ProgramImpl *impl, Context &context,
                                  QueryView view, OP *root,
                                  TABLE *already_checked) {
  auto cmp = CreateCompareRegion(impl, QueryCompare::From(view), context, root);
  root->body.Emplace(root, cmp);

  auto parent = impl->parallel_regions.Create(cmp);
  cmp->body.Emplace(cmp, parent);

  const auto model = impl->view_to_model[view]->FindAs<DataModel>();
  if (model->table) {

    // The caller didn't already do a state transition, so we can do it.
    if (already_checked != model->table) {
      already_checked = model->table;

      const auto orig_parent = parent;
      orig_parent->AddRegion(BuildBottomUpTryMarkUnknown(
          impl, model->table, parent, view.Columns(),
          [&](PARALLEL *par) { parent = par; }));
    }
  } else {
    already_checked = nullptr;
  }

  auto let = impl->operation_regions.CreateDerived<LET>(parent);
  parent->AddRegion(let);

  BuildEagerRemovalRegions(impl, view, context, let, view.Successors(),
                           already_checked);
}

}  // namespace hyde
