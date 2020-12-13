// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/DataFlow/Query.h>
#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Lex/Format.h>
#include <drlojekyll/Parse/ErrorLog.h>
#include <drlojekyll/Parse/Format.h>
#include <drlojekyll/Parse/ModuleIterator.h>
#include <drlojekyll/Parse/Parse.h>
#include <drlojekyll/Util/DisjointSet.h>
#include <drlojekyll/Util/EqualitySet.h>

#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <vector>

#include "Query.h"

namespace hyde {

namespace {

struct VarColumn : DisjointSet {
 public:
  explicit VarColumn(ParsedVariable var_)
      : DisjointSet(var_.Order()),
        var(var_) {}

  const ParsedVariable var;
};

struct ClauseContext {
  void Reset(void) {
    var_id_to_col.clear();
    var_to_col.clear();

    // NOTE(pag): We don't reset `spelling_to_col`.
    col_id_to_constant.clear();
    vars.clear();

    // NOTE(pag): The `hash_join_cache` is preserved.
    views.clear();

    error_heads.clear();

    unapplied_compares.clear();
    functors.clear();
    negated_predicates.clear();

    color = 0;
  }

  // Maps vars to cols. We don't map a `ParsedVariable` because then we'd end up
  // with them all being merged.
  std::unordered_map<uint64_t, VarColumn *> var_id_to_col;

  // Maps vars to cols. Distinct instances of the same logical variable are
  // mapped to the same `VarColumn`.
  std::unordered_map<ParsedVariable, VarColumn *> var_to_col;

  // Spelling of a literal to its associated column.
  std::unordered_map<std::string, COL *> spelling_to_col;

  // Mapping of IDs to constant columns.
  std::vector<COL *> col_id_to_constant;

  // Variables.
  std::vector<std::unique_ptr<VarColumn>> vars;

  // A cache of hashes of JOINs mapping to all the JOINs that hash that way.
  //
  // NOTE(pag): This is shared across all clauses.
  std::unordered_map<uint64_t, std::vector<JOIN *>> hash_join_cache;

  // Work list of all views to join together in various ways, so as to finally
  // produce some data flow variants for this clause.
  std::vector<VIEW *> views;

  // Comparisons that haven't yet been applied.
  std::unordered_set<ParsedComparison> unapplied_compares;

  // Functors that haven't yet been applied.
  std::vector<ParsedPredicate> functors;

  // Negations that haven't yet been applied.
  std::vector<ParsedPredicate> negated_predicates;

  // List of views that failed to produce valid heads.
  std::vector<VIEW *> error_heads;

  // Color to use in the eventual data flow output. Default is black. This
  // is influenced by `ParsedClause::IsHighlighted`, which in turn is enabled
  // by using the `@highlight` pragma after a clause head.
  unsigned color{0};
};

static VarColumn *VarSet(ClauseContext &context, ParsedVariable var) {
  if (auto vc = context.vars[var.Order()].get(); vc) {
    return vc->FindAs<VarColumn>();
  }

  // If this var is a clause parameter.
  if (auto vc = context.var_to_col[var]; vc) {
    return vc->FindAs<VarColumn>();
  }

  assert(false);
  return nullptr;
}

// Look up the ID of `var` in context.
static unsigned VarId(ClauseContext &context, ParsedVariable var) {
  auto var_col = VarSet(context, var);
  if (var_col) {
    return var_col->id;
  } else {
    assert(false);
    return ~0u;
  }
}

// Create a disjoint set for `var`, and merge it with any same-named variables
// in the currentl clause.
static void CreateVarId(ClauseContext &context, ParsedVariable var) {
  const auto order = var.Order();
  if (auto min_size = order + 1u; min_size > context.vars.size()) {
    context.vars.resize(min_size);
  }
  const auto vc_ptr = new VarColumn(var);
  std::unique_ptr<VarColumn> vc(vc_ptr);
  context.vars[order].swap(vc);
  assert(!vc);
  assert(!context.var_id_to_col.count(var.UniqueId()));
  context.var_id_to_col.emplace(var.UniqueId(), vc_ptr);

  auto &prev_vc = context.var_to_col[var];
  if (!prev_vc) {
    prev_vc = vc_ptr;
  } else {
    DisjointSet::UnionInto(vc_ptr, prev_vc);
  }
}

// Ensure that `result` only produces unique columns. Does this by finding
// duplicate columns in `result` and guarding them with equality comparisons.
static VIEW *PromoteOnlyUniqueColumns(QueryImpl *query, VIEW *result) {

  while (true) {
    COL *lhs_col = nullptr;
    COL *rhs_col = nullptr;
    auto num_cols = result->columns.Size();

    // Scan to find two columns that must be compared.
    for (auto i = 0u; i < num_cols; ++i) {
      lhs_col = result->columns[i];
      rhs_col = nullptr;
      for (auto j = i + 1u; j < num_cols; ++j) {
        rhs_col = result->columns[j];
        if (rhs_col->id == lhs_col->id) {
          break;
        }
      }
      if (rhs_col && rhs_col->id == lhs_col->id) {
        break;
      }
    }

    // Didn't find a duplicate; we're done.
    if (!lhs_col || !rhs_col || rhs_col->id != lhs_col->id) {
      break;
    }

    auto col_index = 0u;

    CMP *cmp = query->compares.Create(ComparisonOperator::kEqual);
    cmp->color = result->color;
    cmp->input_columns.AddUse(lhs_col);
    cmp->input_columns.AddUse(rhs_col);
    cmp->columns.Create(lhs_col->var, cmp, lhs_col->id, col_index++);

    for (auto i = 0u; i < num_cols; ++i) {
      if (i != lhs_col->index && i != rhs_col->index) {
        const auto attached_col = result->columns[i];
        cmp->attached_columns.AddUse(attached_col);
        cmp->columns.Create(attached_col->var, cmp, attached_col->id,
                            col_index++);
      }
    }

    result = cmp;
  }

  return result;
}

// Create an initial, unconnected view for this predicate.
static VIEW *BuildPredicate(QueryImpl *query, ClauseContext &context,
                            ParsedPredicate pred, const ErrorLog &log) {
  VIEW *view = nullptr;
  const auto decl = ParsedDeclaration::Of(pred);

  if (decl.IsMessage()) {

    auto &input = query->decl_to_input[decl];
    if (!input) {
      input = query->ios.Create(decl);
    }

    view = query->selects.Create(input, pred.SpellingRange());
    view->color = context.color;
    input->receives.AddUse(view);

  } else if (decl.IsFunctor()) {
    assert(false);

  } else if (decl.IsExport() || decl.IsLocal() || decl.IsQuery()) {
    Node<QueryRelation> *input = nullptr;

    auto &rel = query->decl_to_relation[decl];
    if (!rel) {
      rel = query->relations.Create(decl);
    }
    input = rel;
    view = query->selects.Create(input, pred.SpellingRange());
    view->color = context.color;
    input->selects.AddUse(view);

  } else {
    log.Append(ParsedClause::Containing(pred).SpellingRange(),
               pred.SpellingRange())
        << "Internal error: unrecognized/unexpected predicate type";
    return nullptr;
  }

  // Add the output columns to the VIEW associated with the predicate.
  auto col_index = 0u;
  for (auto var : pred.Arguments()) {
    view->columns.Create(var, view, VarId(context, var), col_index++);
  }

  // Deal with something like `foo(A, A)`, turning it into `foo(A, B), A=B`.
  return PromoteOnlyUniqueColumns(query, view);
}

// Go over all inequality comparisons in the clause body and try to apply as
// many as possible to the `view_ref`, replacing it each time. We apply this
// to the filtered initial views, as well as the final views before pushing
// a head clause.
static VIEW *GuardWithInequality(QueryImpl *query, ParsedClause clause,
                                 ClauseContext &context, VIEW *view) {
  if (context.unapplied_compares.empty()) {
    return view;
  }

  for (auto cmp : clause.Comparisons()) {

    // Skip if it's an equality comparison or if we've already applied it.
    if (ComparisonOperator::kEqual == cmp.Operator() ||
        !context.unapplied_compares.count(cmp)) {
      continue;
    }

    const auto lhs_var = cmp.LHS();
    const auto rhs_var = cmp.RHS();
    const auto lhs_set = VarSet(context, lhs_var);
    const auto rhs_set = VarSet(context, rhs_var);

    const auto lhs_id = lhs_set->id;
    const auto rhs_id = rhs_set->id;

    COL *lhs_col = nullptr;
    COL *rhs_col = nullptr;

    for (auto col : view->columns) {
      if (col->id == lhs_id || col->var == lhs_var) {
        lhs_col = col;
      } else if (col->id == rhs_id || col->var == rhs_var) {
        rhs_col = col;
      }
    }

    if (!lhs_col) {
      lhs_col = context.col_id_to_constant[lhs_id];
    }

    if (!rhs_col) {
      rhs_col = context.col_id_to_constant[rhs_id];
    }

    if (!lhs_col || !rhs_col) {
      continue;
    }

    context.unapplied_compares.erase(cmp);

    CMP *filter = query->compares.Create(cmp.Operator());
    filter->color = context.color;
    filter->spelling_range = cmp.SpellingRange();
    filter->input_columns.AddUse(lhs_col);
    filter->input_columns.AddUse(rhs_col);

    auto col_index = 0u;
    filter->columns.Create(lhs_var, filter, lhs_id, col_index++);
    filter->columns.Create(rhs_var, filter, rhs_id, col_index++);

    for (auto other_col : view->columns) {
      if (other_col != lhs_col && other_col != rhs_col) {
        filter->attached_columns.AddUse(other_col);
        filter->columns.Create(other_col->var, filter, other_col->id,
                               col_index++);
      }
    }

    view = filter;
  }

  return view;
}

// If we have something like `foo(A, A)` or `foo(A, B), A=B`, then we want to
// put a filter above this view that actually implements this requirement.
//
// Similarly, if we have `foo(A, B), A=1` then we want to add that filter in
// as early as possible, and this does that.
//
// Our goal is to end up with the following approximate structures for something
// like `foo(A, B, C), A=B, B=C`.
//
//    SELECT[A, B, C]
//           |  |  |
//      VIEW[A, B, C]      <-- orig `view`
//           |  /  /
//       CMP[A=B, C]
//           \   /
//        CMP[A=C]
//           _|____
//          /  |   |
//    TUPLE[A, B, C]       <-- `view` after execution
static VIEW *GuardViewWithFilter(QueryImpl *query, ParsedClause clause,
                                 ClauseContext &context, VIEW *view) {

  // Now, compare the remaining columns against constants.
  for (auto assign : clause.Assignments()) {
    const auto lhs_var = assign.LHS();
    auto lhs_id = VarId(context, lhs_var);

    for (auto col : view->columns) {
      if (col->id != lhs_id) {
        continue;
      }

      auto const_id = context.var_id_to_col[lhs_var.UniqueId()]->id;
      auto const_col = context.col_id_to_constant[const_id];
      assert(const_col != nullptr);

      CMP *cmp = query->compares.Create(ComparisonOperator::kEqual);
      cmp->color = context.color;
      cmp->input_columns.AddUse(const_col);
      cmp->input_columns.AddUse(col);

      auto col_index = 0u;
      cmp->columns.Create(col->var, cmp, col->id, col_index++);

      for (auto other_col : view->columns) {
        if (other_col != col) {
          cmp->attached_columns.AddUse(other_col);
          cmp->columns.Create(other_col->var, cmp, other_col->id, col_index++);
        }
      }

      view = cmp;

      break;
    }
  }

  return GuardWithInequality(query, clause, context, view);
}

// Try to create a new VIEW that will publish just constants, e.g. `foo(1).`.
static VIEW *AllConstantsView(QueryImpl *query, ParsedClause clause,
                              ClauseContext &context) {

  if (context.spelling_to_col.empty()) {
    return nullptr;
  }

  TUPLE *tuple = query->tuples.Create();
  tuple->color = context.color;
  auto col_index = 0u;
  for (const auto &[spelling, col] : context.spelling_to_col) {
    (void) tuple->columns.Create(col->var, tuple, col->id, col_index);
    tuple->input_columns.AddUse(col);
  }

  return tuple;
}

// Find `var` in the output columns of `view`, or as a constant.
static COL *FindColVarInView(ClauseContext &context, VIEW *view,
                             ParsedVariable var) {
  const auto id = VarId(context, var);

  // Try to find the column in `view`.
  for (auto in_col : view->columns) {
    if (in_col->id == id) {
      return in_col;
    }
  }

#ifndef NDEBUG
  for (auto in_col : view->columns) {
    assert(in_col->var != var);
  }
#endif

  // Try to find the column as a constant.
  return context.col_id_to_constant[id];
}

// Propose `view` as being a source of data for the clause head.
static VIEW *ConvertToClauseHead(QueryImpl *query, ParsedClause clause,
                                 ClauseContext &context, const ErrorLog &log,
                                 VIEW *view, bool report = false) {

  // Proved a zero-argument predicate.
  //
  // NOTE(pag): We totally ignore equivalence classes in these cases.
  if (!clause.Arity()) {
    return view;
  }

  TUPLE *tuple = query->tuples.Create();
  tuple->color = context.color;

  auto col_index = 0u;

  // Go find each clause head variable in the columns of `view`.
  for (auto var : clause.Parameters()) {
    const auto id = VarId(context, var);
    (void) tuple->columns.Create(var, tuple, id, col_index++);

    if (auto in_col = FindColVarInView(context, view, var); in_col) {
      tuple->input_columns.AddUse(in_col);

    // TODO(pag): This comes up intermittently for some tests, but sometimes
    //            some clause orderings succeed. Investigate the failing ones.
    } else {
      tuple->input_columns.Clear();

      // Defer error reporting until we know that we don't have any valid
      // join ordering of the clause body that can satisfy the needs of the
      // clause head.
      if (!report) {
        tuple->PrepareToDelete();
        context.error_heads.push_back(view);
        return nullptr;
      }

      const auto clause_range = clause.SpellingRange();
      auto err = log.Append(clause_range, var.SpellingRange());
      err << "Internal error: could not match variable '" << var
          << "' against any columns";

      for (auto in_col : view->columns) {
        err.Note(clause_range, in_col->var.SpellingRange())
            << "Failed to match against '" << in_col->var << "'";
      }

      return nullptr;
    }
  }

  return tuple;
}

// Create a PRODUCT from multiple VIEWs.
static void CreateProduct(QueryImpl *query, ParsedClause clause,
                          ClauseContext &context, const ErrorLog &log) {
  auto join = query->joins.Create();
  join->color = context.color;
  auto col_index = 0u;
  for (auto view : context.views) {
#ifndef NDEBUG
    auto unique_view = PromoteOnlyUniqueColumns(query, view);
    assert(unique_view == view);
#else
    auto unique_view = view;
#endif

    join->joined_views.AddUse(unique_view);

    for (auto in_col : unique_view->columns) {
      auto out_col =
          join->columns.Create(in_col->var, join, in_col->id, col_index++);
      auto [pivot_set_it, added] = join->out_to_in.emplace(out_col, join);
      assert(added);
      (void) added;
      pivot_set_it->second.AddUse(in_col);
    }
  }

#ifndef NDEBUG
  for (auto &[out, in_cols] : join->out_to_in) {
    assert(!in_cols.Empty());
    (void) out;
  }
#endif

  context.views.clear();
  context.views.push_back(GuardViewWithFilter(query, clause, context, join));
}

// Try to apply `pred`, which is a functor, given `view` as the source of the
// input values to `pred`. This is challenging because there may be multiple
// applicable redeclarations of `pred` that we can apply (due to the declarations
// sharing the same name, parameter types, but different parameter bindings),
// and is further complicated when `view` contains a value that is attributed as
// `free` in `pred`, and thus needs to be checked against the output of applying
// `pred`.
static VIEW *TryApplyFunctor(QueryImpl *query, ClauseContext &context,
                             ParsedPredicate pred, VIEW *view) {

  const auto decl = ParsedDeclaration::Of(pred);
  std::unordered_set<std::string> seen_variants;
  VIEW *out_view = nullptr;

  for (auto redecl : decl.Redeclarations()) {

    // We may have duplicate redeclarations, so don't repeat any.
    std::string binding(redecl.BindingPattern());
    if (seen_variants.count(binding)) {
    try_next_redecl:
      continue;
    }
    seen_variants.insert(std::move(binding));

    // Go through and see if we can satisfy the binding requirements.
    for (auto param : redecl.Parameters()) {
      const auto var = pred.NthArgument(param.Index());
      const auto bound_col = FindColVarInView(context, view, var);
      if (param.Binding() == ParameterBinding::kBound && !bound_col) {
        goto try_next_redecl;
      }
    }

    // We've satisfied the binding constraints; apply `pred` to the columns in
    // `inouts`.
    MAP *map = query->maps.Create(
        ParsedFunctor::From(redecl), pred.SpellingRange(),
        pred.IsPositive());
    map->color = context.color;

    VIEW *result = map;
    auto col_index = 0u;
    unsigned needs_compares = 0;

    for (auto param : redecl.Parameters()) {
      const auto var = pred.NthArgument(param.Index());
      const auto bound_col = FindColVarInView(context, view, var);
      const auto id = VarId(context, var);

      if (param.Binding() == ParameterBinding::kBound) {
        assert(bound_col);
        map->input_columns.AddUse(bound_col);
        map->columns.Create(var, map, id, col_index);

      } else {
        map->columns.Create(var, map, id, col_index);
      }

      ++col_index;
    }

    // Now attach in any columns that need to be double checked, i.e. ones that
    // are `free`-attributed in the functor, but are available via bound
    // arguments. We'll handle these with a tower of comparisons, produced
    // below.
    for (auto param : redecl.Parameters()) {
      if (param.Binding() != ParameterBinding::kBound) {
        const auto var = pred.NthArgument(param.Index());
        const auto bound_col = FindColVarInView(context, view, var);
        if (bound_col) {
          const auto id = VarId(context, var);
          assert(id == bound_col->id);
          map->attached_columns.AddUse(bound_col);
          map->columns.Create(bound_col->var, map, id, col_index);
          ++col_index;
          ++needs_compares;
        }
      }
    }

    // Now attach in any columns from the predecessor `view` that aren't
    // themselves present in `map`.
    for (auto pred_col : view->columns) {
      if (!FindColVarInView(context, map, pred_col->var)) {
        map->columns.Create(pred_col->var, map, pred_col->id, col_index);
        map->attached_columns.AddUse(pred_col);
        ++col_index;
      }
    }

    // Now, while there are still comparisons between column outputs that need
    // to be made, go do them. This happens when we have a bound column
    // available in our map's predecessor for one of the `free`-attributed
    // columns that the map produces.
    if (needs_compares) {
      result = PromoteOnlyUniqueColumns(query, result);
    }

    if (!out_view) {
      out_view = result;

      // NOTE(pag): Remove this `break` if we want to support equivalence
      //            classes of functor applications.
      break;

    // This is the N >= 3 redeclaration of the functor that is applicable; add
    // it into our equivalence class.
    } else if (auto out_eq = out_view->AsMerge(); out_eq) {
      assert(out_eq->merged_views[0]->columns.Size() == result->columns.Size());
      out_eq->merged_views.AddUse(result);

    // This is the second redeclaration of the functor that is applicable to
    // this case. So we'll create an equivalence class of all variants of the
    // functor that can apply to this view, and that will be what we propose.
    } else {
      assert(out_view->columns.Size() == result->columns.Size());

      auto merge = query->merges.Create();
      merge->color = context.color;

      // Create output columns for the merge.
      for (auto col : result->columns) {
        auto vc = context.vars[col->id].get();
        assert(vc != nullptr);
        (void) merge->columns.Create(vc->var, merge, vc->id, col->index);
      }

      merge->merged_views.AddUse(out_view);
      merge->merged_views.AddUse(result);
      out_view = merge;
    }
  }

  return out_view;
}

// Try to apply a negation. This requires that all named, non-constant variables
// are present.
static VIEW *TryApplyNegation(QueryImpl *query, ParsedClause clause,
                              ClauseContext &context, ParsedPredicate pred,
                              VIEW *view, const ErrorLog &log) {
  std::vector<ParsedVariable> needed_vars;
  std::vector<COL *> needed_cols;
  std::vector<bool> needed_params;

  bool all_needed = true;
  for (auto var : pred.Arguments()) {
    auto var_set = VarSet(context, var);
    if (!var_set) {
      log.Append(pred.SpellingRange(), var.SpellingRange())
          << "Internal error: Unable to find variable '" << var
          << "' used by negation";
      return nullptr;
    }

    // The idea here is we may want to say: `!foo(1, _)` and we want this to
    // mean that there doesn't exist any tuple in the `foo` relation with
    // first column as `1`, and the second column as not being cared for. If
    // we give it a name, however, then we
    if (auto col = FindColVarInView(context, view, var); col) {
      needed_cols.push_back(col);
      needed_params.push_back(true);
      needed_vars.push_back(var);

    } else if (var.IsAssigned()) {
      log.Append(pred.SpellingRange(), var.SpellingRange())
          << "Internal error: Failed to discover constant used by variable '"
          << var << "'";
      return nullptr;

    // This should be an unnamed variable only, e.g. `_`.
    } else if (var.IsUnnamed()) {
      needed_params.push_back(false);
      all_needed = false;

    } else {
      return nullptr;
    }
  }

  auto sel = BuildPredicate(query, context, pred, log);
  if (!sel) {
    return nullptr;
  }

#ifndef NDEBUG
  sel->producer = "PRED-NEGATION";
#endif

  if (!all_needed) {
    const auto tuple = query->tuples.Create();
    tuple->color = context.color;
#ifndef NDEBUG
    tuple->producer = "PRED-NEGATION-SUBSET";
#endif
    auto i = 0u;
    auto col_index = 0u;
    for (auto var : pred.Arguments()) {
      const auto in_col = sel->columns[i];
      if (!needed_params[i++]) {
        continue;
      }
      (void) var;
      (void) tuple->columns.Create(
          in_col->var, tuple, VarId(context, in_col->var), col_index++);
      tuple->input_columns.AddUse(in_col);
    }

    sel = tuple;
  }

  sel = GuardViewWithFilter(query, clause, context, sel);
  sel->can_produce_deletions = true;
  sel->is_used_by_negation = true;

  auto negate = query->negations.Create();
  negate->color = context.color;
  negate->negated_view.Emplace(negate, sel);

  auto col_index = 0u;
  for (auto in_col : needed_cols) {
    auto var = needed_vars[col_index];
    negate->input_columns.AddUse(in_col);
    (void) negate->columns.Create(var, negate, in_col->id, col_index++);
  }

  // Now attach in any other columns that `view` was bringing along but that
  // aren't used in the negation itself.
  for (auto in_col : view->columns) {
    if (std::find(needed_cols.begin(), needed_cols.end(), in_col) ==
        needed_cols.end()) {
      negate->attached_columns.AddUse(in_col);
      (void) negate->columns.Create(in_col->var, negate, in_col->id, col_index++);
    }
  }

  return negate;
}

// Try to apply as many functors and negations as possible to `view`.
static bool TryApplyFunctorsAndNegations(QueryImpl *query, ParsedClause clause,
                                         ClauseContext &context,
                                         const ErrorLog &log) {

  const auto num_views = context.views.size();

  std::vector<ParsedPredicate> unapplied_functors;
  std::vector<ParsedPredicate> unapplied_negations;
  unapplied_functors.reserve(context.functors.size());
  unapplied_negations.reserve(context.negated_predicates.size());

  bool updated = false;

  for (auto i = 0u; i < num_views; ++i) {
    auto &view = context.views[i];

    // Try to apply as many functors as possible to `view`.
    for (auto changed = true; changed; ) {
      changed = false;

      // NOTE(pag): We don't `GuardViewWithFilter` because applying a negation
      //            doesn't introduce any new variables.
      unapplied_negations.clear();
      bool applied_negations = false;
      for (auto pred : context.negated_predicates) {
        if (auto out_view = TryApplyNegation(query, clause, context, pred,
                                             view, log);
            out_view) {
          view = out_view;
          updated = true;
          changed = true;
          applied_negations = true;

        } else {
          unapplied_negations.push_back(pred);
        }
      }

      if (applied_negations) {
        context.negated_predicates.swap(unapplied_negations);
      }

      auto applied_functors = false;
      unapplied_functors.clear();
      for (auto pred : context.functors) {

        // If we've already applied a functor, and if there's a chance that
        // there are negations that could depend on the results of the functor
        // application then we'll loop back to try to apply the negations
        // before trying the next functor.
        if (applied_functors && !context.negated_predicates.empty()) {
          unapplied_functors.push_back(pred);

        // If we succeed at applying a functor then update the view.
        } else if (auto out_view = TryApplyFunctor(query, context, pred, view);
                   out_view) {
          view = GuardViewWithFilter(query, clause, context, out_view);
          updated = true;
          changed = true;
          applied_functors = true;

        } else {
          unapplied_functors.push_back(pred);
        }
      }

      if (applied_functors) {
        context.functors.swap(unapplied_functors);
      }
    }
  }
  return updated;
}

// Create a view from an aggregate.
static VIEW *ApplyAggregate(QueryImpl *query, ParsedClause clause,
                            ClauseContext &context, const ErrorLog &log,
                            ParsedAggregate agg) {

  auto base_view = BuildPredicate(query, context, agg.Predicate(), log);
  if (!base_view) {
    return nullptr;
  }

  auto functor_pred = agg.Functor();
  auto functor_decl = ParsedFunctor::From(ParsedDeclaration::Of(functor_pred));
  AGG *view = query->aggregates.Create(functor_decl);
  view->color = context.color;

  auto col_index = 0u;

  for (auto var : agg.GroupVariablesFromPredicate()) {
    auto col = FindColVarInView(context, base_view, var);
    if (!col) {
      log.Append(agg.SpellingRange(), var.SpellingRange())
          << "Could not find grouping variable '" << var << "'";
      return nullptr;
    }

    view->group_by_columns.AddUse(col);
    (void) view->columns.Create(var, view, col->id, col_index++);
  }

  auto do_param = [&](auto cb) {
    auto num_params = functor_decl.Arity();
    for (auto i = 0u; i < num_params; ++i) {
      auto param = functor_decl.NthParameter(i);
      auto var = functor_pred.NthArgument(i);
      cb(param, var);
    }
  };

  auto has_errors = false;

  do_param([&](ParsedParameter param, ParsedVariable var) {
    if (param.Binding() == ParameterBinding::kBound) {
      auto col = FindColVarInView(context, base_view, var);
      if (!col) {
        auto err = log.Append(agg.SpellingRange(), var.SpellingRange());
        err << "Could not find configuration variable '" << var << "'";

        err.Note(functor_decl.SpellingRange(), param.SpellingRange())
            << "Configuration column declared here";

        has_errors = true;
      } else {
        view->config_columns.AddUse(col);
        (void) view->columns.Create(var, view, col->id, col_index++);
      }
    }
  });

  do_param([&](ParsedParameter param, ParsedVariable var) {
    if (param.Binding() == ParameterBinding::kAggregate) {
      auto col = FindColVarInView(context, base_view, var);
      if (!col) {
        auto err = log.Append(agg.SpellingRange(), var.SpellingRange());
        err << "Could not find aggregated variable '" << var << "'";

        err.Note(functor_decl.SpellingRange(), param.SpellingRange())
            << "Aggregated column declared here";

        has_errors = true;
      } else {
        view->aggregated_columns.AddUse(col);
      }
    }
  });

  do_param([&](ParsedParameter param, ParsedVariable var) {
    if (param.Binding() == ParameterBinding::kSummary) {
      auto col = FindColVarInView(context, base_view, var);
      if (col) {
        auto err = log.Append(agg.SpellingRange(), col->var.SpellingRange());
        err << "Variable '" << var
            << "' used for summarization cannot also be aggregated over";

        err.Note(functor_decl.SpellingRange(), param.SpellingRange())
            << "Summary variable declared here";

        err.Note(agg.SpellingRange(), var.SpellingRange())
            << "Summary variable used here";

        has_errors = true;
      } else {
        (void) view->columns.Create(var, view, VarId(context, var),
                                    col_index++);
      }
    }
  });

  if (has_errors) {
    return nullptr;
  }

  return PromoteOnlyUniqueColumns(query, view);
}

// Find `search_col` in all views of `views`, and fill up `found_cols_out`
// appropriately. Unconditionally fills up `found_cols_out` with all matches.
static bool FindColInAllViews(COL *search_col, const std::vector<VIEW *> &views,
                              std::vector<COL *> &found_cols_out) {
  for (auto view : views) {
    for (auto col : view->columns) {
      if (search_col->id == col->id) {
        found_cols_out.push_back(col);
        goto next_view;
      }
    }

  next_view:
    continue;
  }

  return found_cols_out.size() == views.size();
}

// Go find join candidates. This takes the first view in `views` and tries to
// join each of its columns against every other view, then proposes this as
// a new candidate. Updates `work_item` in place.
static bool FindJoinCandidates(QueryImpl *query, ParsedClause clause,
                               ClauseContext &context, const ErrorLog &log) {
  auto &views = context.views;
  const auto num_views = views.size();
  if (1u == num_views) {
    return false;
  }

//  // Nothing left to do but try to publish the view!
//  if (num_views == 1u &&
//      !(work_item.functors.size() + work_item.negated_predicates.size())) {
//    assert(!context.result);
//    ConvertToClauseHead(query, clause, context, log, views[0]);
//    return;
//  }

  std::vector<std::vector<COL *>> pivot_groups;
  std::vector<VIEW *> next_views;
  std::vector<unsigned> pivot_col_ids;

  // Try to find a join candidate. If we fail, then we will rotate
  // `views`.
  for (auto num_rotations = 0u; 1u < num_views && num_rotations < num_views;
       ++num_rotations) {

    pivot_groups.clear();

    // For each column in `views[0]`, get the set of columns against which
    // that column can be joined. We want to find the group of pivots that is
    // largest, i.e. joins together the most views.
    for (auto col : views[0]->columns) {
      FindColInAllViews(col, views, pivot_groups.emplace_back());
    }

    const auto num_cols = views[0]->columns.Size();
    assert(pivot_groups.size() == num_cols);

    COL *best_pivot = nullptr;

    // Go find the pivot that can be used to merge together the most views.
    //
    // TODO(pag): There is probably some kind of dynamic programming algorithm
    //            that could do a better job and find the most "discriminating"
    //            pivot, e.g. one that has other things in common from each of
    //            the views.
    for (auto i = 0u; i < num_cols; ++i) {
      const auto group_size = pivot_groups[i].size();
      if (group_size == 1u) {
        continue;

      } else if (!best_pivot ||
                 pivot_groups[best_pivot->index].size() > group_size) {

        best_pivot = pivot_groups[i][0];
        assert(best_pivot->index == i);
      }
    }

    // We didn't find a best pivot, do a rotation of the views.
    if (!best_pivot) {
      next_views.clear();
      next_views.insert(next_views.end(), views.begin() + 1, views.end());
      next_views.push_back(views[0]);
      views.swap(next_views);
      continue;
    }

    auto join = query->joins.Create();
    join->color = context.color;

    // Collect the set of views against which we will join.
    next_views.clear();
    for (auto best_pivot_in : pivot_groups[best_pivot->index]) {
      next_views.push_back(best_pivot_in->view);
      join->joined_views.AddUse(best_pivot_in->view);
    }

    auto col_index = 0u;
    auto &pivot_cols = pivot_groups[0];

    // Build out the pivot set. This will implicitly capture the `best_pivot`.
    pivot_col_ids.clear();
    for (auto col : views[0]->columns) {
      pivot_cols.clear();
      if (!FindColInAllViews(col, next_views, pivot_cols)) {
        continue;
      }

      ++join->num_pivots;
      const auto pivot_col = join->columns.Create(
          col->var, join, col->id, col_index++);

      auto [pivot_cols_in_it, added] = join->out_to_in.emplace(pivot_col, join);
      assert(added);
      (void) added;

      for (auto pivot_in : pivot_cols) {
        pivot_cols_in_it->second.AddUse(pivot_in);
      }

      pivot_col_ids.push_back(col->id);
    }

    // Now add in all non-pivots.
    for (auto joined_view : next_views) {
      for (auto in_col : joined_view->columns) {
        if (std::find(pivot_col_ids.begin(), pivot_col_ids.end(), in_col->id) ==
            pivot_col_ids.end()) {

          const auto non_pivot_col = join->columns.Create(
              in_col->var, join, in_col->id, col_index++);
          auto [non_pivot_cols_in_it, added] = join->out_to_in.emplace(
              non_pivot_col, join);
          assert(added);
          (void) added;

          non_pivot_cols_in_it->second.AddUse(in_col);
        }
      }
    }

    // It's possibly that some of the views have subsets of their columns
    // matching, but where these subsets aren't fully covered by all joined
    // views, and so we need to wrap the join in a bunch of equality
    // comparisons.
    auto ret = GuardViewWithFilter(query, clause, context,
                                   PromoteOnlyUniqueColumns(query, join));

    // Remove the joined views from `views`, and move `ret` to the end.
    for (auto &view : views) {
      if (std::find(next_views.begin(), next_views.end(), view) !=
          next_views.end()) {
        view = nullptr;
      }
    }

    auto it = std::remove_if(views.begin(), views.end(),
                             [] (VIEW *v) { return !v; });
    views.erase(it, views.end());
    views.push_back(ret);
    return true;
  }

  // Okay, we've rotated the views around, but failed to find any JOIN
  // opportunities. Our next best bet is to try to apply any of the functors or
  // the aggregates.

  // We applied at least one functor and updated `work_item` in place.
//  if (TryApplyFunctorsAndNegations(query, clause, context, log, work_item)) {
//    context.work_list.emplace_back(std::move(work_item));
//    return;
//  }

  return false;
}

// Make the INSERT conditional on any zero-argument predicates.
static void AddConditionsToInsert(QueryImpl *query, ParsedClause clause,
                                  VIEW *insert) {
  std::vector<COND *> conds;

  auto add_conds = [&](NodeRange<ParsedPredicate> range, UseList<COND> &uses,
                       bool is_positive, VIEW *user) {
    conds.clear();

    for (auto pred : range) {
      auto decl = ParsedDeclaration::Of(pred);
      if (decl.Arity() || !decl.IsExport()) {
        continue;
      }

      auto export_ = ParsedExport::From(decl);
      auto &cond = query->decl_to_condition[export_];
      if (!cond) {
        cond = query->conditions.Create(export_);
      }

      conds.push_back(cond);
    }

    std::sort(conds.begin(), conds.end());
    auto it = std::unique(conds.begin(), conds.end());
    conds.erase(it, conds.end());

    for (auto cond : conds) {
      assert(cond);
      uses.AddUse(cond);
      if (is_positive) {
        cond->positive_users.AddUse(user);
      } else {
        cond->negative_users.AddUse(user);
      }
    }
  };

  add_conds(clause.PositivePredicates(), insert->positive_conditions, true,
            insert);
  add_conds(clause.NegatedPredicates(), insert->negative_conditions, false,
            insert);
}

// The goal of this function is to build multiple equivalent dataflows out of
// a single clause body. When we have a bunch of predicates, there are usually
// many ways in which they can be joined.
static bool BuildClause(QueryImpl *query, ParsedClause clause,
                        ClauseContext &context, const ErrorLog &log) {

  auto &pred_views = context.views;

  if (clause.IsHighlighted()) {
    auto decl = ParsedDeclaration::Of(clause);
    uint64_t hash = decl.Hash();
    hash ^= clause.Hash() * RotateRight64(hash, 13u);
    context.color = static_cast<uint32_t>(hash) ^
                    static_cast<uint32_t>(hash >> 32u);
  }

  // NOTE(pag): This applies to body variables, not parameters.
  for (auto var : clause.Variables()) {
    CreateVarId(context, var);
  }

  for (auto pred : clause.PositivePredicates()) {
    const auto decl = ParsedDeclaration::Of(pred);
    if (pred.Arity() && !decl.IsFunctor()) {
      if (auto view = BuildPredicate(query, context, pred, log); view) {
        pred_views.push_back(view);

      } else {
        return false;
      }
    }
  }

  context.col_id_to_constant.resize(context.vars.size(), nullptr);

  const auto clause_range = clause.SpellingRange();

  for (auto assign : clause.Assignments()) {
    const auto var = assign.LHS();
    const auto literal = assign.RHS();

    // The type and spelling of a constant are a reasonable way of finding the
    // unique constants in a clause body. There are some obvious missed things,
    // e.g. `1` and `0x1` are treated differently, but that's OK.
    std::stringstream ss;
    ss << literal.Type().Spelling() << ':';
    if (literal.IsConstant()) {
      ss << static_cast<unsigned>(literal.Type().Kind()) << ':'
         << literal.Literal().IdentifierId();
    } else {
      ss << *literal.Spelling(Language::kUnknown);
    }
    const auto key = ss.str();

    auto vc = context.var_id_to_col[var.UniqueId()];
    if (!vc) {
      log.Append(clause_range, var.SpellingRange())
          << "Internal error: Could not find column for variable '" << var
          << "'";
      continue;
    }

    auto &const_col = context.spelling_to_col[key];
    auto col_id = vc->FindAs<VarColumn>()->id;

    if (!const_col) {
      CONST *stream = query->constants.Create(literal);
      SELECT *select = query->selects.Create(stream, literal.SpellingRange());
      select->color = context.color;
      const_col = select->columns.Create(var, select, col_id);

    // Reset these, just in case they were initialized by another clause.
    } else {
      const_col->var = var;
      const_col->id = col_id;
    }

    context.col_id_to_constant[vc->id] = const_col;
    if (col_id != vc->id) {
      context.col_id_to_constant[col_id] = const_col;
    }
  }

  // Go through the comparisons and merge disjoint sets when we have equality
  // comparisons, e.g. `A=B`.
  for (auto cmp : clause.Comparisons()) {
    const auto lhs_var = cmp.LHS();
    const auto rhs_var = cmp.RHS();
    const auto lhs_vc = context.var_id_to_col[lhs_var.UniqueId()];
    const auto rhs_vc = context.var_id_to_col[rhs_var.UniqueId()];

    if (!lhs_vc) {
      log.Append(clause_range, lhs_var.SpellingRange())
          << "Internal error: Could not find column for variable '" << lhs_var
          << "'";
      continue;
    }

    if (!rhs_vc) {
      log.Append(clause_range, rhs_var.SpellingRange())
          << "Internal error: Could not find column for variable '" << rhs_var
          << "'";
      continue;
    }

    if (cmp.Operator() == ComparisonOperator::kEqual) {
      DisjointSet::Union(lhs_vc, rhs_vc);

    // At the end, this should be empty.
    } else {
      context.unapplied_compares.insert(cmp);
    }
  }

  // Go back through the comparisons and look for clause-local unsatisfiable
  // inequalities.
  for (auto cmp : clause.Comparisons()) {
    const auto lhs_var = cmp.LHS();
    const auto rhs_var = cmp.RHS();
    const auto lhs_id = VarId(context, lhs_var);
    const auto rhs_id = VarId(context, rhs_var);
    if (lhs_id == rhs_id && cmp.Operator() != ComparisonOperator::kEqual) {
      auto err = log.Append(clause_range, cmp.SpellingRange());
      err << "Variables '" << lhs_var << "' and '" << rhs_var
          << "' can be equal, but are marked as not equal here";
      return false;
    }
  }

  // Go back through the assignments and propagate constants.
  for (auto assign : clause.Assignments()) {
    const auto vc = context.var_id_to_col[assign.LHS().UniqueId()];
    const auto final_vc = vc->FindAs<VarColumn>();
    const auto const_col = context.col_id_to_constant[vc->id];
    const_col->id = final_vc->id;
    for (auto &[var_id, found_vc] : context.var_id_to_col) {
      if (found_vc->FindAs<VarColumn>() == final_vc) {
        context.col_id_to_constant[found_vc->id] = const_col;
      }
    }
  }

  // Add the aggregates as views.
  for (auto agg : clause.Aggregates()) {
    if (auto view = ApplyAggregate(query, clause, context, log, agg); view) {
      pred_views.push_back(view);
    } else {
      return false;
    }
  }

  // Do a range-restriction check that all variables in the clause head appear
  // somewhere in the clause body. This shouldn't be technically necessary but
  // having a bit of redundancy doesn't hurt.
  //
  // NOTE(pag): This isn't a 100% check, because for range restriction, you
  //            really need all parameters to themselves be arguments to
  //            predicates.
  for (auto var : clause.Parameters()) {
    if (!context.var_to_col.count(var)) {
      log.Append(clause.SpellingRange(), var.SpellingRange())
          << "Parameter variable '" << var << "' is not range restricted";
      return false;
    }
  }

  // We have no relations, so lets create a single view that has all of the
  // constants. It's possible that we have functors or comparisons that need
  // to operate on these constants, so this is why be bring them in here.
  if (pred_views.empty()) {
    TUPLE *tuple = query->tuples.Create();
    tuple->color = context.color;
    auto col_index = 0u;
    for (auto const_col : context.col_id_to_constant) {
      if (const_col) {
        tuple->input_columns.AddUse(const_col);
        (void) tuple->columns.Create(const_col->var, tuple, const_col->id,
                                     col_index++);
      }
    }
    pred_views.push_back(tuple);
  }

  // Make sure every view only exposes unique columns being contributed. E.g.
  // if we have `foo(A, A)` then we replace it with a COMPARE than does a
  // comparison between the output columns of the original view and then only
  // presents a single `A`.
  for (auto &view : pred_views) {
    view = GuardViewWithFilter(query, clause, context, view);
    view = PromoteOnlyUniqueColumns(query, view);
  }

  // Go add the functors and aggregates in.
  for (auto pred : clause.PositivePredicates()) {
    assert(pred.IsPositive());
    const auto decl = ParsedDeclaration::Of(pred);
    if (decl.IsFunctor()) {
      context.functors.push_back(pred);
    }
  }

  for (auto pred : clause.NegatedPredicates()) {
    assert(pred.IsNegated());
    const auto decl = ParsedDeclaration::Of(pred);
    if (decl.IsFunctor()) {
      context.functors.push_back(pred);
    } else {
      context.negated_predicates.push_back(pred);
    }
  }

  // Everything depends on there being at least view in `pred_views`. We
  // might have something like `pred(1, 2).` and that's it, or
  // `pred(1) : foo(2).`
  if (pred_views.empty()) {
    if (auto const_view = AllConstantsView(query, clause, context);
        const_view) {

      auto view = GuardViewWithFilter(query, clause, context, const_view);
      view = PromoteOnlyUniqueColumns(query, view);
      pred_views.push_back(view);

    } else {
      log.Append(clause_range)
          << "Internal error: Failed to create any data flow nodes for clause";
      return false;
    }
  }

  // Process the work list until we find some order of things that works.
  //
  // NOTE(pag): Remove `!context.result` to enable equivalence-class building.
  for (auto changed = true; changed && !pred_views.empty(); ) {
    changed = false;

    // We applied at least one functor or negation and updated `pred_views`
    // in place (view `context.views`).
    if (TryApplyFunctorsAndNegations(query, clause, context, log)) {
      changed = true;
      continue;
    }

    // Try to join two or more views together. Updates `pred_views` in place
    // (view `context.views`).
    if (FindJoinCandidates(query, clause, context, log)) {
      changed = true;
      continue;
    }

    // We failed to apply functors/negations, and were unable to find a join,
    // so create a cross-product if there are at least two views.
    if (1u < pred_views.size()) {
      CreateProduct(query, clause, context, log);
      changed = true;
      continue;
    }
  }

  // Diagnose functor application failures.
  for (auto pred : context.functors) {
    auto decl = ParsedDeclaration::Of(pred);
    auto err = log.Append(clause.SpellingRange(), pred.SpellingRange());
    err << "Unable to apply functor '" << decl.Name() << "/" << decl.Arity()
        << "' with binding pattern '" << decl.BindingPattern()
        << "' or any of its re-declarations (with different binding patterns)";

    for (auto view : pred_views) {
      auto i = 0u;
      for (auto var : pred.Arguments()) {
        auto param = decl.NthParameter(i++);
        if (!FindColVarInView(context, view, var) &&
            param.Binding() != ParameterBinding::kFree) {

          err.Note(decl.SpellingRange(), param.SpellingRange())
              << "Corresponding parameter is not `free`-attributed";

          err.Note(pred.SpellingRange(), var.SpellingRange()) << "Variable '"
              << var << "' is free here";
        }
      }
    }
    return false;
  }

  // Diagnose negated predicate failures.
  for (auto pred : context.negated_predicates) {
    assert(pred.IsNegated());

    auto decl = ParsedDeclaration::Of(pred);
    auto err = log.Append(clause.SpellingRange(), pred.SpellingRange());
    err << "Unable to negate predicate '" << decl.Name() << "/" << decl.Arity()
        << "'";

    for (auto view : pred_views) {
      auto i = 0u;
      for (auto var : pred.Arguments()) {
        auto param = decl.NthParameter(i++);
        if (!FindColVarInView(context, view, var) && !var.IsUnnamed()) {

          err.Note(pred.SpellingRange(), var.SpellingRange()) << "Variable '"
              << var << "' is free here, but must be bound";

          err.Note(decl.SpellingRange(), param.SpellingRange()) << "Variable '"
              << var << "' corresponds with this parameter";
        }
      }
    }
    return false;
  }

  if (!context.unapplied_compares.empty()) {
    for (auto cmp : context.unapplied_compares) {
      auto err = log.Append(clause_range, cmp.SpellingRange());
      err << "Internal error: Failed to apply inequality comparison "
          << "between '" << cmp.LHS() << "' and '" << cmp.RHS() << "'";
    }
    return false;
  }

  assert(pred_views.size() == 1u);

  auto clause_head = ConvertToClauseHead(
      query, clause, context, log, pred_views[0]);

  // We still don't have a clause head. We might have recorded some "failed
  // heads", so we'll try to re-propose each, but with error reporting turned
  // on.
  //
  // NOTE(pag): The `true` to `ConvertToClauseHead` reports errors.
  if (!clause_head) {
    log.Append(clause.SpellingRange())
        << "No dataflow was produced for this clause";

    for (auto err_head : context.error_heads) {
      ConvertToClauseHead(query, clause, context, log, err_head, true);
    }
    return false;
  }

  const auto decl = ParsedDeclaration::Of(clause);
  INSERT *insert = nullptr;

  // Add the conditions tested.
  if (!clause.PositivePredicates().empty() ||
      !clause.NegatedPredicates().empty()) {
    auto col_index = 0u;
    TUPLE *cond_guard = nullptr;
    if (clause.Arity()) {
      cond_guard = query->tuples.Create();
      cond_guard->color = context.color;
      for (auto var : clause.Parameters()) {
        cond_guard->input_columns.AddUse(clause_head->columns[col_index]);
        (void) cond_guard->columns.Create(var, cond_guard, VarId(context, var),
                                          col_index);
        ++col_index;
      }
    } else {
      cond_guard = clause_head->GuardWithTuple(query, true);
    }

    AddConditionsToInsert(query, clause, cond_guard);
    clause_head = cond_guard;
  }

  // Functor for adding in the `sets_condition` flag. If this is a deletion
  // clause, e.g. `!cond : ...` then we want to add `set_condition` to the
  // DELETE node; however, if it's an insertion clause then we want to add
  // it to the INSERT.
  auto set_condition = false;
  auto add_set_conditon = [=, &set_condition](VIEW *view) {
    if (!set_condition && !decl.Arity()) {
      set_condition = true;
      const auto export_decl = ParsedExport::From(decl);
      auto &cond = query->decl_to_condition[export_decl];
      if (!cond) {
        cond = query->conditions.Create(export_decl);
      }

      view->sets_condition.Emplace(view, cond);
      cond->setters.AddUse(view);
    }
  };

  // The data in `view` must be deleted in our successor.
  if (clause.IsDeletion()) {
    auto col_index = 0u;
    DELETE *const del = query->deletes.Create();
    del->color = context.color;
    if (clause.Arity()) {
      for (auto var : clause.Parameters()) {
        del->input_columns.AddUse(clause_head->columns[col_index]);
        (void) del->columns.Create(var, del, VarId(context, var), col_index);
        ++col_index;
      }
    } else {
      for (auto col : clause_head->columns) {
        del->input_columns.AddUse(col);
        (void) del->columns.Create(col->var, del, VarId(context, col->var),
                                   col_index);
        ++col_index;
      }
    }

    assert(0u < col_index);
    clause_head = del;

    add_set_conditon(clause_head);
  }

  if (decl.IsMessage()) {
    auto &stream = query->decl_to_input[decl];
    if (!stream) {
      stream = query->ios.Create(decl);
    }
    insert = query->inserts.Create(stream, decl);
    insert->color = context.color;
    stream->transmits.AddUse(insert);

  } else {
    auto &rel = query->decl_to_relation[decl];
    if (!rel) {
      rel = query->relations.Create(decl);
    }
    insert = query->inserts.Create(rel, decl);
    insert->color = context.color;
    rel->inserts.AddUse(insert);
  }

  if (clause.IsDeletion()) {
    insert->can_receive_deletions = true;
  }

  for (auto col : clause_head->columns) {
    insert->input_columns.AddUse(col);
  }

  // We just proved a zero-argument predicate, i.e. a condition.
  if (!decl.Arity()) {
    assert(decl.IsExport());
    add_set_conditon(insert);

  } else {
    assert(clause_head->columns.Size() == clause.Arity());
  }

  return true;
}

}  // namespace

std::optional<Query> Query::Build(const ::hyde::ParsedModule &module,
                                  const ErrorLog &log) {

  std::shared_ptr<QueryImpl> impl(new QueryImpl(module));

  ClauseContext context;

  auto num_errors = log.Size();

  for (auto sub_module : ParsedModuleIterator(module)) {
    for (auto clause : sub_module.Clauses()) {
      context.Reset();
      if (!BuildClause(impl.get(), clause, context, log)) {
        return std::nullopt;
      }
    }

    for (auto clause : sub_module.DeletionClauses()) {
      context.Reset();
      if (!BuildClause(impl.get(), clause, context, log)) {
        return std::nullopt;
      }
    }
  }

  impl->RemoveUnusedViews();
  impl->RelabelGroupIDs();
  impl->TrackDifferentialUpdates();

  // TODO(pag): The join canonicalization done in the simplifier introduces
  //            a bug in Solypsis if the dataflow builder builds functors
  //            before joins. I'm not sure why and this is probably serious.
  impl->Simplify(log);
  if (num_errors != log.Size()) {
    return std::nullopt;
  }

  if (!impl->ConnectInsertsToSelects(log)) {
    return std::nullopt;
  }

  impl->RemoveUnusedViews();
  impl->Optimize(log);

  if (num_errors != log.Size()) {
    return std::nullopt;
  }

  impl->ConvertConstantInputsToTuples();
  impl->RemoveUnusedViews();
  impl->ExtractConditionsToTuples();
  impl->TrackDifferentialUpdates(true);
  impl->FinalizeColumnIDs();
  impl->LinkViews();

  return Query(std::move(impl));
}

}  // namespace hyde
