// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/DataFlow/Query.h>
#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Lex/Format.h>
#include <drlojekyll/Parse/ErrorLog.h>
#include <drlojekyll/Parse/Format.h>
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
  COL *col{nullptr};
};

struct WorkItem {
  std::vector<VIEW *> views;
  std::vector<ParsedPredicate> functors;
};

struct ClauseContext {
  void Reset(void) {
    var_id_to_col.clear();
    var_to_col.clear();

    // NOTE(pag): We don't reset `spelling_to_col`.
    col_id_to_constant.clear();
    vars.clear();

    // NOTE(pag): The `hash_join_cache` is preserved.
    work_list.clear();
    error_heads.clear();
    result = nullptr;
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
  std::vector<WorkItem> work_list;

  // List of views that failed to produce valid heads.
  std::vector<VIEW *> error_heads;

  VIEW *result{nullptr};
};

// Look up the ID of `var` in context.
static unsigned VarId(ClauseContext &context, ParsedVariable var) {
  if (auto vc = context.vars[var.Order()].get(); vc) {
    return vc->FindAs<VarColumn>()->id;
  }

  // If this var is a clause parameter.
  if (auto vc = context.var_to_col[var]; vc) {
    const auto ret_id = vc->FindAs<VarColumn>()->id;
    return ret_id;
  }

  assert(false);
  return ~0u;
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
    input->receives.AddUse(view);

  } else if (decl.IsFunctor()) {
    view = query->maps.Create(ParsedFunctor::From(decl), pred.SpellingRange());

  } else if (decl.IsExport() || decl.IsLocal() || decl.IsQuery()) {
    Node<QueryRelation> *input = nullptr;

    if (pred.IsPositive()) {
      auto &rel = query->decl_to_relation[decl];
      if (!rel) {
        rel = query->relations.Create(decl);
      }
      input = rel;

    } else {
      log.Append(ParsedClause::Containing(pred).SpellingRange(),
                 pred.SpellingRange())
          << "TODO: Negations are not yet supported";
      return nullptr;
    }

    view = query->selects.Create(input, pred.SpellingRange());
    input->selects.AddUse(view);

  } else {
    log.Append(ParsedClause::Containing(pred).SpellingRange(),
               pred.SpellingRange())
        << "Internal error: unrecognized/unexpected predicate type";
    return nullptr;
  }

  // Add the output columns to the VIEW associated with the precicate.
  auto col_index = 0u;
  for (auto var : pred.Arguments()) {
    auto vc = context.var_id_to_col[var.UniqueId()];
    assert(vc != nullptr);
    assert(!vc->col);
    vc->col = view->columns.Create(var, view, VarId(context, var), col_index++);
  }

  return view;
}

// Used to semi-deterministically sort columns.
static bool ColumnSort(COL *a, COL *b) {

  const auto a_view_hash = a->view->Hash();
  const auto b_view_hash = b->view->Hash();
  if (a_view_hash != b_view_hash) {
    return a_view_hash < b_view_hash;
  }

  const auto a_index = a->Index();
  const auto b_index = b->Index();
  if (a_index != b_index) {
    return a_index < b_index;
  }

  if (a->id != b->id) {
    return a->id < b->id;
  }

  return a->var.Order() < b->var.Order();
}

// Sort first by hash, second by ID, and then third by order (related to ID),
// then dedup by ID (after a stable sort), then re-sort after deduping.
static void FindUniqueCols(std::vector<COL *> &out_cols) {
  std::sort(out_cols.begin(), out_cols.end(), ColumnSort);
  std::stable_sort(out_cols.begin(), out_cols.end(),
                   [](COL *a, COL *b) { return a->id < b->id; });
  auto it = std::unique(out_cols.begin(), out_cols.end(),
                        [](COL *a, COL *b) { return a->id == b->id; });
  out_cols.erase(it, out_cols.end());
  std::sort(out_cols.begin(), out_cols.end(), ColumnSort);
}

// Find the unique columns proposed by some view. We use this when choosing
// pivot views, filtering, etc.
static void FindUniqueColumns(VIEW *view, std::vector<COL *> &out_cols) {
  out_cols.clear();
  for (auto col : view->columns) {
    out_cols.push_back(col);
  }

  FindUniqueCols(out_cols);
}

// Go over all inequality comparisons in the clause body and try to apply as
// many as possible to the `view_ref`, replacing it each time. We apply this
// to the filtered initial views, as well as the final views before pushing
// a head clause.
static VIEW *GuardWithWithInequality(QueryImpl *query, ParsedClause clause,
                                     ClauseContext &context, VIEW *view) {
  for (auto cmp : clause.Comparisons()) {
    if (ComparisonOperator::kEqual == cmp.Operator()) {
      continue;
    }

    const auto lhs_var = cmp.LHS();
    const auto rhs_var = cmp.RHS();
    const auto lhs_id = VarId(context, lhs_var);
    const auto rhs_id = VarId(context, rhs_var);

    COL *lhs_col = nullptr;
    COL *rhs_col = nullptr;

    for (auto col : view->columns) {
      if (col->id == lhs_id) {
        lhs_col = col;
      } else if (col->id == rhs_id) {
        rhs_col = col;
      }
    }

    if (!lhs_col || !rhs_col) {
      continue;
    }

    CMP *filter = query->compares.Create(cmp.Operator());
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
  const auto initial_view = view;

  // First, canonicalize the column IDs.
  for (auto col : view->columns) {
    const auto vc = context.vars[col->id]->FindAs<VarColumn>();
    if (col->id != vc->id) {
      col->id = vc->id;
    }
  }

  std::vector<COL *> cols;
  cols.reserve(view->columns.Size());

  // First, narrow down the set of columns according to equality comparisons.
  for (auto changed = true; changed;) {
    changed = false;

    cols.clear();
    for (auto col : view->columns) {
      cols.push_back(col);
    }

    // Sort primarily by variable ID, and secondarily by order of the associated
    // column variable.
    std::sort(cols.begin(), cols.end(),
              [](COL *a, COL *b) { return a->id < b->id; });

    for (auto i = 1u; i < cols.size(); ++i) {
      auto col = cols[i];
      auto prev_col = cols[i - 1u];
      if (col->id != prev_col->id) {
        continue;  // Unique.
      }

      CMP *cmp = query->compares.Create(ComparisonOperator::kEqual);
      cmp->input_columns.AddUse(prev_col);
      cmp->input_columns.AddUse(col);

      auto col_index = 0u;
      cmp->columns.Create(col->var, cmp, col->id, col_index++);

      for (auto other_col : view->columns) {
        if (other_col != prev_col && other_col != col) {
          cmp->attached_columns.AddUse(other_col);
          cmp->columns.Create(other_col->var, cmp, other_col->id, col_index++);
        }
      }

      view = cmp;
      changed = true;
      break;
    }
  }

  // Now, compare the remaining columns against constants.
  for (auto assign : clause.Assignments()) {
    const auto lhs_var = assign.LHS();
    auto lhs_id = VarId(context, lhs_var);

    for (auto col : view->columns) {
      if (col->id != lhs_id) {
        continue;
      }

      auto const_col = context.var_id_to_col[lhs_var.UniqueId()]->col;

      CMP *cmp = query->compares.Create(ComparisonOperator::kEqual);
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

  view = GuardWithWithInequality(query, clause, context, view);

  if (view == initial_view) {
    return initial_view;  // We didn't do any filtering.
  }

  // Finally, create a TUPLE that matches up original columns with filtered
  // columns.
  auto col_index = 0u;
  TUPLE *tuple = query->tuples.Create();
  for (auto orig_col : view->columns) {
    const auto vc = context.vars[orig_col->id]->FindAs<VarColumn>();
    for (auto filtered_col : view->columns) {
      if (filtered_col->id == vc->id) {
        (void) tuple->columns.Create(orig_col->var, tuple, vc->id, col_index++);
        tuple->input_columns.AddUse(filtered_col);
        break;
      }
    }
  }

  assert(tuple->columns.Size() == view->columns.Size());

  return tuple;
}

// Returns `true` if a pivot set already includes the column with an equivalence
// class ID of `col_id`.
static bool PivotSetIncludes(const std::vector<COL *> &pivots,
                             size_t num_joined_views, unsigned col_id) {
  for (size_t i = 0u; i < pivots.size(); i += num_joined_views) {
    if (col_id == pivots[i]->id) {
      return true;
    }
  }
  return false;
}

// Go try to expand the pivot set in `base_pivots`. This modifies `base_pivots`
// in place. The result is something like this:
//
//                 .- Initial value of `base_pivots`.
//     .-----------+------------.
//    [ V0_C0, V1_C0, ..., Vn_C0 ]              .- New pivots added in.
//        |      |           |    .-------------+--------------.
//    [ V0_C0, V1_C0, ..., Vn_C0, ..., V0_Cn, V1_Cn, ..., Vn_Cn ]
//
// This function pervasively uses `FindUniqueColumns` to try to put columns in
// a canonical order so that we're likely to see the same behaviour when
// exploring different permutations of the same clause body (pointer order would
// be fine here), as well as across different clauses, e.g. in the case of
// repeated clause bodies or body parts, on different clause heads.
static void FindRelatedPivots(std::vector<COL *> &base_pivots) {
  const auto num_views = base_pivots.size();

  std::vector<COL *> unique_cols;
  std::vector<COL *> smallest_cols;

  // Find the view with the smallest number of columns. That is the maximum
  // number of pivots we can have.
  VIEW *smallest_view = nullptr;
  for (auto col : base_pivots) {
    const auto col_view = col->view;
    if (!smallest_view) {
      smallest_view = col_view;
      FindUniqueColumns(col_view, smallest_cols);
      continue;
    }

    FindUniqueColumns(col_view, unique_cols);
    if (unique_cols.size() < smallest_cols.size()) {
      smallest_view = col_view;
      smallest_cols.swap(unique_cols);
    }
  }

  // We will treat every `col` of `smallest_col` as a possible pivot column.
  // In this case, it must match against all other pivots, and it must not
  // already be a pivot.
  for (auto col : smallest_cols) {

    // Make sure `col` isn't already part of the pivot set.
    //
    // TODO(pag): Think about `foo(A, A), bar(A, A)`.
    if (PivotSetIncludes(base_pivots, num_views, col->id)) {
      continue;
    }

    const auto prev_size = base_pivots.size();

    for (auto i = 0u; i < num_views; ++i) {
      const auto other_view = base_pivots[i]->view;
      if (other_view == smallest_view) {
        base_pivots.push_back(col);
        goto found_possible_pivot;
      }

      FindUniqueColumns(other_view, unique_cols);

      // Go look for a possible pivot in `other_view` that matches the `col`.
      for (auto other_col : unique_cols) {
        if (col->id == other_col->id) {
          base_pivots.push_back(other_col);
          goto found_possible_pivot;
        }
      }

      break;  // Didn't find a possible pivot.

    found_possible_pivot:
      continue;
    }

    // Remove all
    if (auto expected_size = (prev_size + num_views);
        expected_size > base_pivots.size()) {
      base_pivots.resize(prev_size);
    }
  }
}

// Lookup `join` in the hash join cache. If we find it, then mark `join` for
// deletion and update and use the cached join. If we don't find it then cache
// `join` and return it.
static JOIN *CacheJoin(ClauseContext &context, JOIN *join) {
  auto &cached_joins = context.hash_join_cache[join->Hash()];

  EqualitySet eq_set;
  for (auto cached_join : cached_joins) {
    if (!join->Equals(eq_set, cached_join)) {
      continue;
    }

    // It could be that we're taking this cached join from a totally
    // different clause, so we need to update the column variables and IDs
    // to be "clause local".
    auto i = 0u;
    for (auto col : cached_join->columns) {
      const auto curr_col = join->columns[i++];
      assert(col->Hash() == curr_col->Hash());
      assert(col->type.Kind() == curr_col->type.Kind());
      col->var = curr_col->var;
      col->id = curr_col->id;
    }

    join->PrepareToDelete();
    return cached_join;
  }

  cached_joins.push_back(join);

  return join;
}

// When we used cached JOINs, it might end up being the case that we try to
// JOIN the same JOIN twice, and so we want to put an intermediate TUPLE view
// in front of every JOIN so that they look different.
static TUPLE *CreateIntermediary(QueryImpl *query, VIEW *view) {
  auto tuple = query->tuples.Create();
  auto col_index = 0u;
  for (auto col : view->columns) {
    tuple->input_columns.AddUse(col);
    (void) tuple->columns.Create(col->var, tuple, col->id, col_index++);
  }
  return tuple;
}

// Create a JOIN from the pivots in `pivots`.
static JOIN *CreateJoinFromPivots(QueryImpl *query, ParsedClause clause,
                                  ClauseContext &context,
                                  std::vector<COL *> &pivots,
                                  size_t num_joined_views) {

  // Sort then expand the pivot set. Before sorting, `pivots` will contain
  // `N` columns, each with different views. After expanding, `pivots` will
  // contain `M*N` pivots, where `pivots[N*m + j] for m < M and j < N` each
  // share the same view.
  std::sort(pivots.begin(), pivots.end(), ColumnSort);
  assert(pivots.size() == num_joined_views);

  auto join = query->joins.Create();

  for (auto i = 0u; i < num_joined_views; ++i) {
    assert(!i || pivots[i]->id == pivots[i - 1u]->id);
    assert(!i || pivots[i]->view != pivots[i - 1u]->view);
    join->joined_views.AddUse(pivots[i]->view);
  }

  // Expand the pivot set.
  FindRelatedPivots(pivots);

  // Fill in the pivoted views.
  join->num_pivots = static_cast<unsigned>(pivots.size() / num_joined_views);

  const auto num_pivots = pivots.size();
  assert(!(num_pivots % num_joined_views));

  std::vector<std::pair<COL *, unsigned>> pivot_cols;

  auto col_index = 0u;
  for (size_t i = 0u; i < num_pivots; i += num_joined_views) {

    // Try to choose a leader column for this join pivot in a way that is
    // consistent across duplicated clause bodies.
    COL *leader_col = pivots[i];
    for (auto j = 0u; j < num_joined_views; ++j) {
      if (ColumnSort(pivots[i + j], leader_col)) {
        leader_col = pivots[i + j];
      }
    }

    pivot_cols.emplace_back(leader_col, i);
  }

  // Make sure the pivots are sorted in a way that will be consistent across
  // clauses.
  std::sort(pivot_cols.begin(), pivot_cols.end(),
            [](std::pair<COL *, unsigned> a, std::pair<COL *, unsigned> b) {
              return ColumnSort(a.first, b.first);
            });

  for (auto [leader_col, i] : pivot_cols) {

    // Add an output column for this pivot leader, and all columns into the
    // pivot set.
    auto out_col = join->columns.Create(leader_col->var, join, leader_col->id,
                                        col_index++);
    join->out_to_in.emplace(out_col, join);

    auto pivot_set_it = join->out_to_in.find(out_col);
    for (auto j = 0u; j < num_joined_views; ++j) {
      const auto in_col = pivots[i + j];
      pivot_set_it->second.AddUse(in_col);
    }
  }

  std::vector<COL *> unique_cols;

  // Add in all non pivot columns, and only if they don't already shadow a
  // pivot column.
  for (auto i = 0u; i < num_joined_views; ++i) {
    auto pivot_view = pivots[i]->view;
    FindUniqueColumns(pivot_view, unique_cols);

    for (auto in_col : unique_cols) {
      if (!PivotSetIncludes(pivots, num_joined_views, in_col->id)) {
        auto out_col =
            join->columns.Create(in_col->var, join, in_col->id, col_index++);
        join->out_to_in.emplace(out_col, join);
        auto pivot_set_it = join->out_to_in.find(out_col);
        pivot_set_it->second.AddUse(in_col);
      }
    }
  }

#ifndef NDEBUG
  for (auto &[out, in_cols] : join->out_to_in) {
    assert(!in_cols.Empty());
    (void) out;
  }
#endif

  return CacheJoin(context, join);
}

// Try to create a new VIEW that will publish just constants, e.g. `foo(1).`.
static VIEW *FindConstantClauseHead(QueryImpl *query, ParsedClause clause,
                                    ClauseContext &context) {
  std::vector<COL *> const_cols;
  for (auto var : clause.Parameters()) {
    const auto var_id = VarId(context, var);

    for (auto &vc : context.vars) {
      if (!vc) {
        continue;
      }

      const auto found_id = vc->FindAs<VarColumn>()->id;
      if (var_id != found_id) {
        continue;
      }

      if (auto const_col = context.col_id_to_constant[found_id]; const_col) {
        const_cols.push_back(const_col);
        goto found_col;
      }
    }

    return nullptr;

  found_col:
    continue;
  }

  TUPLE *tuple = query->tuples.Create();
  auto col_index = 0u;
  for (auto var : clause.Parameters()) {
    (void) tuple->columns.Create(var, tuple, VarId(context, var), col_index);
    tuple->input_columns.AddUse(const_cols[col_index++]);
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

  // Try to find the column as a constant.
  return context.col_id_to_constant[id];
}

// Propose `view` as being a source of data for the clause head.
static bool ConvertToClauseHead(QueryImpl *query, ParsedClause clause,
                                ClauseContext &context, const ErrorLog &log,
                                VIEW *view, bool report = false) {

  // Applies equality and inequality checks.
  view = GuardViewWithFilter(query, clause, context, view);

  // Proved a zero-argument predicate.
  //
  // NOTE(pag): We totally ignore equivalence classes in these cases.
  if (!clause.Arity()) {
    context.result = view;
    return true;
  }

  TUPLE *tuple = query->tuples.Create();

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
        context.error_heads.push_back(view);
        return true;
      }

      const auto clause_range = clause.SpellingRange();
      auto err = log.Append(clause_range, var.SpellingRange());
      err << "Internal error: could not match variable '" << var
          << "' against any columns";

      for (auto in_col : view->columns) {
        err.Note(clause_range, in_col->var.SpellingRange())
            << "Failed to match against '" << in_col->var << "'";
      }

      return context.result ? true : false;
    }
  }

  const auto tuple_hash = tuple->Hash();

  if (!context.result) {
    context.result = tuple;

  } else if (tuple_hash == context.result->Hash()) {
    assert(false && "TODO: What was this??");

  } else if (auto other_tuple = context.result->AsTuple(); other_tuple) {
    MERGE *merge = query->merges.Create();
    merge->merged_views.AddUse(tuple);
    merge->merged_views.AddUse(other_tuple);

    auto col_index = 0u;
    for (auto var : clause.Parameters()) {
      (void) merge->columns.Create(var, merge, VarId(context, var),
                                   col_index++);
    }

    context.result = merge;

  } else if (auto merge = context.result->AsMerge(); merge) {

    // Given that we're saying this MERGE is an equivalence class, we can say
    // that if any two things have the same hash, even if they aren't
    // necessarily unique, we can ignore one of them, because they are
    // equivalent anyway.
    for (auto incoming_view : merge->merged_views) {
      if (incoming_view->Hash() == tuple_hash) {
        return true;
      }
    }
    merge->merged_views.AddUse(tuple);

  } else {
    assert(false && "context.result should always be a TUPLE * or a MERGE *");
    return false;
  }

  return true;
}

// Create a PRODUCT from multiple VIEWs.
static void CreateProduct(QueryImpl *query, ParsedClause clause,
                          ClauseContext &context, const ErrorLog &log,
                          WorkItem &work_item) {
  auto &views = work_item.views;

  std::vector<COL *> unique_cols;
  for (auto view : views) {
    for (auto col : view->columns) {
      unique_cols.push_back(col);
    }
  }

  FindUniqueCols(unique_cols);

  std::sort(views.begin(), views.end(),
            [](VIEW *a, VIEW *b) { return a->Hash() < b->Hash(); });

  auto join = query->joins.Create();
  for (auto view : views) {
    join->joined_views.AddUse(view);
  }

  auto col_index = 0u;
  for (auto in_col : unique_cols) {
    auto out_col =
        join->columns.Create(in_col->var, join, in_col->id, col_index++);
    join->out_to_in.emplace(out_col, join);
    auto pivot_set_it = join->out_to_in.find(out_col);
    pivot_set_it->second.AddUse(in_col);
  }

#ifndef NDEBUG
  for (auto &[out, in_cols] : join->out_to_in) {
    assert(!in_cols.Empty());
    (void) out;
  }
#endif

  join = CacheJoin(context, join);

  work_item.views.clear();
  work_item.views.push_back(join);
  context.work_list.emplace_back(std::move(work_item));
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
  std::vector<std::variant<std::pair<ParsedVariable, COL *>, ParsedVariable>>
      inouts;
  std::vector<std::pair<COL *, COL *>> checks;
  std::unordered_set<std::string> seen_variants;
  VIEW *out_view = nullptr;

  for (auto redecl : decl.Redeclarations()) {
    inouts.clear();
    checks.clear();

    // We may have duplicate redeclarations, so don't repeat any.
    std::string binding(redecl.BindingPattern());
    if (seen_variants.count(binding)) {
    try_next_redecl:
      continue;
    }
    seen_variants.insert(std::move(binding));

    auto needs_checks = false;

    for (auto param : redecl.Parameters()) {
      const auto var = pred.NthArgument(param.Index());
      const auto bound_col = FindColVarInView(context, view, var);

      if (param.Binding() == ParameterBinding::kBound) {
        if (bound_col) {
          checks.emplace_back(nullptr, nullptr);
          inouts.emplace_back(std::make_pair(var, bound_col));
        } else {
          goto try_next_redecl;
        }

      } else if (bound_col) {
        checks.emplace_back(bound_col, bound_col);
        inouts.emplace_back(var);
        needs_checks = true;

      } else {
        checks.emplace_back(nullptr, nullptr);
        inouts.emplace_back(var);
      }
    }

    // Apply `pred` to the columns in `inouts`.
    MAP *map =
        query->maps.Create(ParsedFunctor::From(redecl), pred.SpellingRange());
    VIEW *result = map;
    auto col_index = 0u;

    for (auto &var_col_or_var : inouts) {

      COL *out_col = nullptr;

      // It's just a variable, i.e. an output.
      if (var_col_or_var.index()) {
        const auto var = std::get<1>(var_col_or_var);
        const auto id = VarId(context, var);
        out_col = map->columns.Create(var, map, id, col_index);

      // It's a variable and a COLumn, i.e. it's an input that we'll forward
      // as an output.
      } else {
        const auto [var, col] = std::get<0>(var_col_or_var);
        const auto id = VarId(context, var);
        out_col = map->columns.Create(var, map, id, col_index);
        map->input_columns.AddUse(col);
      }

      // Do we need to record what the output column is to check against it
      // later?
      auto &check = checks[col_index++];
      if (check.first) {
        check.first = out_col;
      }
    }

    // Attach other columns from the view to the functor.
    for (auto in_col : view->columns) {
      if (!FindColVarInView(context, map, in_col->var)) {
        map->columns.Create(in_col->var, map, in_col->id, col_index++);
        map->attached_columns.AddUse(in_col);
      }
    }

    // Some of the `free`-attributed columns of the functor are associated with
    // columns in `view`. We need to make sure that the values all match. This
    // means guarding the MAP with a bunch of CMPs.
    if (needs_checks) {

      // First, keep track of the set of goal columns that we want to get back
      // to after doing all the comparisons.
      std::vector<COL *> goal_cols;
      goal_cols.reserve(map->columns.Size());
      for (auto col : map->columns) {
        goal_cols.push_back(col);
      }

      // Then, attach more columns to the MAP that pass around the old,
      // possibly same-ID'd values, around from inputs to outputs. This is
      // needed so that the CMPs always operate on inputs from the same view
      // and so that we don't need to JOIN `view` back into `map` just so that
      // we can do the equality comparisons.
      for (auto &check : checks) {
        if (check.second) {
          map->attached_columns.AddUse(check.second);
          check.second = map->columns.Create(check.second->var, map,
                                             check.second->id, col_index++);
        }
      }

      auto i = 1u;
      const auto num_checks = checks.size();
      for (auto &check : checks) {
        if (!check.first || !check.second) {
          ++i;
          continue;
        }

        CMP *cmp = query->compares.Create(ComparisonOperator::kEqual);
        cmp->input_columns.AddUse(check.first);
        cmp->input_columns.AddUse(check.second);
        cmp->columns.Create(check.first->var, cmp, check.first->id, 0u);

        for (auto col : result->columns) {
          if (col == check.first || col == check.second) {
            continue;
          }
          cmp->attached_columns.AddUse(col);
          auto out_col =
              cmp->columns.Create(col->var, cmp, col->id, cmp->columns.Size());

          // Rewrite the next checks in terms of the results of the `cmp`.
          for (auto j = i; j < num_checks; ++j) {
            auto &next_check = checks[j];
            if (next_check.first == col) {
              next_check.first = out_col;
            } else if (next_check.second == col) {
              next_check.second = out_col;
            }
          }
        }

        result = cmp;

        ++i;
      }

      // Now make a TUPLE that re-exposes the correct columns in the correct
      // order.
      TUPLE *tuple = query->tuples.Create();
      for (auto goal_col : goal_cols) {
        auto in_col = FindColVarInView(context, result, goal_col->var);
        assert(in_col != nullptr);
        tuple->input_columns.AddUse(in_col);
        tuple->columns.Create(goal_col->var, tuple, goal_col->id,
                              goal_col->index);
      }

      result = tuple;
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

// Try to apply as many functors as possible to `view`.
static bool TryApplyFunctors(QueryImpl *query, ParsedClause clause,
                             ClauseContext &context, const ErrorLog &log,
                             WorkItem &work_item) {

  const auto num_views = work_item.views.size();

  std::vector<ParsedPredicate> unapplied_functors;
  unapplied_functors.reserve(work_item.functors.size());

  bool updated = false;

  for (auto i = 0u; i < num_views; ++i) {
    auto &view = work_item.views[i];

    unapplied_functors.clear();
    for (auto pred : work_item.functors) {
      if (auto out_view = TryApplyFunctor(query, context, pred, view);
          out_view) {
        view = out_view;
        updated = true;

      } else {
        unapplied_functors.push_back(pred);
      }
    }

    if (updated) {
      work_item.functors.swap(unapplied_functors);
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

  return view;
}

// Go find join candidates. This takes the first view in `views` and tries to
// join each of its columns against every other view, then proposes this as
// a new candidate.
static void FindJoinCandidates(QueryImpl *query, ParsedClause clause,
                               ClauseContext &context, const ErrorLog &log,
                               WorkItem &work_item) {
  auto &views = work_item.views;
  const auto num_views = views.size();

  // Nothing left to do but try to publish the view!
  if (num_views == 1u && work_item.functors.empty()) {
    ConvertToClauseHead(query, clause, context, log, views[0]);
    return;
  }

  std::vector<COL *> unique_cols;
  std::vector<COL *> pivots;
  std::vector<VIEW *> unjoined_views;

  auto num_joins_produced = 0u;

  // Try to find a join candidate. If we fail, then we will rotate
  // `views`.
  for (auto num_rotations = 0u; 1u < num_views && num_rotations < num_views;
       ++num_rotations) {

    FindUniqueColumns(views[0], unique_cols);
    unjoined_views.clear();

    for (auto col : unique_cols) {
      unjoined_views.clear();

      pivots.clear();
      pivots.push_back(col);

      for (auto i = 1u; i < num_views; ++i) {
        const auto target_view = views[i];
        for (auto maybe_pivot_col : target_view->columns) {

          // Take the first column that matches. We don't need to worry about
          // missing a situation like `foo(A), bar(A, A)` where we join against
          // the first `A` of `bar` because in `BuildClause` we explore the
          // permutation `bar(A, A), foo(A)`, and we'll try using the second `A`
          // of `bar(A, A)` as a base pivot.
          //
          // TODO(pag): The more than two case, e.g. `foo(A, A, A), bar(A, A, A)`,
          //            could be problematic.
          if (col->id == maybe_pivot_col->id) {
            pivots.push_back(maybe_pivot_col);
            goto next_view;
          }
        }

        unjoined_views.push_back(target_view);

      next_view:
        continue;
      }

      // Failed to join `col` against anything else.
      const auto num_joined_views = pivots.size();
      if (1u >= num_joined_views) {
        continue;
      }

      assert((pivots.size() + unjoined_views.size()) == views.size());

      // Add this join variant into the work list.
      ++num_joins_produced;

      // Add the JOIN to the *end* of the `unjoined_views` list. This has the
      // effect of trying to make a join out of everything previously unjoined,
      // using whatever was at the front of that list. If we always put it at
      // the front then we'd be more likely to have a tall tree of JOINs rather
      // than a wide tree of JOINs.
      auto join = CreateJoinFromPivots(query, clause, context, pivots,
                                       num_joined_views);
      unjoined_views.push_back(CreateIntermediary(query, join));

      context.work_list.emplace_back();
      auto &new_work_item = context.work_list.back();
      unjoined_views.swap(new_work_item.views);
      new_work_item.functors = work_item.functors;

      // NOTE(pag): Comment this out if additional JOIN exploration is desirable.
      return;
    }

    // At least one join was produced.
    if (num_joins_produced) {
      return;  // Don't rotate; we succeeded.
    }

    // We might have failed to find a pivot in `views[0]`, but there still might
    // be joinable stuff in `views[1:]`, so re-run on the tail, moving `views[0]`
    // to the end of the list.
    unjoined_views.clear();
    for (auto i = 1u; i < num_views; ++i) {
      unjoined_views.push_back(views[i]);
    }
    unjoined_views.push_back(views[0]);
    views.swap(unjoined_views);
  }

  // Okay, we've rotated the views around, but failed to find any JOIN
  // opportunities. Our next best bet is to try to apply any of the functors or
  // the aggregates.

  // We applied at least one functor and updated `work_item` in place.
  if (TryApplyFunctors(query, clause, context, log, work_item)) {
    context.work_list.emplace_back(std::move(work_item));
    return;
  }

  if (1 == num_views && !work_item.functors.empty()) {
    for (auto pred : work_item.functors) {
      auto decl = ParsedDeclaration::Of(pred);
      auto err = log.Append(clause.SpellingRange(), pred.SpellingRange());
      err << "Unable to apply functor '" << decl.Name() << "/" << decl.Arity()
          << "' with binding pattern '" << decl.BindingPattern()
          << "' or any of its re-declarations (with different binding patterns)";

      auto i = 0u;
      for (auto var : pred.Arguments()) {
        auto param = decl.NthParameter(i++);
        if (!FindColVarInView(context, work_item.views[0], var) &&
            param.Binding() != ParameterBinding::kFree) {

          err.Note(decl.SpellingRange(), param.SpellingRange())
              << "Corresponding parameter is not `free`-attributed";

          err.Note(pred.SpellingRange(), var.SpellingRange())
              << "Variable '" << var << "' is free here";
        }
      }
    }

    return;
  }

  // We're basically done: we need to form the cross-product of all views and
  // propose that to the clause head.
  assert(1u < num_views);

  CreateProduct(query, clause, context, log, work_item);
}

// Make the INSERT conditional on any zero-argument predicates.
static void AddConditionsToInsert(QueryImpl *query, ParsedClause clause,
                                  INSERT *insert) {
  std::vector<COND *> conds;

  auto add_conds = [&](NodeRange<ParsedPredicate> range, UseList<COND> &uses) {
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
      uses.AddUse(cond);
    }
  };

  add_conds(clause.PositivePredicates(), insert->positive_conditions);
  add_conds(clause.NegatedPredicates(), insert->negative_conditions);
}

// The goal of this function is to build multiple equivalent dataflows out of
// a single clause body. When we have a bunch of predicates, there are usually
// many ways in which they can be joined.
static bool BuildClause(QueryImpl *query, ParsedClause clause,
                        ClauseContext &context, const ErrorLog &log) {

  std::vector<VIEW *> pred_views;

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

  for (auto pred : clause.NegatedPredicates()) {
    if (pred.Arity()) {
      if (auto view = BuildPredicate(query, context, pred, log); view) {
        assert(false && "TODO: Handle negated predicates");
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
    ss << literal.Type().Spelling() << ':' << literal.Spelling();
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

    vc->col = const_col;
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
  // if we have `foo(A, A)` then we replace it with a FILTER than does a
  // comparison between the output columns of the original view and then only
  // presents a single `A`.
  for (auto &view : pred_views) {
    view = GuardViewWithFilter(query, clause, context, view);
  }

  // Add the views in order as the first work item.
  //
  // NOTE(pag): Add in all rotations of `pred_views` or all permutations of
  //            `pred_views` to enable searching of most of the JOIN space
  //            and produce equivalence classes.
  context.work_list.emplace_back();
  auto &work_item = context.work_list.back();
  work_item.views.swap(pred_views);

  // Go add the functors and aggregates in.
  for (auto pred : clause.PositivePredicates()) {
    const auto decl = ParsedDeclaration::Of(pred);
    if (decl.IsFunctor()) {
      work_item.functors.push_back(pred);
    }
  }

  // Process the work list until we find some order of things that works.
  //
  // NOTE(pag): Remove `!context.result` to enable equivalence-class building.
  while (!context.work_list.empty() && !context.result) {
    auto work_item = std::move(context.work_list.back());
    context.work_list.pop_back();

    FindJoinCandidates(query, clause, context, log, work_item);
  }

  // If the clause is something like `foo(1, 2, 3).` then there would have
  // been no joins or anything else that would have fixed it up.
  if (!context.result) {
    if (auto const_view = FindConstantClauseHead(query, clause, context);
        const_view) {
      ConvertToClauseHead(query, clause, context, log, const_view);
    }
  }

  // We still don't have a clause head. We might have recorded some "failed
  // heads", so we'll try to re-propose each, but with error reporting turned
  // on.
  if (!context.result) {
    log.Append(clause.SpellingRange())
        << "No dataflow was produced for this clause";

    for (auto err_head : context.error_heads) {
      ConvertToClauseHead(query, clause, context, log, err_head, true);
    }
    return false;
  }

  const auto decl = ParsedDeclaration::Of(clause);
  INSERT *insert = nullptr;

  if (decl.IsMessage()) {
    auto &stream = query->decl_to_input[decl];
    if (!stream) {
      stream = query->ios.Create(decl);
    }
    insert = query->inserts.Create(stream, decl, !clause.IsDeletion());
    stream->transmits.AddUse(insert);

  } else {
    auto &rel = query->decl_to_relation[decl];
    if (!rel) {
      rel = query->relations.Create(decl);
    }
    insert = query->inserts.Create(rel, decl, !clause.IsDeletion());
    rel->inserts.AddUse(insert);
  }

  for (auto col : context.result->columns) {
    insert->input_columns.AddUse(col);
  }

  AddConditionsToInsert(query, clause, insert);

  // We just proved a zero-argument predicate, i.e. a condition.
  if (!decl.Arity()) {
    assert(decl.IsExport());
    const auto export_decl = ParsedExport::From(decl);
    auto &cond = query->decl_to_condition[export_decl];
    if (!cond) {
      cond = query->conditions.Create(export_decl);
    }

    insert->sets_condition.Emplace(insert, cond);
    cond->setters.AddUse(insert);
  }

  return true;
}

}  // namespace

std::optional<Query> Query::Build(const ParsedModule &module,
                                  const ErrorLog &log) {

  std::shared_ptr<QueryImpl> impl(new QueryImpl);

  ClauseContext context;

  auto num_errors = log.Size();

  for (auto clause : module.Clauses()) {
    context.Reset();
    if (!BuildClause(impl.get(), clause, context, log)) {
      return std::nullopt;
    }
  }

  for (auto clause : module.DeletionClauses()) {
    context.Reset();
    if (!BuildClause(impl.get(), clause, context, log)) {
      return std::nullopt;
    }
  }

  impl->RemoveUnusedViews();
  impl->RelabelGroupIDs();
  impl->TrackDifferentialUpdates();
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

  //  impl->SinkConditions();
  impl->TrackDifferentialUpdates();
  impl->FinalizeColumnIDs();
  impl->LinkViews();

  return Query(std::move(impl));
}

}  // namespace hyde
