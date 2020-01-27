// Copyright 2019, Trail of Bits. All rights reserved.

#include <drlojekyll/Rel/Builder.h>

#include <set>
#include <tuple>
#include <unordered_set>

#include <drlojekyll/Sema/SIPSScore.h>
#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Lex/Format.h>
#include <drlojekyll/Parse/Format.h>
#include <drlojekyll/Util/EqualitySet.h>

#include "Query.h"

namespace hyde {

extern OutputStream *gOut;

class QueryBuilderImpl : public SIPSVisitor {
 public:
  QueryBuilderImpl(void)
      : context(std::make_shared<query::QueryContext>()),
        query(std::make_shared<QueryImpl>(context)) {}

  QueryBuilderImpl(const std::shared_ptr<query::QueryContext> &context_)
      : context(context_),
        query(std::make_shared<QueryImpl>(context_)) {}


  virtual ~QueryBuilderImpl(void) = default;

  REL *TableFor(ParsedDeclaration decl, bool is_positive=true) {
    assert(decl.IsLocal() || decl.IsExport() || decl.IsQuery());

    auto &rels = is_positive ? context->decl_to_pos_relation :
                               context->decl_to_neg_relation;
    auto &table = rels[decl];
    if (!table) {
      table = context->relations.Create(decl, is_positive);
    }

    return table;
  }

  // Get the table for a given predicate.
  REL *TableFor(ParsedPredicate pred) {
    return TableFor(ParsedDeclaration::Of(pred), pred.IsPositive());
  }

  STREAM *StreamFor(ParsedLiteral literal) {
    if (literal.IsNumber() || literal.IsString()) {
      std::string spelling(literal.Spelling());
      spelling += literal.Type().Spelling();  // Make them type-specific.

      // TODO(pag): Render the spelling into an actual integer value.
      auto &stream = context->spelling_to_constant[spelling];
      if (!stream) {
        stream = context->constants.Create(literal);
      }
      return stream;

    } else {
      assert(false);
      return nullptr;
    }
  }

  STREAM *StreamFor(ParsedDeclaration decl) {
    auto &stream = context->decl_to_input[decl];
    if (!stream) {
      stream = context->inputs.Create(decl);
    }
    return stream;
  }

  STREAM *StreamFor(ParsedFunctor functor) {
    auto &stream = context->decl_to_generator[functor];
    if (!stream) {
      stream = context->generators.Create(functor);
    }
    return stream;
  }

  SELECT *SelectFor(ParsedPredicate pred) {
    const auto decl = ParsedDeclaration::Of(pred);
    if (decl.IsMessage()) {
      const auto stream = StreamFor(ParsedMessage::From(decl));
      const auto select = query->selects.Create(stream);
      select->group_ids.push_back(select_group_id);
      return select;

    } else if (decl.IsFunctor()) {
      const auto stream = StreamFor(ParsedFunctor::From(decl));
      const auto select = query->selects.Create(stream);
      select->group_ids.push_back(select_group_id);
      return select;

    } else {
      auto table = TableFor(pred);
      const auto select = query->selects.Create(table);
      select->group_ids.push_back(select_group_id);
      return select;
    }
  }

  // Add a column to a view.
  COL *AddColumn(VIEW *view, const Column &column) {
    return view->columns.Create(column.var, view, column.id, column.n);
  }

  void Begin(ParsedPredicate pred) override {
    id_to_col.clear();
    pending_compares.clear();
    next_pending_compares.clear();
    unresolved_compares.clear();
    next_unresolved_compares.clear();
    pending_presence_checks.clear();
    joined_cols.clear();
    where_cols.clear();
    sips_cols.clear();
    select_group_id += 1;

    const auto decl = ParsedDeclaration::Of(pred);
    if (decl.IsMessage()) {
      initial_view = query->selects.Create(StreamFor(decl));
      input_view = initial_view;
    } else {
      initial_view = query->selects.Create(TableFor(decl));
      input_view = query->selects.Create(StreamFor(decl));
    }

    initial_view->group_ids.push_back(select_group_id);
  }

  void DeclareParameter(const Column &param) override {
    auto &prev_colset = id_to_col[param.id];
    const auto param_col = AddColumn(initial_view, param);
    if (input_view != initial_view) {
      AddColumn(input_view, param);
    }
    if (prev_colset) {
      const auto prev_col = prev_colset->Leader();
      pending_compares.emplace_back(
          ComparisonOperator::kEqual, prev_col->var, prev_col,
          param_col->var, param_col);
    } else {
      prev_colset = param_col->equiv_columns;
    }
  }

  // Constants are like infinitely sized tables with a single size. You
  // select from them.
  void DeclareConstant(ParsedLiteral val, unsigned id) override {

    const auto stream = StreamFor(val);

    auto &prev_colset = id_to_col[id];
    if (prev_colset) {
      const auto prev_col = prev_colset->Leader();
      const auto sel = prev_col->view->AsSelect();
      assert(sel != nullptr);
      assert(sel->stream);
      assert(sel->stream->AsConstant() != nullptr);
      return;
    }

    const auto select = query->selects.Create(stream);
    const auto col = select->columns.Create(
        ParsedVariable::AssignedTo(val), select, id, 0u);

    prev_colset = col->equiv_columns;
  }

  template <typename T>
  std::vector<COL *> ViewFor(const T &cols) {
    std::vector<COL *> ret_cols;

    VIEW *tuple_view = cols[0]->Find()->view;
    for (COL *col : cols) {
      col = col->Find();
      if (col->view != tuple_view) {
        goto make_tuple;
      }
      ret_cols.push_back(col);
    }

    return ret_cols;

  make_tuple:
    ret_cols.clear();
    const auto tuple = query->tuples.Create();
    for (COL *col : cols) {
      col = col->Find();
      auto out_col = tuple->columns.Create(
          col->var, tuple, col->id, tuple->columns.Size());
      COL::Union(col, out_col);
      col->ReplaceAllUsesWith(out_col);
      ret_cols.push_back(out_col);
    }

    for (COL *col : cols) {
      tuple->input_columns.AddUse(col);
    }

    return ret_cols;
  }

  // Create a join of some set of columns against all columns in a particular
  // relation.
  //
  // TODO(pag): We do `Find` on all columns in `cols`, but not all columns
  //            in `select`. Maybe do that too.
  void CreateFullJoin(VIEW *select, std::vector<COL *> &cols) {
    if (cols.empty()) {
      return;
    }

    const auto num_cols = select->columns.Size();
    assert(num_cols == cols.size());

    const auto select_cols = ViewFor(select->columns);
    const auto tuple_cols = ViewFor(cols);

    assert(select_cols.size() == num_cols);
    assert(tuple_cols.size() == num_cols);

    const auto lhs_view = select_cols[0]->view;
    const auto rhs_view = tuple_cols[0]->view;

    // This isn't actually a full join! This can happen when the most up-to-date
    // versions of the columns from either the select or the tuple come from
    // "larger" views, i.e. views that have more columns than `num_cols`.
    if (lhs_view->columns.Size() != num_cols ||
        rhs_view->columns.Size() != num_cols) {
      for (auto i = 0u; i < num_cols; ++i) {
        const auto lhs_col = select_cols[i];
        const auto rhs_col = tuple_cols[i];
        pending_compares.emplace_back(
            ComparisonOperator::kEqual, lhs_col->var, lhs_col,
            rhs_col->var, rhs_col);
      }
      return;
    }

    auto join = query->joins.Create();
    for (auto i = 0u; i < num_cols; ++i) {
      const auto sel_col = select_cols[i];
      const auto tuple_col = tuple_cols[i];
      auto join_col = join->columns.Create(
          sel_col->var, join, sel_col->id, join->columns.Size());

      COL::Union(sel_col, join_col);
      COL::Union(tuple_col, join_col);

      tuple_col->ReplaceAllUsesWith(join_col);
      sel_col->ReplaceAllUsesWith(join_col);

      join->out_to_in.emplace(join_col, join);
      join->num_pivots++;
    }

    for (auto i = 0u; i < num_cols; ++i) {
      const auto sel_col = select_cols[i];
      const auto tuple_col = tuple_cols[i];
      auto input_cols = join->out_to_in.find(join->columns[i]);
      input_cols->second.AddUse(sel_col);
      input_cols->second.AddUse(tuple_col);
    }
  }

  // Create a join that is the cross-product of two relations. Return the new
  // values of `lhs` and `rhs` in this product table.
  std::pair<COL *, COL *> CreateProduct(COL *lhs, COL *rhs) {
    std::vector<COL *> seen;
    seen.reserve(2);

    // Drill down and find the source of `col`. If `col` is from another
    // cross-product, then go take its source column, rather than possibly
    // merging in the whole product relation.
    auto drill_down = [&seen] (COL *col) {
      while (true) {
        if (std::find(seen.begin(), seen.end(), col) != seen.end()) {
          return col;
        }

        seen.push_back(col);

        if (auto view_join = col->view->AsJoin()) {
          if (view_join->num_pivots) {
            return col;

          } else {
            auto in_set = view_join->out_to_in.find(col);
            assert(in_set != view_join->out_to_in.end());
            assert(in_set->second.Size() == 1);

            col = in_set->second[0];
          }
        } else {
          return col;
        }
      }
    };

    lhs = drill_down(lhs);
    rhs = drill_down(rhs);

    if (lhs->view == rhs->view) {
      return {lhs, rhs};
    }

    std::pair<COL *, COL *> ret = {nullptr, nullptr};

    auto join = query->joins.Create();
    for (auto view : {lhs->view, rhs->view}) {
      for (auto col : view->columns) {
        const auto out_col = join->columns.Create(
            col->var, join, col->id, join->columns.Size());

        join->out_to_in.emplace(out_col, join);

        COL::Union(out_col, col);
        col->ReplaceAllUsesWith(out_col);

        if (col == lhs) {
          ret.first = out_col;
        } else if (col == rhs) {
          ret.second = out_col;
        }
      }
    }

    // Now add the uses.
    auto i = 0u;
    for (auto view : {lhs->view, rhs->view}) {
      for (auto col : view->columns) {
        const auto out_col = join->columns[i++];
        join->out_to_in.find(out_col)->second.AddUse(col);
      }
    }

    assert(ret.first != nullptr);
    assert(ret.second != nullptr);

    return ret;
  }

  // Create a join based off of an equivalence class of columns.
  void CreateJoin(const std::unordered_set<COL *> &eq_class) {
    if (eq_class.empty() || eq_class.size() == 1) {
      return;
    }

    std::unordered_set<VIEW *> eq_views;
    for (auto col : eq_class) {
      eq_views.insert(col->view);
    }

    if (eq_views.size() == 1) {
      return;
    }

    const auto join = query->joins.Create();

    // Then, group all incoming columns by their equivalence classes. We
    // may have more than one join pivot to deal with.
    std::unordered_map<COL *, std::vector<COL *>> grouped_cols;
    for (auto view : eq_views) {
      for (auto col : view->columns) {
        grouped_cols[col->Find()].push_back(col);
      }
    }

    // First, handle column groups, where the number of grouped columns matches
    // the number of columns in our pivot's equivalence class. These can be
    // used as additional pivots.
    for (auto &col_group : grouped_cols) {
      if (col_group.second.size() == eq_class.size()) {
        ++join->num_pivots;

        const auto pivot_col = join->columns.Create(
            col_group.first->var, join, col_group.first->id,
            join->columns.Size());

        join->out_to_in.emplace(pivot_col, join);

        COL::Union(col_group.first, pivot_col);

        for (auto prev_col : col_group.second) {
          prev_col->ReplaceAllUsesWith(pivot_col);
        }
      }
    }

    // Next, handle column groups where the column group size does not match
    // the pivot's equivalence class size. These cannot be used as pivots of
    // the join.
    for (auto &col_group : grouped_cols) {
      if (col_group.second.size() != eq_class.size()) {
        for (auto prev_col : col_group.second) {
          const auto published_col = join->columns.Create(
              prev_col->var, join, prev_col->id, join->columns.Size());

          join->out_to_in.emplace(published_col, join);
          COL::Union(prev_col, published_col);
          prev_col->ReplaceAllUsesWith(published_col);
        }
      }
    }

    // Add the uses in. We need to make sure to do all of this in the same
    // order in which we added the original columns in.

    auto i = 0u;
    for (auto &col_group : grouped_cols) {
      if (col_group.second.size() == eq_class.size()) {
        const auto pivot_col = join->columns[i++];
        auto input_cols = join->out_to_in.find(pivot_col);
        for (auto col : col_group.second) {
          input_cols->second.AddUse(col);
        }
      }
    }
    for (auto &col_group : grouped_cols) {
      if (col_group.second.size() != eq_class.size()) {
        for (auto prev_col : col_group.second) {
          const auto published_col = join->columns[i++];
          auto input_cols = join->out_to_in.find(published_col);
          input_cols->second.AddUse(prev_col);
        }
      }
    }
  }

  // Create a comparison. If the two columns being compared do not belong to
  // the same view, then a product view (a type of join) is created.
  //
  // Comparisons forward all of their input views columns along as additional
  // outputs.
  CMP *CreateComparison(ComparisonOperator op,
                        ParsedVariable lhs_var, COL *lhs_col_,
                        ParsedVariable rhs_var, COL *rhs_col_) {

    auto lhs_col = lhs_col_->Find();
    auto rhs_col = rhs_col_->Find();

    CMP *cmp = nullptr;

    // If we're not sourcing the columns from the same view, then create a
    // product column.
    if (lhs_col->view != rhs_col->view) {
      const auto ret = CreateProduct(lhs_col, rhs_col);
      lhs_col = ret.first;
      rhs_col = ret.second;
    }

    assert(lhs_col->view == rhs_col->view);

    if (ComparisonOperator::kEqual == op) {
      if (lhs_col == rhs_col) {
        return nullptr;
      }

      cmp = query->constraints.Create(op);
      const auto new_eq_col = cmp->columns.Create(
          (lhs_var.Order() < rhs_var.Order() ? lhs_var : rhs_var),
          cmp, lhs_col->id < rhs_col->id ? lhs_col->id : rhs_col->id,
          0u);

      COL::Union(lhs_col, new_eq_col);
      COL::Union(rhs_col, new_eq_col);

      lhs_col->ReplaceAllUsesWith(new_eq_col);
      rhs_col->ReplaceAllUsesWith(new_eq_col);

    } else {
      assert(lhs_col != rhs_col);

      cmp = query->constraints.Create(op);
      const auto new_lhs_col = cmp->columns.Create(
          lhs_var, cmp, lhs_col->id, 0u);

      const auto new_rhs_col = cmp->columns.Create(
          rhs_var, cmp, rhs_col->id, 1u);

      COL::Union(lhs_col, new_lhs_col);
      COL::Union(rhs_col, new_rhs_col);

      lhs_col->ReplaceAllUsesWith(new_lhs_col);
      rhs_col->ReplaceAllUsesWith(new_rhs_col);
    }

    cmp->input_columns.AddUse(lhs_col);
    cmp->input_columns.AddUse(rhs_col);

    int found = 0;

    // Now go add in the remainder of the product columns.
    for (auto col : lhs_col->view->columns) {
      if (col != lhs_col && col != rhs_col) {
        const auto new_col = cmp->columns.Create(
            col->var, cmp, col->id, cmp->columns.Size());
        col->ReplaceAllUsesWith(new_col);
        COL::Union(col, new_col);

      } else {
        ++found;
      }
    }

    assert(found == 2);

    // Now go add in the uses of the remainder of the product columns.
    for (auto col : lhs_col->view->columns) {
      if (col != lhs_col && col != rhs_col) {
        cmp->input_columns.AddUse(col);
      }
    }

    return cmp;
  }

  void AssertEqual(ParsedVariable lhs_var, unsigned lhs_id,
                   ParsedVariable rhs_var, unsigned rhs_id) override {
    if (lhs_id == rhs_id) {
      return;
    }

    auto &lhs_colset = id_to_col[lhs_id];
    auto &rhs_colset = id_to_col[rhs_id];

    if (lhs_colset && rhs_colset) {
      const auto lhs_col = lhs_colset->Leader();
      const auto rhs_col = rhs_colset->Leader();
      pending_compares.emplace_back(
          ComparisonOperator::kEqual, lhs_var, lhs_col, rhs_var, rhs_col);

    } else if (lhs_colset) {
      rhs_colset = lhs_colset;

    } else if (rhs_colset) {
      lhs_colset = rhs_colset;

    } else {
      unresolved_compares.emplace_back(
          ComparisonOperator::kEqual, lhs_var, lhs_id, rhs_var, rhs_id);
    }
  }

  void AssertInequality(ComparisonOperator op,
                        ParsedVariable lhs_var, unsigned lhs_id,
                        ParsedVariable rhs_var, unsigned rhs_id) {
    auto &lhs_colset = id_to_col[lhs_id];
    auto &rhs_colset = id_to_col[rhs_id];

    if (lhs_colset && rhs_colset) {
      const auto lhs_col = lhs_colset->Leader();
      const auto rhs_col = rhs_colset->Leader();
      pending_compares.emplace_back(op, lhs_var, lhs_col, rhs_var, rhs_col);

    } else {
      unresolved_compares.emplace_back(op, lhs_var, lhs_id, rhs_var, rhs_id);
    }
  }

  void AssertNotEqual(ParsedVariable lhs_var, unsigned lhs_id,
                      ParsedVariable rhs_var, unsigned rhs_id) override {
    AssertInequality(
        ComparisonOperator::kNotEqual, lhs_var, lhs_id, rhs_var, rhs_id);
  }

  void AssertLessThan(ParsedVariable lhs_var, unsigned lhs_id,
                      ParsedVariable rhs_var, unsigned rhs_id) override {
    AssertInequality(
        ComparisonOperator::kLessThan, lhs_var, lhs_id, rhs_var, rhs_id);
  }

  void AssertGreaterThan(ParsedVariable lhs_var, unsigned lhs_id,
                         ParsedVariable rhs_var, unsigned rhs_id) override {
    AssertInequality(
        ComparisonOperator::kGreaterThan, lhs_var, lhs_id, rhs_var, rhs_id);
  }

  virtual void EnterFromSelect(
      ParsedPredicate pred, ParsedDeclaration decl,
      const Column *select_begin, const Column *select_end) override {

    ProcessUnresolvedCompares();

    auto select = SelectFor(pred);
    for (auto col = select_begin; col < select_end; ++col) {
      AddColumn(select, *col);
    }

    // We might have a `foo(A, A)` where `A` is free, so add a comparison.
    for (auto col : select->columns) {
      auto &prev_colset = id_to_col[col->id];
      if (prev_colset) {
        const auto prev_col = prev_colset->Leader();
        pending_compares.emplace_back(
            ComparisonOperator::kEqual, prev_col->var, prev_col,
            col->var, col);
      } else {
        prev_colset = col->equiv_columns;
      }
    }
  }

  void EnterFromWhereSelect(
      ParsedPredicate pred, ParsedDeclaration decl,
      const Column *where_begin, const Column *where_end,
      const Column *select_begin, const Column *select_end) override {

    ProcessUnresolvedCompares();

    VIEW *view = nullptr;
    const auto is_map = decl.IsFunctor() && where_begin < where_end;
    if (is_map) {
      view = query->maps.Create(ParsedFunctor::From(decl));
    } else {
      view = SelectFor(pred);
    }

    sips_cols.clear();
    sips_cols.resize(pred.Arity());

    for (auto col = where_begin; col < where_end; ++col) {
      assert(!sips_cols[col->n]);
      sips_cols[col->n] = col;
    }

    for (auto col = select_begin; col < select_end; ++col) {
      assert(!sips_cols[col->n]);
      sips_cols[col->n] = col;
    }

    // Create columns for the select, but give each column a totally unique ID.
    for (auto col : sips_cols) {
      assert(col != nullptr);
      (void) AddColumn(view, *col);
    }

    where_cols.clear();
    where_cols.resize(sips_cols.size());

    for (auto col = where_begin; col < where_end; ++col) {
      const auto &prev_colset = id_to_col[col->id];
      assert(prev_colset.get() != nullptr);
      where_cols[col->n] = prev_colset->Leader();
    }

    for (auto col = where_begin; col < where_end; ++col) {
      const auto where_col = view->columns[col->n];
      auto &prev_colset = id_to_col[col->id];
      const auto prev_col = prev_colset->Leader();
      if (!is_map || prev_col->view == view) {
        pending_compares.emplace_back(
            ComparisonOperator::kEqual, prev_col->var, prev_col,
            col->var, where_col);
      }
      prev_colset = where_col->equiv_columns;
    }

    for (auto col = select_begin; col < select_end; ++col) {
      const auto select_col = view->columns[col->n];
      auto &prev_colset = id_to_col[col->id];
      if (prev_colset) {
        const auto prev_col = prev_colset->Leader();
        if ((!is_map || prev_col->view == view)) {
          pending_compares.emplace_back(
              ComparisonOperator::kEqual, prev_col->var, prev_col,
              col->var, select_col);
        }
      }
      prev_colset = select_col->equiv_columns;
    }

    if (is_map) {
      for (auto col = where_begin; col < where_end; ++col) {
        if (const auto where_col = where_cols[col->n]) {
          where_col->ReplaceAllUsesWith(view->columns[col->n]);
        }
      }
      for (auto col = where_begin; col < where_end; ++col) {
        if (const auto where_col = where_cols[col->n]) {
          view->input_columns.AddUse(where_col);
        }
      }
    }
  }

  void EnterAggregation(
      ParsedPredicate pred, ParsedDeclaration decl,
      const Column *group_by_begin, const Column *group_by_end,
      const Column *bound_begin, const Column *bound_end) override {

    ProcessUnresolvedCompares();
    assert(unresolved_compares.empty());

    assert(decl.IsFunctor());

    auto functor = ParsedFunctor::From(decl);
    const auto agg = query->aggregates.Create(functor);
    query->pending_aggregates.push_back(agg);

    // Start with a new "scope". `do_col` will fill it in with the bound
    // columns.
    agg->id_to_col.swap(id_to_col);

    // Make the inputs visible to the aggregate.

    for (auto col = group_by_begin; col < group_by_end; ++col) {
      const auto &prev_colset = agg->id_to_col[col->id];
      assert(prev_colset);

      const auto prev_col = prev_colset->Leader();
      agg->group_by_columns.AddUse(prev_col);
      id_to_col.emplace(col->id, prev_col->equiv_columns);
    }

    for (auto col = bound_begin; col < bound_end; ++col) {
      const auto &prev_colset = agg->id_to_col[col->id];
      assert(prev_colset);
      const auto prev_col = prev_colset->Leader();
      agg->bound_columns.AddUse(prev_col);
      id_to_col.emplace(col->id, prev_col->equiv_columns);
    }
  }

  void Collect(
      ParsedPredicate, ParsedDeclaration,
      const Column *aggregate_begin,
      const Column *aggregate_end) override {

    ProcessUnresolvedCompares();

    assert(!query->pending_aggregates.empty());
    auto agg = query->pending_aggregates.back();

    for (auto col = aggregate_begin; col < aggregate_end; ++col) {
      const auto &prev_colset = id_to_col[col->id];
      assert(prev_colset);
      const auto prev_col = prev_colset->Leader();
      agg->summarized_columns.AddUse(prev_col);
    }
  }

  void EnterSelectFromSummary(
      ParsedPredicate, ParsedDeclaration,
      const Column *, const Column *,  // Group by.
      const Column *, const Column *,  // Bound.
      const Column *summary_begin, const Column *summary_end) override {

    ProcessUnresolvedCompares();

    assert(unresolved_compares.empty());
    assert(!query->pending_aggregates.empty());

    const auto agg = query->pending_aggregates.back();
    query->pending_aggregates.pop_back();

    // Swap back to the old scope. This helps ensure that summarized columns
    // don't leak.
    agg->id_to_col.swap(id_to_col);

    // The summary variables are now available.
    for (auto col = summary_begin; col < summary_end; ++col) {
      (void) AddColumn(agg, *col);
    }

    // "Publish" the aggregate's summary columns for use by everything else.
    for (auto col : agg->columns) {
      auto &prev_colset = id_to_col[col->id];
      if (prev_colset) {
        const auto prev_col = prev_colset->Leader();
        pending_compares.emplace_back(
            ComparisonOperator::kEqual, prev_col->var, prev_col,
            col->var, col);
      } else {
        prev_colset = col->equiv_columns;
      }
    }
  }

  void AssertPresent(
      ParsedDeclaration decl, ParsedPredicate pred, const Column *begin,
      const Column *end) override {
    if (decl.IsFunctor()) {
      EnterFromWhereSelect(
          pred, decl, begin, end, nullptr, nullptr);

    } else {
      assert(!decl.IsMessage());

      ProcessUnresolvedCompares();

      std::vector<COL *> input_cols;
      auto select = query->selects.Create(TableFor(pred));
      select->group_ids.push_back(select_group_id);

      for (auto col = begin; col < end; ++col) {
        const auto &prev_colset = id_to_col[col->id];
        assert(prev_colset);
        AddColumn(select, *col);

        const auto prev_col = prev_colset->Leader();
        input_cols.push_back(prev_col);
      }

      pending_presence_checks.emplace_back(select, std::move(input_cols));
    }
  }

  void AssertAbsent(
      ParsedDeclaration decl, ParsedPredicate pred, const Column *begin,
      const Column *end) override {
    AssertPresent(decl, pred, begin, end);
  }

  bool ProcessUnresolvedCompares(void) {
    auto made_progress = unresolved_compares.empty();
    while (!unresolved_compares.empty()) {
      const auto prev_len = unresolved_compares.size();
      next_unresolved_compares.swap(unresolved_compares);
      unresolved_compares.clear();
      for (auto cmp : next_unresolved_compares) {
        auto [op, lhs_var, lhs_id, rhs_var, rhs_id] = cmp;
        if (ComparisonOperator::kEqual == op) {
          AssertEqual(lhs_var, lhs_id, rhs_var, rhs_id);
        } else {
          AssertInequality(op, lhs_var, lhs_id, rhs_var, rhs_id);
        }
      }
      if (prev_len > unresolved_compares.size()) {
        made_progress = true;
      } else {
        break;
      }
    }
    return made_progress;
  }

//  // Perform common subexpression elimination, which will first identify
//  // candidate subexpressions for possible elimination using hashing, and
//  // then will perform recursive equality checks.
//  bool CSE(void) {
//    using CandidateList = std::vector<VIEW *>;
//    using CandidateLists = std::unordered_map<uint64_t, CandidateList>;
//
//    auto apply_list = [=] (EqualitySet &eq, CandidateList &list,
//                           CandidateList &ipr) -> bool {
//      ipr.clear();
//      for (auto i = 0u; i < list.size(); ++i) {
//        auto v1 = list[i];
//        ipr.push_back(v1);
//
//        for (auto j = i + 1; j < list.size(); ++j) {
//          auto v2 = list[j];
////          eq.Clear();
//          if (!QueryView(v2).ReplaceAllUsesWith(eq, QueryView(v1))) {
//            ipr.push_back(v2);
//          }
//        }
//
//        list.swap(ipr);
//        ipr.clear();
//      }
//      return false;
//    };
//
//    std::vector<VIEW *> in_progress;
//    std::vector<VIEW *> ordered_views;
//    EqualitySet equalities;
//    CandidateLists candidates;
//    auto made_progress = false;
//
//    for (auto changed = true; changed; ) {
//      changed = false;
//      ordered_views.clear();
//      query->ForEachView([&] (VIEW *view) {
//        if (!view->columns.empty()) {
//          candidates[view->Hash()].push_back(view);
//          ordered_views.push_back(view);
//        }
//      });
//
//      // Sort the views so that we process the ones closer to the inputs
//      // before the ones that are close to the outputs (inserts).
//      std::sort(ordered_views.begin(), ordered_views.end(),
//                [] (VIEW *a, VIEW *b) {
//                  return a->Depth() < b->Depth();
//                });
//
//      // Apply CSE in reverse postorder.
//      for (auto view : ordered_views) {
//        if (view->columns.empty()) {
//          continue;  // We've replaced it.
//        }
//
//        auto &eq_views = candidates[view->Hash()];
//        if (1 >= eq_views.size()) {
//          continue;  // Doesn't look structurally equivalent to anything.
//        }
//
//        in_progress.clear();
//        if (apply_list(equalities, eq_views, in_progress)) {
//          changed = true;
//          made_progress = true;
//        }
//      }
//
//      candidates.clear();
//    }
//
//    return made_progress;
//  }

  void Insert(
      ParsedDeclaration decl,
      const Column *begin,
      const Column *end) override {

    // There may be unresolved comparisons, i.e. where the SIPS visitor had
    // us compare IDs, but we didn't have columns associated with them at that
    // time. Go resolve those now.
    ProcessUnresolvedCompares();
    assert(unresolved_compares.empty());

//    // The behaviour of this is to extract out constants into their own kind
//    // of relation, and join against that. This prevents many smaller joins
//    // against constants.
//    //
//    // TODO(pag): If I move this below then things break... why?
//    JoinConstantComparisons();

    // Convert all pending comparisons into either joins or constraints.
    ReifyPendingComparisons();

    // Presence/absence checks are deferred, and converted into "full joins"
    // here.
    for (auto &[select, cols] : pending_presence_checks) {
      CreateFullJoin(select, cols);
    }

    // If the initial view wasn't derived from a message, then we want to
    // join against the stream associated with taking inputs. In the case of
    // rule bodies that are multiply recursive, e.g. transitive closure, this
    // helps to enable a certain amount of common subexpression elimination.
    // It helps CSE because we do this "late", i.e. not far from the insert
    // itself, which means that all sorts of joins and things have already
    // happened. This maximizes that amount of pre-existing common structure.
    if (initial_view != input_view) {
      JoinAgainstInputs();
    }

    // Full joins might add more pending comparisons, to reify them.
    ReifyPendingComparisons();

    // Empty out all equivalence classes. We don't want them interfering with
    // one-another across different clauses.
    EmptyEquivalenceClasses();

    auto table = TableFor(decl);
    auto insert = query->inserts.Create(table, decl);

    for (auto col = begin; col < end; ++col) {
      (void) AddColumn(insert, *col);
      const auto &prev_colset = id_to_col[col->id];
      assert(prev_colset);
      const auto prev_col = prev_colset->Leader();
      insert->input_columns.AddUse(prev_col);
    }

    // Look to see if there's any negative use of `decl`, and insert into
    // there as well. This is equivalent to removing from the negative table.
    for (auto pred : decl.NegativeUses()) {
      table = TableFor(pred);
      insert = query->inserts.Create(table, decl);

      for (auto col = begin; col < end; ++col) {
        (void) AddColumn(insert, *col);
        const auto &prev_colset = id_to_col[col->id];
        assert(prev_colset);
        const auto prev_col = prev_colset->Leader();
        insert->input_columns.AddUse(prev_col);
      }
      break;  // Don't need more than one.
    }
  }

  // Do a full join of the initial relation select against the input stream.
  // We only do this if the initial select was not itself from a message.
  void JoinAgainstInputs(void) {
    where_cols.clear();
    for (auto col : initial_view->columns) {
      where_cols.push_back(col);
    }
    CreateFullJoin(input_view, where_cols);
  }

  // Reify pending comparisons into constraint relations or into join relations.
  void ReifyPendingComparisons(void) {
    if (pending_compares.empty()) {
      return;
    }

    next_pending_compares.clear();
    next_pending_compares.swap(pending_compares);

    for (auto [op, lhs_var, lhs_col, rhs_var, rhs_col] : next_pending_compares) {
      if (ComparisonOperator::kEqual == op) {
        COL::Union(lhs_col, rhs_col);
      }
    }

    std::unordered_map<COL *, std::unordered_set<COL *>> equiv_classes;
    for (auto [op, lhs_var, lhs_col, rhs_var, rhs_col] : next_pending_compares) {
      if (ComparisonOperator::kEqual == op) {
        auto &eq_set = equiv_classes[lhs_col->Find()];
        eq_set.insert(lhs_col);
        eq_set.insert(rhs_col);
      } else {
        pending_compares.emplace_back(op, lhs_var, lhs_col, rhs_var, rhs_col);
      }
    }

    std::vector<const std::unordered_set<COL *> *> sorted_equiv_class;
    for (const auto &leader_set : equiv_classes) {
      sorted_equiv_class.push_back(&(leader_set.second));
    }

    while (!sorted_equiv_class.empty()) {
      std::sort(sorted_equiv_class.begin(), sorted_equiv_class.end(),
                [] (const std::unordered_set<COL *> *a,
                          const std::unordered_set<COL *> *b) {
                  auto a_depth = 0u;
                  for (auto col : *a) {
                    a_depth = std::max(a_depth, col->view->Depth());
                  }
                  auto b_depth = 0u;
                  for (auto col : *b) {
                    b_depth = std::max(b_depth, col->view->Depth());
                  }

                  // Order deeper ones (further form input streams) earlier so
                  // that we process them later.
                  if (a_depth > b_depth) {
                    return true;

                  } else if (a_depth < b_depth) {
                    return false;

                  // Bigger ones second when there's a tie-breaker. This means
                  // that we process the bigger ones first, because they will
                  // be ordered later, and we do `back`.
                  } else {
                    return a->size() < b->size();
                  }
                });

      auto set = sorted_equiv_class.back();
      sorted_equiv_class.pop_back();
      CreateJoin(*set);
    }

    next_pending_compares.clear();
    next_pending_compares.swap(pending_compares);

    while (!next_pending_compares.empty()) {

      // We sort the pending comparisons by maximum depth, as we'll be placing
      // join at that max depth + 1, and we continually process the deepest
      // comparison (furthest from the input/streams) via `back`. The key to
      // realize is that comparisons forward the columns of their input views
      // along, so if we started with least deep first, we'd end up with massive
      // propagation by the time we got to the deepest, whereas starting deepest
      // first ends up getting us closer to only propagating what is needed.
      std::sort(next_pending_compares.begin(),
                next_pending_compares.end(),
                [] (const PendingCompare &a, const PendingCompare &b) {
                  const auto &[a0, a1, a_col1, a2, a_col2] = a;
                  const auto &[b0, b1, b_col1, b2, b_col2] = b;
                  const auto a_depth = std::max(a_col1->Find()->view->Depth(),
                                                a_col2->Find()->view->Depth());
                  const auto b_depth = std::max(b_col1->Find()->view->Depth(),
                                                b_col2->Find()->view->Depth());
                  return a_depth < b_depth;
                });

      auto [op, lhs_var, lhs_col, rhs_var, rhs_col] = next_pending_compares.back();
      next_pending_compares.pop_back();

      assert(ComparisonOperator::kEqual != op);
      CreateComparison(op, lhs_var, lhs_col, rhs_var, rhs_col);;
    }
  }

  // Go through all column definitions, and reset their `DisjointSet` parents.
  // Equivalence classes do not generalize beyond a single clause, and so we
  // can't risk leaving them around. Consider this example:
  //
  //    foo(A) : b(A), A = 1.
  //    foo(A) : b(A), A != 1.
  //
  // If we start with the initial assumption `b(A)`, then in one we will put `A`
  // and `1` in the same equivalence class, and in the other, we'll assert that
  // they cannot possibly be in the same equivalnce class. This is fine so long
  // as the equivalence classes are all emptied / treated as independent across
  // clause bodies.
  void EmptyEquivalenceClasses(void) {
    query->ForEachView([] (VIEW *view) {
      for (auto col : view->columns) {
        col->equiv_columns.reset();
      }
    });
  }

//  // Go through all the pending comparisons and pull out equality comparisons
//  // against constants. We want to extract these into a tuple and then join
//  // against that tuple.
//  void JoinConstantComparisons(void) {
//    auto is_constant = [] (COL *col) {
//      if (auto sel = col->view->AsSelect()) {
//        if (auto stream = sel->stream.get()) {
//          return stream->AsConstant() != nullptr;
//        }
//      }
//      return false;
//    };
//
//    next_pending_compares.clear();
//
//    std::unordered_map<COL *, std::unordered_set<COL *>> const_equalities;
//
//    for (auto &cmp : pending_compares) {
//      auto [op, lhs_var, lhs_col, rhs_var, rhs_col] = cmp;
//
//      if (ComparisonOperator::kEqual != op) {
//        next_pending_compares.emplace_back(std::move(cmp));
//        continue;
//      }
//
//      auto lhs_is_const = is_constant(lhs_col);
//      auto rhs_is_const = is_constant(rhs_col);
//
//      if (lhs_is_const && rhs_is_const) {
//        next_pending_compares.emplace_back(std::move(cmp));
//
//      } else if (lhs_is_const) {
//        const_equalities[lhs_col].insert(rhs_col->Find());
//
//      } else if (rhs_is_const) {
//        const_equalities[rhs_col].insert(lhs_col->Find());
//
//      } else {
//        next_pending_compares.emplace_back(std::move(cmp));
//      }
//    }
//
//    pending_compares.swap(next_pending_compares);
//
//    if (const_equalities.empty()) {
//      return;
//    }
//
//    auto tuple = query->tuples.Create();
//    where_cols.clear();
//
//    for (auto &[const_col, var_cols] : const_equalities) {
//      if (var_cols.empty()) {
//        assert(false);
//        continue;
//      }
//
//      auto new_const_col = tuple->columns.Create(
//          const_col->var, tuple, const_col->id, tuple->columns.Size());
//
//      const_col->ReplaceAllUsesWith(new_const_col);
//      tuple->input_columns.AddUse(const_col);
//
//      COL::Union(const_col, new_const_col);
//
//      auto it = var_cols.begin();
//      const auto end = var_cols.end();
//      const auto first = *it++;
//
//      // If the constant is compared for equality against some other columns,
//      // then re-introduce comparisons among those columns so that when we
//      // reify the pending compares, all these columns end up in the same
//      // equivalence class.
//      for (; it != end; ++it) {
//        pending_compares.emplace_back(
//            ComparisonOperator::kEqual, first->var, first, (*it)->var, *it);
//      }
//
//      where_cols.push_back(first);
//    }
//
//    CreateFullJoin(tuple, where_cols);
//  }

  // Context shared by all queries created by this query builder. E.g. all
  // tables are shared across queries.
  std::shared_ptr<query::QueryContext> context;

  // Query that we're building.
  std::shared_ptr<QueryImpl> query;

  // The initial view from which we're selecting.
  SELECT *initial_view{nullptr};
  SELECT *input_view{nullptr};

  // All columns in some select...where.
  std::vector<const Column *> sips_cols;

  // All query columns in some where.
  std::vector<COL *> where_cols;

  // Maps variable IDs to columns.
  std::unordered_map<unsigned, std::shared_ptr<ColumnSet>> id_to_col;

  using UnresolvedCompare = std::tuple<
      ComparisonOperator, ParsedVariable, unsigned, ParsedVariable, unsigned>;

  using PendingCompare = std::tuple<
      ComparisonOperator, ParsedVariable, COL *, ParsedVariable, COL *>;


  std::vector<std::pair<VIEW *, std::vector<COL *>>> pending_presence_checks;

  std::vector<UnresolvedCompare> unresolved_compares;
  std::vector<UnresolvedCompare> next_unresolved_compares;
  std::vector<PendingCompare> pending_compares;
  std::vector<PendingCompare> next_pending_compares;

  std::unordered_map<COL *, COL *> unique_cols;
  std::vector<std::pair<COL *, COL *>> joined_cols;

  // Selects within the same group cannot be merged. A group comes from
  // importing a clause, given an assumption.
  unsigned select_group_id{0};
};

// Build an insertion query for the best scoring, according to `scorer`,
// permutation of some clause body, given some predicate, as generated by
// `generator`.
void QueryBuilder::VisitClauseWithAssumption(
    SIPSScorer &scorer, SIPSGenerator &generator) {

  if (!impl->query) {
    impl->query = std::make_shared<QueryImpl>(impl->context);
  }

  (void) SIPSScorer::VisitBestScoringPermuation(
      scorer, *impl, generator);
}

// Return the final query, which may include several different inserts.
Query QueryBuilder::BuildQuery(void) {
//  impl->JoinGroups();
//  impl->JoinColumns();

//  // Comparisons between `QueryConstraint` nodes are not structural but pointer-
//  // based, so we sometimes need an extra push :-/
//  for (auto i = 0; i < 2; ++i) {
//    for (auto changed = true; changed;) {
//      changed = false;
//      if (impl->CSE()) {
//        changed = true;
//        i = 0;
//      }
//    }
//  }
//
//  impl->LinkEverything();

  Query ret(std::move(impl->query));
  impl.reset(new QueryBuilderImpl(impl->context));
  return ret;
}

QueryBuilder::QueryBuilder(void)
    : impl(std::make_unique<QueryBuilderImpl>()) {}

QueryBuilder::~QueryBuilder(void) {}

}  // namespace hyde
