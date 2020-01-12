// Copyright 2019, Trail of Bits. All rights reserved.

#include <drlojekyll/Rel/Builder.h>

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

  Node<QueryRelation> *TableFor(ParsedDeclaration decl, bool is_positive=true) {
    assert(decl.IsLocal() || decl.IsExport() || decl.IsQuery());

    auto &rels = is_positive ? context->relations : context->negative_relations;
    auto &table = rels[decl];
    if (!table) {
      table.reset(new Node<QueryRelation>(
          decl, context->next_relation,
          context->next_relation, is_positive));
      context->next_relation = table.get();
    }

    return table.get();
  }

  // Get the table for a given predicate.
  Node<QueryRelation> *TableFor(ParsedPredicate pred) {
    return TableFor(ParsedDeclaration::Of(pred), pred.IsPositive());
  }

  Node<QueryStream> *StreamFor(ParsedLiteral literal) {
    std::string spelling(literal.Spelling());
    spelling += literal.Type().Spelling();  // Make them type-specific.

    if (literal.IsNumber()) {
      // TODO(pag): Render the spelling into an actual integer value.
      auto &stream = context->constant_integers[spelling];
      if (!stream) {
        stream.reset(new Node<QueryConstant>(
            literal, context->next_stream, context->next_constant));
        context->next_stream = stream.get();
        context->next_constant = stream.get();
      }
      return stream.get();

    } else if (literal.IsString()) {
      // TODO(pag): Render the spelling into an actual string value.
      auto &stream = context->constant_strings[spelling];
      if (!stream) {
        stream.reset(new Node<QueryConstant>(
            literal, context->next_stream, context->next_constant));
        context->next_stream = stream.get();
        context->next_constant = stream.get();
      }
      return stream.get();

    } else {
      assert(false);
      return nullptr;
    }
  }

  Node<QueryStream> *StreamFor(ParsedDeclaration decl) {
    auto &stream = context->inputs[decl];
    if (!stream) {
      stream.reset(new Node<QueryInput>(
          decl, context->next_stream, context->next_input));
      context->next_stream = stream.get();
      context->next_input = stream.get();
    }
    return stream.get();
  }

  Node<QueryStream> *StreamFor(ParsedFunctor functor) {
    auto &stream = context->generators[functor];
    if (!stream) {
      stream.reset(new Node<QueryGenerator>(
          functor, context->next_stream, context->next_generator));
      context->next_stream = stream.get();
      context->next_generator = stream.get();
    }
    return stream.get();
  }

  Node<QuerySelect> *SelectFor(ParsedPredicate pred) {
    const auto decl = ParsedDeclaration::Of(pred);
    if (decl.IsMessage()) {
      const auto stream = StreamFor(ParsedMessage::From(decl));
      const auto select = new Node<QuerySelect>(
          query.get(), nullptr, stream);
      select->group_id = select_group_id;
      query->selects.emplace_back(select);
      return select;

    } else if (decl.IsFunctor()) {
      const auto stream = StreamFor(ParsedFunctor::From(decl));
      const auto select = new Node<QuerySelect>(
          query.get(), nullptr, stream);
      // NOTE(pag): Not setting `select->group_id` because every read from the
      //            generator should be distinct, and seen as producing
      //            potentially new results.
      query->selects.emplace_back(select);
      return select;

    } else {
      auto table = TableFor(pred);
      const auto select = new Node<QuerySelect>(
          query.get(), table, nullptr);
      select->group_id = select_group_id;
      query->selects.emplace_back(select);
      return select;
    }
  }

  // Add a column to a view.
  Node<QueryColumn> *AddColumn(Node<QueryView> *view, const Column &column) {
    const auto col = new Node<QueryColumn>(
        column.var, view, column.id, column.n);
    view->columns.push_back(col);
    query->columns.emplace_back(col);
    return col;
  }

  void Begin(ParsedPredicate pred) override {
    id_to_col.clear();
    next_pending_equalities.clear();
    pending_equalities.clear();
    pending_compares.clear();
    next_pending_compares.clear();

    auto select = new Node<QuerySelect>(
        query.get(), nullptr,
        StreamFor(ParsedDeclaration::Of(pred)));
    select->group_id = select_group_id;
    query->selects.emplace_back(select);
    initial_view = select;
  }

  void DeclareParameter(const Column &param) override {
    id_to_col[param.id] = AddColumn(initial_view, param);
  }

  // Constants are like infinitely sized tables with a single size. You
  // select from them.
  void DeclareConstant(ParsedLiteral val, unsigned id) override {
    const auto stream = StreamFor(val);
    const auto select = new Node<QuerySelect>(
        query.get(), nullptr, stream);

    // NOTE(pag): This is not using the traditional select group IDs. Instead,
    //            we want all constant selects to be marked as being from
    //            different groups so that they can all be subject to merging.
    select->group_id = static_cast<unsigned>(query->selects.size());

    query->selects.emplace_back(select);

    const auto col = new Node<QueryColumn>(
        ParsedVariable::AssignedTo(val), select, id, 0);
    select->columns.push_back(col);
    query->columns.emplace_back(col);

    auto &prev_col = id_to_col[id];
    assert(!prev_col);
    prev_col = select->columns[0];
  }

  void AssertEqualImpl(Node<QueryColumn> *lhs_col, Node<QueryColumn> *rhs_col) {
    DisjointSet::Union(lhs_col, rhs_col);

    assert(!lhs_col->view->IsJoin());
    assert(!rhs_col->view->IsJoin());

    if (lhs_col == rhs_col) {
      return;
    }

    if (lhs_col->view == rhs_col->view) {
      assert(false && "TODO");

    } else {
      auto join = new Node<QueryJoin>;
      query->joins.emplace_back(join);
      join->joined_columns.emplace_back(lhs_col);
      join->joined_columns.emplace_back(rhs_col);
    }
  }

  void AssertEqual(unsigned lhs_id, unsigned rhs_id) override {
    if (lhs_id == rhs_id) {
      return;
    }

    auto &lhs_col = id_to_col[lhs_id];
    auto &rhs_col = id_to_col[rhs_id];

    if (lhs_col && rhs_col) {
      AssertEqualImpl(lhs_col, rhs_col);

    } else if (lhs_col) {
      rhs_col = lhs_col;

    } else if (rhs_col) {
      lhs_col = rhs_col;

    } else {
      pending_equalities.emplace_back(lhs_id, rhs_id);
    }
  }

  void AssertInequality(ComparisonOperator op,
                        unsigned lhs_id, unsigned rhs_id) {
    auto lhs_col = id_to_col[lhs_id];
    auto rhs_col = id_to_col[rhs_id];
    if (lhs_col && rhs_col) {
      assert(lhs_col != rhs_col);

      const auto constraint = new Node<QueryConstraint>(
          op, lhs_col, rhs_col);

      auto new_lhs_col = new Node<QueryColumn>(
          lhs_col->var, constraint, lhs_col->id, 0);

      auto new_rhs_col = new Node<QueryColumn>(
          rhs_col->var, constraint, rhs_col->id, 1);

      DisjointSet::UnionInto(lhs_col, new_lhs_col);
      DisjointSet::UnionInto(rhs_col, new_rhs_col);

      Node<QueryColumn>::ReplaceAllUsesWith(query.get(), lhs_col, new_lhs_col);
      Node<QueryColumn>::ReplaceAllUsesWith(query.get(), rhs_col, new_rhs_col);

      query->constraints.emplace_back(constraint);
      query->columns.emplace_back(new_lhs_col);
      query->columns.emplace_back(new_rhs_col);
      constraint->columns.push_back(new_lhs_col);
      constraint->columns.push_back(new_rhs_col);

      // Make sure everything uses the filtered versions.
      for (auto &id_col : id_to_col) {
        if (id_col.second->Find() == new_lhs_col) {
          id_col.second = new_lhs_col;
        } else if (id_col.second->Find() == new_rhs_col) {
          id_col.second = new_rhs_col;
        }
      }

    } else {
      pending_compares.emplace_back(op, lhs_id, rhs_id);
    }
  }

  void AssertNotEqual(unsigned lhs_id, unsigned rhs_id) override {
    AssertInequality(ComparisonOperator::kNotEqual, lhs_id, rhs_id);
  }

  void AssertLessThan(unsigned lhs_id, unsigned rhs_id) override {
    AssertInequality(ComparisonOperator::kLessThan, lhs_id, rhs_id);
  }

  void AssertGreaterThan(unsigned lhs_id, unsigned rhs_id) override {
    AssertInequality(ComparisonOperator::kGreaterThan, lhs_id, rhs_id);
  }

  void AddMap(ParsedFunctor functor, const Column *select_begin,
              const Column *select_end, const Column *where_begin,
              const Column *where_end) {

    assert(where_begin < where_end);

    (void) ProcessPendingEqualities();

    auto map = new Node<QueryMap>(functor);
    query->maps.emplace_back(map);

    auto i = 0u;
    for (auto col = where_begin; col < where_end; ++col) {
      auto &prev_val = id_to_col[col->id];
      assert(prev_val != nullptr);
      auto mapped_col = new Node<QueryColumn>(col->var, map, col->id, i);
      query->columns.emplace_back(mapped_col);
      map->columns.push_back(mapped_col);
      map->input_columns.emplace_back(prev_val);
      ++i;
    }

    // Separate out the overwriting of `prev_val` in the case of something
    // like `foo(A, A)`, where we don't want a self-reference from one column
    // into another.
    i = 0u;
    for (auto col = where_begin; col < where_end; ++col) {
      auto &prev_val = id_to_col[col->id];
      if (prev_val->view != map) {
        prev_val = map->columns[i];
      }
      ++i;
    }

    // Add the additional output columns that correspond with the free
    // parameters to the map.
    for (auto col = select_begin; col < select_end; ++col) {
      auto mapped_col = new Node<QueryColumn>(col->var, map, col->id, i);
      query->columns.emplace_back(mapped_col);
      map->columns.push_back(mapped_col);
      auto &prev_val = id_to_col[col->id];
      assert(!prev_val);  // TODO(pag): Is this right?
      prev_val = mapped_col;
    }
  }

  virtual void EnterFromSelect(
      ParsedPredicate pred, ParsedDeclaration decl,
      const Column *select_begin, const Column *select_end) override {

    auto select = SelectFor(pred);
    for (auto col = select_begin; col < select_end; ++col) {
      auto new_col = AddColumn(select, *col);
      auto &prev_col = id_to_col[col->id];
      assert(!prev_col);
      prev_col = new_col;
    }
  }

  // Treat from-where-selects inside of the summarization scope of an aggregate
  // just like unconditional select alls, because the summarizing aggregate
  // itself knows about what should be grouped. This helps us avoid joins on
  // in-flows to aggregates, and instead join on the results of aggregates.
  void AggEnterFromWhereSelect(
      ParsedPredicate pred, ParsedDeclaration decl,
      const Column *where_begin, const Column *where_end,
      const Column *select_begin, const Column *select_end) {
    auto select = SelectFor(pred);

    struct {
      const Column *begin, *end;
    } ranges[2] = {
      {where_begin, where_end},
      {select_begin, select_end}
    };

    for (auto range : ranges) {
      for (auto col = range.begin; col < range.end; ++col) {
        auto new_col = AddColumn(select, *col);
        auto &prev_col = id_to_col[col->id];
        assert(!prev_col);
        prev_col = new_col;
      }
    }
  }

  void EnterFromWhereSelect(
      ParsedPredicate pred, ParsedDeclaration decl,
      const Column *where_begin, const Column *where_end,
      const Column *select_begin, const Column *select_end) override {

    if (!query->pending_aggregates.empty()) {
      AggEnterFromWhereSelect(pred, decl, where_begin, where_end,
                              select_begin, select_end);
      return;
    }

    if (decl.IsFunctor()) {
      auto functor = ParsedFunctor::From(decl);
      if (!functor.IsAggregate()) {
        AddMap(functor, select_begin, select_end, where_begin, where_end);
        return;
      }
    }

    auto select = SelectFor(pred);
    columns.clear();
    columns.resize(pred.Arity());

    for (auto col = where_begin; col < where_end; ++col) {
      assert(!columns[col->n]);
      columns[col->n] = col;
    }

    for (auto col = select_begin; col < select_end; ++col) {
      assert(!columns[col->n]);
      columns[col->n] = col;
    }

    // Create columns for the select, but give each column a totally unique ID.
    for (auto col : columns) {
      assert(col != nullptr);
      (void) AddColumn(select, *col);
    }

    for (auto col = where_begin; col < where_end; ++col) {
      const auto where_col = select->columns[col->n];
      const auto prev_col = id_to_col[col->id];
      assert(prev_col != nullptr);
      AssertEqualImpl(prev_col, where_col);
    }

    // Go through and rewrite all assignments to the newly selected column,
    // but don't actually join the two into the same set.
    for (auto col = where_begin; col < where_end; ++col) {
      auto where_col = select->columns[col->n];
      auto &prev_col = id_to_col[col->id];
      if (prev_col->view != select) {
        for (auto &assign : id_to_col) {
          if (assign.second == prev_col) {
            assign.second = where_col;
          }
        }
      }
    }

    for (auto col = select_begin; col < select_end; ++col) {
      const auto select_col = select->columns[col->n];
      auto &prev_col = id_to_col[col->id];
      if (prev_col) {
        assert(false);  // Hrmm, shouldn't happen.
        DisjointSet::Union(prev_col, select_col);
        pending_equalities.emplace_back(col->id, select_col->id);

      } else {
        prev_col = select_col;
      }
    }
  }

  void EnterAggregation(
      ParsedPredicate pred, ParsedDeclaration decl,
      const Column *, const Column *,
      const Column *, const Column *) override {

    assert(decl.IsFunctor());
    auto functor = ParsedFunctor::From(decl);
    auto agg = new Node<QueryAggregate>(functor);
    query->pending_aggregates.emplace_back(agg);

    // Start with a new "scope". `do_col` will fill it in with the bound
    // columns.
    agg->id_to_col.swap(id_to_col);



//    // Make the aggregate's group variables available to the internal
//    // selections that will use this group variable. Later we'll unpublish
//    // this. The idea is that we don't want to have the aggregated predicates
//    // join with stuff that is "outside" the scope of the variables provided
//    // by the aggregate itself. This leads to suboptimal join patterns.
//    for (auto col : agg->columns) {
//      id_to_col[col->id] = col;
//    }
  }

  void Collect(
      ParsedPredicate, ParsedDeclaration, const Column *aggregate_begin,
      const Column *aggregate_end) override {
    assert(!query->pending_aggregates.empty());
    auto agg = query->pending_aggregates.back().get();

    for (auto col = aggregate_begin; col < aggregate_end; ++col) {
      auto prev_col = id_to_col[col->id];
      assert(prev_col != nullptr);
      agg->summarized_columns.emplace_back(prev_col);
    }
  }

  void EnterSelectFromSummary(
      ParsedPredicate, ParsedDeclaration,
      const Column *group_begin, const Column *group_end,
      const Column *bound_begin, const Column *bound_end,
      const Column *summary_begin, const Column *summary_end) override {
    assert(!query->pending_aggregates.empty());

    auto agg = query->pending_aggregates.back().release();
    query->pending_aggregates.pop_back();
    query->aggregates.emplace_back(agg);

    // Swap back to the old scope. This helps ensure that summarized columns
    // don't leak.
    agg->id_to_col.swap(id_to_col);

    auto do_col = [&] (const Column *col, bool is_bound) {
      auto prev_col = id_to_col[col->id];  // Outside the aggregate.
      auto scoped_col = agg->id_to_col[col->id];  // Inside the aggregate.

      assert(prev_col != nullptr);
      assert(scoped_col != nullptr);
      agg->group_by_columns.emplace_back(scoped_col);

      // This is one of the bound parameters passed in to the summarizing
      // functor.
      if (is_bound) {
        agg->bound_columns.emplace_back(scoped_col);
      }

      auto out_col = new Node<QueryColumn>(
          col->var, agg, col->id, static_cast<unsigned>(agg->columns.size()));
      agg->columns.push_back(out_col);
      query->columns.emplace_back(out_col);

      // Join the grouped-by column with the version of the column visible
      // before aggregation.
      AssertEqualImpl(prev_col, out_col);
    };

    for (auto col = group_begin; col < group_end; ++col) {
      do_col(col, false);
    }

    for (auto col = bound_begin; col < bound_end; ++col) {
      do_col(col, true);
    }

    // The summary variables are now available.
    for (auto col = summary_begin; col < summary_end; ++col) {
      auto out_col = new Node<QueryColumn>(
          col->var, agg, col->id, static_cast<unsigned>(agg->columns.size()));

      agg->columns.push_back(out_col);
      query->columns.emplace_back(out_col);

      id_to_col[col->id] = out_col;
    }

    // "Publish" the aggregate's summary columns for use by everything else.
    for (auto i = agg->group_by_columns.size(); i < agg->columns.size(); ++i) {
      auto col = agg->columns[i];
      auto &prev_col = id_to_col[col->id];
      if (prev_col) {
        assert(prev_col->view == agg);
      } else {
        prev_col = col;
      }
    }
  }

  void AssertPresent(
      ParsedPredicate pred, const Column *begin,
      const Column *end) override {
    EnterFromWhereSelect(pred, ParsedDeclaration::Of(pred),
                         begin, end, nullptr, nullptr);
  }

  void AssertAbsent(
      ParsedPredicate pred, const Column *begin,
      const Column *end) override {
    EnterFromWhereSelect(pred, ParsedDeclaration::Of(pred),
                         begin, end, nullptr, nullptr);
  }

  bool ProcessPendingEqualities(void) {
    while (!pending_equalities.empty()) {
      const auto prev_len = pending_equalities.size();
      next_pending_equalities.swap(pending_equalities);
      pending_equalities.clear();
      for (auto eq : next_pending_equalities) {
        AssertEqual(eq.first, eq.second);
      }
      if (prev_len == pending_equalities.size()) {
        return false;
      }
    }
    return true;
  }

  void ProcessPendingCompares(void) {
    while (!pending_compares.empty()) {
      const auto prev_len = pending_compares.size();
      next_pending_compares.swap(pending_compares);
      pending_compares.clear();
      for (auto cmp : next_pending_compares) {
        AssertInequality(
            std::get<0>(cmp), std::get<1>(cmp), std::get<2>(cmp));
      }
      assert(prev_len > pending_compares.size());
    }
  }

  void MergeAndProjectJoins(void) {

    std::unordered_map<ParsedVariable, unsigned> join_weight;
    for (const auto &join : query->joins) {
      for (auto col : join->joined_columns) {
        join_weight[col->var] += 1;
      }
    }

    std::unordered_map<unsigned, std::vector<Node<QueryColumn> *>>
        promoted_ids;

    std::sort(query->joins.begin(), query->joins.end(),
              [&](const std::unique_ptr<Node<QueryJoin>> &a,
                 const std::unique_ptr<Node<QueryJoin>> &b) {
                auto wa = join_weight[a->joined_columns[0]->var] +
                          join_weight[a->joined_columns[1]->var];
                auto wb = join_weight[b->joined_columns[0]->var] +
                          join_weight[b->joined_columns[1]->var];
                return wa < wb;
              });

    for (const auto &join : query->joins) {

      assert(join->pivot_columns.empty());
      assert(join->columns.empty());
      assert(join->joined_columns.size() == 2);

      auto col0 = join->joined_columns[0];
      auto col1 = join->joined_columns[1];
      if (col0->var.Order() > col1->var.Order()) {
        std::swap(col0, col1);
      }

      join->joined_columns.clear();

      // The two columns to be joined already belong to the same join. When
      // we doing the join processing here, we make sure to go find all the
      // join pivots, so we can safely ignore these guys.
      if (col0->view == col1->view && col0->view->IsJoin()) {
        continue;
      }

      promoted_ids.clear();

      for (auto joined_col : col0->view->columns) {
        const auto id = joined_col->Find()->id;
        promoted_ids[id].push_back(joined_col);
      }

      if (col1->view != col0->view) {
        for (auto joined_col : col1->view->columns) {
          const auto id = joined_col->Find()->id;
          promoted_ids[id].push_back(joined_col);
        }
      } else {
        assert(false && "TODO?");
      }

      // `promoted_ids` is now a mapping from an ID to its equivalence
      // class of columns, where every column in the equivalence class is
      // related to at least one other column by ID or by variable name.
      //
      // Go and fill up the output columns of the join, which is the set of
      // unique input columns. For each output column that we create, we replace
      // all uses of the input column with the output column.
      for (const auto &id_col_set : promoted_ids) {
        const auto id = id_col_set.first;
        const auto &col_set = id_col_set.second;
        if (col_set.empty()) {
          continue;
        }

        auto min_var = col_set[0]->var;
        for (auto col : col_set) {
          if (min_var.Order() > col->var.Order()) {
            min_var = col->var;
          }
        }

        auto output_col = new Node<QueryColumn>(
            min_var, join.get(), id,
            static_cast<unsigned>(join->columns.size()));

        query->columns.emplace_back(output_col);
        join->columns.push_back(output_col);

        for (auto col : col_set) {
          Node<QueryColumn>::ReplaceAllUsesWith(query.get(), col, output_col);
          DisjointSet::UnionInto(col, output_col);
        }

        if (1 < col_set.size()) {
          join->pivot_columns.push_back(output_col);
        }
      }

      // Add in the joined columns after, so that they don't get replaced.
      for (const auto &id_col_set : promoted_ids) {
        const auto &col_set = id_col_set.second;
        if (col_set.empty()) {
          continue;
        }

        for (auto col : col_set) {
          join->joined_columns.emplace_back(col);
        }
      }
    }

    std::unordered_set<Node<QueryJoin> *> joined_joins;
    std::unordered_set<Node<QueryView> *> joined_views;

    // Try to combine join trees that all operate on the same pivots.
    bool found = false;
    for (const auto &join : query->joins) {
      if (join->joined_columns.empty()) {
        continue;  // Taken over or merged.
      }

      joined_joins.clear();
      joined_views.clear();

      for (auto joined_col : join->joined_columns) {

        // We only care about joins of joins.
        if (!joined_col->view->IsJoin()) {
          joined_views.insert(joined_col->view);
          continue;
        }

        auto joined_join = reinterpret_cast<Node<QueryJoin> *>(
            joined_col->view);

        // Make sure that all pivots of the lower join participate in at least
        // one pivot condition of this join.
        found = false;
        for (auto joined_pivot_col : joined_join->pivot_columns) {
          for (auto pivot_col : join->pivot_columns) {
            if (pivot_col->Find() == joined_pivot_col->Find() ||
                pivot_col->var == joined_pivot_col->var) {
              found = true;
              break;
            }
          }
          if (!found) {
            goto try_next_join;
          }
        }

        joined_joins.insert(joined_join);
      }

      if (joined_joins.empty()) {
      try_next_join:
        continue;
      }

      join->joined_columns.clear();

      for (auto joined_join : joined_joins) {
        join->joined_columns.insert(
            join->joined_columns.end(),
            joined_join->joined_columns.begin(),
            joined_join->joined_columns.end());

        joined_join->joined_columns.clear();
        joined_join->pivot_columns.clear();
      }

      for (auto joined_view : joined_views) {
        join->joined_columns.insert(
            join->joined_columns.end(),
            joined_view->columns.begin(),
            joined_view->columns.end());
      }
    }
  }

  // Perform common subexpression elimination, which will first identify
  // candidate subexpressions for possible elimination using hashing, and
  // then will perform recursive equality checks.
  bool CSE(void) {

    using CandidateList = std::vector<Node<QueryView> *>;
    using CandidateLists = std::unordered_map<uint64_t, CandidateList>;

    auto apply_list = [=] (EqualitySet &eq, CandidateList &list,
                           CandidateList &ipr) -> bool {
      ipr.clear();
      for (auto i = 0u; i < list.size(); ++i) {
        auto v1 = list[i];
        ipr.push_back(v1);

        for (auto j = i + 1; j < list.size(); ++j) {
          auto v2 = list[j];
          eq.Clear();
          if (v1->Equals(eq, v2)) {
            assert(v1 != v2);
            if (!QueryView(v2).ReplaceAllUsesWith(QueryView(v1))) {
              ipr.push_back(v2);
            }
          } else {
            ipr.push_back(v2);
          }
        }

        list.swap(ipr);
        ipr.clear();
      }
      return false;
    };

    auto apply_candidates = [=] (EqualitySet &eq, CandidateLists &lists,
                                 CandidateList &ipr) -> bool {
      bool changed = false;
      for (auto &hash_list : lists) {
        auto &list = hash_list.second;
        if (1 < list.size()) {
          if (apply_list(eq, list, ipr)) {
            changed = true;
          }
        }
      }
      return changed;
    };

    std::vector<Node<QueryView> *> in_progress;
    EqualitySet equalities;
    CandidateLists candidates;
    auto made_progress = false;

    for (auto changed = true; changed; ) {
      changed = false;
      query->ForEachView([&] (Node<QueryView> *view) {
        if (!view->columns.empty()) {
          candidates[view->Hash()].push_back(view);
        }
      });

      if (apply_candidates(equalities, candidates, in_progress)) {
        changed = true;
        made_progress = true;
      }
      candidates.clear();
    }

    return made_progress;
  }

  void LinkEverything(void) {
    auto &next_select = query->next_select;
    auto &next_join = query->next_join;
    auto &next_view = query->next_view;
    auto &next_insert = query->next_insert;
    auto &next_map = query->next_map;
    auto &next_aggregate = query->next_aggregate;
    auto &next_merge = query->next_merge;
    auto &next_constraint = query->next_constraint;

    Node<QueryColumn> *dummy = nullptr;
    Node<QueryColumn> **prev_col_ptr = &dummy;

    auto do_view = [&] (auto &view, auto &next_view_spec) {
      if (view->columns.empty()) {
        return;
      }

      view->next = next_view_spec;
      view->next_view = next_view;
      next_view = next_view_spec = view.get();

      prev_col_ptr = &dummy;
      for (auto col : view->columns) {
        *prev_col_ptr = col;
        prev_col_ptr = &(col->next_in_view);
      }
    };

    for (const auto &view : query->maps) {
      do_view(view, next_map);
    }

    for (const auto &view : query->selects) {
      do_view(view, next_select);
    }

    for (const auto &view : query->aggregates) {
      do_view(view, next_aggregate);
    }

    for (const auto &view : query->merges) {
      do_view(view, next_merge);
    }

    for (const auto &view : query->constraints) {
      do_view(view, next_constraint);
    }

    // Link all the "full" joins together, and clear out and ignore the
    // outdated joins.
    for (const auto &view : query->joins) {
      if (view->columns.empty() || view->joined_columns.empty()) {
        view->columns.clear();
        view->pivot_columns.clear();
        view->joined_columns.clear();

      } else {
        assert(!view->pivot_columns.empty());
        assert(!view->columns.empty());

        prev_col_ptr = &dummy;
        for (auto col : view->columns) {
          *prev_col_ptr = col;
          prev_col_ptr = &(col->next_in_view);
        }

        view->next = next_join;
        view->next_view = next_view;
        next_view = next_join = view.get();
      }
    }

    for (const auto &insert : query->inserts) {
      insert->next = next_insert;
      next_insert = insert.get();
    }
  }

  void Insert(
      ParsedDeclaration decl,
      const Column *begin,
      const Column *end) override {

    const auto processed_eqs = ProcessPendingEqualities();
    assert(processed_eqs);
    (void) processed_eqs;

    ProcessPendingCompares();

    auto table = TableFor(decl);
    auto insert = new Node<QueryInsert>(
        reinterpret_cast<Node<QueryRelation> *>(table), decl);

    for (auto col = begin; col < end; ++col) {
      const auto output_col = id_to_col[col->id];
      assert(output_col != nullptr);
      insert->input_columns.emplace_back(output_col);
    }

    query->inserts.emplace_back(insert);

    // Look to see if there's any negative use of `decl`, and insert into
    // there as well. This is equivalent to removing from the negative table.
    for (auto pred : decl.NegativeUses()) {
      table = TableFor(pred);
      insert = new Node<QueryInsert>(
          reinterpret_cast<Node<QueryRelation> *>(table), decl);

      for (auto col = begin; col < end; ++col) {
        const auto output_col = id_to_col[col->id];
        assert(output_col != nullptr);
        insert->input_columns.emplace_back(output_col);
      }

      query->inserts.emplace_back(insert);
      break;  // Don't need more than one.
    }
  }

  // When we build aggregates, we create new scopes for them, and in those
  // scopes, fresh variables. If we used the existing set of bound variables,
  // then we would observe JOINs flowing into the aggregates, which would make
  // any of their groupings redundant. However, we do want to ensure that the
  // grouped columns join with anything in the outer scopes, and so we go over
  // group by columns here, and inject join conditions.
  void JoinGroups(void) {
    std::unordered_map<unsigned, std::vector<Node<QueryColumn> *>> cols;
    std::unordered_set<unsigned> seen;
    for (auto &agg : query->aggregates) {
      seen.clear();
      for (auto i = 0u; i < agg->group_by_columns.size(); ++i) {
        auto group_col = agg->columns[i];
        auto id = group_col->Find()->id;
        if (!seen.count(id)) {
          cols[id].push_back(group_col);
          seen.insert(id);
        }
      }
    }

    for (auto &id_cols : cols) {
      for (auto i = 1u; i < id_cols.second.size(); ++i) {
        AssertEqualImpl(id_cols.second[i - 1u], id_cols.second[i]);
      }
    }
  }

  // Context shared by all queries created by this query builder. E.g. all
  // tables are shared across queries.
  std::shared_ptr<query::QueryContext> context;

  // Query that we're building.
  std::shared_ptr<QueryImpl> query;

  // The initial view from which we're selecting.
  Node<QueryView> *initial_view{nullptr};

  // All columns in some select...where.
  std::vector<const Column *> columns;

  // Maps variable IDs to columns.
  std::unordered_map<unsigned, Node<QueryColumn> *> id_to_col;

  std::vector<std::pair<unsigned, unsigned>> pending_equalities;
  std::vector<std::pair<unsigned, unsigned>> next_pending_equalities;

  std::vector<std::tuple<ComparisonOperator, unsigned, unsigned>>
      pending_compares;

  std::vector<std::tuple<ComparisonOperator, unsigned, unsigned>>
      next_pending_compares;

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

  impl->select_group_id += 1;

  (void) SIPSScorer::VisitBestScoringPermuation(
      scorer, *impl, generator);
}

// Return the final query, which may include several different inserts.
Query QueryBuilder::BuildQuery(void) {
  impl->JoinGroups();
  impl->MergeAndProjectJoins();
  impl->query->ForEachView([&] (Node<QueryView> *view) {
    view->query = impl->query.get();
  });
  impl->CSE();
  impl->LinkEverything();
  Query ret(std::move(impl->query));
  impl.reset(new QueryBuilderImpl(impl->context));
  return ret;
}

QueryBuilder::QueryBuilder(void)
    : impl(std::make_unique<QueryBuilderImpl>()) {}

QueryBuilder::~QueryBuilder(void) {}

}  // namespace hyde
