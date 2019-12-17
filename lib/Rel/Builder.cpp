// Copyright 2019, Trail of Bits. All rights reserved.

#include <drlojekyll/Rel/Builder.h>

#include <tuple>
#include <unordered_set>

#include <drlojekyll/Sema/SIPSScore.h>

#include "Query.h"

namespace hyde {

class QueryBuilderImpl : public SIPSVisitor {
 public:
  QueryBuilderImpl(void)
      : context(std::make_shared<query::QueryContext>()) {}

  virtual ~QueryBuilderImpl(void) = default;

  Node<QueryTable> *TableFor(ParsedDeclaration decl, bool is_positive=true) {
    auto &rels = is_positive ? context->relations : context->negative_relations;
    auto &table = rels[decl];
    if (!table) {
      table.reset(new Node<QueryRelation>(
          decl, context->next_table, context->next_relation, is_positive));
      context->next_table = table.get();
      context->next_relation = table.get();
    }

    return table.get();
  }

  // Get the table for a given predicate.
  Node<QueryTable> *TableFor(ParsedPredicate pred) {
    const auto prev_next_table = context->next_table;
    const auto decl = ParsedDeclaration::Of(pred);
    const auto table = TableFor(decl, pred.IsPositive());

    // We just added this negative table. Go and create a select for it from
    // the positive table, and create a fake insert into the negative table.
    if (prev_next_table != context->next_table && !pred.IsPositive()) {
      const auto positive_table = TableFor(decl, true);
      const auto select = new Node<QuerySelect>(query.get(), positive_table);
      query->selects.emplace_back(select);
      for (unsigned i = 0, max_i = decl.Arity(); i < max_i; ++i) {
        Column column(decl.NthParameter(i), pred.NthArgument(i), i, 0);
        AddColumn(select, column);
      }

      assert(table->IsRelation());
      const auto insert = new Node<QueryInsert>(
          reinterpret_cast<Node<QueryRelation> *>(table), decl);
      insert->columns = select->columns;
      query->inserts.emplace_back(insert);
    }

    return table;
  }

  Node<QueryTable> *TableFor(ParsedLiteral literal) {
    std::string spelling(literal.Spelling());
    if (literal.IsNumber()) {
      auto &table = context->constant_integers[spelling];
      if (!table) {
        table.reset(new Node<QueryConstant>(
            literal, context->next_table, context->next_constant));
        context->next_table = table.get();
        context->next_constant = table.get();
      }
      return table.get();

    } else if (literal.IsString()) {
      auto &table = context->constant_strings[spelling];
      if (!table) {
        table.reset(new Node<QueryConstant>(
            literal, context->next_table, context->next_constant));
        context->next_table = table.get();
        context->next_constant = table.get();
      }
      return table.get();

    } else {
      assert(false);
      return nullptr;
    }
  }

  Node<QuerySelect> *SelectFor(ParsedPredicate pred) {
    auto table = TableFor(pred);
    auto select = new Node<QuerySelect>(query.get(), table);
    query->selects.emplace_back(select);
    return select;
  }

  // Add a column to a view.
  Node<QueryColumn> *AddColumn(Node<QueryView> *view, const Column &column) {
    const auto col = new Node<QueryColumn>(view, column.id, column.n);
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
    initial_view = SelectFor(pred);
  }

  void DeclareParameter(const Column &param) override {
    id_to_col[param.id] = AddColumn(initial_view, param);
  }

  // Constants are like infinitely sized tables with a single size. You
  // select from them.
  void DeclareConstant(ParsedLiteral val, unsigned id) override {
    const auto table = TableFor(val);
    const auto select = new Node<QuerySelect>(query.get(), table);
    query->selects.emplace_back(select);

    const auto col = new Node<QueryColumn>(select, id, 0);
    select->columns.push_back(col);
    query->columns.emplace_back(col);

    auto &prev_col = id_to_col[id];
    assert(!prev_col);
    prev_col = select->columns[0];
  }

  void AssertEqual(unsigned lhs_id, unsigned rhs_id) override {
    auto &lhs_col = id_to_col[lhs_id];
    auto &rhs_col = id_to_col[rhs_id];

    if (lhs_col && rhs_col) {
      const auto lhs_parent = lhs_col->Find();
      const auto rhs_parent = rhs_col->Find();
      if (lhs_parent == rhs_parent) {
        return;
      }

      lhs_col = lhs_parent;
      rhs_col = rhs_parent;

      // This is a condition between two different columns from the same view.
      // Thus, we don't want to do any kind of self joining.
      if (lhs_col->view == rhs_col->view) {
        const auto constraint = new Node<QueryConstraint>(
            ComparisonOperator::kEqual, lhs_col, rhs_col);
        query->constraints.emplace_back(constraint);
        return;
      }

      auto lhs_is_join = lhs_col->view->IsJoin();
      auto rhs_is_join = rhs_col->view->IsJoin();

      // Both are joins. This is interesting, we should merge the joins.
      if (lhs_is_join && rhs_is_join) {
        auto first_join = reinterpret_cast<Node<QueryJoin> *>(lhs_col->view);
        auto second_join = reinterpret_cast<Node<QueryJoin> *>(rhs_col->view);
        assert(first_join->columns[0] == lhs_col);
        assert(second_join->columns[0] == rhs_col);

        const auto joined_col = DisjointSet::Union(lhs_col, rhs_col);
        if (second_join->columns[0] == joined_col) {
          std::swap(first_join, second_join);
        }

        for (auto col : second_join->joined_columns) {
          first_join->joined_columns.push_back(col);
        }

        // Make the old second join useless.
        second_join->joined_columns.clear();

      } else if (lhs_is_join) {
        JoinInto(rhs_col, lhs_col);

      } else if (rhs_is_join) {
        JoinInto(lhs_col, rhs_col);

      // Both are SELECTs, create a join.
      } else {
        const auto joined_col = JoinFor(
            lhs_col, rhs_col, std::min(lhs_col->id, rhs_col->id));
        id_to_col[lhs_col->id] = joined_col;
        id_to_col[rhs_col->id] = joined_col;
      }

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
      assert(lhs_col->Find() != rhs_col->Find());

      const auto constraint = new Node<QueryConstraint>(
          op, lhs_col->Find(), rhs_col->Find());
      query->constraints.emplace_back(constraint);

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

  Node<QueryColumn> *JoinFor(Node<QueryColumn> *prev_col,
                             Node<QueryColumn> *where_col,
                             unsigned id) {
    const auto join = new Node<QueryJoin>(query.get());
    query->joins.emplace_back(join);

    const auto joined_col = new Node<QueryColumn>(join, id, 0);
    query->columns.emplace_back(joined_col);
    join->pivot_columns.push_back(joined_col);
    join->joined_columns.push_back(prev_col);
    join->joined_columns.push_back(where_col);

    DisjointSet::UnionInto(prev_col, joined_col);
    DisjointSet::UnionInto(where_col, joined_col);

    return joined_col;
  }

  void JoinInto(Node<QueryColumn> *child_col, Node<QueryColumn> *parent_col) {
    const auto join = reinterpret_cast<Node<QueryJoin> *>(parent_col->view);
    join->joined_columns.push_back(child_col);
    DisjointSet::UnionInto(child_col, parent_col);
  }

  void AddMap(ParsedFunctor functor, const Column *select_begin,
              const Column *select_end, const Column *where_begin,
              const Column *where_end) {
    if (where_begin >= where_end) {
      // TODO(pag): Create a "value pump", e.g. primary keys.
      assert(false);

    } else {
      auto map = new Node<QueryMap>(query.get(), functor);
      query->maps.emplace_back(map);

      auto i = 0u;
      for (auto col = where_begin; col < where_end; ++col) {
        auto &prev_val = id_to_col[col->id];
        assert(prev_val != nullptr);
        auto mapped_col = new Node<QueryColumn>(map, col->id, i);
        query->columns.emplace_back(mapped_col);
        map->columns.push_back(mapped_col);
        map->input_columns.push_back(prev_val);
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
        auto mapped_col = new Node<QueryColumn>(map, col->id, i);
        query->columns.emplace_back(mapped_col);
        map->columns.push_back(mapped_col);
        auto &prev_val = id_to_col[col->id];
        assert(!prev_val);  // TODO(pag): Is this right?
        prev_val = mapped_col;
      }
    }
  }

  virtual void EnterFromSelect(
      ParsedPredicate pred, ParsedDeclaration decl,
      const Column *select_begin, const Column *select_end) override {

    if (decl.IsFunctor()) {
      auto functor = ParsedFunctor::From(decl);
      if (functor.IsAggregate()) {
        assert(false);  // TODO.
      } else {
        AddMap(functor, select_begin, select_end, nullptr, nullptr);
        return;
      }
    }

    auto select = SelectFor(pred);
    for (auto col = select_begin; col < select_end; ++col) {
      auto new_col = AddColumn(select, *col);
      auto &prev_col = id_to_col[col->id];
      assert(!prev_col);
      prev_col = new_col;
    }
  }

  void EnterFromWhereSelect(
      ParsedPredicate pred, ParsedDeclaration decl,
      const Column *where_begin, const Column *where_end,
      const Column *select_begin, const Column *select_end) override {

    if (decl.IsFunctor()) {
      auto functor = ParsedFunctor::From(decl);
      if (functor.IsAggregate()) {
        assert(false);  // TODO.
      } else {
        AddMap(functor, select_begin, select_end, where_begin, where_end);
        return;
      }
    }

    auto select = SelectFor(pred);
    columns.clear();
    output_columns.clear();
    columns.resize(pred.Arity());

    for (auto col = where_begin; col < where_end; ++col) {
      assert(!columns[col->n]);
      columns[col->n] = col;
    }

    for (auto col = select_begin; col < select_end; ++col) {
      assert(!columns[col->n]);
      columns[col->n] = col;
    }

    for (auto col : columns) {
      assert(col != nullptr);
      output_columns.push_back(AddColumn(select, *col));
    }

    (void) ProcessPendingEqualities();

    for (auto col = where_begin; col < where_end; ++col) {
      const auto where_col = output_columns[col->n];
      auto &prev_col = id_to_col[col->id];
      if (!prev_col) {
        assert(false);  // Shouldn't happen?
        prev_col = where_col;
        continue;
      }

      const auto prev_view = prev_col->Find()->view;

      // The previous view is a selection, and we're currently doing a new
      // selection where we're matching on the column, so create a new join
      // for this select and the previous one.
      if (prev_view->IsSelect()) {
        prev_col = JoinFor(prev_col->Find(), where_col, col->id);

      // The previous view for this column is a join, so merge with it.
      } else if (prev_view->IsJoin()) {
        JoinInto(where_col, prev_col);

      } else {
        assert(false);
      }
    }

    for (auto col = select_begin; col < select_end; ++col) {
      auto &prev_col = id_to_col[col->id];
      assert(!prev_col);
      prev_col = select->columns[col->n];
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
        AssertInequality(std::get<0>(cmp), std::get<1>(cmp), std::get<2>(cmp));
      }
      assert(prev_len > pending_compares.size());
    }
  }

  void MergeAndProjectJoins(void) {
    std::sort(query->joins.begin(), query->joins.end(),
              [](const std::unique_ptr<Node<QueryJoin>> &a,
                 const std::unique_ptr<Node<QueryJoin>> &b) {
                return a->joined_columns.size() < b->joined_columns.size();
              });

    std::unordered_set<Node<QueryView> *> joined_views;

    for (const auto &join : query->joins) {
      if (join->joined_columns.empty()) {
        continue;  // Taken over or merged.
      }

      joined_views.clear();

      // Go through the original pivot sources, which are originally stored in
      // the `joined_columns` table, and merge them in. All original joins are
      // based off of a single pivot model. We want to collect all unique views
      // that this join is combining together, so we can fill up
      // `joined_columns` with records representing the product of those tables.
      for (auto joined_col : join->joined_columns) {
        for (auto copied_col : joined_col->view->columns) {
          assert(copied_col->view != join.get());
          joined_views.insert(copied_col->view);
        }
      }

      // Clear out the old pivots, we're going to replace with all the columns
      // that are being joined together.
      join->joined_columns.clear();

      // Go through and have the join "steal" the original columns, and
      // replace them with equivalents.
      for (auto joined_view : joined_views) {
        for (auto &steal_col : joined_view->columns) {
          auto index = static_cast<unsigned>(join->columns.size());
          auto replace_col = new Node<QueryColumn>(
              steal_col->view, steal_col->id, steal_col->index);

          query->columns.emplace_back(replace_col);
          join->columns.push_back(steal_col);
          join->joined_columns.push_back(replace_col);

          steal_col->index = index;
          steal_col->view = join.get();

          steal_col = replace_col;
        }
      }
    }

    // Now we have a bunch of canonical joins, where the "joins" own their
    // product columns, by virtual of stealing them from their original views.
    // We might have a tower of joins, though, in that one join takes all
    // of its columns from the next join down, and so we'd want to compress
    // that down into a join on two pivots, then compress further if need be.
    for (const auto &join : query->joins) {
      if (join->joined_columns.empty() || join->pivot_columns.empty()) {
        continue;  // Taken over or merged.
      }

      joined_views.clear();
      for (auto joined_col : join->joined_columns) {
        joined_views.insert(joined_col->view);
      }

      if (joined_views.size() != 1) {
        continue;
      }

      const auto joined_view = *joined_views.begin();
      if (!joined_view->IsJoin()) {
        continue;
      }

      assert(joined_view != join.get());
      assert(join->joined_columns.size() == join->columns.size());

      const auto joined_join = reinterpret_cast<Node<QueryJoin> *>(joined_view);
      assert(join->joined_columns.size() == joined_join->columns.size());
      assert(joined_join->joined_columns.size() == joined_join->columns.size());

      for (auto joined_pivot : joined_join->pivot_columns) {
        joined_pivot->view = join.get();
        joined_pivot->index = static_cast<unsigned>(join->pivot_columns.size());
        join->pivot_columns.push_back(joined_pivot);
      }

      auto i = 0u;
      for (auto joined_col : joined_join->joined_columns) {
        auto &incoming = join->joined_columns[i];
        const auto old_outgoing = joined_join->columns[i];

        assert(incoming == old_outgoing);
        assert(incoming->Find() == incoming);

        // TODO(pag): Union things together?

        incoming = joined_col;
        ++i;
      }

      joined_join->pivot_columns.clear();
      joined_join->joined_columns.clear();
      joined_join->columns.clear();
    }
  }

  void LinkEverything(void) {
    auto &next_select = query->next_select;
    auto &next_join = query->next_join;
    auto &next_view = query->next_view;
    auto &next_insert = query->next_insert;
    auto &next_map = query->next_map;

    Node<QueryColumn> *dummy = nullptr;
    Node<QueryColumn> **prev_col_ptr = &dummy;

    for (const auto &view : query->maps) {
      view->next = next_map;
      view->next_view = next_view;
      next_view = next_map = view.get();

      prev_col_ptr = &dummy;
      for (auto col : view->columns) {
        *prev_col_ptr = col;
        prev_col_ptr = &(col->next_in_view);
      }
    }

    for (const auto &view : query->selects) {
      view->next = next_select;
      view->next_view = next_view;
      next_view = next_select = view.get();

      prev_col_ptr = &dummy;
      for (auto col : view->columns) {
        *prev_col_ptr = col;
        prev_col_ptr = &(col->next_in_view);
      }
    }

    for (const auto &view : query->joins) {
      prev_col_ptr = &dummy;
      for (auto col : view->columns) {
        *prev_col_ptr = col;
        prev_col_ptr = &(col->next_in_view);
      }

      if (!view->joined_columns.empty() && !view->pivot_columns.empty()) {
        view->next = next_join;
        view->next_view = next_view;
        next_view = next_join = view.get();
      }
    }

    for (const auto &insert : query->inserts) {
      insert->next = next_insert;
      next_insert = insert.get();
    }

    prev_col_ptr = &dummy;
    for (const auto &col : query->columns) {
      *prev_col_ptr = col.get();
      prev_col_ptr = &(col->next);
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
    MergeAndProjectJoins();

    auto table = TableFor(decl);
    assert(table->IsRelation());
    auto insert = new Node<QueryInsert>(
        reinterpret_cast<Node<QueryRelation> *>(table), decl);

    for (auto col = begin; col < end; ++col) {
      const auto output_col = id_to_col[col->id];
      assert(output_col != nullptr);
      insert->columns.push_back(output_col->Find());
    }

    query->inserts.emplace_back(insert);
  }

  void Commit(ParsedPredicate) override {
    LinkEverything();
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

  // All columns in some select...where.
  std::vector<Node<QueryColumn> *> output_columns;

  // Maps variable IDs to columns.
  std::unordered_map<unsigned, Node<QueryColumn> *> id_to_col;

  std::vector<std::pair<unsigned, unsigned>> pending_equalities;
  std::vector<std::pair<unsigned, unsigned>> next_pending_equalities;

  std::vector<std::tuple<ComparisonOperator, unsigned, unsigned>>
      pending_compares;

  std::vector<std::tuple<ComparisonOperator, unsigned, unsigned>>
      next_pending_compares;
};

// Build an insertion query for the best scoring, according to `scorer`,
// permutation of some clause body, given some predicate, as generated by
// `generator`.
Query QueryBuilder::BuildInsert(SIPSScorer &scorer, SIPSGenerator &generator) {
  impl->query = std::make_shared<QueryImpl>(impl->context);

  if (SIPSScorer::VisitBestScoringPermuation(
          scorer, *impl, generator)) {
    return Query(std::move(impl->query));

  } else if (auto empty_query = impl->context->empty_query.lock()) {
    return Query(std::move(empty_query));

  } else {
    impl->query->columns.clear();
    impl->query->joins.clear();
    impl->query->selects.clear();
    impl->query->constraints.clear();
    impl->query->columns.clear();

    impl->context->empty_query = impl->query;
    return Query(std::move(impl->query));
  }
}

QueryBuilder::QueryBuilder(void)
    : impl(std::make_unique<QueryBuilderImpl>()) {}

QueryBuilder::~QueryBuilder(void) {}

}  // namespace hyde
