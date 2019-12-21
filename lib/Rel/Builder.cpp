// Copyright 2019, Trail of Bits. All rights reserved.

#include <drlojekyll/Rel/Builder.h>

#include <tuple>
#include <unordered_set>

#include <drlojekyll/Sema/SIPSScore.h>
#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Lex/Format.h>
#include <drlojekyll/Parse/Format.h>

#include "Query.h"

namespace hyde {

OutputStream *gOut = nullptr;

class QueryBuilderImpl : public SIPSVisitor {
 public:
  QueryBuilderImpl(void)
      : context(std::make_shared<query::QueryContext>()) {}

  virtual ~QueryBuilderImpl(void) = default;

  Node<QueryRelation> *TableFor(ParsedDeclaration decl, bool is_positive=true) {
    assert(decl.IsLocal() || decl.IsExport() || decl.IsQuery());

    auto &rels = is_positive ? context->relations : context->negative_relations;
    auto &table = rels[decl];
    if (!table) {
      table.reset(new Node<QueryRelation>(
          decl, context->next_relation, context->next_relation, is_positive));
      context->next_relation = table.get();
    }

    return table.get();
  }

  // Get the table for a given predicate.
  Node<QueryRelation> *TableFor(ParsedPredicate pred) {
    const auto prev_next_table = context->next_relation;
    const auto decl = ParsedDeclaration::Of(pred);
    const auto table = TableFor(decl, pred.IsPositive());

    // We just added this negative table. Go and create a select for it from
    // the positive table, and create a fake insert into the negative table.
    if (prev_next_table != context->next_relation && !pred.IsPositive()) {
      const auto positive_table = TableFor(decl, true);
      const auto select = new Node<QuerySelect>(
          query.get(), positive_table, nullptr);
      query->selects.emplace_back(select);
      for (unsigned i = 0, max_i = decl.Arity(); i < max_i; ++i) {
        Column column(decl.NthParameter(i), pred.NthArgument(i), i, 0);
        AddColumn(select, column);
      }

      const auto insert = new Node<QueryInsert>(
          reinterpret_cast<Node<QueryRelation> *>(table), decl);
      insert->columns = select->columns;
      query->inserts.emplace_back(insert);
    }

    return table;
  }

  Node<QueryStream> *StreamFor(ParsedLiteral literal) {
    std::string spelling(literal.Spelling());
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

  Node<QueryStream> *StreamFor(ParsedMessage message) {
    auto &stream = context->messages[message];
    if (!stream) {
      stream.reset(new Node<QueryMessage>(
          message, context->next_stream, context->next_message));
      context->next_stream = stream.get();
      context->next_message = stream.get();
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
    auto decl = ParsedDeclaration::Of(pred);
    if (decl.IsMessage()) {
      auto stream = StreamFor(ParsedMessage::From(decl));
      auto select = new Node<QuerySelect>(
          query.get(), nullptr, stream);
      query->selects.emplace_back(select);
      return select;

    } else if (decl.IsFunctor()) {
      auto stream = StreamFor(ParsedFunctor::From(decl));
      auto select = new Node<QuerySelect>(
          query.get(), nullptr, stream);
      query->selects.emplace_back(select);
      return select;

    } else {
      auto table = TableFor(pred);
      auto select = new Node<QuerySelect>(
          query.get(), table, nullptr);
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
    initial_view = SelectFor(pred);
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
    query->selects.emplace_back(select);

    const auto col = new Node<QueryColumn>(
        ParsedVariable::AssignedTo(val), select, id, 0);
    select->columns.push_back(col);
    query->columns.emplace_back(col);

    auto &prev_col = id_to_col[id];
    assert(!prev_col);
    prev_col = select->columns[0];
  }

  void AssertEqual(unsigned lhs_id, unsigned rhs_id) override {
    if (lhs_id == rhs_id) {
      return;
    }

    auto &lhs_col = id_to_col[lhs_id];
    auto &rhs_col = id_to_col[rhs_id];

    if (lhs_col && rhs_col) {

      assert(!lhs_col->view->IsJoin());
      assert(!rhs_col->view->IsJoin());

      if (lhs_col == rhs_col) {
        return;
      }
      if (lhs_col->view == rhs_col->view) {
        assert(false && "TODO");

      } else {
        auto join = new Node<QueryJoin>(query.get());

        query->joins.emplace_back(join);
        join->joined_columns.push_back(lhs_col);
        join->joined_columns.push_back(rhs_col);
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
      assert(lhs_col != rhs_col);

      const auto constraint = new Node<QueryConstraint>(
          op, lhs_col, rhs_col);
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

  void AddMap(ParsedFunctor functor, const Column *select_begin,
              const Column *select_end, const Column *where_begin,
              const Column *where_end) {

    assert(where_begin < where_end);

    (void) ProcessPendingEqualities();

    auto map = new Node<QueryMap>(query.get(), functor);
    query->maps.emplace_back(map);

    auto i = 0u;
    for (auto col = where_begin; col < where_end; ++col) {
      auto &prev_val = id_to_col[col->id];
      assert(prev_val != nullptr);
      auto mapped_col = new Node<QueryColumn>(col->var, map, col->id, i);
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


    if (decl.IsFunctor()) {
      auto functor = ParsedFunctor::From(decl);
      if (functor.IsAggregate()) {
        assert(false);  // TODO.
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
    columns.resize(pred.Arity());

    for (auto col = where_begin; col < where_end; ++col) {
      assert(!columns[col->n]);
      columns[col->n] = col;
    }

    for (auto col = select_begin; col < select_end; ++col) {
      assert(!columns[col->n]);
      columns[col->n] = col;
    }

    // Create columns for the select, but give each column a totally unique
    // ID.
    for (auto col : columns) {
      assert(col != nullptr);
      (void) AddColumn(select, *col);
    }

    for (auto col = where_begin; col < where_end; ++col) {
      const auto where_col = select->columns[col->n];
      const auto prev_col = id_to_col[col->id];
      assert(prev_col != nullptr);

      auto join = new Node<QueryJoin>(query.get());
      query->joins.emplace_back(join);

      join->joined_columns.push_back(prev_col);
      join->joined_columns.push_back(where_col);
    }

    // Go through and rewrite all assignments to the newly selected column,
    // but don't actually join the two into the same set.
    for (auto col = where_begin; col < where_end; ++col) {
      auto where_col = select->columns[col->n];
      auto &prev_col = id_to_col[col->id];
//      if (!prev_col) {
//        prev_col = where_col;
//      }
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
        pending_equalities.emplace_back(col->id, select_col->id);

      } else {
        prev_col = select_col;
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

  void ReplaceAllUsesWith(
      Node<QueryColumn> *old_col, Node<QueryColumn> *new_col) {
    for (auto &join : query->joins) {
      for (auto &input_col : join->joined_columns) {
        if (input_col == old_col) {
          input_col = new_col;
        }
      }
    }

    for (auto &map : query->maps) {
      for (auto &input_col : map->input_columns) {
        if (input_col == old_col) {
          input_col = new_col;
        }
      }
    }

    for (auto &insert : query->inserts) {
      for (auto &input_col : insert->columns) {
        if (input_col == old_col) {
          input_col = new_col;
        }
      }
    }

    for (auto &cmp : query->constraints) {
      if (cmp->lhs == old_col) {
        cmp->lhs = new_col;
      }
      if (cmp->rhs == old_col) {
        cmp->rhs = new_col;
      }
    }
  }

  void MergeAndProjectJoins(void) {

    std::unordered_map<ParsedVariable, unsigned> join_weight;
    for (const auto &join : query->joins) {
      for (auto col : join->joined_columns) {
        join_weight[col->var] += 1;
      }
    }
//
//    for (const auto &join : query->joins) {
//      for (auto col : join->joined_columns) {
//        num_joins[join.get()] += num_joins[col->view];
//      }
//    }

    std::unordered_map<unsigned, std::vector<Node<QueryColumn> *>>
        promoted_ids;

    std::unordered_map<ParsedVariable, std::vector<Node<QueryColumn> *>>
        promoted_vars;

    std::unordered_map<Node<QueryColumn> *, DisjointSet> eqc;

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
      assert(join->pivot_conditions.empty());
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

      promoted_vars.clear();
      promoted_ids.clear();
      eqc.clear();

      auto min_id = ~0u;
      auto max_id = 0u;

      for (auto joined_col : col0->view->columns) {
        eqc.emplace(joined_col, joined_col->id);
        min_id = std::min(min_id, joined_col->id);
        max_id = std::max(max_id, joined_col->id);
        promoted_ids[joined_col->id].push_back(joined_col);
        promoted_vars[joined_col->var].push_back(joined_col);
      }

      if (col1->view != col0->view) {
        for (auto joined_col : col1->view->columns) {
          assert(!eqc.count(joined_col));
          eqc.emplace(joined_col, joined_col->id);
          min_id = std::min(min_id, joined_col->id);
          max_id = std::max(max_id, joined_col->id);
          promoted_ids[joined_col->id].push_back(joined_col);
          promoted_vars[joined_col->var].push_back(joined_col);
        }
      } else {
        assert(false && "TODO?");
      }

      // Union together all columns that should belong to the same equivalence
      // class. We're looking at columns that share the same variable, or id,
      // or both.
      for (auto &ent : eqc) {
        auto joined_col = ent.first;
        auto &our_eqc = ent.second;
        for (auto rel_col : promoted_ids[joined_col->id]) {
          auto it = eqc.find(rel_col);
          assert(it != eqc.end());
          DisjointSet::Union(&our_eqc, &(it->second));
        }
        for (auto rel_col : promoted_vars[joined_col->var]) {
          auto it = eqc.find(rel_col);
          assert(it != eqc.end());
          DisjointSet::Union(&our_eqc, &(it->second));
        }
      }

      // Remake `promoted_ids` so that it takes the equivalence classes into
      // account.
      promoted_ids.clear();
      for (auto &ent : eqc) {
        auto joined_col = ent.first;
        auto &our_eqc = ent.second;
        promoted_ids[our_eqc.Find()->id].push_back(joined_col);
      }

      // `promoted_ids` is now a mapping from the smallest ID to its equivalence
      // class of columns, where every column in the equivalence class is
      // related to at least one other column by ID or by variable name.
      //
      // Go and fill up the output columns of the join, which is the set of
      // unique input columns. For each output column that we create, we replace
      // all uses of the input column with the output column.
      for (auto i = min_id; i <= max_id; ++i) {
        auto &col_set = promoted_ids[i];
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
            min_var, join.get(), i,
            static_cast<unsigned>(join->columns.size()));

        query->columns.emplace_back(output_col);
        join->columns.push_back(output_col);

        for (auto col : col_set) {
          ReplaceAllUsesWith(col, output_col);
        }

        if (1 < col_set.size()) {
          join->pivot_columns.push_back(output_col);
        }
      }

      // Add in the joined columns after, so that they don't get replaced.
      for (auto i = min_id; i <= max_id; ++i) {
        auto &col_set = promoted_ids[i];
        if (col_set.empty()) {
          continue;
        }

        if (1 < col_set.size()) {

          // Add in the pivot condition. Has to happen *after* the `ReplaceAllUses`
          // that happens in `replace_col`.
          auto first_pivot_col = col_set[0];
          for (auto k = 1u; k < col_set.size(); ++k) {
            auto next_pivot_col = col_set[k];

            auto pivot_cond = new Node<QueryConstraint>(
                ComparisonOperator::kEqual, first_pivot_col, next_pivot_col);
            query->constraints.emplace_back(pivot_cond);
            join->pivot_conditions.push_back(pivot_cond);
          }
        }

        for (auto col : col_set) {
          join->joined_columns.push_back(col);
        }
      }
    }

    std::unordered_set<Node<QueryJoin> *> joined_joins;

    // Try to combine join trees that all operate on the same pivots
    bool found = false;
    for (const auto &join : query->joins) {
      if (join->joined_columns.empty()) {
        continue;  // Taken over or merged.
      }

      joined_joins.clear();
      for (auto joined_col : join->joined_columns) {

        // We only care about joins of joins.
        if (!joined_col->view->IsJoin()) {
          goto try_next_join;
        }

        auto joined_join = reinterpret_cast<Node<QueryJoin> *>(
            joined_col->view);

        // Make sure that all pivots of the lower join participate in at least
        // one pivot condition of this join.
        found = false;
        for (auto joined_pivot_col : joined_join->pivot_columns) {
          for (auto pivot_cond : join->pivot_conditions) {
            if (joined_pivot_col == pivot_cond->lhs ||
                joined_pivot_col == pivot_cond->rhs) {
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
      goto merge_joins;

    try_next_join:
      continue;

    merge_joins:

      join->joined_columns.clear();
      join->pivot_conditions.clear();

      for (auto joined_join : joined_joins) {
        join->joined_columns.insert(
            join->joined_columns.end(),
            joined_join->joined_columns.begin(),
            joined_join->joined_columns.end());

        join->pivot_conditions.insert(
            join->pivot_conditions.end(),
            joined_join->pivot_conditions.begin(),
            joined_join->pivot_conditions.end());

        joined_join->pivot_conditions.clear();
        joined_join->joined_columns.clear();
        joined_join->pivot_columns.clear();
      }

//
//      assert(1 < joined_joins.size());
//
//      const auto joined_view = *joined_views.begin();
//      if (!joined_view->IsJoin()) {
//        continue;
//      }
//
//      assert(joined_view != join.get());
//      assert(join->joined_columns.size() == join->columns.size());
//
//      const auto joined_join = reinterpret_cast<Node<QueryJoin> *>(joined_view);
//      assert(join->joined_columns.size() == joined_join->columns.size());
//      assert(joined_join->joined_columns.size() == joined_join->columns.size());
//
//      for (auto joined_pivot : joined_join->pivot_columns) {
//        joined_pivot->view = join.get();
//        joined_pivot->index = static_cast<unsigned>(join->pivot_columns.size());
//        join->pivot_columns.push_back(joined_pivot);
//      }
//
//      auto i = 0u;
//      for (auto joined_col : joined_join->joined_columns) {
//        auto &incoming = join->joined_columns[i];
//        const auto old_outgoing = joined_join->columns[i];
//
//        assert(incoming == old_outgoing);
//        //assert(incoming->Find() == incoming);
//
//        // TODO(pag): Union things together?
//
//        incoming = joined_col;
//        ++i;
//      }
//
//      joined_join->pivot_columns.clear();
//      joined_join->joined_columns.clear();
//      joined_join->columns.clear();
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

      prev_col_ptr = &dummy;
      for (auto col : view->pivot_columns) {
        *prev_col_ptr = col;
        prev_col_ptr = &(col->next_pivot_in_join);
      }

      if (!view->joined_columns.empty()) {
        view->next = next_join;
        view->next_view = next_view;
        next_view = next_join = view.get();
      }

      for (auto i = 1u; i < view->pivot_conditions.size(); ++i) {
        auto cond = view->pivot_conditions[i];
        auto prev_cond = view->pivot_conditions[i - 1];
        prev_cond->next_pivot_condition = cond;
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

    auto table = TableFor(decl);
    auto insert = new Node<QueryInsert>(
        reinterpret_cast<Node<QueryRelation> *>(table), decl);

    for (auto col = begin; col < end; ++col) {
      const auto output_col = id_to_col[col->id];
      assert(output_col != nullptr);
      insert->columns.push_back(output_col);
    }

    query->inserts.emplace_back(insert);
  }

  void Commit(ParsedPredicate) override {
    MergeAndProjectJoins();
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
