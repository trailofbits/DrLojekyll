// Copyright 2019, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/Rel/Query.h>

#include <string>
#include <unordered_map>
#include <vector>

#include <drlojekyll/Parse/Parse.h>
#include <drlojekyll/Util/DisjointSet.h>

namespace hyde {
namespace query {

// Contextual information shared by all queries generated by the same builder.
class QueryContext {
 public:
  QueryContext(void);
  ~QueryContext(void);

  // A cache of the empty query. We may generate empty queries for some
  // clauses.
  std::weak_ptr<QueryImpl> empty_query;


  // The streams associated with generators, i.e. functors that only output
  // values.
  std::unordered_map<ParsedFunctor, std::unique_ptr<Node<QueryGenerator>>>
      generators;

  // The streams associated with messages.
  std::unordered_map<ParsedMessage, std::unique_ptr<Node<QueryMessage>>>
      messages;

  // The tables available within any query sharing this context.
  std::unordered_map<ParsedDeclaration, std::unique_ptr<Node<QueryRelation>>>
      relations;

  // Negative tables, these are basically "opposites" of normal tables.
  // Selections from negative tables must be involved in a "full" join.
  std::unordered_map<ParsedDeclaration, std::unique_ptr<Node<QueryRelation>>>
      negative_relations;

  std::unordered_map<std::string, std::unique_ptr<Node<QueryConstant>>>
      constant_integers;

  std::unordered_map<std::string, std::unique_ptr<Node<QueryConstant>>>
      constant_strings;

  unsigned next_join_id{~0U};

  // The next table.
  Node<QueryStream> *next_stream{nullptr};
  Node<QueryRelation> *next_relation{nullptr};
  Node<QueryConstant> *next_constant{nullptr};
  Node<QueryMessage> *next_message{nullptr};
  Node<QueryGenerator> *next_generator{nullptr};
};

}  // namespace query

template <>
class Node<QueryRelation> {
 public:
  inline Node(ParsedDeclaration decl_, Node<QueryRelation> *next_table_,
              Node<QueryRelation> *next_, bool is_positive_)
      : decl(decl_),
        next(next_),
        is_positive(is_positive_) {}

  const ParsedDeclaration decl;

  // Next relation, not specific to a query.
  Node<QueryRelation> * const next;

  // Is this a positive or negative table?
  const bool is_positive;
};

template <>
class Node<QueryStream> {
 public:
  virtual ~Node(void);

  inline Node(Node<QueryStream> *next_)
      : next_stream(next_) {}

  virtual bool IsConstant(void) const noexcept = 0;
  virtual bool IsGenerator(void) const noexcept = 0;
  virtual bool IsMessage(void) const noexcept = 0;

  // Next stream, not specific to a query.
  Node<QueryStream> * const next_stream;
};

template <>
class Node<QueryConstant> final : public Node<QueryStream> {
 public:
  virtual ~Node(void);

  inline Node(ParsedLiteral literal_, Node<QueryStream> *next_stream_,
              Node<QueryConstant> *next_)
      : Node<QueryStream>(next_stream_),
        literal(literal_),
        next(next_) {}

  bool IsConstant(void) const noexcept override;
  bool IsGenerator(void) const noexcept override;
  bool IsMessage(void) const noexcept override;

  const ParsedLiteral literal;

  // Next constant, not specific to a query.
  Node<QueryConstant> * const next;
};

template <>
class Node<QueryGenerator> final : public Node<QueryStream> {
 public:
  virtual ~Node(void);

  inline Node(ParsedFunctor functor_, Node<QueryStream> *next_stream_,
              Node<QueryGenerator> *next_)
      : Node<QueryStream>(next_stream_),
        functor(functor_),
        next(next_) {}

  bool IsConstant(void) const noexcept override;
  bool IsGenerator(void) const noexcept override;
  bool IsMessage(void) const noexcept override;

  const ParsedFunctor functor;

  // Next generator, not specific to a query.
  Node<QueryGenerator> * const next;
};

template <>
class Node<QueryMessage> final : public Node<QueryStream> {
 public:
  virtual ~Node(void);

  inline Node(ParsedMessage message_, Node<QueryStream> *next_stream_,
              Node<QueryMessage> *next_)
      : Node<QueryStream>(next_stream_),
        message(message_),
        next(next_) {}

  bool IsConstant(void) const noexcept override;
  bool IsGenerator(void) const noexcept override;
  bool IsMessage(void) const noexcept override;

  const ParsedMessage message;

  // Next message, not specific to a query.
  Node<QueryMessage> * const next;
};

template <>
class Node<QueryView> {
 public:
  virtual ~Node(void);

  virtual bool IsSelect(void) const noexcept = 0;
  virtual bool IsJoin(void) const noexcept = 0;
  virtual bool IsMap(void) const noexcept = 0;

  inline Node(QueryImpl *query_)
      : query(query_) {}

  // Query containing this view.
  QueryImpl * const query{nullptr};

  // The selected columns.
  std::vector<Node<QueryColumn> *> columns;

  // Next view (select or join) in this query.
  Node<QueryView> *next_view{nullptr};
};

template <>
class Node<QuerySelect> final : public Node<QueryView> {
 public:
  inline Node(QueryImpl *query_, Node<QueryRelation> *relation_,
              Node<QueryStream> *stream_)
      : Node<QueryView>(query_),
        relation(relation_),
        stream(stream_) {}

  virtual ~Node(void);
  bool IsSelect(void) const noexcept override;
  bool IsJoin(void) const noexcept override;
  bool IsMap(void) const noexcept override;

  // Next select in this query.
  Node<QuerySelect> *next{nullptr};

  // The table from which this select takes its columns.
  Node<QueryRelation> * const relation;
  Node<QueryStream> * const stream;
};

template <>
class Node<QueryJoin> final : public Node<QueryView> {
 public:
  using Node<QueryView>::Node;

  virtual ~Node(void);
  bool IsSelect(void) const noexcept override;
  bool IsJoin(void) const noexcept override;
  bool IsMap(void) const noexcept override;

  // Next join in this query.
  Node<QueryJoin> *next{nullptr};

  // The columns that are all joined together.
  std::vector<Node<QueryColumn> *> joined_columns;

  // Tells us which columns are pivots.
  std::vector<Node<QueryColumn> *> pivot_columns;

  // Conditions on the input columns.
  std::vector<Node<QueryConstraint> *> pivot_conditions;
};

template <>
class Node<QueryMap> final : public Node<QueryView> {
 public:
  using Node<QueryView>::Node;

  virtual ~Node(void);
  bool IsSelect(void) const noexcept override;
  bool IsJoin(void) const noexcept override;
  bool IsMap(void) const noexcept override;

  inline Node(QueryImpl *query_, ParsedFunctor functor_)
      : Node<QueryView>(query_),
        functor(functor_) {}

  const ParsedFunctor functor;

  // Next join in this query.
  Node<QueryMap> *next{nullptr};

  // The columns that are are the inputs to the functor. These correspond
  // with the bound arguments to the functor.
  std::vector<Node<QueryColumn> *> input_columns;
};

template <>
class Node<QueryConstraint> {
 public:
  inline Node(ComparisonOperator op_, Node<QueryColumn> *lhs_,
              Node<QueryColumn> *rhs_)
      : lhs(lhs_),
        rhs(rhs_),
        op(op_) {}

  Node<QueryColumn> *lhs;
  Node<QueryColumn> *rhs;

  // Next such constraint in this query.
  Node<QueryConstraint> *next{nullptr};

  // Next such constraint in this pivot list.
  Node<QueryConstraint> *next_pivot_condition{nullptr};

  const ComparisonOperator op;
};

template <>
class Node<QueryColumn> : public DisjointSet {
 public:
  inline explicit Node(ParsedVariable var_, Node<QueryView> *view_,
                       unsigned id_, unsigned index_)
      : DisjointSet(id_),
        var(var_),
        view(view_),
        index(index_) {}

  const ParsedVariable var;

  // View to which this column belongs.
  Node<QueryView> * const view;

  // Tells us this column can be found at `view->columns[index]`.
  const unsigned index;

  // Next column in the whole query.
  Node<QueryColumn> *next{nullptr};

  // Next column within the same view.
  Node<QueryColumn> *next_in_view{nullptr};

  // Next pivot column within the same join.
  Node<QueryColumn> *next_pivot_in_join{nullptr};
};

template <>
class Node<QueryInsert> {
 public:
  inline Node(Node<QueryRelation> *relation_, ParsedDeclaration decl_)
      : relation(relation_),
        decl(decl_) {}

  Node<QueryInsert> *next{nullptr};

  Node<QueryRelation> * const relation;
  const ParsedDeclaration decl;
  std::vector<Node<QueryColumn> *> columns;
};

class QueryImpl {
 public:
  inline QueryImpl(std::shared_ptr<query::QueryContext> context_)
      : context(context_) {}

  const std::shared_ptr<query::QueryContext> context;

  Node<QuerySelect> *next_select{nullptr};
  Node<QueryView> *next_view{nullptr};
  Node<QueryJoin> *next_join{nullptr};
  Node<QueryInsert> *next_insert{nullptr};
  Node<QueryMap> *next_map{nullptr};

  std::vector<std::unique_ptr<Node<QuerySelect>>> selects;
  std::vector<std::unique_ptr<Node<QueryJoin>>> joins;
  std::vector<std::unique_ptr<Node<QueryMap>>> maps;
  std::vector<std::unique_ptr<Node<QueryConstraint>>> constraints;
  std::vector<std::unique_ptr<Node<QueryColumn>>> columns;
  std::vector<std::unique_ptr<Node<QueryInsert>>> inserts;
};

}  // namespace hyde
