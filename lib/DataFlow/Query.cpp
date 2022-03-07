// Copyright 2019, Trail of Bits. All rights reserved.

#include "Query.h"

#include <cassert>
#include <functional>
#include <optional>
#include <sstream>
#include <unordered_set>

#include "EquivalenceSet.h"

#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Wstrict-aliasing"
#endif

namespace hyde {

QueryImpl::QueryImpl(const ParsedModule &module_)
    : module(module_.RootModule()) {}

QueryImpl::~QueryImpl(void) {

  for (auto rel : relations) {
    rel->inserts.ClearWithoutErasure();
    rel->selects.ClearWithoutErasure();
  }

  for (auto io : ios) {
    io->transmits.ClearWithoutErasure();
    io->receives.ClearWithoutErasure();
  }

  ForEachView([](VIEW *view) {
    view->input_columns.ClearWithoutErasure();
    view->attached_columns.ClearWithoutErasure();
    view->positive_conditions.ClearWithoutErasure();
    view->negative_conditions.ClearWithoutErasure();
    view->sets_condition.ClearWithoutErasure();
    view->predecessors.ClearWithoutErasure();
    view->successors.ClearWithoutErasure();

    if (auto info = view->induction_info.get()) {
      if (info->cyclic_views) {
        if (info->cyclic_views->Owner() == view) {
          info->cyclic_views->ClearWithoutErasure();
        }
        info->cyclic_views.reset();
      }
      info->inductive_successors.ClearWithoutErasure();
      info->inductive_predecessors.ClearWithoutErasure();
      info->noninductive_successors.ClearWithoutErasure();
      info->noninductive_predecessors.ClearWithoutErasure();
      view->induction_info.reset();
    }
  });

  for (auto join : joins) {
    for (auto &[out_col, in_cols] : join->out_to_in) {
      (void) out_col;
      in_cols.ClearWithoutErasure();
    }
    join->out_to_in.clear();
    join->joined_views.ClearWithoutErasure();
  }

  for (auto agg : aggregates) {
    agg->group_by_columns.ClearWithoutErasure();
    agg->config_columns.ClearWithoutErasure();
    agg->aggregated_columns.ClearWithoutErasure();
  }

  for (auto merge : merges) {
    merge->merged_views.ClearWithoutErasure();
  }

  for (auto cond : conditions) {
    cond->positive_users.ClearWithoutErasure();
    cond->negative_users.ClearWithoutErasure();
    cond->setters.ClearWithoutErasure();
  }

  for (auto select : selects) {
    select->relation.ClearWithoutErasure();
    select->stream.ClearWithoutErasure();
    select->inserts.ClearWithoutErasure();
  }

  for (auto insert : inserts) {
    insert->relation.ClearWithoutErasure();
    insert->stream.ClearWithoutErasure();
  }

  for (auto negation : negations) {
    negation->negated_view.ClearWithoutErasure();
  }
}

QueryStream QueryStream::From(const QuerySelect &sel) noexcept {
  const auto stream = sel.impl->stream.get();
  assert(stream != nullptr);
  return QueryStream(stream);
}

QueryStream QueryStream::From(const QueryInsert &ins) noexcept {
  const auto stream = ins.impl->stream.get();
  assert(stream != nullptr);
  return QueryStream(stream);
}

QueryStream::QueryStream(const QueryIO &io) noexcept
    : QueryStream(io.impl) {}

QueryStream::QueryStream(const QueryConstant &const_) noexcept
    : QueryStream(const_.impl) {}

const char *QueryStream::KindName(void) const noexcept {
  return impl->KindName();
}

bool QueryStream::IsConstant(void) const noexcept {
  return impl->AsConstant() != nullptr;
}

bool QueryStream::IsTag(void) const noexcept {
  return impl->AsTag() != nullptr;
}

bool QueryStream::IsIO(void) const noexcept {
  return impl->AsIO() != nullptr;
}

QueryView QueryView::Containing(QueryColumn col) {
  return QueryView(col.impl->view);
}

QueryView::QueryView(const QuerySelect &view) : QueryView(view.impl) {}

QueryView::QueryView(const QueryTuple &view) : QueryView(view.impl) {}

QueryView::QueryView(const QueryKVIndex &view) : QueryView(view.impl) {}

QueryView::QueryView(const QueryJoin &view) : QueryView(view.impl) {}

QueryView::QueryView(const QueryMap &view) : QueryView(view.impl) {}

QueryView::QueryView(const QueryAggregate &view) : QueryView(view.impl) {}

QueryView::QueryView(const QueryMerge &view) : QueryView(view.impl) {}

QueryView::QueryView(const QueryNegate &view) : QueryView(view.impl) {}

QueryView::QueryView(const QueryCompare &view) : QueryView(view.impl) {}

QueryView::QueryView(const QueryInsert &view) : QueryView(view.impl) {}


// Returns the `nth` output column.
QueryColumn QueryView::NthColumn(unsigned n) const noexcept {
  assert(n < impl->columns.Size());
  return QueryColumn(impl->columns[n]);
}

QueryView QueryView::From(const QuerySelect &view) noexcept {
  return reinterpret_cast<const QueryView &>(view);
}

QueryView QueryView::From(const QueryTuple &view) noexcept {
  return reinterpret_cast<const QueryView &>(view);
}

QueryView QueryView::From(const QueryKVIndex &view) noexcept {
  return reinterpret_cast<const QueryView &>(view);
}

QueryView QueryView::From(const QueryJoin &view) noexcept {
  return reinterpret_cast<const QueryView &>(view);
}

QueryView QueryView::From(const QueryMap &view) noexcept {
  return reinterpret_cast<const QueryView &>(view);
}

QueryView QueryView::From(const QueryAggregate &view) noexcept {
  return reinterpret_cast<const QueryView &>(view);
}

QueryView QueryView::From(const QueryMerge &view) noexcept {
  return reinterpret_cast<const QueryView &>(view);
}

QueryView QueryView::From(const QueryNegate &view) noexcept {
  return reinterpret_cast<const QueryView &>(view);
}

QueryView QueryView::From(const QueryCompare &view) noexcept {
  return reinterpret_cast<const QueryView &>(view);
}

QueryView QueryView::From(const QueryInsert &view) noexcept {
  return reinterpret_cast<const QueryView &>(view);
}

DefinedNodeRange<QueryColumn> QueryView::Columns(void) const {
  return {DefinedNodeIterator<QueryColumn>(impl->columns.begin()),
          DefinedNodeIterator<QueryColumn>(impl->columns.end())};
}

DefinedNodeRange<QueryColumn> QueryJoin::Columns(void) const {
  return QueryView(impl).Columns();
}

// List of the output pivot columns.
DefinedNodeRange<QueryColumn> QueryJoin::PivotColumns(void) const noexcept {
  const auto it = impl->columns.begin();
  return {DefinedNodeIterator<QueryColumn>(it),
          DefinedNodeIterator<QueryColumn>(it + NumPivotColumns())};
}

// List of the output non-pivot columns.
DefinedNodeRange<QueryColumn> QueryJoin::MergedColumns(void) const noexcept {
  return {DefinedNodeIterator<QueryColumn>(impl->columns.begin() +
                                           NumPivotColumns()),
          DefinedNodeIterator<QueryColumn>(impl->columns.end())};
}

const char *QueryView::KindName(void) const noexcept {
  return impl->KindName();
}

void QueryView::SetTableId(unsigned id) const noexcept {
  impl->table_id.emplace(id);
}

std::optional<unsigned> QueryView::TableId(void) const noexcept {
  return impl->table_id;
}

// Id assigned by building the equivalence set in BuildEquivalenceSets
// return std::nullopt if the EquivalenceSet hasn't been built yet
unsigned QueryView::EquivalenceSetId(void) const noexcept {
  if (!impl->equivalence_set) {
    return ~0u;
  } else {
    return impl->equivalence_set->Find()->id;
  }
}

// VIEWs in this nodes equivelence set
UsedNodeRange<QueryView> QueryView::EquivalenceSetViews(void) const {
  if (auto set = impl->equivalence_set.get()) {
    return {UsedNodeIterator<QueryView>(set->Find()->views_in_set.begin()),
            UsedNodeIterator<QueryView>(set->Find()->views_in_set.end())};
  } else {
    return {};
  }
}

bool QueryView::IsConstantAfterInitialization(void) const noexcept {
  return impl->is_const_after_init;
}

bool QueryView::IsSelect(void) const noexcept {
  return impl->AsSelect() != nullptr;
}

bool QueryView::IsTuple(void) const noexcept {
  return impl->AsTuple() != nullptr;
}

bool QueryView::IsKVIndex(void) const noexcept {
  return impl->AsKVIndex() != nullptr;
}

bool QueryView::IsJoin(void) const noexcept {
  return impl->AsJoin() != nullptr;
}

bool QueryView::IsMap(void) const noexcept {
  return impl->AsMap() != nullptr;
}

bool QueryView::IsAggregate(void) const noexcept {
  return impl->AsAggregate() != nullptr;
}

bool QueryView::IsMerge(void) const noexcept {
  return impl->AsMerge() != nullptr;
}

bool QueryView::IsNegate(void) const noexcept {
  return impl->AsNegate() != nullptr;
}

bool QueryView::IsCompare(void) const noexcept {
  return impl->AsCompare() != nullptr;
}

bool QueryView::IsInsert(void) const noexcept {
  return impl->AsInsert() != nullptr;
}

// Returns `true` if this node is used by a `QueryNegate`.
bool QueryView::IsUsedByNegation(void) const noexcept {
  return impl->is_used_by_negation;
}

// Returns `true` if this node is used by a `QueryJoin`.
bool QueryView::IsUsedByJoin(void) const noexcept {
  return impl->is_used_by_join;
}

// Returns `true` if this node is used by a `QueryMerge`.
bool QueryView::IsUsedByMerge(void) const noexcept {
  return impl->is_used_by_merge;
}

// Apply a callback `on_negate` to each negation using this view.
void QueryView::ForEachNegation(
    std::function<void(QueryNegate)> on_negate) const {
  auto found_any = false;
  impl->ForEachUse<NEGATION>(
      [&on_negate, &found_any](NEGATION *negate, VIEW *) {
        on_negate(QueryNegate(negate));
        found_any = true;
      });
  assert(found_any == impl->is_used_by_negation);
  (void) found_any;
}

// Can this view receive inputs that should logically "delete" entries?
bool QueryView::CanReceiveDeletions(void) const noexcept {
  return impl->can_receive_deletions;
}

// Can this view produce outputs that should logically "delete" entries?
bool QueryView::CanProduceDeletions(void) const noexcept {
  return impl->can_produce_deletions;
}

// Returns `true` if all users of this view use all the columns of this
// view.
bool QueryView::AllUsersUseAllColumns(void) const noexcept {
  const auto is_insert = IsInsert();
  auto num_cols = Columns().size();
  if (is_insert) {
    num_cols = QueryInsert::From(*this).NumInputColumns();
  }

  std::vector<bool> used_cols(num_cols);
  for (auto succ : Successors()) {
    succ.ForEachUse([&](QueryColumn in_col, InputColumnRole,
                        std::optional<QueryColumn> out_col) {
      if (QueryView::Containing(in_col) == *this) {
        if (is_insert) {  // `out_col` is a select.
          used_cols[*(out_col->Index())] = true;
        } else {

          used_cols[*(in_col.Index())] = true;
        }
      }
    });

    for (auto i = 0u; i < num_cols; ++i) {
      auto is_used_ref = used_cols[i];
      if (!is_used_ref) {
        return false;  // Found an unused column for `succ`.
      }

      // Reset for the next successor.
      is_used_ref = false;
    }
  }

  return true;
}

// Returns `true` if this view has a single predecessor, and if all of the
// columns of that predecessor are used.
bool QueryView::AllColumnsOfSinglePredecessorAreUsed(void) const noexcept {
  const auto preds = Predecessors();
  if (preds.size() != 1) {
    return false;
  }

  const auto pred = preds[0];
  std::vector<bool> is_used(pred.Columns().size());
  this->ForEachUse(
      [&](QueryColumn in_col, InputColumnRole, std::optional<QueryColumn>) {
        if (QueryView::Containing(in_col) == pred) {
          is_used[*(in_col.Index())] = true;
        }
      });

  for (auto col_is_used : is_used) {
    if (!col_is_used) {
      return false;
    }
  }

  return true;
}

// Returns the depth of this node in the graph. This is defined as depth
// from an input (associated with a message receive) node, where the deepest
// nodes are typically responses to queries, or message publications.
unsigned QueryView::Depth(void) const noexcept {
  return impl->depth;
}

// Color value for formatting. This is influenced by the `@highlight`
// pragma, for example:
//
//      predicate(...) @highlight : body0(...), ..., bodyN(...).
//
// The color itself is mostly randomly generated, and best effort is applied
// to maintain the coloring through optimzation. In some cases, new colors
// are invented, e.g. when merging nodes when doing common sub-expression
// elimination. In other cases, the color may be lost.
unsigned QueryView::Color(void) const noexcept {
  return impl->color;
}

// Returns a useful string of internal metadata about this view.
OutputStream &QueryView::DebugString(OutputStream &os) const noexcept {
  return impl->DebugString(os);
}

// Get a hash of this view.
uint64_t QueryView::Hash(void) const noexcept {
  return impl->hash ? impl->hash : impl->Hash();
}

// What condition this view sets, if any.
std::optional<QueryCondition> QueryView::SetCondition(void) const noexcept {
  if (impl->sets_condition) {
    return QueryCondition(impl->sets_condition.get());
  } else {
    return std::nullopt;
  }
}

// Positive conditions, i.e. zero-argument predicates, that must be true
// for tuples to be accepted into this node.
UsedNodeRange<QueryCondition>
QueryView::PositiveConditions(void) const noexcept {
  return {impl->positive_conditions.begin(), impl->positive_conditions.end()};
}

UsedNodeRange<QueryCondition>
QueryView::NegativeConditions(void) const noexcept {
  return {impl->negative_conditions.begin(), impl->negative_conditions.end()};
}

// Successor and predecessor views of this view.
UsedNodeRange<QueryView> QueryView::Successors(void) const noexcept {
  return {impl->successors.begin(), impl->successors.end()};
}

UsedNodeRange<QueryView> QueryView::Predecessors(void) const noexcept {
  return {impl->predecessors.begin(), impl->predecessors.end()};
}

// Apply a callback `with_user` to each view that uses the columns of this
// view.
void QueryView::ForEachUser(std::function<void(QueryView)> with_user) const {
  for (auto succ_view : Predecessors()) {
    with_user(succ_view);
  }
}

// Apply a callback `with_col` to each input column of this view.
//
// NOTE(pag): This does not provide any guarantees on column visiting order
//            and one should assume the worst-case order.
void QueryView::ForEachUse(std::function<void(QueryColumn, InputColumnRole,
                                              std::optional<QueryColumn>)>
                               with_col) const {
  if (auto sel = impl->AsSelect(); sel) {
    QuerySelect(sel).ForEachUse(std::move(with_col));

  } else if (auto join = impl->AsJoin(); join) {
    QueryJoin(join).ForEachUse(std::move(with_col));

  } else if (auto map = impl->AsMap(); map) {
    QueryMap(map).ForEachUse(std::move(with_col));

  } else if (auto agg = impl->AsAggregate(); agg) {
    QueryAggregate(agg).ForEachUse(std::move(with_col));

  } else if (auto merge = impl->AsMerge(); merge) {
    QueryMerge(merge).ForEachUse(std::move(with_col));

  } else if (auto cmp = impl->AsCompare(); cmp) {
    QueryCompare(cmp).ForEachUse(std::move(with_col));

  } else if (auto insert = impl->AsInsert(); insert) {
    QueryInsert(insert).ForEachUse(std::move(with_col));

  } else if (auto tuple = impl->AsTuple(); tuple) {
    QueryTuple(tuple).ForEachUse(std::move(with_col));

  } else if (auto kv = impl->AsKVIndex(); kv) {
    QueryKVIndex(kv).ForEachUse(std::move(with_col));

  } else if (auto neg = impl->AsNegate(); neg) {
    QueryNegate(neg).ForEachUse(std::move(with_col));

  } else {
    assert(false);
  }
}

DefinedNodeRange<QueryColumn> QuerySelect::Columns(void) const {
  return QueryView(impl).Columns();
}

bool QueryColumn::IsSelect(void) const noexcept {
  return impl->view->AsSelect() != nullptr;
}

bool QueryColumn::IsJoin(void) const noexcept {
  return impl->view->AsJoin() != nullptr;
}

bool QueryColumn::IsMap(void) const noexcept {
  return impl->view->AsMap() != nullptr;
}

bool QueryColumn::IsAggregate(void) const noexcept {
  return impl->view->AsAggregate() != nullptr;
}

bool QueryColumn::IsMerge(void) const noexcept {
  return impl->view->AsMerge() != nullptr;
}

bool QueryColumn::IsNegate(void) const noexcept {
  return impl->view->AsNegate() != nullptr;
}

bool QueryColumn::IsConstraint(void) const noexcept {
  return impl->view->AsCompare() != nullptr;
}

bool QueryColumn::IsConstant(void) const noexcept {
  return impl->IsConstant();
}

bool QueryColumn::IsConstantRef(void) const noexcept {
  return impl->IsConstantRef();
}

bool QueryColumn::IsConstantOrConstantRef(void) const noexcept {
  return impl->IsConstantOrConstantRef();
}

// Number of uses of this column.
unsigned QueryColumn::NumUses(void) const noexcept {
  return impl->NumUses();
}

// Apply a function to each user.
void QueryColumn::ForEachUser(std::function<void(QueryView)> user_cb) const {
  impl->view->ForEachUse<VIEW>(
      [&user_cb](VIEW *view, VIEW *) { user_cb(QueryView(view)); });

  impl->ForEachUse<VIEW>(
      [&user_cb](VIEW *view, COL *) { user_cb(QueryView(view)); });
}

std::optional<QueryColumn> QueryColumn::AsConstantColumn(void) const noexcept {
  if (auto const_col = impl->TryResolveToConstant()) {
    return QueryColumn(const_col);
  } else {
    return std::nullopt;
  }
}

std::optional<ParsedVariable> QueryColumn::Variable(void) const noexcept {
  return impl->var;
}

// Unique identifier for columns.
unsigned QueryColumn::Id(void) const noexcept {
  return impl->id;
}

// Comma separated list of all column ids in forwards column taint set
std::string QueryColumn::ForwardsTaintIds(void) const {
  if (!impl->forwards_col_taints || impl->forwards_col_taints->Empty()) {
    return "";
  }

  std::stringstream ss;
  for (auto col : *impl->forwards_col_taints) {
    ss << col->id << ", ";
  }

  return ss.str().substr(0, ss.str().length() - 2);
}

// Comma separated list of all column ids in backwards column taint set
std::string QueryColumn::BackwardsTaintIds(void) const {
  if (!impl->backwards_col_taints || impl->backwards_col_taints->Empty()) {
    return "";
  }

  std::stringstream ss;
  for (auto col : *impl->backwards_col_taints) {
    ss << col->id << ", ";
  }

  return ss.str().substr(0, ss.str().length() - 2);
}

// Forwards column taint set
UsedNodeRange<QueryColumn> QueryColumn::ForwardsColumnTaints(void) const {
  if (!impl->forwards_col_taints || impl->forwards_col_taints->Empty()) {
    return {};
  }
  return {UsedNodeIterator<QueryColumn>(impl->forwards_col_taints->begin()),
          UsedNodeIterator<QueryColumn>(impl->forwards_col_taints->end())};
}

// Backwards column taint set
UsedNodeRange<QueryColumn> QueryColumn::BackwardsColumnTaints(void) const {
  if (!impl->backwards_col_taints || impl->backwards_col_taints->Empty()) {
    return {};
  }
  return {UsedNodeIterator<QueryColumn>(impl->backwards_col_taints->begin()),
          UsedNodeIterator<QueryColumn>(impl->backwards_col_taints->end())};
}

// Index of this column in its defining view. Returns nothing if this column
// is a constant.
std::optional<unsigned> QueryColumn::Index(void) const noexcept {
  if (!impl->IsConstant()) {
    return impl->Index();
  } else {
    return std::nullopt;
  }
}

const TypeLoc &QueryColumn::Type(void) const noexcept {
  return impl->type;
}

bool QueryColumn::operator==(QueryColumn that) const noexcept {
  return impl == that.impl;
}

bool QueryColumn::operator!=(QueryColumn that) const noexcept {
  return impl != that.impl;
}

// The declaration of the
const std::optional<ParsedDeclaration> &
QueryCondition::Predicate(void) const noexcept {
  return impl->declaration;
}

// The list of views that produce nodes iff this condition is true.
UsedNodeRange<QueryView> QueryCondition::PositiveUsers(void) const {
  impl->positive_users.RemoveNull();
  impl->positive_users.Unique();
  return {impl->positive_users.begin(), impl->positive_users.end()};
}

// The list of views that produce nodes iff this condition is false.
UsedNodeRange<QueryView> QueryCondition::NegativeUsers(void) const {
  impl->negative_users.RemoveNull();
  impl->negative_users.Unique();
  return {impl->negative_users.begin(), impl->negative_users.end()};
}

// The list of views that set or unset this condition.
UsedNodeRange<QueryView> QueryCondition::Setters(void) const {
  return {impl->setters.begin(), impl->setters.end()};
}

// Depth of this node.
unsigned QueryCondition::Depth(void) const noexcept {
  if (impl->in_depth_calc) {
    assert(false);  // Suggests a condition is dependent on itself.
    return 1u;
  }
  impl->in_depth_calc = true;
  auto depth = 1u;
  for (VIEW *setter : impl->setters) {
    depth = std::max(depth, setter->Depth());
  }
  impl->in_depth_calc = false;
  return depth + 1u;
}

std::optional<ParsedLiteral> QueryConstant::Literal(void) const noexcept {
  if (impl->AsTag()) {
    return std::nullopt;
  } else {
    return impl->literal;
  }
}

// What is the type of this constant?
TypeLoc QueryConstant::Type(void) const noexcept {

  // Tags are all 16 bit integers, as we usually don't need that many of
  // them, but it's nice to be able to invent unique ones on-the-fly on
  // an as-needed basis.
  if (impl->AsTag()) {
    return TypeKind::kUnsigned16;

  } else {
    assert(impl->literal.has_value());
    return impl->literal->Type();
  }
}

// Returns `true` if this is a tag value.
bool QueryConstant::IsTag(void) const {
  return impl->AsTag() != nullptr;
}

QueryConstant::QueryConstant(const QueryTag &tag) : QueryConstant(tag.impl) {}

QueryConstant QueryConstant::From(const QueryStream &stream) {
  assert(stream.IsConstant());
  return reinterpret_cast<const QueryConstant &>(stream);
}

QueryConstant QueryConstant::From(QueryColumn col) {
  assert(col.IsConstantOrConstantRef());
  return QueryConstant(
      col.impl->AsConstant()->view->AsSelect()->stream->AsConstant());
}

QueryTag QueryTag::From(const QueryConstant &const_val) {
  assert(const_val.IsTag());
  return QueryTag(const_val.impl->AsTag());
}

// What is the type of this constant? Tags are always unsigned, 16-bit
// integers.
TypeLoc QueryTag::Type(void) const noexcept {
  return TypeKind::kUnsigned16;
}

// The value of this tag.
uint16_t QueryTag::Value(void) const {
  return impl->val;
}

QueryIO QueryIO::From(const QueryStream &stream) {
  assert(stream.IsIO());
  return reinterpret_cast<const QueryIO &>(stream);
}

const ParsedDeclaration &QueryIO::Declaration(void) const noexcept {
  return impl->declaration;
}

// The list of sends to this I/O.
UsedNodeRange<QueryView> QueryIO::Transmits(void) const {
  return {impl->transmits.begin(), impl->transmits.end()};
}

// The list of receives of this I/O.
UsedNodeRange<QueryView> QueryIO::Receives(void) const {
  return {impl->receives.begin(), impl->receives.end()};
}

QueryRelation QueryRelation::From(const QuerySelect &sel) noexcept {
  const auto rel = sel.impl->relation.get();
  assert(rel != nullptr);
  return QueryRelation(rel);
}

const ParsedDeclaration &QueryRelation::Declaration(void) const noexcept {
  return impl->declaration;
}

// The list of inserts into this relation.
UsedNodeRange<QueryView> QueryRelation::Inserts(void) const {
  return {impl->inserts.begin(), impl->inserts.end()};
}

// The list of SELECTs from this relation.
UsedNodeRange<QueryView> QueryRelation::Selects(void) const {
  return {impl->selects.begin(), impl->selects.end()};
}

QuerySelect QuerySelect::From(QueryView view) {
  assert(view.IsSelect());
  return reinterpret_cast<QuerySelect &>(view);
}

bool QuerySelect::IsRelation(void) const noexcept {
  return impl->relation;
}

bool QuerySelect::IsStream(void) const noexcept {
  return impl->stream;
}

QueryRelation QuerySelect::Relation(void) const noexcept {
  assert(impl->relation);
  return QueryRelation(impl->relation.get());
}

QueryStream QuerySelect::Stream(void) const noexcept {
  assert(impl->stream);
  return QueryStream(impl->stream.get());
}

OutputStream &QuerySelect::DebugString(OutputStream &os) const noexcept {
  return impl->DebugString(os);
}

// Apply a callback `with_col` to each input column of this view.
//
// NOTE(pag): This will only call `with_col` if there is a corresponding
//            `INSERT` on the underlying relation.
void QuerySelect::ForEachUse(std::function<void(QueryColumn, InputColumnRole,
                                                std::optional<QueryColumn>)>
                                 with_col) const {
  const auto max_i = impl->columns.Size();
  for (auto view : impl->inserts) {
    for (auto i = 0u; i < max_i; ++i) {
      const auto out_col = impl->columns[i];
      const auto in_col = view->columns[i];
      with_col(QueryColumn(in_col), InputColumnRole::kCopied,
               QueryColumn(out_col));
    }
  }
}

// Apply a callback `with_col` to each input column of this view.
void QueryJoin::ForEachUse(std::function<void(QueryColumn, InputColumnRole,
                                              std::optional<QueryColumn>)>
                               with_col) const {
#ifndef NDEBUG
  std::unordered_set<COL *> seen;
#endif

  for (COL *out_col : impl->columns) {
    auto &in_cols = impl->out_to_in.at(out_col);
    const auto role = in_cols.Size() == 1u ? InputColumnRole::kJoinNonPivot
                                           : InputColumnRole::kJoinPivot;
    for (COL *in_col : in_cols) {
#ifndef NDEBUG
      if (seen.count(in_col)) {
        impl->color = 0xff;
      }
      //assert(!seen.count(in_col));
      seen.insert(in_col);
#endif
      with_col(QueryColumn(in_col), role, QueryColumn(out_col));
    }
  }
}

QueryJoin QueryJoin::From(QueryView view) {
  assert(view.IsJoin());
  return reinterpret_cast<QueryJoin &>(view);
}

// The number of output columns. This is the number of all non-pivot incoming
// columns.
unsigned QueryJoin::NumMergedColumns(void) const noexcept {
  return impl->columns.Size() - impl->num_pivots;
}

unsigned QueryJoin::NumPivotColumns(void) const noexcept {
  return impl->num_pivots;
}

// The number of views joined together.
unsigned QueryJoin::NumJoinedViews(void) const noexcept {
  return static_cast<unsigned>(impl->joined_views.Size());
}

// Return a list of the joined views.
UsedNodeRange<QueryView> QueryJoin::JoinedViews(void) const noexcept {
  return {impl->joined_views.begin(), impl->joined_views.end()};
}

// Returns the input columns corresponding with the Nth output pivot column.
// All of the input columns must have matching values in order for the
// JOIN to succeed.
UsedNodeRange<QueryColumn>
QueryJoin::NthInputPivotSet(unsigned n) const noexcept {
  assert(n < impl->num_pivots);
  auto use_list = impl->out_to_in.find(impl->columns[n]);
  assert(use_list != impl->out_to_in.end());
  assert(1u < use_list->second.Size());
  return {use_list->second.begin(), use_list->second.end()};
}

// Returns the set of input columns proposed by the Nth incoming view.
QueryColumn QueryJoin::NthInputMergedColumn(unsigned n) const noexcept {
  assert((n + impl->num_pivots) < impl->columns.Size());
  auto use_list = impl->out_to_in.find(impl->columns[n + impl->num_pivots]);
  assert(use_list != impl->out_to_in.end());
  assert(1u == use_list->second.Size());
  return QueryColumn(use_list->second[0]);
}

// Returns the `nth` output column.
QueryColumn QueryJoin::NthOutputMergedColumn(unsigned n) const noexcept {
  assert((n + impl->num_pivots) < impl->columns.Size());
  return QueryColumn(impl->columns[n + impl->num_pivots]);
}

// Returns the `nth` pivot output column.
QueryColumn QueryJoin::NthOutputPivotColumn(unsigned n) const noexcept {
  assert(n < impl->columns.Size());
  assert(n < impl->num_pivots);
  return QueryColumn(impl->columns[n]);
}

OutputStream &QueryJoin::DebugString(OutputStream &os) const noexcept {
  return impl->DebugString(os);
}

QueryMap QueryMap::From(QueryView view) {
  assert(view.IsMap());
  return reinterpret_cast<QueryMap &>(view);
}

unsigned QueryMap::NumInputColumns(void) const noexcept {
  return static_cast<unsigned>(impl->input_columns.Size());
}

QueryColumn QueryMap::NthInputColumn(unsigned n) const noexcept {
  assert(n < impl->input_columns.Size());
  return QueryColumn(impl->input_columns[n]);
}

UsedNodeRange<QueryColumn> QueryMap::InputColumns(void) const noexcept {
  return {impl->input_columns.begin(), impl->input_columns.end()};
}

// The range of input group columns.
UsedNodeRange<QueryColumn> QueryMap::InputCopiedColumns(void) const {
  return {impl->attached_columns.begin(), impl->attached_columns.end()};
}

DefinedNodeRange<QueryColumn> QueryMap::Columns(void) const {
  return {DefinedNodeIterator<QueryColumn>(impl->columns.begin()),
          DefinedNodeIterator<QueryColumn>(impl->columns.end())};
}

// The resulting mapped columns.
DefinedNodeRange<QueryColumn> QueryMap::MappedColumns(void) const {
  const auto num_copied_cols = impl->attached_columns.Size();
  return {
      DefinedNodeIterator<QueryColumn>(impl->columns.begin()),
      DefinedNodeIterator<QueryColumn>(impl->columns.end() - num_copied_cols)};
}

// The resulting grouped columns.
DefinedNodeRange<QueryColumn> QueryMap::CopiedColumns(void) const {
  const auto num_cols = impl->columns.Size();
  const auto num_copied_cols = impl->attached_columns.Size();
  const auto first_copied_col_index = num_cols - num_copied_cols;
  return {DefinedNodeIterator<QueryColumn>(impl->columns.begin() +
                                           first_copied_col_index),
          DefinedNodeIterator<QueryColumn>(impl->columns.end())};
}

// Returns the number of output columns.
unsigned QueryMap::Arity(void) const noexcept {
  return static_cast<unsigned>(impl->columns.Size());
}

// Returns whether or not this map behaves more like a filter, i.e. if the
// number of `free`-attributed parameters in `Functor()` is zero.
bool QueryMap::IsFilterLike(void) const noexcept {
  return 0 == impl->num_free_params;
}

// Returns the `nth` output column.
QueryColumn QueryMap::NthColumn(unsigned n) const noexcept {
  assert(n < (impl->columns.Size() - impl->attached_columns.Size()));
  return QueryColumn(impl->columns[n]);
}

const ParsedFunctor &QueryMap::Functor(void) const noexcept {
  return impl->functor;
}

// Is this a positive application of the functor, or a negative application?
// The meaning of a negative application is that it produces zero outputs. It
// is invalid to negate a functor that is declared as
bool QueryMap::IsPositive(void) const noexcept {
  return impl->is_positive;
}


// Returns the number of columns used for grouping.
unsigned QueryMap::NumCopiedColumns(void) const noexcept {
  return impl->attached_columns.Size();
}

// Returns the `nth` output grouping column.
QueryColumn QueryMap::NthCopiedColumn(unsigned n) const noexcept {
  const auto num_cols = impl->columns.Size();
  const auto num_group_cols = impl->attached_columns.Size();
  assert(n < num_group_cols);
  assert(num_group_cols < num_cols);
  return QueryColumn(impl->columns[n + (num_cols - num_group_cols)]);
}

// Returns the `nth` input grouping column.
QueryColumn QueryMap::NthInputCopiedColumn(unsigned n) const noexcept {
  assert(n < impl->attached_columns.Size());
  return QueryColumn(impl->attached_columns[n]);
}

OutputStream &QueryMap::DebugString(OutputStream &os) const noexcept {
  return impl->DebugString(os);
}

// Apply a callback `with_col` to each input column of this view.
void QueryMap::ForEachUse(std::function<void(QueryColumn, InputColumnRole,
                                             std::optional<QueryColumn>)>
                              with_col) const {
  auto i = 0u;
  auto j = 0u;
  auto max_i = impl->functor.Arity();
  for (j = 0u; i < max_i; ++i) {
    if (impl->functor.NthParameter(i).Binding() == ParameterBinding::kFree) {
      continue;  // It's an output column.
    }

    const auto out_col = impl->columns[i];
    const auto in_col = impl->input_columns[j++];
    with_col(QueryColumn(in_col), InputColumnRole::kFunctorInput,
             QueryColumn(out_col));
  }
  assert(j == impl->input_columns.Size());

  max_i = impl->columns.Size();
  for (j = 0u; i < max_i; ++i, ++j) {
    const auto out_col = impl->columns[i];
    const auto in_col = impl->attached_columns[j];
    with_col(QueryColumn(in_col), InputColumnRole::kCopied,
             QueryColumn(out_col));
  }
  assert(j == impl->attached_columns.Size());
}

QueryAggregate QueryAggregate::From(QueryView view) {
  assert(view.IsAggregate());
  return reinterpret_cast<QueryAggregate &>(view);
}

// The resulting mapped columns.
DefinedNodeRange<QueryColumn> QueryAggregate::Columns(void) const noexcept {
  return {DefinedNodeIterator<QueryColumn>(impl->columns.begin()),
          DefinedNodeIterator<QueryColumn>(impl->columns.end())};
}

// Subsequences of the above.
DefinedNodeRange<QueryColumn>
QueryAggregate::GroupColumns(void) const noexcept {
  if (impl->group_by_columns.Empty()) {
    return {DefinedNodeIterator<QueryColumn>(impl->columns.end()),
            DefinedNodeIterator<QueryColumn>(impl->columns.end())};
  } else {
    auto begin = impl->columns.begin();
    return {DefinedNodeIterator<QueryColumn>(begin),
            DefinedNodeIterator<QueryColumn>(begin +
                                             impl->group_by_columns.Size())};
  }
}

DefinedNodeRange<QueryColumn>
QueryAggregate::ConfigurationColumns(void) const noexcept {
  if (impl->config_columns.Empty()) {
    return {DefinedNodeIterator<QueryColumn>(impl->columns.end()),
            DefinedNodeIterator<QueryColumn>(impl->columns.end())};
  } else {
    auto begin = impl->columns.begin() + impl->group_by_columns.Size();
    return {
        DefinedNodeIterator<QueryColumn>(begin),
        DefinedNodeIterator<QueryColumn>(begin + impl->config_columns.Size())};
  }
}

// NOTE(pag): There should always be at least one summary column.
DefinedNodeRange<QueryColumn>
QueryAggregate::SummaryColumns(void) const noexcept {
  auto begin = impl->columns.begin() +
               (impl->group_by_columns.Size() + impl->config_columns.Size());
  return {DefinedNodeIterator<QueryColumn>(begin),
          DefinedNodeIterator<QueryColumn>(impl->columns.end())};
}

UsedNodeRange<QueryColumn>
QueryAggregate::InputGroupColumns(void) const noexcept {
  return {UsedNodeIterator<QueryColumn>(impl->group_by_columns.begin()),
          UsedNodeIterator<QueryColumn>(impl->group_by_columns.end())};
}

UsedNodeRange<QueryColumn>
QueryAggregate::InputConfigurationColumns(void) const noexcept {
  return {UsedNodeIterator<QueryColumn>(impl->config_columns.begin()),
          UsedNodeIterator<QueryColumn>(impl->config_columns.end())};
}

UsedNodeRange<QueryColumn>
QueryAggregate::InputAggregatedColumns(void) const noexcept {
  return {UsedNodeIterator<QueryColumn>(impl->aggregated_columns.begin()),
          UsedNodeIterator<QueryColumn>(impl->aggregated_columns.end())};
}

// Returns the number of output columns.
unsigned QueryAggregate::Arity(void) const noexcept {
  return static_cast<unsigned>(impl->columns.Size());
}

// Returns the number of columns used for grouping.
unsigned QueryAggregate::NumGroupColumns(void) const noexcept {
  return impl->group_by_columns.Size();
}

// Returns the number of columns used for configuration.
unsigned QueryAggregate::NumConfigurationColumns(void) const noexcept {
  return impl->config_columns.Size();
}

// Returns the number of columns being summarized.
unsigned QueryAggregate::NumAggregateColumns(void) const noexcept {
  return impl->aggregated_columns.Size();
}

// Returns the number of sumary columns being produced.
unsigned QueryAggregate::NumSummaryColumns(void) const noexcept {
  const auto num_group_cols = impl->group_by_columns.Size();
  const auto num_bound_cols = impl->config_columns.Size();
  return impl->columns.Size() - (num_group_cols + num_bound_cols);
}

// Returns the `nth` output grouping column.
QueryColumn QueryAggregate::NthGroupColumn(unsigned n) const noexcept {
  assert(n < impl->group_by_columns.Size());
  assert(n < impl->columns.Size());
  return QueryColumn(impl->columns[n]);
}

// Returns the `nth` output config column.
QueryColumn QueryAggregate::NthConfigurationColumn(unsigned n) const noexcept {
  const auto num_group_cols = impl->group_by_columns.Size();
  assert((n + num_group_cols) < impl->config_columns.Size());
  return QueryColumn(impl->columns[n + num_group_cols]);
}

// Returns the `nth` output summarized column.
QueryColumn QueryAggregate::NthSummaryColumn(unsigned n) const noexcept {
  const auto num_group_cols = impl->group_by_columns.Size();
  const auto num_bound_cols = impl->config_columns.Size();
  const auto disp = num_group_cols + num_bound_cols;
  assert((n + disp) < impl->columns.Size());
  return QueryColumn(impl->columns[n + disp]);
}

// Returns the `nth` input grouping column.
QueryColumn QueryAggregate::NthInputGroupColumn(unsigned n) const noexcept {
  assert(n < impl->group_by_columns.Size());
  return QueryColumn(impl->group_by_columns[n]);
}

// Returns the `nth` input config column.
QueryColumn
QueryAggregate::NthInputConfigurationColumn(unsigned n) const noexcept {
  assert(n < impl->config_columns.Size());
  return QueryColumn(impl->config_columns[n]);
}

// Returns the `nth` input summarized column.
QueryColumn QueryAggregate::NthInputAggregateColumn(unsigned n) const noexcept {
  assert(n < impl->aggregated_columns.Size());
  return QueryColumn(impl->aggregated_columns[n]);
}

// The functor doing the aggregating.
const ParsedFunctor &QueryAggregate::Functor(void) const noexcept {
  return impl->functor;
}

OutputStream &QueryAggregate::DebugString(OutputStream &os) const noexcept {
  return impl->DebugString(os);
}

// Apply a callback `with_col` to each input column of this view.
void QueryAggregate::ForEachUse(std::function<void(QueryColumn, InputColumnRole,
                                                   std::optional<QueryColumn>)>
                                    with_col) const {
  auto i = 0u;
  for (auto in_col : impl->group_by_columns) {
    const auto out_col = impl->columns[i++];
    with_col(QueryColumn(in_col), InputColumnRole::kAggregateGroup,
             QueryColumn(out_col));
  }

  for (auto in_col : impl->config_columns) {
    const auto out_col = impl->columns[i++];
    with_col(QueryColumn(in_col), InputColumnRole::kAggregateConfig,
             QueryColumn(out_col));
  }

  for (auto in_col : impl->aggregated_columns) {
    with_col(QueryColumn(in_col), InputColumnRole::kAggregatedColumn,
             std::nullopt);
  }
}

QueryMerge QueryMerge::From(QueryView view) {
  assert(view.IsMerge());
  return reinterpret_cast<QueryMerge &>(view);
}

// The resulting mapped columns.
DefinedNodeRange<QueryColumn> QueryMerge::Columns(void) const {
  return {DefinedNodeIterator<QueryColumn>(impl->columns.begin()),
          DefinedNodeIterator<QueryColumn>(impl->columns.end())};
}

// Returns the number of output columns.
unsigned QueryMerge::Arity(void) const noexcept {
  return static_cast<unsigned>(impl->columns.Size());
}

// Returns the `nth` output column.
QueryColumn QueryMerge::NthColumn(unsigned n) const noexcept {
  assert(n < impl->columns.Size());
  return QueryColumn(impl->columns[n]);
}

// Number of views that are merged together at this point.
unsigned QueryMerge::NumMergedViews(void) const noexcept {
  return static_cast<unsigned>(impl->merged_views.Size());
}

// Nth view that is merged together at this point.
QueryView QueryMerge::NthMergedView(unsigned n) const noexcept {
  assert(n < impl->merged_views.Size());
  return QueryView(impl->merged_views[n]);
}

// Range of views unioned together by this MERGE.
UsedNodeRange<QueryView> QueryMerge::MergedViews(void) const {
  return {UsedNodeIterator<QueryView>(impl->merged_views.begin()),
          UsedNodeIterator<QueryView>(impl->merged_views.end())};
}

OutputStream &QueryMerge::DebugString(OutputStream &os) const noexcept {
  return impl->DebugString(os);
}

// Apply a callback `with_col` to each input column of this view.
void QueryMerge::ForEachUse(std::function<void(QueryColumn, InputColumnRole,
                                               std::optional<QueryColumn>)>
                                with_col) const {
  const auto num_cols = impl->columns.Size();
  for (auto view : impl->merged_views) {
    for (auto i = 0u, max_i = num_cols; i < max_i; ++i) {
      const auto out_col = impl->columns[i];
      COL *in_col = nullptr;
      if (auto insert = view->AsInsert(); insert) {
        in_col = insert->input_columns[i];
      } else {
        in_col = view->columns[i];
      }

      with_col(QueryColumn(in_col), InputColumnRole::kMergedColumn,
               QueryColumn(out_col));
    }
  }
}

bool QueryMerge::CanReceiveDeletions(void) const {
  return impl->can_receive_deletions;
}

bool QueryMerge::CanProduceDeletions(void) const {
  return impl->can_produce_deletions;
}

// A unique integer that labels all UNIONs in the same induction.
std::optional<unsigned> QueryView::InductionGroupId(void) const {
  if (auto info = impl->induction_info.get()) {
    return info->merge_set_id;
  } else {
    return std::nullopt;
  }
}

// A total ordering on the "depth" of inductions. Two inductions at the same
// depth can be processed in parallel.
std::optional<unsigned> QueryView::InductionDepth(void) const {
  if (auto info = impl->induction_info.get()) {
    return info->merge_depth;
  } else {
    return std::nullopt;
  }
}

UsedNodeRange<QueryView> QueryView::InductiveSuccessors(void) const {
  if (auto info = impl->induction_info.get()) {
    return {UsedNodeIterator<QueryView>(info->inductive_successors.begin()),
            UsedNodeIterator<QueryView>(info->inductive_successors.end())};
  } else {
    return {};
  }
}

UsedNodeRange<QueryView> QueryView::InductivePredecessors(void) const {
  if (auto info = impl->induction_info.get()) {
    return {UsedNodeIterator<QueryView>(info->inductive_predecessors.begin()),
            UsedNodeIterator<QueryView>(info->inductive_predecessors.end())};
  } else {
    return {};
  }
}

UsedNodeRange<QueryView> QueryView::NonInductiveSuccessors(void) const {
  if (auto info = impl->induction_info.get()) {
    return {UsedNodeIterator<QueryView>(info->noninductive_successors.begin()),
            UsedNodeIterator<QueryView>(info->noninductive_successors.end())};
  } else {
    return {};
  }
}

UsedNodeRange<QueryView> QueryView::NonInductivePredecessors(void) const {
  if (auto info = impl->induction_info.get()) {
    return {
        UsedNodeIterator<QueryView>(info->noninductive_predecessors.begin()),
        UsedNodeIterator<QueryView>(info->noninductive_predecessors.end())};
  } else {
    return {};
  }
}

// All UNIONs, including this one, in the same inductive set.
UsedNodeRange<QueryView> QueryView::InductiveSet(void) const {
  if (auto info = impl->induction_info.get()) {
    return {UsedNodeIterator<QueryView>(info->cyclic_views->begin()),
            UsedNodeIterator<QueryView>(info->cyclic_views->end())};
  } else {
    return {};
  }
}

// Can this view reach back to itself without first going through another
// inductive union?
bool QueryView::IsOwnIndirectInductiveSuccessor(void) const {
  if (auto info = impl->induction_info.get()) {
    return info->can_reach_self_not_through_another_induction;
  } else {
    return false;
  }
}

ComparisonOperator QueryCompare::Operator(void) const {
  return impl->op;
}

QueryCompare QueryCompare::From(QueryView view) {
  assert(view.IsCompare());
  return reinterpret_cast<QueryCompare &>(view);
}

QueryColumn QueryCompare::LHS(void) const {
  return QueryColumn(impl->columns[0]);
}

QueryColumn QueryCompare::RHS(void) const {
  if (ComparisonOperator::kEqual == impl->op) {
    return QueryColumn(impl->columns[0]);
  } else {
    return QueryColumn(impl->columns[1]);
  }
}

QueryColumn QueryCompare::InputLHS(void) const {
  return QueryColumn(impl->input_columns[0]);
}

QueryColumn QueryCompare::InputRHS(void) const {
  return QueryColumn(impl->input_columns[1]);
}

unsigned QueryCompare::NumCopiedColumns(void) const noexcept {
  return impl->attached_columns.Size();
}

QueryColumn QueryCompare::NthCopiedColumn(unsigned n) const noexcept {
  auto offset = ComparisonOperator::kEqual == impl->op ? (n + 1) : (n + 2);
  assert(offset < impl->columns.Size());
  return QueryColumn(impl->columns[offset]);
}

DefinedNodeRange<QueryColumn> QueryCompare::CopiedColumns(void) const {
  auto begin = impl->columns.begin();
  const auto end = impl->columns.end();
  ++begin;
  if (ComparisonOperator::kEqual == impl->op) {
    return {begin, end};
  } else {
    ++begin;
    return {begin, end};
  }
}

UsedNodeRange<QueryColumn> QueryCompare::InputCopiedColumns(void) const {
  return {impl->attached_columns.begin(), impl->attached_columns.end()};
}

OutputStream &QueryCompare::DebugString(OutputStream &os) const noexcept {
  return impl->DebugString(os);
}

// Apply a callback `with_col` to each input column of this view.
void QueryCompare::ForEachUse(std::function<void(QueryColumn, InputColumnRole,
                                                 std::optional<QueryColumn>)>
                                  with_col) const {
  auto i = 0u;
  if (ComparisonOperator::kEqual == impl->op) {
    with_col(QueryColumn(impl->input_columns[0]), InputColumnRole::kCompareLHS,
             QueryColumn(impl->columns[0]));
    with_col(QueryColumn(impl->input_columns[1]), InputColumnRole::kCompareRHS,
             QueryColumn(impl->columns[0]));
    i = 1u;
  } else {
    with_col(QueryColumn(impl->input_columns[0]), InputColumnRole::kCompareLHS,
             QueryColumn(impl->columns[0]));
    with_col(QueryColumn(impl->input_columns[1]), InputColumnRole::kCompareRHS,
             QueryColumn(impl->columns[1]));
    i = 2u;
  }

  for (auto j = 0u, max_i = impl->columns.Size(); i < max_i; ++i, ++j) {
    with_col(QueryColumn(impl->attached_columns[j]), InputColumnRole::kCopied,
             QueryColumn(impl->columns[i]));
  }
}

QueryNegate QueryNegate::From(QueryView view) {
  assert(view.IsNegate());
  return reinterpret_cast<QueryNegate &>(view);
}

// The deleted columns.
DefinedNodeRange<QueryColumn> QueryNegate::Columns(void) const {
  return {DefinedNodeIterator<QueryColumn>(impl->columns.begin()),
          DefinedNodeIterator<QueryColumn>(impl->columns.end())};
}

QueryColumn QueryNegate::NthColumn(unsigned n) const noexcept {
  assert(n < impl->columns.Size());
  return QueryColumn(impl->columns[n]);
}

// If a negation has a never hint, then we know that if some data goes through
// the output, then it will always go through, and nothing will get set in
// the negated view that will result in the prior data being retracted.
bool QueryNegate::HasNeverHint(void) const noexcept {
  return impl->is_never;
}

// The resulting copied columns.
DefinedNodeRange<QueryColumn> QueryNegate::CopiedColumns(void) const {
  return {DefinedNodeIterator<QueryColumn>(impl->columns.begin() +
                                           impl->input_columns.Size()),
          DefinedNodeIterator<QueryColumn>(impl->columns.end())};
}

// The resulting negated columns.
DefinedNodeRange<QueryColumn> QueryNegate::NegatedColumns(void) const {
  return {DefinedNodeIterator<QueryColumn>(impl->columns.begin()),
          DefinedNodeIterator<QueryColumn>(impl->columns.begin() +
                                           impl->input_columns.Size())};
}

// Returns the `nth` input copied column.
QueryColumn QueryNegate::NthInputCopiedColumn(unsigned n) const noexcept {
  assert(n < impl->attached_columns.Size());
  return QueryColumn(impl->attached_columns[n]);
}

unsigned QueryNegate::NumCopiedColumns(void) const noexcept {
  return static_cast<unsigned>(impl->attached_columns.Size());
}

unsigned QueryNegate::NumInputColumns(void) const noexcept {
  return static_cast<unsigned>(impl->input_columns.Size());
}

QueryColumn QueryNegate::NthInputColumn(unsigned n) const noexcept {
  assert(n < impl->input_columns.Size());
  return QueryColumn(impl->input_columns[n]);
}

UsedNodeRange<QueryColumn> QueryNegate::InputColumns(void) const noexcept {
  return {UsedNodeIterator<QueryColumn>(impl->input_columns.begin()),
          UsedNodeIterator<QueryColumn>(impl->input_columns.end())};
}

UsedNodeRange<QueryColumn>
QueryNegate::InputCopiedColumns(void) const noexcept {
  return {UsedNodeIterator<QueryColumn>(impl->attached_columns.begin()),
          UsedNodeIterator<QueryColumn>(impl->attached_columns.end())};
}

// Incoming view that represents a flow of data between the relation and
// the negation.
QueryView QueryNegate::NegatedView(void) const noexcept {
  return QueryView(impl->negated_view.get());
}

OutputStream &QueryNegate::DebugString(OutputStream &os) const noexcept {
  return impl->DebugString(os);
}

// Apply a callback `with_col` to each input column of this view.
void QueryNegate::ForEachUse(std::function<void(QueryColumn, InputColumnRole,
                                                std::optional<QueryColumn>)>
                                 with_col) const {
  auto i = 0u;
  for (auto in_col : impl->input_columns) {
    with_col(QueryColumn(in_col), InputColumnRole::kCopied,
             QueryColumn(impl->columns[i++]));
  }
  for (auto in_col : impl->attached_columns) {
    with_col(QueryColumn(in_col), InputColumnRole::kCopied,
             QueryColumn(impl->columns[i++]));
  }

  i = 0u;
  for (auto our_in_col : impl->input_columns) {
    (void) our_in_col;
    auto negated_in_col = impl->negated_view->columns[i];
    with_col(QueryColumn(negated_in_col), InputColumnRole::kNegated,
             QueryColumn(impl->columns[i]));
    ++i;
  }
}

QueryInsert QueryInsert::From(QueryView view) {
  assert(view.IsInsert());
  return reinterpret_cast<QueryInsert &>(view);
}

ParsedDeclaration QueryInsert::Declaration(void) const noexcept {
  return impl->declaration;
}

bool QueryInsert::IsRelation(void) const noexcept {
  return impl->relation.get() != nullptr;
}

bool QueryInsert::IsStream(void) const noexcept {
  return impl->stream.get() != nullptr;
}

QueryRelation QueryInsert::Relation(void) const noexcept {
  const auto rel = impl->relation.get();
  assert(rel != nullptr);
  return QueryRelation(rel);
}

QueryStream QueryInsert::Stream(void) const noexcept {
  const auto stream = impl->stream.get();
  assert(stream != nullptr);
  return QueryStream(stream);
}

unsigned QueryInsert::NumInputColumns(void) const noexcept {
  return static_cast<unsigned>(impl->input_columns.Size());
}

QueryColumn QueryInsert::NthInputColumn(unsigned n) const noexcept {
  assert(n < impl->input_columns.Size());
  return QueryColumn(impl->input_columns[n]);
}

UsedNodeRange<QueryColumn> QueryInsert::InputColumns(void) const noexcept {
  return {UsedNodeIterator<QueryColumn>(impl->input_columns.begin()),
          UsedNodeIterator<QueryColumn>(impl->input_columns.end())};
}

OutputStream &QueryInsert::DebugString(OutputStream &os) const noexcept {
  return impl->DebugString(os);
}

// Apply a callback `with_col` to each input column of this view.
void QueryInsert::ForEachUse(std::function<void(QueryColumn, InputColumnRole,
                                                std::optional<QueryColumn>)>
                                 with_col) const {
  if (IsStream()) {
    for (auto in_col : impl->input_columns) {
      with_col(QueryColumn(in_col), InputColumnRole::kPublished, std::nullopt);
    }

  } else if (IsRelation()) {
    if (impl->successors.Empty()) {
      for (auto in_col : impl->input_columns) {
        with_col(QueryColumn(in_col), InputColumnRole::kMaterialized,
                 std::nullopt);
      }
    } else {
      for (auto user : impl->successors) {
        if (user->AsMerge() || user->AsSelect()) {
          for (auto out_col : user->columns) {
            auto in_col = impl->input_columns[out_col->index];
            with_col(QueryColumn(in_col), InputColumnRole::kMaterialized,
                     QueryColumn(out_col));
          }
        } else {
          assert(false);
        }
      }
    }

  } else {
    assert(false);
  }
}

QueryTuple QueryTuple::From(QueryView view) {
  assert(view.IsTuple());
  return reinterpret_cast<QueryTuple &>(view);
}

// The resulting mapped columns.
DefinedNodeRange<QueryColumn> QueryTuple::Columns(void) const {
  return {DefinedNodeIterator<QueryColumn>(impl->columns.begin()),
          DefinedNodeIterator<QueryColumn>(impl->columns.end())};
}

unsigned QueryTuple::Arity(void) const noexcept {
  return impl->columns.Size();
}

QueryColumn QueryTuple::NthColumn(unsigned n) const noexcept {
  assert(n < impl->columns.Size());
  return QueryColumn(impl->columns[n]);
}

unsigned QueryTuple::NumInputColumns(void) const noexcept {
  return impl->input_columns.Size();
}

QueryColumn QueryTuple::NthInputColumn(unsigned n) const noexcept {
  assert(n < impl->input_columns.Size());
  return QueryColumn(impl->input_columns[n]);
}

UsedNodeRange<QueryColumn> QueryTuple::InputColumns(void) const noexcept {
  return {UsedNodeIterator<QueryColumn>(impl->input_columns.begin()),
          UsedNodeIterator<QueryColumn>(impl->input_columns.end())};
}

OutputStream &QueryTuple::DebugString(OutputStream &os) const noexcept {
  return impl->DebugString(os);
}

// Apply a callback `with_col` to each input column of this view.
void QueryTuple::ForEachUse(std::function<void(QueryColumn, InputColumnRole,
                                               std::optional<QueryColumn>)>
                                with_col) const {
  for (auto i = 0u, max_i = impl->columns.Size(); i < max_i; ++i) {
    const auto out_col = impl->columns[i];
    const auto in_col = impl->input_columns[i];
    with_col(QueryColumn(in_col), InputColumnRole::kCopied,
             QueryColumn(out_col));
  }
}

QueryKVIndex QueryKVIndex::From(QueryView view) {
  assert(view.IsKVIndex());
  return reinterpret_cast<QueryKVIndex &>(view);
}

unsigned QueryKVIndex::Arity(void) const noexcept {
  return impl->columns.Size();
}

DefinedNodeRange<QueryColumn> QueryKVIndex::Columns(void) const {
  return {DefinedNodeIterator<QueryColumn>(impl->columns.begin()),
          DefinedNodeIterator<QueryColumn>(impl->columns.end())};
}

DefinedNodeRange<QueryColumn> QueryKVIndex::KeyColumns(void) const {
  const auto it = impl->columns.begin();
  return {DefinedNodeIterator<QueryColumn>(it),
          DefinedNodeIterator<QueryColumn>(it + impl->input_columns.Size())};
}

DefinedNodeRange<QueryColumn> QueryKVIndex::ValueColumns(void) const {
  const auto it = impl->columns.begin();
  return {DefinedNodeIterator<QueryColumn>(it + impl->input_columns.Size()),
          DefinedNodeIterator<QueryColumn>(impl->columns.end())};
}

unsigned QueryKVIndex::NumKeyColumns(void) const noexcept {
  return impl->input_columns.Size();
}

unsigned QueryKVIndex::NumValueColumns(void) const noexcept {
  return impl->attached_columns.Size();
}

QueryColumn QueryKVIndex::NthKeyColumn(unsigned n) const noexcept {
  assert(n < impl->input_columns.Size());
  return QueryColumn(impl->columns[n]);
}

QueryColumn QueryKVIndex::NthInputKeyColumn(unsigned n) const noexcept {
  assert(n < impl->input_columns.Size());
  return QueryColumn(impl->input_columns[n]);
}

UsedNodeRange<QueryColumn> QueryKVIndex::InputKeyColumns(void) const noexcept {
  return {UsedNodeIterator<QueryColumn>(impl->input_columns.begin()),
          UsedNodeIterator<QueryColumn>(impl->input_columns.end())};
}

QueryColumn QueryKVIndex::NthValueColumn(unsigned n) const noexcept {
  assert(n < impl->attached_columns.Size());
  return QueryColumn(impl->columns[impl->input_columns.Size() + n]);
}

QueryColumn QueryKVIndex::NthInputValueColumn(unsigned n) const noexcept {
  assert(n < impl->attached_columns.Size());
  return QueryColumn(impl->attached_columns[n]);
}

UsedNodeRange<QueryColumn>
QueryKVIndex::InputValueColumns(void) const noexcept {
  return {UsedNodeIterator<QueryColumn>(impl->attached_columns.begin()),
          UsedNodeIterator<QueryColumn>(impl->attached_columns.end())};
}

const ParsedFunctor &
QueryKVIndex::NthValueMergeFunctor(unsigned n) const noexcept {
  assert(n < impl->attached_columns.Size());
  return impl->merge_functors[n];
}

OutputStream &QueryKVIndex::DebugString(OutputStream &os) const noexcept {
  return impl->DebugString(os);
}

// Apply a callback `with_col` to each input column of this view.
void QueryKVIndex::ForEachUse(std::function<void(QueryColumn, InputColumnRole,
                                                 std::optional<QueryColumn>)>
                                  with_col) const {
  auto i = 0u;
  for (auto in_col : impl->input_columns) {
    const auto out_col = impl->columns[i++];
    with_col(QueryColumn(in_col), InputColumnRole::kIndexKey,
             QueryColumn(out_col));
  }

  for (auto in_col : impl->attached_columns) {
    with_col(QueryColumn(in_col), InputColumnRole::kIndexValue, std::nullopt);
  }
}

DefinedNodeRange<QueryCondition> Query::Conditions(void) const {
  return {DefinedNodeIterator<QueryCondition>(impl->conditions.begin()),
          DefinedNodeIterator<QueryCondition>(impl->conditions.end())};
}

DefinedNodeRange<QueryJoin> Query::Joins(void) const {
  return {DefinedNodeIterator<QueryJoin>(impl->joins.begin()),
          DefinedNodeIterator<QueryJoin>(impl->joins.end())};
}

DefinedNodeRange<QuerySelect> Query::Selects(void) const {
  return {DefinedNodeIterator<QuerySelect>(impl->selects.begin()),
          DefinedNodeIterator<QuerySelect>(impl->selects.end())};
}

DefinedNodeRange<QueryTuple> Query::Tuples(void) const {
  return {DefinedNodeIterator<QueryTuple>(impl->tuples.begin()),
          DefinedNodeIterator<QueryTuple>(impl->tuples.end())};
}

DefinedNodeRange<QueryKVIndex> Query::KVIndices(void) const {
  return {DefinedNodeIterator<QueryKVIndex>(impl->kv_indices.begin()),
          DefinedNodeIterator<QueryKVIndex>(impl->kv_indices.end())};
}

DefinedNodeRange<QueryRelation> Query::Relations(void) const {
  return {DefinedNodeIterator<QueryRelation>(impl->relations.begin()),
          DefinedNodeIterator<QueryRelation>(impl->relations.end())};
}

DefinedNodeRange<QueryConstant> Query::Constants(void) const {
  return {DefinedNodeIterator<QueryConstant>(impl->constants.begin()),
          DefinedNodeIterator<QueryConstant>(impl->constants.end())};
}

DefinedNodeRange<QueryTag> Query::Tags(void) const {
  return {DefinedNodeIterator<QueryTag>(impl->tags.begin()),
          DefinedNodeIterator<QueryTag>(impl->tags.end())};
}

DefinedNodeRange<QueryIO> Query::IOs(void) const {
  return {DefinedNodeIterator<QueryIO>(impl->ios.begin()),
          DefinedNodeIterator<QueryIO>(impl->ios.end())};
}

DefinedNodeRange<QueryInsert> Query::Inserts(void) const {
  return {DefinedNodeIterator<QueryInsert>(impl->inserts.begin()),
          DefinedNodeIterator<QueryInsert>(impl->inserts.end())};
}

DefinedNodeRange<QueryNegate> Query::Negations(void) const {
  return {DefinedNodeIterator<QueryNegate>(impl->negations.begin()),
          DefinedNodeIterator<QueryNegate>(impl->negations.end())};
}

DefinedNodeRange<QueryMap> Query::Maps(void) const {
  return {DefinedNodeIterator<QueryMap>(impl->maps.begin()),
          DefinedNodeIterator<QueryMap>(impl->maps.end())};
}

DefinedNodeRange<QueryAggregate> Query::Aggregates(void) const {
  return {DefinedNodeIterator<QueryAggregate>(impl->aggregates.begin()),
          DefinedNodeIterator<QueryAggregate>(impl->aggregates.end())};
}

DefinedNodeRange<QueryMerge> Query::Merges(void) const {
  return {DefinedNodeIterator<QueryMerge>(impl->merges.begin()),
          DefinedNodeIterator<QueryMerge>(impl->merges.end())};
}

DefinedNodeRange<QueryCompare> Query::Compares(void) const {
  return {DefinedNodeIterator<QueryCompare>(impl->compares.begin()),
          DefinedNodeIterator<QueryCompare>(impl->compares.end())};
}

Query::~Query(void) {}

::hyde::ParsedModule Query::ParsedModule(void) const noexcept {
  return impl->module;
}

}  // namespace hyde

#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic pop
#endif
