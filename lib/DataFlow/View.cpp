// Copyright 2020, Trail of Bits. All rights reserved.

#include "Query.h"

#include <iomanip>
#include <sstream>

#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Parse/Format.h>

#include "Optimize.h"

namespace hyde {

Node<QueryView>::~Node(void) {}

const char *Node<QuerySelect>::KindName(void) const noexcept {
  if (relation) {
    return "PUSH";
  } else if (auto s = stream.get(); s) {
    if (s->AsConstant()) {
      return "CONST";
    } else if (s->AsIO()) {
      return "RECV";
    } else {
      assert(false);
      return "STREAM";
    }
  } else {
    return "SELECT";
  }
}

const char *Node<QueryTuple>::KindName(void) const noexcept {
  return "TUPLE";
}

const char *Node<QueryKVIndex>::KindName(void) const noexcept {
  return "KVINDEX";
}

const char *Node<QueryJoin>::KindName(void) const noexcept {
  if (num_pivots) {
    return "JOIN";
  } else {
    return "PRODUCT";
  }
}

const char *Node<QueryMap>::KindName(void) const noexcept {
  if (num_free_params) {
    if (functor.IsPure()) {
      return "MAP";
    } else {
      return "FUNCTION";
    }
  } else {
    if (functor.IsPure()) {
      return "PREDICATE";
    } else {
      return "FILTER";
    }
  }
}

const char *Node<QueryAggregate>::KindName(void) const noexcept {
  return "AGGREGATE";
}

const char *Node<QueryMerge>::KindName(void) const noexcept {
  if (is_equivalence_class) {
    return "EQ-CLASS";
  } else {
    return "UNION";
  }
}

const char *Node<QueryConstraint>::KindName(void) const noexcept {
  return "COMPARE";
}

const char *Node<QueryInsert>::KindName(void) const noexcept {
  if (declaration.Kind() == DeclarationKind::kQuery) {
    return "MATERIALIZE";

  } else if (declaration.Kind() == DeclarationKind::kMessage) {
    return "SEND";

  } else if (is_insert) {
    if (declaration.Arity()) {
      return "INSERT";
    } else {
      return "INCREMENT";
    }

  } else {
    if (declaration.Arity()) {
      return "DELETE";
    } else {
      return "DECREMENT";
    }
  }
}

Node<QuerySelect> *Node<QueryView>::AsSelect(void) noexcept {
  return nullptr;
}

Node<QueryTuple> *Node<QueryView>::AsTuple(void) noexcept {
  return nullptr;
}

Node<QueryKVIndex> *Node<QueryView>::AsKVIndex(void) noexcept {
  return nullptr;
}

Node<QueryJoin> *Node<QueryView>::AsJoin(void) noexcept {
  return nullptr;
}

Node<QueryMap> *Node<QueryView>::AsMap(void) noexcept {
  return nullptr;
}

Node<QueryAggregate> *Node<QueryView>::AsAggregate(void) noexcept {
  return nullptr;
}

Node<QueryMerge> *Node<QueryView>::AsMerge(void) noexcept {
  return nullptr;
}

Node<QueryConstraint> *Node<QueryView>::AsConstraint(void) noexcept {
  return nullptr;
}

Node<QueryInsert> *Node<QueryView>::AsInsert(void) noexcept {
  return nullptr;
}

// Useful for communicating low-level debug info back to the formatter.
OutputStream &Node<QueryView>::DebugString(OutputStream &ss) noexcept {
  if (!group_ids.empty()) {
    auto sep = "group-ids(";
    for (auto group_id : group_ids) {
      ss << sep << group_id;
      sep = ", ";
    }
    ss << ") ";
  }

  ss << "depth=" << Depth();
  if (is_dead) {
    ss << " dead=1";
  }
  ss << " hash=" << std::hex << this->Hash() << std::dec;
  switch (valid) {
    case kValid:
      break;
    case kInvalidBeforeCanonicalize:
      ss << "<B><FONT COLOR=\"RED\">BEFORE";
      if (invalid_var) {
        ss << ' ' << invalid_var->SpellingRange();
      }
      ss << "</FONT></B>";
      break;
    case kInvalidAfterCanonicalize:
      ss << "<B><FONT COLOR=\"RED\">AFTER";
      if (invalid_var) {
        ss << ' ' << invalid_var->SpellingRange();
      }
      ss << "</FONT></B>";
      break;
  }
  if (!producer.empty()) {
    ss << ' ' << producer;
  }
  return ss;
}

// Return a number that can be used to help sort this node. The idea here
// is that we often want to try to merge together two different instances
// of the same underlying node when we can.
uint64_t Node<QueryView>::Sort(void) noexcept {
  return Hash();
}

// Returns `true` if this view is being used. This is defined in terms of
// whether or not the view is used in a merge, or whether or not any of its
// columns are used.
bool Node<QueryView>::IsUsed(void) const noexcept {
  if (sets_condition) {
    assert(!is_dead);
    return true;

  } else if (this->Def<Node<QueryView>>::IsUsed()) {
    if (is_dead) {
#ifndef NDEBUG
      ForEachUse<VIEW>([](VIEW *user_view, VIEW *) {
        assert(user_view->is_dead || user_view->AsMerge());
      });
#endif
      return false;

    } else {
      return true;
    }
  }

  for (auto col : columns) {
    if (col->Def<Node<QueryColumn>>::IsUsed()) {
      if (is_dead) {
#ifndef NDEBUG
        col->ForEachUse<VIEW>(
            [](VIEW *user_view, COL *) { assert(user_view->is_dead); });
#endif
        continue;
      }
      return true;
    }
  }

  return false;
}

// Invoked any time time that any of the columns used by this view are
// modified.
void Node<QueryView>::Update(uint64_t next_timestamp) {
  if (is_canonical) {
    is_canonical = false;
    for (auto col : columns) {
      col->ForEachUse<VIEW>(
          [=](VIEW *user, COL *) { user->is_canonical = false; });
    }
  }

  //  if (timestamp >= next_timestamp) {
  //    return;
  //  }
  //
  //  timestamp = next_timestamp;
  //  hash = 0;
  //  depth = 0;
  //

  //
  //  // Update merges.
  //  ForEachUse<VIEW>([=] (VIEW *user, VIEW *) {
  //    user->Update(next_timestamp);
  //  });
}

// Sort the `positive_conditions` and `negative_conditions`.
void Node<QueryView>::OrderConditions(void) {
  auto cb = [](COND *cond) { return cond->setters.Empty(); };

  positive_conditions.RemoveIf(cb);
  negative_conditions.RemoveIf(cb);

  positive_conditions.Unique();
  negative_conditions.Unique();
}

// Canonicalizes an input/output column pair. Returns `true` in the first
// element if non-local changes are made, and `true` in the second element
// if the column pair can be removed.
std::pair<bool, bool> Node<QueryView>::CanonicalizeColumnPair(
    COL *in_col, COL *out_col, const OptimizationContext &opt) noexcept {
  const auto out_col_is_constref = out_col->IsConstantRef();
  const auto out_col_is_directly_used = out_col->IsUsedIgnoreMerges();
  auto non_local_changes = false;

  if (in_col->IsConstant()) {
    if (opt.can_replace_outputs_with_constants &&
        !IntroducesControlDependency()) {
      if (!out_col_is_constref || out_col_is_directly_used) {
        non_local_changes = true;
      }
      out_col->CopyConstant(in_col);
      out_col->ReplaceAllUsesWith(in_col);

    } else {
      if (!out_col_is_constref) {
        non_local_changes = true;
        out_col->CopyConstant(in_col);
      }
    }
  } else if (in_col->IsConstantRef()) {
    if (!out_col_is_constref) {
      non_local_changes = true;
      out_col->CopyConstant(in_col);

    } else if (opt.can_replace_inputs_with_constants) {
      non_local_changes = true;
    }
  }

  auto can_remove = false;
  if (opt.can_remove_unused_columns && !out_col->IsUsed()) {
    can_remove = true;
  }

  return {non_local_changes, can_remove};
}

// Check to see if the attached columns are ordered and unique. If they're
// not unique then we can deduplicate them.
std::pair<bool, bool> Node<QueryView>::CanonicalizeAttachedColumns(
    unsigned first_output, const OptimizationContext &opt) noexcept {
  auto i = first_output;
  auto attached_are_canonical = true;
  auto non_local_changes = false;

  for (auto max_i = columns.Size(), j = 0u; i < max_i; ++i, ++j) {
    auto in_col = attached_columns[j];
    auto out_col = columns[i];

    auto [changed, can_remove] = CanonicalizeColumnPair(in_col, out_col, opt);

    non_local_changes = non_local_changes || changed;
    attached_are_canonical = attached_are_canonical && !can_remove;
  }

  // Look for equivalent attached columns and try to eliminate them.
  const auto num_attached_cols = attached_columns.Size();
  for (auto j = 0u; j < num_attached_cols; ++j) {
    for (auto k = j + 1u; k < num_attached_cols; ++k) {
      if (attached_columns[j] == attached_columns[k]) {
        auto k_out_col = columns[k + first_output];
        auto j_out_col = columns[j + first_output];
        if (k_out_col->IsUsedIgnoreMerges()) {
          k_out_col->ReplaceAllUsesWith(j_out_col);
          non_local_changes = true;

          if (opt.can_remove_unused_columns && !k_out_col->IsUsed()) {
            attached_are_canonical = false;
          }
        }
      }
    }
  }

  return {attached_are_canonical, non_local_changes};
}

// Put this view into a canonical form.
bool Node<QueryView>::Canonicalize(QueryImpl *, const OptimizationContext &) {
  is_canonical = true;
  return false;
}

unsigned Node<QueryView>::Depth(void) noexcept {
  if (depth) {
    return depth;
  }

  auto estimate = EstimateDepth(input_columns, 1u);
  estimate = EstimateDepth(attached_columns, depth);
  estimate = EstimateDepth(positive_conditions, depth);
  estimate = EstimateDepth(negative_conditions, depth);
  depth = estimate + 1u;

  auto real = GetDepth(input_columns, 1u);
  real = GetDepth(attached_columns, real);
  real = GetDepth(positive_conditions, real);
  real = GetDepth(negative_conditions, real);
  depth = real + 1u;

  return depth;
}

unsigned Node<QueryView>::EstimateDepth(const UseList<COL> &cols,
                                        unsigned depth) {
  for (const auto input_col : cols) {
    const auto input_depth = input_col->view->depth;
    if (input_depth >= depth) {
      depth = input_depth;
    }
  }
  return depth;
}

unsigned Node<QueryView>::EstimateDepth(const UseList<COND> &conds,
                                        unsigned depth) {
  auto cond_depth = 2u;
  auto has_conds = false;
  for (const auto cond : conds) {
    has_conds = true;
    for (auto view : cond->setters) {
      cond_depth = std::max(cond_depth, view->depth);
    }
  }
  return has_conds ? std::max(depth, cond_depth + 1u) : depth;
}

unsigned Node<QueryView>::GetDepth(const UseList<COL> &cols, unsigned depth) {
  for (const auto input_col : cols) {
    const auto input_depth = input_col->view->Depth();
    if (input_depth >= depth) {
      depth = input_depth;
    }
  }
  return depth;
}

unsigned Node<QueryView>::GetDepth(const UseList<COND> &conds, unsigned depth) {
  for (const auto cond : conds) {
    auto cond_depth = QueryCondition(cond).Depth();
    if (cond_depth >= depth) {
      depth = cond_depth;
    }
  }
  return depth;
}

// Return the number of uses of this view.
unsigned Node<QueryView>::NumUses(void) const noexcept {
  std::vector<VIEW *> users;
  users.reserve(columns.Size() * 2);

  for (auto col : columns) {
    col->ForEachUser([&users](VIEW *user) { users.push_back(user); });
  }

  std::sort(users.begin(), users.end());
  auto it = std::unique(users.begin(), users.end());
  users.erase(it, users.end());
  return static_cast<unsigned>(users.size());
}

static const std::hash<const char *> kCStrHasher;

// Initializer for an updated hash value.
uint64_t Node<QueryView>::HashInit(void) const noexcept {
  uint64_t init_hash = kCStrHasher(this->KindName());
  init_hash <<= 1u;
  init_hash |= can_receive_deletions;
  init_hash <<= 1u;
  init_hash |= can_produce_deletions;

  init_hash ^= RotateRight64(init_hash, 33) * (columns.Size() + 7u);

  for (auto positive_cond : this->positive_conditions) {
    init_hash ^= RotateRight64(init_hash, 33) *
                 reinterpret_cast<uintptr_t>(positive_cond);
  }

  for (auto negative_cond : this->negative_conditions) {
    init_hash ^= RotateRight64(init_hash, 33) *
                 ~reinterpret_cast<uintptr_t>(negative_cond);
  }

  return init_hash;
}

// Upward facing hash. The idea here is that we sometimes have multiple nodes
// that have the same hash, and thus are candidates for CSE, and we want to
// decide: among those candidates, which nodes /should/ be merged. We decide
// this by looking up the dataflow graph (to some limited depth) and creating
// a rough hash of how this node gets used.
uint64_t Node<QueryView>::UpHash(unsigned depth) const noexcept {
  auto up_hash = HashInit();

  if (!depth) {
    return up_hash;
  }

  unsigned i = 0u;
  for (auto col : columns) {
    col->ForEachUse<VIEW>([=, &up_hash](VIEW *user, COL *) {
      up_hash ^=
          RotateRight64(up_hash, (i + 7u) % 64u) * user->UpHash(depth - 1u);
    });
    ++i;
  }

  return up_hash;
}

// Prepare to delete this node. This tries to drop all dependencies and
// unlink this node from the dataflow graph. It returns `true` if successful
// and `false` if it has already been performed.
bool Node<QueryView>::PrepareToDelete(void) {
  if (is_dead) {
    return false;
  }

  hash = 0;
  is_canonical = true;
  is_dead = true;

  input_columns.Clear();
  attached_columns.Clear();

  const auto is_this_view = [this](VIEW *v) { return v == this; };

  for (auto cond : positive_conditions) {
    cond->positive_users.RemoveIf(is_this_view);
  }
  for (auto cond : negative_conditions) {
    cond->negative_users.RemoveIf(is_this_view);
  }

  positive_conditions.Clear();
  negative_conditions.Clear();

  if (auto cond = sets_condition.get(); cond) {
    WeakUseRef<COND>().Swap(sets_condition);
    cond->setters.RemoveIf(is_this_view);
  }

  if (auto merge = AsMerge(); merge) {
    merge->merged_views.Clear();

  } else if (auto agg = AsAggregate(); agg) {
    agg->group_by_columns.Clear();
    agg->config_columns.Clear();
    agg->aggregated_columns.Clear();

  } else if (auto join = AsJoin(); join) {
    join->out_to_in.clear();
    join->joined_views.Clear();
    join->num_pivots = 0u;

  } else if (auto select = AsSelect(); select) {
    if (auto stream = select->stream.get(); stream) {
      WeakUseRef<STREAM>().Swap(select->stream);
      if (auto io = stream->AsIO(); io) {
        io->receives.RemoveIf(is_this_view);
      } else {
        assert(stream->AsConstant());
      }

    } else if (auto rel = select->relation.get(); rel) {
      WeakUseRef<REL>().Swap(select->relation);
      rel->selects.RemoveIf(is_this_view);
    }

  } else if (auto insert = AsInsert(); insert) {
    if (auto stream = insert->stream.get(); stream) {
      WeakUseRef<STREAM>().Swap(insert->stream);
      if (auto io = stream->AsIO(); io) {
        io->sends.RemoveIf(is_this_view);
      } else {
        assert(false);
      }

    } else if (auto rel = insert->relation.get(); rel) {
      WeakUseRef<REL>().Swap(insert->relation);
      rel->inserts.RemoveIf(is_this_view);
    }
  }

  return true;
}

// Copy all positive and negative conditions from `this` into `that`.
void Node<QueryView>::CopyTestedConditionsTo(Node<QueryView> *that) {
  for (auto cond : positive_conditions) {
    that->positive_conditions.AddUse(cond);
    cond->positive_users.AddUse(that);
  }

  for (auto cond : negative_conditions) {
    that->negative_conditions.AddUse(cond);
    cond->negative_users.AddUse(that);
  }

  that->OrderConditions();
}

// If `sets_condition` is non-null, then transfer the setter to `that`.
void Node<QueryView>::TransferSetConditionTo(Node<QueryView> *that) {
  if (auto cond = sets_condition.get(); cond) {
    auto is_this_or_that = [=](VIEW *v) { return v == this || v == that; };

    // Simple case: transfer "settership" of the condition.
    if (!that->sets_condition) {
      that->sets_condition.Swap(sets_condition);
      cond->setters.RemoveIf(is_this_or_that);
      cond->setters.AddUse(that);

    } else if (auto that_cond = that->sets_condition.get()) {
      // Next simplest case: `that` is also setting the same condition, so we'll
      // just unlink `this` from `cond`s setter list.
      if (that_cond == cond) {
        cond->setters.RemoveIf(is_this_or_that);
        cond->setters.AddUse(that);

      } else {
        // If `cond` is only set by `this`, and `that` already has its own
        // condition, then we'll let that other condition take over this
        // condition.
        //
        // TODO(pag): It's totally possible for `that_cond` to be stronger /
        // more
        //            constrained than `cond`, which could be problematic.
        if (cond->setters.Size() == 1u) {
          for (auto view : cond->positive_users) {
            that_cond->positive_users.AddUse(view);
          }
          for (auto view : cond->negative_users) {
            that_cond->negative_users.AddUse(view);
          }

          cond->ReplaceAllUsesWith(that_cond);
          cond->setters.Clear();
          cond->positive_users.Clear();
          cond->negative_users.Clear();

          // Our condition is set by multiple different VIEWs. We'll constrain
          // `that_cond` by adding `cond` as a tested condition to `that`.
        } else {
          cond->setters.RemoveIf([=](VIEW *v) { return v == this; });
          cond->positive_users.AddUse(that);
          that->positive_conditions.AddUse(cond);
        }
        that->is_canonical = false;
      }

      WeakUseRef<COND>().Swap(sets_condition);
    }
  }
}

// Replace all uses of `this` with `that`. The semantic here is that `this`
// remains valid and used.
void Node<QueryView>::SubstituteAllUsesWith(Node<QueryView> *that) {
  unsigned i = 0u;
  for (auto col : columns) {
    col->ReplaceAllUsesWith(that->columns[i++]);
  }
  this->Def<Node<QueryView>>::ReplaceAllUsesWith(that);

  // Maintain the set of group IDs, to prevent over-merging.
  that->group_ids.insert(that->group_ids.end(), group_ids.begin(),
                         group_ids.end());
  std::sort(that->group_ids.begin(), that->group_ids.end());

  if (can_receive_deletions) {
    that->can_receive_deletions = true;
  }

  if (can_produce_deletions) {
    that->can_produce_deletions = true;
  }

  TransferSetConditionTo(that);
}

// Replace all uses of `this` with `that`. The semantic here is that `this`
// is completely subsumed/replaced by `that`.
void Node<QueryView>::ReplaceAllUsesWith(Node<QueryView> *that) {
  SubstituteAllUsesWith(that);
  CopyTestedConditionsTo(that);
  PrepareToDelete();
}

// Does this view introduce a control dependency? If a node introduces a
// control dependency then it generally needs to be kept around.
bool Node<QueryView>::IntroducesControlDependency(void) const noexcept {
  return !positive_conditions.Empty() || !negative_conditions.Empty() ||
         nullptr != const_cast<VIEW *>(this)->AsConstraint();
}

// Returns `true` if all output columns are used.
bool Node<QueryView>::AllColumnsAreUsed(void) const noexcept {
  if (this->Def<Node<QueryView>>::IsUsed()) {
    return true;  // Used in a MERGE.
  }

  for (auto col : columns) {
    if (!col->IsUsedIgnoreMerges()) {
      return false;
    }
  }

  return true;
}

// Returns `true` if we had to "guard" this view with a tuple so that we
// can put it into canonical form.
//
// If this view is used by a merge then we're not allowed to re-order the
// columns. Instead, what we can do is create a tuple that will maintain
// the ordering, and the canonicalize the join order below that tuple.
Node<QueryTuple> *Node<QueryView>::GuardWithTuple(QueryImpl *query,
                                                  bool force) {
  if (!force && !this->Def<Node<QueryView>>::IsUsed()) {
    return nullptr;
  }

  const auto tuple = query->tuples.Create();

  for (auto col : columns) {
    auto out_col = tuple->columns.Create(col->var, tuple, col->id);
    out_col->CopyConstant(col);
  }

  // Make any merges use the tuple.
  SubstituteAllUsesWith(tuple);

  for (auto col : columns) {
    tuple->input_columns.AddUse(col);
  }

#ifndef NDEBUG
  std::stringstream ss;
  ss << "GUARD(" << KindName();
  if (!producer.empty()) {
    ss << ": " << producer;
  }
  ss << ')';
  tuple->producer = ss.str();
#endif

  return tuple;
}

// Proxy this node with a comparison of `lhs_col` and `rhs_col`, where
// `lhs_col` and `rhs_col` either belong to `this->columns` or are constants.
Node<QueryTuple> *Node<QueryView>::ProxyWithComparison(QueryImpl *query,
                                                       ComparisonOperator op,
                                                       COL *lhs_col,
                                                       COL *rhs_col) {
  // Prefer to have the constant first.
  if ((ComparisonOperator::kEqual == op ||
       ComparisonOperator::kNotEqual == op) &&
      rhs_col->IsConstant() && !lhs_col->IsConstant()) {
    return ProxyWithComparison(query, op, rhs_col, lhs_col);
  }

  // Now fill in the tuple to use a CMP that takes its input from `this`.

  in_to_out.clear();

  auto col_index = 0u;
  CMP *cmp = query->constraints.Create(op);

  cmp->input_columns.AddUse(lhs_col);
  auto lhs_out_col =
      cmp->columns.Create(lhs_col->var, cmp, lhs_col->id, col_index++);

  lhs_out_col->CopyConstant(lhs_col);
  in_to_out.emplace(lhs_col, lhs_out_col);

  cmp->input_columns.AddUse(rhs_col);
  if (ComparisonOperator::kEqual == op) {
    lhs_out_col->CopyConstant(rhs_col);
    in_to_out.emplace(rhs_col, lhs_out_col);

  } else {
    auto rhs_out_col =
        cmp->columns.Create(rhs_col->var, cmp, rhs_col->id, col_index++);
    rhs_out_col->CopyConstant(rhs_col);
    in_to_out.emplace(rhs_col, rhs_out_col);
  }

  assert(cmp->input_columns.Size() == 2);

  // Add in the other columns.
  for (auto col : columns) {
    if (col != lhs_col && col != rhs_col) {
      cmp->attached_columns.AddUse(col);
      const auto attached_col =
          cmp->columns.Create(col->var, cmp, col->id, col_index++);
      attached_col->CopyConstant(col);
      in_to_out.emplace(col, attached_col);
    }
  }

  // Create a tuple that re-orders the output of the CMP to preserve it.
  TUPLE *tuple = query->tuples.Create();

  col_index = 0u;
  for (auto orig_col : columns) {
    const auto in_col = in_to_out[orig_col];
    auto out_col =
        tuple->columns.Create(orig_col->var, tuple, orig_col->id, col_index++);
    tuple->input_columns.AddUse(in_col);
    out_col->CopyConstant(in_col);
  }

#ifndef NDEBUG
  std::stringstream ss;
  ss << "PROXY-CMP(" << KindName();
  if (!producer.empty()) {
    ss << ": " << producer;
  }
  ss << ')';
  cmp->producer = ss.str();
#endif

  return tuple;
}

// Utility for comparing use lists.
bool Node<QueryView>::ColumnsEq(EqualitySet &eq, const UseList<COL> &c1s,
                                const UseList<COL> &c2s) {
  const auto num_cols = c1s.Size();
  if (num_cols != c2s.Size()) {
    return false;
  }
  for (auto i = 0u; i < num_cols; ++i) {
    auto a = c1s[i];
    auto b = c2s[i];
    if (a->type.Kind() != b->type.Kind() || !a->view->Equals(eq, b->view)) {
      return false;
    }
  }
  return true;
}

// Figure out what the incoming view to `cols1` is.
VIEW *Node<QueryView>::GetIncomingView(const UseList<COL> &cols1) {
  for (auto col : cols1) {
    if (!col->IsConstant()) {
      return col->view;
    }
  }
  return nullptr;
}

VIEW *Node<QueryView>::GetIncomingView(const UseList<COL> &cols1,
                                       const UseList<COL> &cols2) {
  for (auto col : cols1) {
    if (!col->IsConstant()) {
      return col->view;
    }
  }
  for (auto col : cols2) {
    if (!col->IsConstant()) {
      return col->view;
    }
  }
  return nullptr;
}

// Returns a pointer to the only user of this node, or nullptr if there are
// zero users, or more than one users.
Node<QueryView> *Node<QueryView>::OnlyUser(void) const noexcept {
  VIEW *only_user = nullptr;
  bool fail = false;
  for (auto col : columns) {
    col->ForEachUser([&](VIEW *user) {
      if (!only_user) {
        only_user = user;
      } else if (only_user != user) {
        fail = true;
      }
    });
    if (fail) {
      return nullptr;
    }
  }
  this->ForEachUse<VIEW>([&](VIEW *user, VIEW *) {
    if (!only_user) {
      only_user = user;
    } else if (only_user != user) {
      fail = true;
    }
  });

  return fail ? nullptr : only_user;
}

// Create or inherit a condition created on `view`.
COND *Node<QueryView>::CreateOrInheritConditionOnView(QueryImpl *query,
                                                      Node<QueryView> *view,
                                                      UseList<COL> cols) {
  COND *condition = nullptr;

  // Inherit the condition proven by `incoming_view`.
  if (auto incoming_cond = view->sets_condition.get();
      incoming_cond && incoming_cond->setters.Size() == 1u) {
    condition = incoming_cond;

    // Invent a new condition for `incoming_view`.
  } else {
    condition = query->conditions.Create();
    //    if (view->IsUsed()) {
    //      view = query->tuples.Create();
    //      view->force_is_canonical = true;
    //      view->input_columns.Swap(cols);
    //    }

    assert(!view->sets_condition);
    WeakUseRef<COND>(view, condition).Swap(view->sets_condition);

    condition->setters.AddUse(view);
  }

  return condition;
}

// Check that all non-constant views in `cols1` match.
bool Node<QueryView>::CheckIncomingViewsMatch(const UseList<COL> &cols1) const {
#ifndef NDEBUG
  VIEW *prev_view = nullptr;
  for (auto col : cols1) {
    if (!col->IsConstant()) {
      if (prev_view) {
        if (prev_view != col->view) {
          invalid_var = col->var;
          return false;
        }
      } else {
        prev_view = col->view;
      }
    }
  }
#else
  (void)cols1;
#endif
  return true;
}

// Check that all non-constant views in `cols1` and `cols2` match.
//
// NOTE(pag): This isn't a pairwise matching; instead it checks that all
//            columns in both of the lists independently reference the same
//            view.
bool Node<QueryView>::CheckIncomingViewsMatch(const UseList<COL> &cols1,
                                              const UseList<COL> &cols2) const {
#ifndef NDEBUG
  VIEW *prev_view = nullptr;

  auto do_cols = [this, &prev_view](const auto &cols) -> bool {
    for (auto col : cols) {
      if (!col->IsConstant()) {
        if (prev_view) {
          if (prev_view != col->view) {
            this->invalid_var = col->var;
            return false;
          }
        } else {
          prev_view = col->view;
        }
      }
    }
    return true;
  };
  return do_cols(cols1) && do_cols(cols2);
#else
  (void)cols1;
  (void)cols2;
  return true;
#endif
}

// Check if the `group_ids` of two views have any overlaps.
//
// Two selects in the same logical clause are not allowed to be merged,
// except in rare cases like constant streams. For example, consider the
// following:
//
//    node_pairs(A, B) : node(A), node(B).
//
// `node_pairs` is the cross-product of `node`. The two selects associated
// with each invocation of `node` are structurally the same, but cannot
// be merged because otherwise we would not get the cross product.
//
// NOTE(pag): The `group_ids` are sorted.
bool Node<QueryView>::InsertSetsOverlap(Node<QueryView> *a,
                                        Node<QueryView> *b) {
  //  if (a->check_group_ids != b->check_group_ids) {
  //    return true;
  //  }
  //
  //  if (!a->check_group_ids) {
  //    return false;
  //  }

  for (auto i = 0u, j = 0u;
       i < a->group_ids.size() && j < b->group_ids.size();) {
    if (a->group_ids[i] == b->group_ids[j]) {
      return true;

    } else if (a->group_ids[i] < b->group_ids[j]) {
      ++i;

    } else {
      ++j;
    }
  }

  return false;
}

}  // namespace hyde
