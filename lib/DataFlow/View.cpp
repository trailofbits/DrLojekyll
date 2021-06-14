// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Parse/Format.h>

#include <iomanip>
#include <sstream>

#include "EquivalenceSet.h"
#include "Optimize.h"
#include "Query.h"

namespace hyde {

Node<QueryView>::~Node(void) {}

Node<QueryView>::Node(void)
    : Def<Node<QueryView>>(this),
      User(this),
      columns(this),
      input_columns(this),
      attached_columns(this),
      positive_conditions(this),
      negative_conditions(this),
      predecessors(this),
      successors(this) {
  assert(reinterpret_cast<uintptr_t>(static_cast<User *>(this)) ==
         reinterpret_cast<uintptr_t>(this));
}

const char *Node<QuerySelect>::KindName(void) const noexcept {
  if (relation) {
    return "PUSH";
  } else if (auto s = stream.get(); s) {
    if (s->AsConstant()) {
      return "CONST";
    } else if (s->AsIO()) {
      return "RECEIVE";
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
  return "UNION";
}

const char *Node<QueryCompare>::KindName(void) const noexcept {
  return "COMPARE";
}

const char *Node<QueryNegate>::KindName(void) const noexcept {
  if (is_never) {
    return "AND-NEVER";
  } else {
    return "AND-NOT";
  }
}

const char *Node<QueryInsert>::KindName(void) const noexcept {
  if (declaration.Kind() == DeclarationKind::kQuery) {
    return "MATERIALIZE";

  } else if (declaration.Kind() == DeclarationKind::kMessage) {
    return "TRANSMIT";

  } else {
    if (declaration.Arity()) {
      return "INSERT";
    } else {
      return "INCREMENT";
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

Node<QueryNegate> *Node<QueryView>::AsNegate(void) noexcept {
  return nullptr;
}

Node<QueryCompare> *Node<QueryView>::AsCompare(void) noexcept {
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
  if (is_locked) {
    ss << " locked=1";
  }
  ss << " hash=" << std::hex << this->Hash() << std::dec;
  switch (valid) {
    case kValid: break;
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
#ifndef NDEBUG
  if (!producer.empty()) {
    ss << ' ' << producer;
  }
#endif
  return ss;
}

// Return a number that can be used to help sort this node. The idea here
// is that we often want to try to merge together two different instances
// of the same underlying node when we can.
uint64_t Node<QueryView>::Sort(void) noexcept {
  return Hash();
}

// Is this view directly being used? This does not check columns, but does
// check conditions.
bool Node<QueryView>::IsUsedDirectly(void) const noexcept {

  // If this view sets a condition, and there is at least one user of the
  // condition, then assume we're used.
  //
  // NOTE(pag): We could feasibly do a recursive check against those users.
  if (sets_condition && 0u < (sets_condition->positive_users.Size() +
                              sets_condition->negative_users.Size())) {
    return true;
  }

  if (this->Def<Node<QueryView>>::IsUsed()) {
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

  return false;
}

// Returns `true` if this view is being used. This is defined in terms of
// whether or not the view is used in a merge, or whether or not any of its
// columns are used.
bool Node<QueryView>::IsUsed(void) const noexcept {
  if (IsUsedDirectly()) {
    return true;
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

// Record the mapping between `in_col` and `out_col` into `this->in_to_out`,
// do constant propagation, and possibly to replacements. Sets
// `is_canonical = false;` if anything is changed or should be changed.
Node<QueryView>::Discoveries
Node<QueryView>::CanonicalizeColumn(const OptimizationContext &, COL *in_col,
                                    COL *out_col, bool is_attached,
                                    Node<QueryView>::Discoveries has) {
  auto [it, added] = in_to_out.emplace(in_col, out_col);

  const auto in_col_is_constant = in_col->IsConstantOrConstantRef();
  auto out_col_is_constant_ref = out_col->IsConstantRef();
  if (in_col_is_constant && !out_col_is_constant_ref) {

    // Mark it as a constant.
    is_canonical = false;
    has.non_local_changes = true;
    out_col->CopyConstantFrom(in_col);
    out_col_is_constant_ref = true;
  }

  const auto is_directly_used = out_col->IsUsedIgnoreMerges();
  if (is_directly_used) {
    has.directly_used_column = true;
  }

  if (!out_col->IsUsed()) {
    has.unused_column = true;

    if (is_attached) {
      is_canonical = false;
    }
    if (out_col_is_constant_ref) {
      has.guardable_constant_output = true;
    }
  }

  if (!added) {
    if (is_directly_used) {
      out_col->ReplaceAllUsesWith(it->second);
      has.non_local_changes = true;
      is_canonical = false;
    }

    has.duplicated_input_column = true;
  }

  return has;
}

// Canonicalizes an input/output column pair. Returns `true` in the first
// element if non-local changes are made, and `true` in the second element
// if the column pair can be removed.
std::pair<bool, bool> Node<QueryView>::CanonicalizeColumnPair(
    COL *in_col, COL *out_col, const OptimizationContext &opt) noexcept {

  const auto out_col_is_constref = out_col->IsConstantRef();

  //  const auto out_col_is_directly_used = out_col->IsUsedIgnoreMerges();
  auto non_local_changes = false;

  if (in_col->IsConstant()) {
    if (!out_col_is_constref) {
      non_local_changes = true;
      out_col->CopyConstantFrom(in_col);
    }
  } else if (in_col->IsConstantRef()) {
    if (!out_col_is_constref) {
      non_local_changes = true;
      out_col->CopyConstantFrom(in_col);

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

// Put this view into a canonical form.
bool Node<QueryView>::Canonicalize(QueryImpl *, const OptimizationContext &,
                                   const ErrorLog &) {
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

// Converts this node to be unconditional, it doesn't affect set conditions.
void Node<QueryView>::DropTestedConditions(void) {
  const auto is_this_view = [this](VIEW *v) { return v == this; };

#ifndef NDEBUG
  std::vector<COND *> conds_seen;
  for (auto cond : positive_conditions) {
    conds_seen.push_back(cond);
  }
  for (auto cond : negative_conditions) {
    conds_seen.push_back(cond);
  }
  for (auto cond : conds_seen) {
    assert(cond->UsersAreConsistent());
  }
#endif

  for (auto cond : positive_conditions) {
    cond->positive_users.RemoveIf(is_this_view);
  }
  for (auto cond : negative_conditions) {
    cond->negative_users.RemoveIf(is_this_view);
  }

  positive_conditions.Clear();
  negative_conditions.Clear();

#ifndef NDEBUG
  for (auto cond : conds_seen) {
    assert(cond->UsersAreConsistent());
  }
#endif
}

// Converts this node to not set any conditions.
void Node<QueryView>::DropSetConditions(void) {
  auto cond = sets_condition.get();
  if (!cond) {
    return;
  }

  const auto is_this_view = [this](VIEW *v) { return v == this; };
  sets_condition.Clear();
  cond->setters.RemoveIf(is_this_view);

  if (!cond->setters.Empty()) {
    return;
  }

  const auto is_cond = [cond](COND *c) { return c == cond; };
  for (auto tester : cond->positive_users) {
    tester->positive_conditions.RemoveIf(is_cond);
  }
  for (auto tester : cond->negative_users) {
    tester->negative_conditions.RemoveIf(is_cond);
  }

  cond->positive_users.Clear();
  cond->negative_users.Clear();
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

  DropTestedConditions();
  DropSetConditions();

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
      select->stream.Clear();
      if (auto io = stream->AsIO(); io) {
        io->receives.RemoveIf(is_this_view);
      } else {
        assert(stream->AsConstant());
      }

    } else if (auto rel = select->relation.get(); rel) {
      select->relation.Clear();
      rel->selects.RemoveIf(is_this_view);
    }

  } else if (auto insert = AsInsert(); insert) {
    if (auto stream = insert->stream.get(); stream) {
      insert->stream.Clear();
      if (auto io = stream->AsIO(); io) {
        io->transmits.RemoveIf(is_this_view);
      } else {
        assert(false);
      }

    } else if (auto rel = insert->relation.get(); rel) {
      insert->relation.Clear();
      rel->inserts.RemoveIf(is_this_view);
    }

  } else if (auto negate = AsNegate(); negate) {
    negate->negated_view.Clear();
  }

  return true;
}

// Copy all positive and negative conditions from `this` into `that`.
void Node<QueryView>::CopyTestedConditionsTo(Node<QueryView> *that) {
  assert(this != that);

#ifndef NDEBUG
  std::vector<COND *> conds_seen;
  for (COND *cond : positive_conditions) {
    conds_seen.push_back(cond);
  }
  for (COND *cond : negative_conditions) {
    conds_seen.push_back(cond);
  }
  for (COND *cond : conds_seen) {
    assert(cond->UsersAreConsistent());
    assert(cond->SettersAreConsistent());
  }
#endif

  for (COND *cond : positive_conditions) {
    assert(cond);
    assert(cond != that->sets_condition.get());
    that->positive_conditions.AddUse(cond);
    cond->positive_users.AddUse(that);
  }

  for (COND *cond : negative_conditions) {
    assert(cond);
    assert(cond != that->sets_condition.get());
    that->negative_conditions.AddUse(cond);
    cond->negative_users.AddUse(that);
  }

  that->OrderConditions();

#ifndef NDEBUG
  for (COND *cond : conds_seen) {
    assert(cond->UsersAreConsistent());
    assert(cond->SettersAreConsistent());
  }
#endif
}

// Transfer all positive and negative conditions from `this` into `that`.
void Node<QueryView>::TransferTestedConditionsTo(Node<QueryView> *that) {
  this->CopyTestedConditionsTo(that);
  this->DropTestedConditions();
}

// If `sets_condition` is non-null, then transfer the setter to `that`.
void Node<QueryView>::TransferSetConditionTo(Node<QueryView> *that) {
  assert(this != that);

  COND *const cond = sets_condition.get();
  if (!cond) {
    return;
  }

  assert(cond->SettersAreConsistent());

  auto is_this_or_that = [=](VIEW *v) { return v == this || v == that; };

#ifndef NDEBUG

  // Don't introduce cycles?
  for (auto tested_cond : that->positive_conditions) {
    assert(tested_cond != cond);
  }
  for (auto tested_cond : that->negative_conditions) {
    assert(tested_cond != cond);
  }
#endif

  // Simple case: transfer "settership" of the condition.
  COND *const that_cond = that->sets_condition.get();
  if (!that_cond) {
    that->sets_condition.Swap(sets_condition);
    cond->setters.RemoveIf(is_this_or_that);
    cond->setters.AddUse(that);

    assert(!sets_condition);
    assert(cond->UsersAreConsistent());
    assert(cond->SettersAreConsistent());
    return;
  }

  // Next simplest case: `that` is also setting the same condition, so we'll
  // just unlink `this` from `cond`s setter list.
  if (that_cond == cond) {
    cond->setters.RemoveIf(is_this_or_that);
    cond->setters.AddUse(that);
    sets_condition.Clear();

    assert(!sets_condition);
    assert(cond->UsersAreConsistent());
    assert(cond->SettersAreConsistent());
    return;
  }

  // TODO(pag): Think more about refactoring below. Might need to force a guard
  //            tuple.
  assert(false);

  // If `cond` is only set by `this`, and `that` already has its own
  // condition, then we'll let that other condition take over this
  // condition.
  //
  // TODO(pag): It's totally possible for `that_cond` to be stronger / more
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
  sets_condition.Clear();
}

// Copy the group IDs and the receive/produce deletions from `this` to `that`.
void Node<QueryView>::CopyDifferentialAndGroupIdsTo(Node<QueryView> *that) {

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
}

// Replace all uses of `this` with `that`. The semantic here is that `this`
// remains valid and used.
void Node<QueryView>::SubstituteAllUsesWith(Node<QueryView> *that) {
  if (is_used_by_negation) {
    that->is_used_by_negation = true;
    is_used_by_negation = false;
  }

#ifndef NDEBUG
  std::vector<COND *> conds_seen;
  for (auto cond : positive_conditions) {
    conds_seen.push_back(cond);
  }
  for (auto cond : negative_conditions) {
    conds_seen.push_back(cond);
  }
  if (auto cond = sets_condition.get()) {
    conds_seen.push_back(cond);
  }
  for (auto cond : conds_seen) {
    assert(cond->UsersAreConsistent());
    assert(cond->SettersAreConsistent());
  }
#endif

  unsigned i = 0u;
  for (auto col : columns) {
    col->ReplaceAllUsesWith(that->columns[i++]);
  }

  // We don't want to replace the weak uses of `this` in any condition's
  // `positive_users` or `negative_users`, nor in any `COND::setters` lists.
  this->VIEW::ReplaceUsesWithIf<User>(
      that, [=](User *user, VIEW *) { return !dynamic_cast<COND *>(user); });

  CopyDifferentialAndGroupIdsTo(that);
  TransferSetConditionTo(that);

#ifndef NDEBUG
  for (auto cond : conds_seen) {
    assert(cond->UsersAreConsistent());
    assert(cond->SettersAreConsistent());
  }
#endif

  if (color && that->color) {
    if (color != that->color) {
      that->color ^= RotateRight32(color, (color % 13) + 1u);
    }

  } else if (!that->color) {
    that->color = color;
  }
}

// Replace all uses of `this` with `that`. The semantic here is that `this`
// is completely subsumed/replaced by `that`.
void Node<QueryView>::ReplaceAllUsesWith(Node<QueryView> *that) {
  SubstituteAllUsesWith(that);  // Will do `TransferSetConditionsTo`.
  TransferTestedConditionsTo(that);
  PrepareToDelete();
}

// Does this view introduce a control dependency? If a node introduces a
// control dependency then it generally needs to be kept around.
bool Node<QueryView>::IntroducesControlDependency(void) const noexcept {
//  if (this->AsMap()) {
//    return true;
//  }

  // TODO(pag): Think about whether or not 1:1 MAPs are control dependencies.

  std::unordered_map<VIEW *, bool> is_conditional;
  return VIEW::IsConditional(const_cast<VIEW *>(this), is_conditional);
}

// Returns `true` if all output columns are used.
bool Node<QueryView>::AllColumnsAreUsed(void) const noexcept {
  if (IsUsedDirectly()) {
    return true;  // Used in a MERGE or CONDition.
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

  if (!force && !IsUsedDirectly()) {
    return nullptr;
  }

  const auto tuple = query->tuples.Create();
  tuple->color = color;

  assert(!AsInsert());  // INSERTs don't have output columns.

  auto col_index = 0u;
  for (auto col : columns) {
    auto out_col =
        tuple->columns.Create(col->var, col->type, tuple, col->id, col_index++);
    out_col->CopyConstantFrom(col);
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

// This is like an "optimized" form of `GuardWithTuple`, that also knows
// about attached columns. It tries to propagate constants, remove duplicates
// (via `in_to_out`) and maintains a backward reference to `this` if it drops
// all references.
//
// NOTE(pag): `incoming_view` is there to tell is if `this` ever even had any
//            dependencies. This is really only relevant to TUPLEs, and so
//            it's permissible for things like MAPs, NEGATEs, etc. to pass
//            in `this` for `incoming_view`, to force a non-NULL.
//
// NOTE(pag): This assumes `in_to_out` is filled up, and operates on
//            `input_columns` and `attached_columns` to find the best version
//            of a column from `in_to_out`.
Node<QueryTuple> *
Node<QueryView>::GuardWithOptimizedTuple(QueryImpl *query,
                                         unsigned first_attached_col,
                                         Node<QueryView> *incoming_view) {

  auto tuple = query->tuples.Create();
  tuple->color = color;

#ifndef NDEBUG
  std::stringstream ss;
  ss << "OPT-GUARD(" << KindName();
  if (!producer.empty()) {
    ss << ": " << producer;
  }
  ss << ')';
  tuple->producer = ss.str();
#endif

  const auto num_cols = columns.Size();
  for (auto i = 0u; i < num_cols; ++i) {
    const auto col = columns[i];
    const auto new_col =
        tuple->columns.Create(col->var, col->type, tuple, col->id, i);
    new_col->CopyConstantFrom(col);
  }

  SubstituteAllUsesWith(tuple);
  const auto is_map = !!this->AsMap();

  for (auto i = 0u; i < num_cols; ++i) {
    const auto col = columns[i];
    if (auto const_col = col->AsConstant(); const_col) {
      tuple->input_columns.AddUse(const_col);


    // If it's not an attached column then map through
    } else if (i < first_attached_col) {

      // Maps follow non-traditional rules for input-to-output mappings for
      // columns; there isn't alignment (or even shifted alignment) between
      // input and output columns because `bound`- and `free`-attributed
      // parameters can be intermixed, and the output columns follow the same
      // order as the functor parameters.
      if (is_map) {
        tuple->input_columns.AddUse(col);

      } else {
        tuple->input_columns.AddUse(in_to_out[input_columns[i]]);
      }

    // Drop duplicates if we have them.
    } else {
      tuple->input_columns.AddUse(
          in_to_out[attached_columns[i - first_attached_col]]);
    }
  }

  // We've made our tuple; if it has dropped all references to us then make
  // it conditional on our refcount.
  //
  // We only do this if we `this` actually depended on any incoming views in
  // the first place, and if they themselves were conditional.
  if (VIEW::GetIncomingView(tuple->input_columns) != this) {

    // Figure out if it's even reasonable to create a dependency.
    if (auto this_tuple = this->AsTuple();
        this_tuple && !this_tuple->IntroducesControlDependency()) {

      auto all_const = true;
      for (auto in_col : this_tuple->input_columns) {
        if (!in_col->IsConstant()) {
          all_const = false;
          break;
        }
      }

      // It's not worth introducing a condition variable against an
      // unconditional, all constant input tuple.
      if (all_const) {
        return tuple;
      }
    }

    tuple->CreateDependencyOnView(query, this);
  }

  return tuple;
}

// Proxy this node with a comparison of `lhs_col` and `rhs_col`, where
// `lhs_col` and `rhs_col` either belong to `this->columns` or are constants.
Node<QueryTuple> *
Node<QueryView>::ProxyWithComparison(QueryImpl *query, ComparisonOperator op,
                                     COL *lhs_col, COL *rhs_col) {

  // Prefer to have the constant first.
  if ((ComparisonOperator::kEqual == op ||
       ComparisonOperator::kNotEqual == op) &&
      rhs_col->IsConstant() && !lhs_col->IsConstant()) {
    return ProxyWithComparison(query, op, rhs_col, lhs_col);
  }

  // Now fill in the tuple to use a CMP that takes its input from `this`.

  in_to_out.clear();

  auto col_index = 0u;
  CMP *cmp = query->compares.Create(op);
  cmp->color = color;

  cmp->input_columns.AddUse(lhs_col);
  auto lhs_out_col = cmp->columns.Create(lhs_col->var, lhs_col->type, cmp,
                                         lhs_col->id, col_index++);

  lhs_out_col->CopyConstantFrom(lhs_col);
  in_to_out.emplace(lhs_col, lhs_out_col);

  cmp->input_columns.AddUse(rhs_col);
  if (ComparisonOperator::kEqual == op) {
    lhs_out_col->CopyConstantFrom(rhs_col);
    in_to_out.emplace(rhs_col, lhs_out_col);

  } else {
    auto rhs_out_col = cmp->columns.Create(rhs_col->var, rhs_col->type, cmp,
                                           rhs_col->id, col_index++);
    rhs_out_col->CopyConstantFrom(rhs_col);
    in_to_out.emplace(rhs_col, rhs_out_col);
  }

  assert(cmp->input_columns.Size() == 2);

  // Add in the other columns.
  for (auto col : columns) {
    if (col != lhs_col && col != rhs_col) {
      cmp->attached_columns.AddUse(col);
      const auto attached_col =
          cmp->columns.Create(col->var, col->type, cmp, col->id, col_index++);
      attached_col->CopyConstantFrom(col);
      in_to_out.emplace(col, attached_col);
    }
  }

  // Create a tuple that re-orders the output of the CMP to preserve it.
  TUPLE *tuple = query->tuples.Create();
  tuple->color = color;

  col_index = 0u;
  for (auto orig_col : columns) {
    const auto in_col = in_to_out[orig_col];
    auto out_col = tuple->columns.Create(orig_col->var, orig_col->type, tuple,
                                         orig_col->id, col_index++);
    tuple->input_columns.AddUse(in_col);
    out_col->CopyConstantFrom(in_col);
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
    if (a == b) {
      continue;

    } else if (a->view == b->view) {
      return false;

    } else if (a->type.Kind() != b->type.Kind() ||
               !a->view->Equals(eq, b->view) || a->Index() != b->Index()) {
      return false;
    }
  }
  return true;
}

// If `cols1:cols2` pull their data from a tuple, and if that tuple is
// unconditional, or if its conditions are trivial, then update `cols1:cols2`
// to point at the source of the data of those tuples.
//
// Takes in the `incoming_view` pulled from by `cols1:cols2` and returns the
// updated `incoming_view`.
//
// NOTE(pag): This updates `is_canonical = false` if it changes anything.
Node<QueryView> *Node<QueryView>::PullDataFromBeyondTrivialTuples(
    VIEW *incoming_view, UseList<COL> &cols1, UseList<COL> &cols2) {

  if (!incoming_view || this == incoming_view) {
    return incoming_view;
  }

  if (!incoming_view->negative_conditions.Empty()) {
    return incoming_view;
  }

  for (auto pos_condition : incoming_view->positive_conditions) {
    if (!pos_condition->IsTrivial()) {
      return incoming_view;
    }
  }

  const auto tuple = incoming_view->AsTuple();
  if (!tuple) {
    return PullDataFromBeyondTrivialUnions(incoming_view, cols1, cols2);
  }

  UseList<COL> new_cols1(this);
  UseList<COL> new_cols2(this);

  for (auto col : cols1) {
    if (col->view == tuple) {
      new_cols1.AddUse(tuple->input_columns[col->Index()]);
    } else {
      assert(col->IsConstant());
      new_cols1.AddUse(col);
    }
  }

  for (auto col : cols2) {
    if (col->view == tuple) {
      new_cols2.AddUse(tuple->input_columns[col->Index()]);
    } else {
      assert(col->IsConstant());
      new_cols2.AddUse(col);
    }
  }

  is_canonical = false;

  cols1.Swap(new_cols1);
  cols2.Swap(new_cols2);

  // See `recursion.dr`.
  const auto next_incoming_view = GetIncomingView(cols1, cols2);
  if (next_incoming_view == incoming_view) {
    return next_incoming_view;
  }

  return PullDataFromBeyondTrivialTuples(next_incoming_view, cols1, cols2);
}

Node<QueryView> *Node<QueryView>::PullDataFromBeyondTrivialUnions(
    Node<QueryView> *maybe_merge, UseList<COL> &cols1, UseList<COL> &cols2) {

  const auto merge = maybe_merge->AsMerge();
  if (!merge) {
    return maybe_merge;
  }

  if (!merge->negative_conditions.Empty()) {
    return maybe_merge;
  }

  for (auto pos_condition : merge->positive_conditions) {
    if (!pos_condition->IsTrivial()) {
      return maybe_merge;
    }
  }

  VIEW *incoming_view = nullptr;
  for (auto merged_view : merge->merged_views) {

    if (merged_view == this || merged_view == merge ||
        merged_view == incoming_view) {
      continue;

    // This is the second non-trivial data source to the merge, thus it's not
    // a trivial union.
    } else if (incoming_view) {
      return maybe_merge;

    } else {
      incoming_view = merged_view;
    }
  }

  if (!incoming_view) {
    return maybe_merge;
  }

  UseList<COL> new_cols1(this);
  UseList<COL> new_cols2(this);

  for (auto col : cols1) {
    if (col->view == merge) {
      new_cols1.AddUse(incoming_view->columns[col->Index()]);
    } else {
      assert(col->IsConstant());
      new_cols1.AddUse(col);
    }
  }

  for (auto col : cols2) {
    if (col->view == merge) {
      new_cols2.AddUse(incoming_view->columns[col->Index()]);
    } else {
      assert(col->IsConstant());
      new_cols2.AddUse(col);
    }
  }

  is_canonical = false;

  cols1.Swap(new_cols1);
  cols2.Swap(new_cols2);

  return PullDataFromBeyondTrivialTuples(incoming_view, cols1, cols2);
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

// Figure out what the incoming view to `cols1` and/or `cols2` is.
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

// Try to figure out if `view` is conditional. That could mean that it
// depends directly on a condition, or that it depends on something that
// may be present or may be absent (e.g. the output of a `JOIN`).
//
// Conditional in this case means: if data comes into `view`, then does data
// *always* come out of `view`? If the answer is "no" then it is conditional,
// otherwise it isn't. The relevant thing here is CONDitions, which are
// implemented as reference counts on some VIEW. If that VIEW will always have
// data, then we say that the view isn't conditional.
bool Node<QueryView>::IsConditional(
    VIEW *view, std::unordered_map<VIEW *, bool> &conditional_views) {
  if (conditional_views.count(view)) {
    return conditional_views[view];
  }

  auto &is_cond = conditional_views[view];
  is_cond = false;  // Sets a base case.

  if (!view->negative_conditions.Empty()) {
    is_cond = true;
    return true;
  }

  for (auto cond : view->positive_conditions) {
    if (!cond->IsTrivial(conditional_views)) {
      is_cond = true;
      return true;
    }
  }

  // These all introduce control dependencies. It's too annoying to truly
  // detect if the effective tests (e.g. compare `1=1`) actually are conditional
  // so we just assume these things are conditional.
  if (view->AsJoin() || view->AsCompare() || view->AsNegate() ||
      view->AsAggregate() || view->AsKVIndex()) {

    is_cond = true;
    return true;

  // Maps are not conditional iff their input view is not conditional and the
  // functor's range is one-to-one.
  } else if (MAP *map = view->AsMap()) {
    if (FunctorRange::kOneToOne != map->functor.Range()) {
      is_cond = true;
      return true;
    }

    VIEW *incoming_view = VIEW::GetIncomingView(view->input_columns,
                                                view->attached_columns);
    if (!incoming_view) {
      is_cond = false;
      return false;
    } else {
      is_cond = IsConditional(incoming_view, conditional_views);
      return is_cond;
    }

  } else if (MERGE *merge = view->AsMerge()) {
    for (VIEW *merged_view : merge->merged_views) {
      if (IsConditional(merged_view, conditional_views)) {
        is_cond = true;
        return true;
      }
    }
    return false;

  } else if (SELECT *sel = view->AsSelect()) {
    if (auto stream = sel->stream.get()) {
      if (stream->AsIO()) {
        is_cond = true;
        return true;
      } else {
        return false;
      }
    } else if (auto rel = sel->relation.get()) {
      for (VIEW *insert : rel->inserts) {
        if (IsConditional(insert, conditional_views)) {
          is_cond = true;
          return true;
        }
      }
    }

    return false;

  } else if (view->AsTuple() || view->AsInsert()) {
    if (VIEW *incoming_view = VIEW::GetIncomingView(view->input_columns)) {
      is_cond = IsConditional(incoming_view, conditional_views);
    } else {
      is_cond = false;
    }
    return is_cond;

  } else {
    assert(false);
    return true;
  }
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
void Node<QueryView>::CreateDependencyOnView(QueryImpl *query,
                                             Node<QueryView> *view) {
  assert(this != view);
  COND *condition = nullptr;
  if (auto incoming_cond = view->sets_condition.get(); incoming_cond) {

    // It's safe to inherit the condition of `view`.
    if (incoming_cond->setters.Size() == 1u) {
      condition = incoming_cond;

    // It's not safe to inherit the condition of `view`; it looks like it's
    // set by someone else as well, so inheriting it might result in us testing
    // a looser condition. We'll force a guard tuple, and the set condition on
    // `view` will transfer there; then we'll set a new condition on `view`.
    } else {
      (void) view->GuardWithTuple(query, true);
    }
  }

  // Invent a new condition for `incoming_view`.
  if (!condition) {
    condition = query->conditions.Create();

    assert(!view->sets_condition);
    view->sets_condition.Emplace(view, condition);
    condition->setters.AddUse(view);
  }

  if (!condition->IsTrivial()) {
    positive_conditions.AddUse(condition);
    condition->positive_users.AddUse(this);
  }

  assert(condition->UsersAreConsistent());
  assert(condition->SettersAreConsistent());
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
        if (prev_view == this) {
          return false;
        }
      }
    }
  }
#else
  (void) cols1;
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
          if (prev_view == this) {
            return false;
          }
        }
      }
    }
    return true;
  };
  return do_cols(cols1) && do_cols(cols2);
#else
  (void) cols1;
  (void) cols2;
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

  if (a->group_ids.empty() || b->group_ids.empty()) {
    return false;
  }

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

  //double_check:
  //  auto a_used_by_join = false;
  //  a->ForEachUse<JOIN>([&a_used_by_join] (JOIN *, VIEW *) {
  //    a_used_by_join = true;
  //  });
  //
  //  auto b_used_by_join = false;
  //  b->ForEachUse<JOIN>([&b_used_by_join] (JOIN *, VIEW *) {
  //    b_used_by_join = true;
  //  });
  //
  //  return a_used_by_join || b_used_by_join;
}

}  // namespace hyde
