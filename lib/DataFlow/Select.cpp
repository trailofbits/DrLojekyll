// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/Parse/ErrorLog.h>
#include <drlojekyll/Util/EqualitySet.h>

#include "Query.h"

namespace hyde {

QuerySelectImpl::~QuerySelectImpl(void) {}

QuerySelectImpl *QuerySelectImpl::AsSelect(void) noexcept {
  return this;
}

uint64_t QuerySelectImpl::Hash(void) noexcept {
  if (hash) {
    return hash;
  }

  hash = HashInit();
  assert(hash != 0);
  const auto hash_ror = RotateRight64(hash, 33u);

  if (relation) {
    hash ^= hash_ror * relation->declaration.Id();

  } else if (stream) {
    if (auto const_tag = stream->AsTag()) {
      hash ^= hash_ror * (const_tag->val + 1ull);

    } else if (auto const_stream = stream->AsConstant()) {
      if (const_stream->literal->IsConstant()) {
        hash ^= hash_ror * const_stream->literal->Literal().IdentifierId();
      } else {
        hash ^= hash_ror *
                std::hash<std::string_view>()(
                    *const_stream->literal->Spelling(Language::kUnknown));
      }

    } else if (auto input_stream = stream->AsIO()) {
      hash ^= hash_ror * input_stream->declaration.Id();

    } else {
      assert(is_dead);
    }
  } else {
    assert(is_dead);
  }
  return hash;
}

// Return a number that can be used to help sort this node. The idea here
// is that we often want to try to merge together two different instances
// of the same underlying node when we can.
uint64_t QuerySelectImpl::Sort(void) noexcept {
  return position.Index();
}

unsigned QuerySelectImpl::Depth(void) noexcept {
  if (depth) {
    return depth;
  }

  auto estimate = EstimateDepth(input_columns, 0u);
  estimate = EstimateDepth(positive_conditions, estimate);
  estimate = EstimateDepth(negative_conditions, estimate);
  depth = estimate + 1u;  // Base case if there are cycles.

  auto real = GetDepth(input_columns, 0u);
  real = GetDepth(positive_conditions, real);
  real = GetDepth(negative_conditions, real);

  if (relation) {
    for (auto insert : relation->inserts) {
      real = std::max(real, insert->Depth());
    }
  }

  depth = real + 1u;

  return depth;
}

// Put this view into a canonical form. Returns `true` if changes were made
// beyond the scope of this view.
//
// NOTE(pag): We have a kind of manual/duplicate version of VIEW::IsUsed here
//            because the actual RELATION or STREAM nodes might be holding
//            references to this VIEW, and thus make it look used when it's
//            not.
//
// TODO(pag): This really shouldn't be needed. We probably have a bug in
//            `connect` or something like that. If we disable this function
//            then there's an orphaned SELECT in `average_weight.dr`. This
//            is because the RELation or IO holds onto a use of the SELECT
//            and so the SELECT always looks used.
bool QuerySelectImpl::Canonicalize(QueryImpl *query,
                                     const OptimizationContext &opt,
                                     const ErrorLog &err) {

  if (is_dead || sets_condition) {
    return false;
  }

  if (sets_condition && 0u < (sets_condition->positive_users.Size() +
                              sets_condition->negative_users.Size())) {
    return true;
  }

  for (auto col : columns) {
    if (col->IsUsedIgnoreMerges()) {
      return false;
    }
  }

  auto is_really_used = false;
  ForEachUse<VIEW>(
      [&is_really_used](VIEW *, VIEW *) { is_really_used = true; });

  if (!is_really_used) {

    // We're dropping a `RECEIVE` on a message. This could be a sign of a bug,
    // or of a condition not being satisfiable higher up.
    if (stream) {
      if (auto io = stream->AsIO(); io && io->receives.Size() == 1u) {
        auto predicate = *pred;
        auto decl = ParsedDeclaration::Of(predicate);
        auto clause = ParsedClause::Containing(predicate);
        err.Append(clause.SpellingRange(), predicate.SpellingRange())
            << "Last receive of message '" << decl.Name() << '/' << decl.Arity()
            << "' is unused";
      }
    }

    PrepareToDelete();
    return true;

  } else {
    return false;
  }
}

// Equality over SELECTs is a mix of structural and pointer-based.
bool QuerySelectImpl::Equals(EqualitySet &eq,
                               QueryViewImpl *that_) noexcept {
  const auto that = that_->AsSelect();
  if (!that || can_receive_deletions != that->can_receive_deletions ||
      can_produce_deletions != that->can_produce_deletions ||
      positive_conditions != that->positive_conditions ||
      negative_conditions != that->negative_conditions ||
      columns.Size() != that->columns.Size() ||
      input_columns.Size() != that->input_columns.Size()) {
    return false;
  }

  if (eq.Contains(this, that)) {
    return true;
  }

  if (stream) {
    if (stream.get() != that->stream.get()) {
      return false;
    }

    if (stream->AsConstant()) {
      return true;
    }

  } else if (relation) {
    if (!that->relation ||
        relation->declaration.Id() != that->relation->declaration.Id()) {
      return false;
    }
  }

  if (InsertSetsOverlap(this, that)) {
    return false;
  }

  eq.Insert(this, that);
  return true;
}

}  // namespace hyde
