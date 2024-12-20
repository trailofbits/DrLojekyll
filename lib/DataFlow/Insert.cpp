// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/Util/EqualitySet.h>

#include "Query.h"

namespace hyde {

QueryInsertImpl::~QueryInsertImpl(void) {}

QueryInsertImpl::QueryInsertImpl(QueryRelationImpl *relation_, ParsedDeclaration decl_)
    : relation(this, relation_),
      declaration(decl_) {}

QueryInsertImpl::QueryInsertImpl(QueryStreamImpl *stream_, ParsedDeclaration decl_)
    : stream(this, stream_),
      declaration(decl_) {}

QueryInsertImpl *QueryInsertImpl::AsInsert(void) noexcept {
  return this;
}

const char *QueryInsertImpl::KindName(void) const noexcept {
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

uint64_t QueryInsertImpl::Hash(void) noexcept {
  if (hash) {
    return hash;
  }

  // Start with an initial hash just in case there's a cycle somewhere.
  hash = HashInit() ^ declaration.Id();
  assert(hash != 0);

  auto local_hash = hash;

  // Mix in the hashes of the input by columns; these are ordered.
  for (auto col : input_columns) {
    local_hash ^= RotateRight64(local_hash, 33) * col->Hash();
  }

  hash = local_hash;
  return local_hash;
}

bool QueryInsertImpl::Canonicalize(QueryImpl *, const OptimizationContext &,
                                     const ErrorLog &) {
  is_canonical = true;
  if (valid == VIEW::kValid && !CheckIncomingViewsMatch(input_columns)) {
    valid = VIEW::kInvalidBeforeCanonicalize;
  }

  assert(columns.Empty());
  assert(attached_columns.Empty());

  // NOTE(pag): This may update `is_canonical`.
  auto incoming_view = PullDataFromBeyondTrivialTuples(
      GetIncomingView(input_columns), input_columns, attached_columns);

  // An unsatisfiable INSERT is dropped.
  if (!is_unsat && incoming_view && incoming_view->is_unsat) {
    MarkAsUnsatisfiable();
    PrepareToDelete();
    return true;
  }

  if (!is_canonical) {
    is_canonical = true;
    return true;
  }

  return false;
}

// Equality over inserts is structural.
bool QueryInsertImpl::Equals(EqualitySet &eq, VIEW *that_) noexcept {

  if (eq.Contains(this, that_)) {
    return true;
  }

  const auto that = that_->AsInsert();
  if (!that || can_produce_deletions != that->can_produce_deletions ||
      declaration.Id() != that->declaration.Id() ||
      columns.Size() != that->columns.Size() ||
      positive_conditions != that->positive_conditions ||
      negative_conditions != that->negative_conditions) {
    return false;
  }

  eq.Insert(this, that);
  if (!ColumnsEq(eq, input_columns, that->input_columns)) {
    eq.Remove(this, that);
    return false;
  }

  return true;
}

}  // namespace hyde
