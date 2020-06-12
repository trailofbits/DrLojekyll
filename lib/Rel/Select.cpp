// Copyright 2020, Trail of Bits. All rights reserved.

#include "Query.h"

#include <drlojekyll/Util/EqualitySet.h>

namespace hyde {

Node<QuerySelect>::~Node(void) {}

Node<QuerySelect> *Node<QuerySelect>::AsSelect(void) noexcept {
  return this;
}

uint64_t Node<QuerySelect>::Hash(void) noexcept {
  if (hash) {
    return hash;
  }

  hash = HashInit();

  if (relation) {
    hash ^= relation->declaration.Id();

  } else if (stream) {
    if (auto generator_stream = stream->AsGenerator()) {
      hash ^= generator_stream->functor.Id();

    } else if (auto const_stream  = stream->AsConstant()) {
      hash ^= std::hash<std::string_view>()(
          const_stream->literal.Spelling());

    } else if (auto input_stream = stream->AsInput()) {
      hash ^= input_stream->declaration.Id();
    }
  }
  return hash;
}

// Return a number that can be used to help sort this node. The idea here
// is that we often want to try to merge together two different instances
// of the same underlying node when we can.
uint64_t Node<QuerySelect>::Sort(void) noexcept {
  return position.Index();
}

unsigned Node<QuerySelect>::Depth(void) noexcept {
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
  depth = real + 1u;

  return depth;
}

// Equality over SELECTs is a mix of structural and pointer-based.
bool Node<QuerySelect>::Equals(
    EqualitySet &eq, Node<QueryView> *that_) noexcept {
  const auto that = that_->AsSelect();
  if (!that ||
      can_receive_deletions != that->can_receive_deletions ||
      can_produce_deletions != that->can_produce_deletions ||
      positive_conditions != that->positive_conditions ||
      negative_conditions != that->negative_conditions ||
      columns.Size() != that->columns.Size() ||
      input_columns.Size() != that->input_columns.Size()) {
    return false;
  }

  if (stream) {
    if (stream.get() != that->stream.get()) {
      return false;
    }

    if (stream->AsInput() || stream->AsConstant()) {
      return true;

    // Never let generators be merged, e.g. imagine that we have a generating
    // functor that emulates SQL's "primary key auto increment". That should
    // never be merged, even across `group_ids`.
    } else if (stream->AsGenerator()) {
      return false;

    } else {
      assert(false);
      return false;
    }

  } else if (relation) {
    if (!that->relation ||
        relation->declaration.Id() != that->relation->declaration.Id()) {
      return false;
    }

    if (eq.Contains(this, that)) {
      return true;
    }

    if (InsertSetsOverlap(this, that)) {
      return false;
    }

    eq.Insert(this, that);
    return true;

  } else {
    assert(false);
    return false;
  }
}

}  // namespace hyde
