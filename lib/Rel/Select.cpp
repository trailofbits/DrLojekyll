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

  if (relation) {
    hash = relation->decl.Id();

  } else if (stream) {
    if (auto generator_stream = stream->AsGenerator()) {
      hash = generator_stream->functor.Id();

    } else if (auto const_stream  = stream->AsConstant()) {
      hash = std::hash<std::string_view>()(
          const_stream->literal.Spelling());

    } else if (auto input_stream = stream->AsInput()) {
      hash = input_stream->declaration.Id();

    } else {
      hash = 0;
    }
  }

  hash <<= 4;
  hash |= query::kSelectId;
  return hash;
}

// Return a number that can be used to help sort this node. The idea here
// is that we often want to try to merge together two different instances
// of the same underlying node when we can.
uint64_t Node<QuerySelect>::Sort(void) noexcept {
  return position.Index();
}

unsigned Node<QuerySelect>::Depth(void) noexcept {
  depth = 1;  // Base case for cycles.
  depth = GetDepth(input_columns, 0) + 1;
  return depth;
}

// Equality over SELECTs is a mix of structural and pointer-based.
bool Node<QuerySelect>::Equals(
    EqualitySet &eq, Node<QueryView> *that_) noexcept {
  const auto that = that_->AsSelect();
  if (!that || columns.Size() != that->columns.Size() ||
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
    if (!that->relation || relation->decl.Id() != that->relation->decl.Id()) {
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
