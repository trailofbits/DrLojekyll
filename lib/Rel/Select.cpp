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
  hash |= 1;
  return hash;
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
    for (auto i = 0u, j = 0u;
         i < group_ids.size() && j < that->group_ids.size(); ) {

      if (group_ids[i] == that->group_ids[j]) {
        return false;

      } else if (group_ids[i] < that->group_ids[j]) {
        ++i;

      } else {
        ++j;
      }
    }

    eq.Insert(this, that);
    return true;

  } else {
    assert(false);
    return false;
  }
}

}  // namespace hyde
