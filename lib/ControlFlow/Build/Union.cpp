// Copyright 2020, Trail of Bits. All rights reserved.

#include "Build.h"

namespace hyde {

// Build an eager region for a `QueryMerge` that is NOT part of an inductive
// loop, and thus passes on its data to the next thing down as long as that
// data is unique.
void BuildEagerUnionRegion(ProgramImpl *impl, QueryView pred_view,
                           QueryMerge view, Context &usage, OP *parent,
                           TABLE *last_model) {
  assert(false && "TODO!");

  // TODO(pag): Think about whether or not we need to actually de-duplicate
  //            anything. It could be that we only need to dedup if we're on
  //            the edge between eager/lazy, or if we're in lazy.
}

}  // namespace hyde
