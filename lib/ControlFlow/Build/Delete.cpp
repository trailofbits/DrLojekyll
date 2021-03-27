// Copyright 2020, Trail of Bits. All rights reserved.

#include "Build.h"

namespace hyde {

// Deleting from a relation.
//
// TODO(pag): The situation where there can be a `last_model` leading into
//            a DELETE node is one where we might have something like:
//
//                !foo(...) : message(...A...), condition(...A...).
//
//            If we ever hit this case, it likely means we need to introduce
//            a second table that is different than `last_model`, I think.
//            Overall I'm not super sure.
void BuildEagerDeleteRegion(ProgramImpl *impl, QueryView view, Context &context,
                            OP *parent) {

  // We don't permit `!foo : message(...).`
  assert(!view.SetCondition());

  BuildEagerRemovalRegions(impl, view, context, parent, view.Successors(),
                           nullptr);
}

// The interesting thing with DELETEs is that they don't have a data model;
// whereas an INSERT might share its data model with its corresponding SELECTs,
// as well as with the node feeding it, a DELETE is more a signal saying "my
// successor must delete this data from /its/ model."
void CreateBottomUpDeleteRemover(ProgramImpl *impl, Context &context,
                                 QueryView view, OP *parent) {
  BuildEagerRemovalRegions(impl, view, context, parent, view.Successors(),
                           nullptr);
}

}  // namespace hyde
