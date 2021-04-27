// Copyright 2020, Trail of Bits. All rights reserved.

#include "Build.h"

namespace hyde {

// Build an eager region for a `QueryMerge` that is NOT part of an inductive
// loop, and thus passes on its data to the next thing down as long as that
// data is unique.
//
// NOTE(pag): These merges could actually be part of an induction set, but
//            really the induction loop belongs to another merge which dominates
//            this merge.
void BuildEagerUnionRegion(ProgramImpl *impl, QueryView pred_view,
                           QueryMerge merge, Context &context, OP *parent_,
                           TABLE *last_table_) {
  const QueryView view(merge);
  auto [parent, table, last_table] =
      InTryInsert(impl, context, view, parent_, last_table_);

#ifndef NDEBUG
  if (1u < view.Predecessors().size()) {
    for (auto col : view.Columns()) {
      assert(!col.IsConstantRef());
    }
  }
#endif

  BuildEagerInsertionRegions(impl, view, context, parent,
                             view.Successors(), last_table);
}

// Build a top-down checker on a union. This applies to non-differential
// unions.
REGION *BuildTopDownUnionChecker(
    ProgramImpl *impl, Context &context, REGION *proc, QueryMerge merge,
    std::vector<QueryColumn> &view_cols, TABLE *already_checked) {
  const QueryView view(merge);

  // Organize the checking so that we check the non-differential predecessors
  // first, then the differential predecessors.
  const auto seq = impl->series_regions.Create(proc);
  const auto par_normal = impl->parallel_regions.Create(seq);
  const auto par_diff = impl->parallel_regions.Create(seq);
  seq->AddRegion(par_normal);
  seq->AddRegion(par_diff);
  seq->AddRegion(BuildStateCheckCaseReturnFalse(impl, seq));

  auto do_rec_check = [&] (QueryView pred_view, PARALLEL *parent) -> REGION * {
    return CallTopDownChecker(
        impl, context, parent, view, view_cols, pred_view, already_checked,
        [=] (REGION *parent_if_true) -> REGION * {
          return BuildStateCheckCaseReturnTrue(impl, parent_if_true);
        },
        [] (REGION *) -> REGION * { return nullptr; });
  };

  for (auto pred_view : merge.MergedViews()) {

    // If the predecessor can produce deletions, then check it in `par_diff`.
    if (pred_view.CanProduceDeletions()) {
      const auto rec_check = do_rec_check(pred_view, par_diff);
      par_diff->AddRegion(rec_check);

      COMMENT( rec_check->comment =
          __FILE__ ": BuildTopDownUnionChecker call differential predecessor"; )

    // If the predecessor can't produce deletions, then check it in
    // `par_normal`.
    } else {
      const auto rec_check = do_rec_check(pred_view, par_normal);
      par_normal->AddRegion(rec_check);

      COMMENT( rec_check->comment =
          __FILE__ ": BuildTopDownUnionChecker call normal predecessor"; )
    }
  }

  return seq;
}

void CreateBottomUpUnionRemover(ProgramImpl *impl, Context &context,
                                QueryView view, OP *parent,
                                TABLE *already_removed) {
  BuildEagerRemovalRegions(impl, view, context, parent,
                           view.Successors(), already_removed);
}

}  // namespace hyde
