// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

#include "Build.h"

namespace hyde {

// A work item whose `Run` method is invoked after all initialization paths
// into an inductive region have been covered.
class ContinueInductionWorkItem final : public WorkItem {
 public:
  virtual ~ContinueInductionWorkItem(void) = default;

  ContinueInductionWorkItem(Context &context, QueryView merge,
                            INDUCTION *induction_);

  // Find the common ancestor of all initialization regions.
  REGION *FindCommonAncestorOfInitRegions(
      const std::vector<REGION *> &regions) const;

  void Run(ProgramImpl *impl, Context &context) override;

  // NOTE(pag): Multiple `ContinueInductionWorkItem` workers might share
  //            the same `induction`.
  INDUCTION *const induction;
};

INDUCTION *GetOrInitInduction(ProgramImpl *impl, QueryView view,
                              Context &context, OP *parent);

void AppendToInductionInputVectors(
    ProgramImpl *impl, QueryView view, Context &context, OP *parent,
    INDUCTION *induction, bool for_add);

REGION *AppendToInductionOutputVectors(
    ProgramImpl *impl, QueryView view, Context &context, INDUCTION *induction,
    REGION *parent);

}  // namespace hyde
