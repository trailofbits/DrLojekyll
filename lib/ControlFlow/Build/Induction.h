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

class ContinueJoinWorkItem final : public WorkItem {
 public:
  virtual ~ContinueJoinWorkItem(void) {}

  ContinueJoinWorkItem(Context &context, QueryView view_,
                       VECTOR *input_pivot_vec_, VECTOR *swap_pivot_vec_,
                       INDUCTION *induction_);

  // Find the common ancestor of all insert regions.
  REGION *FindCommonAncestorOfInsertRegions(void) const;

  void Run(ProgramImpl *program, Context &context) override;

  std::vector<OP *> inserts;

 private:
  const QueryView view;
  VECTOR * const input_pivot_vec;
  VECTOR * const swap_pivot_vec;
  INDUCTION * const induction;
};

class ContinueProductWorkItem final : public WorkItem {
 public:
  virtual ~ContinueProductWorkItem(void) {}

  ContinueProductWorkItem(Context &context, QueryView view_,
                          INDUCTION *induction_);

  // Find the common ancestor of all insert regions.
  REGION *FindCommonAncestorOfAppendRegions(void) const;

  void Run(ProgramImpl *program, Context &context) override;

  // Maps tables to their product input vectors.
  std::map<TABLE *, VECTOR *> product_vector;

  std::vector<VECTOR *> vectors;
  std::vector<OP *> appends;

 private:
  QueryView view;
  INDUCTION * const induction;
};

INDUCTION *GetOrInitInduction(ProgramImpl *impl, QueryView view,
                              Context &context, OP *parent);

void AppendToInductionInputVectors(
    ProgramImpl *impl, QueryView vec_view, QueryView inductive_view,
    Context &context, OP *parent, INDUCTION *induction, bool for_add);

REGION *AppendToInductionOutputVectors(
    ProgramImpl *impl, QueryView vec_view, Context &context, INDUCTION *induction,
    REGION *parent);

}  // namespace hyde
