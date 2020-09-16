// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

#include <set>
#include <unordered_set>

#include <drlojekyll/DataFlow/Query.h>
#include <drlojekyll/Util/DefUse.h>

#include "../Program.h"

namespace hyde {

// Equivalence classes of `QueryMerge`s. Two or more `QueryMerge`s belong to
// the same equivalence class iff their cycles intersect.
class InductionSet : public DisjointSet {
 public:
  using DisjointSet::DisjointSet;

  std::vector<QueryView> merges;
};

class Context;

// Generic action for lifting to the control flow representation. Work item
// actions enable us to defer some liting to the control flow representation
// until `Run` is invoked.
class WorkItem {
 public:
  static constexpr unsigned kRunAsLateAsPossible = ~0u;

  virtual ~WorkItem(void);
  virtual void Run(ProgramImpl *program, Context &context) = 0;

  explicit WorkItem(unsigned order_)
      : order(order_) {}

  const unsigned order;
};

using WorkItemPtr = std::unique_ptr<WorkItem>;

// General wrapper around data used when lifting from the data flow to the
// control flow representation.
class Context {
 public:

  // Mapping of `QueryMerge` instances to their equivalence classes.
  std::unordered_map<QueryView, InductionSet> merge_sets;

  // Set of successors of a `QueryMerge` that may lead back to the merge
  // (inductive) and never lead back to the merge (noninductive), respectively.
  std::unordered_map<QueryView, std::unordered_set<QueryView>> inductive_successors;
  std::unordered_map<QueryView, std::unordered_set<QueryView>> noninductive_successors;

//  // Set of predecessors of a `QueryMerge` whose data transitively depends upon
//  // an inductive
//  std::unordered_map<QueryView, std::unordered_set<QueryView>> inductive_predecessors;
//  std::unordered_map<QueryView, std::unordered_set<QueryView>> noninductive_predecessors;

  // Set of regions that execute eagerly. In practice this means that these
  // regions are directly needed in order to produce an output message.
  std::unordered_set<QueryView> eager;

  // Map `QueryMerge`s to INDUCTIONs. One or more `QueryMerge`s might map to
  // the same INDUCTION if they belong to the same "inductive set". This happens
  // when two or more `QueryMerge`s are cyclic, and their cycles intersect.
  std::unordered_map<QueryView, INDUCTION *> view_to_induction;

  // Boolean variable to test if we've ever produced anything for this product,
  // and thus should push data through.
  std::unordered_map<QueryView, VAR *> product_guard_var;

  // Work list of actions to invoke to build the execution tree.
  std::vector<WorkItemPtr> work_list;
  std::vector<WorkItemPtr> prev_work_list;
  std::unordered_map<QueryView, WorkItem *> view_to_work_item;
};

// Build an eager region. This guards the execution of the region in
// conditionals if the view itself is conditional.
void BuildEagerRegion(ProgramImpl *impl, QueryView pred_view,
                      QueryView view, Context &context, OP *parent);

// Build an eager region for a `QueryMerge` that is part of an inductive
// loop. This is interesting because we use a WorkItem as a kind of "barrier"
// to accumulate everything leading into the inductions before proceeding.
void BuildEagerInductiveRegion(ProgramImpl *impl, QueryView pred_view,
                               QueryMerge view, Context &context, OP *parent);

// Build an eager region for a `QueryMerge` that is NOT part of an inductive
// loop, and thus passes on its data to the next thing down as long as that
// data is unique.
void BuildEagerUnionRegion(ProgramImpl *impl, QueryView pred_view,
                           QueryMerge view, Context &context, OP *parent);

// Build an eager region for publishing data, or inserting it. This might end
// up passing things through if this isn't actually a message publication.
void BuildEagerInsertRegion(ProgramImpl *impl, QueryView pred_view,
                            QueryInsert view, Context &context, OP *parent);

// Build an eager region for a join.
void BuildEagerJoinRegion(ProgramImpl *impl, QueryView pred_view,
                          QueryJoin view, Context &context, OP *parent);

// Build an eager region for cross-product.
void BuildEagerProductRegion(ProgramImpl *impl, QueryView pred_view,
                             QueryJoin view, Context &context, OP *parent);

// Add in all of the successors of a view inside of `parent`, which is
// usually some kind of loop. The successors execute in parallel.
template <typename List>
void BuildEagerSuccessorRegions(ProgramImpl *impl, QueryView view,
                                Context &context, OP *parent,
                                List &&successors) {
  const auto par = impl->parallel_regions.Create(parent);
  UseRef<REGION>(parent, par).Swap(parent->body);

  for (QueryView succ_view : successors) {
    const auto let = impl->operation_regions.CreateDerived<LET>(par);

    let->ExecuteAlongside(impl, par);

    succ_view.ForEachUse([=] (QueryColumn in_col, InputColumnRole,
                              std::optional<QueryColumn> out_col) {
      if (out_col && in_col.Id() != out_col->Id() &&
          (QueryView::Containing(in_col) == view || in_col.IsConstant())) {

        const auto src_var = par->VariableFor(impl, in_col);
        let->used_vars.AddUse(src_var);
        const auto dst_var = let->defined_vars.Create(
            out_col->Id(), VariableRole::kLetBinding);
        dst_var->query_column = *out_col;
        let->col_id_to_var.emplace(out_col->Id(), dst_var);
      }
    });

    BuildEagerRegion(impl, view, succ_view, context, let);
  }
}

}  // namespace hyde
