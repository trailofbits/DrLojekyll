// Copyright 2020, Trail of Bits. All rights reserved.

#include "Build.h"

namespace hyde {
namespace {

// Create a procedure for a view.
static void ExtendEagerProcedure(ProgramImpl *impl, QueryIO io,
                                 Context &context, PROC *proc,
                                 PARALLEL *parent) {
  const auto receives = io.Receives();
  if (receives.empty()) {
    return;
  }

  assert(io.Declaration().IsMessage());
  const auto message = ParsedMessage::From(io.Declaration());
  (void) message;

  VECTOR *removal_vec = nullptr;
  const auto vec =
      proc->VectorFor(impl, VectorKind::kParameter, receives[0].Columns());

  // Loop over the receives for adding.
  for (auto receive : receives) {

    // Add a removal vector if any of the receives can receive deletions.
    if (!removal_vec && receive.CanReceiveDeletions()) {
      removal_vec = proc->VectorFor(
          impl, VectorKind::kParameter, receive.Columns());
    }

    const auto loop = impl->operation_regions.CreateDerived<VECTORLOOP>(
        impl->next_id++, parent, ProgramOperation::kLoopOverInputVector);
    parent->AddRegion(loop);
    loop->vector.Emplace(loop, vec);

    for (auto col : receives[0].Columns()) {
      const auto var = loop->defined_vars.Create(impl->next_id++,
                                                 VariableRole::kVectorVariable);
      var->query_column = col;
      loop->col_id_to_var.emplace(col.Id(), var);
    }

    BuildEagerInsertionRegions(impl, receive, context, loop,
                               receive.Successors(), nullptr);
  }

  if (!removal_vec) {
    assert(!message.IsDifferential());
    return;
  }

  assert(message.IsDifferential());

  // Loop over the receives for adding.
  for (auto receive : receives) {
    if (!receive.CanReceiveDeletions()) {
      continue;
    }

    const auto loop = impl->operation_regions.CreateDerived<VECTORLOOP>(
        impl->next_id++, parent, ProgramOperation::kLoopOverInputVector);
    parent->AddRegion(loop);
    loop->vector.Emplace(loop, removal_vec);

    for (auto col : receives[0].Columns()) {
      const auto var = loop->defined_vars.Create(impl->next_id++,
                                                 VariableRole::kVectorVariable);
      var->query_column = col;
      loop->col_id_to_var.emplace(col.Id(), var);
    }

    BuildEagerRemovalRegions(impl, receive, context, loop,
                             receive.Successors(), nullptr);
  }
}

// Builds an I/O procedure, which goes and invokes the primary data flow
// procedure.
static void BuildIOProcedure(ProgramImpl *impl, Query query, QueryIO io,
                             Context &context, PROC *proc) {
  const auto receives = io.Receives();
  if (receives.empty()) {
    return;
  }

  assert(io.Declaration().IsMessage());
  const auto message = ParsedMessage::From(io.Declaration());

  const auto io_proc = impl->procedure_regions.Create(
      impl->next_id++, ProcedureKind::kMessageHandler);
  io_proc->io = io;

  const auto io_vec =
      io_proc->VectorFor(impl, VectorKind::kParameter, receives[0].Columns());

  VECTOR *io_remove_vec = nullptr;
  if (message.IsDifferential()) {
    io_remove_vec =
        io_proc->VectorFor(impl, VectorKind::kParameter, receives[0].Columns());
  }

  auto seq = impl->series_regions.Create(io_proc);
  io_proc->body.Emplace(io_proc, seq);

  auto call = impl->operation_regions.CreateDerived<CALL>(
      impl->next_id++, seq, proc);
  seq->AddRegion(call);

  auto ret = impl->operation_regions.CreateDerived<RETURN>(
      seq, ProgramOperation::kReturnTrueFromProcedure);
  seq->AddRegion(ret);

  for (auto other_io : query.IOs()) {
    const auto other_receives = other_io.Receives();
    if (other_receives.empty()) {
      continue;
    }

    // Pass in our input vector for additions, and possibly our input vector
    // for removals.
    if (io == other_io) {
      call->arg_vecs.AddUse(io_vec);
      if (io_remove_vec) {
        call->arg_vecs.AddUse(io_remove_vec);
      }

    // Pass in the empty vector once or twice for other messages.
    } else {
      const auto empty_vec = io_proc->VectorFor(
          impl, VectorKind::kEmpty, other_receives[0].Columns());
      call->arg_vecs.AddUse(empty_vec);
      if (other_receives[0].CanReceiveDeletions()) {
        call->arg_vecs.AddUse(empty_vec);
      }
    }
  }
}

struct CompareVectors {
 public:
  inline bool operator()(VECTOR *a, VECTOR *b) const noexcept {
    return a->id < b->id;
  }
};

// Classifies usage of a vector into "read" or "written" (or both) by `region`.
static void ClassifyVector(VECTOR *vec, REGION *region,
                           std::set<VECTOR *, CompareVectors> &read,
                           std::set<VECTOR *, CompareVectors> &written) {
  if (region->AsInduction()) {
    read.insert(vec);

  } else if (auto op = region->AsOperation(); op) {
    switch (op->op) {
      case ProgramOperation::kAppendToInductionVector:
      case ProgramOperation::kClearInductionVector:
      case ProgramOperation::kAppendUnionInputToVector:
      case ProgramOperation::kClearUnionInputVector:
      case ProgramOperation::kAppendJoinPivotsToVector:
      case ProgramOperation::kClearJoinPivotVector:
      case ProgramOperation::kAppendToProductInputVector:
      case ProgramOperation::kClearProductInputVector:
      case ProgramOperation::kScanTable:
      case ProgramOperation::kClearScanVector:
        written.insert(vec);
        break;

      case ProgramOperation::kSwapInductionVector:
      case ProgramOperation::kSortAndUniqueInductionVector:
      case ProgramOperation::kSortAndUniquePivotVector:
      case ProgramOperation::kSortAndUniqueProductInputVector:
        read.insert(vec);
        written.insert(vec);
        break;

      case ProgramOperation::kLoopOverInductionVector:
      case ProgramOperation::kLoopOverUnionInputVector:
      case ProgramOperation::kJoinTables:
      case ProgramOperation::kCrossProduct:
      case ProgramOperation::kLoopOverScanVector:
      case ProgramOperation::kLoopOverInputVector:
        read.insert(vec);
        break;

      default:
        assert(false);
    }
  // Parameter; by construction, neither the entry nor the primary procedures
  // have inout parameters.
  } else if (region->AsProcedure()) {
    read.insert(vec);

  } else {
    assert(false);
  }
}

// From the initial procedure, "extract" the primary procedure. The entry
// procedure operates on vectors from message receipt, and then does everything.
// Our goal is to split it up into two procedures:
//
//    1) The simplified entry procedure, which will only read from the
//       message vectors, do some joins perhaps, and append to induction
//       vectors / output message vectors.
//
//    2) The primary data flow procedure, which takes as input the induction
//       vectors which do the remainder of the data flow.
static void ExtractPrimaryProcedure(ProgramImpl *impl, PROC *entry_proc,
                                    Context &context) {
  const auto primary_proc = impl->procedure_regions.Create(
      impl->next_id++, ProcedureKind::kPrimaryDataFlowFunc);

  std::vector<REGION *> regions_to_extract;
  std::unordered_set<REGION *> seen;

  // First, go find the regions leading to the uses of the message vectors.
  // We go up to the enclosing inductions so that we can also capture things
  // like JOINs that will happen before those inductions.
  for (auto message_vec : entry_proc->input_vecs) {
    message_vec->ForEachUse<REGION>([&] (REGION *region, VECTOR *) {
      if (auto [it, added] = seen.insert(region); added) {
        regions_to_extract.push_back(region);
      }
    });
  }

  // Add the discovered regions into the entry function, replacing them with
  // LET expressions.
  auto entry_seq = impl->series_regions.Create(entry_proc);
  auto entry_par = impl->parallel_regions.Create(entry_seq);
  entry_seq->AddRegion(entry_par);

  if (!entry_proc->input_vecs.Empty()) {
    assert(!regions_to_extract.empty());
  }

  for (auto region : regions_to_extract) {
    auto let = impl->operation_regions.CreateDerived<LET>(region->parent);
    region->ReplaceAllUsesWith(let);
    region->parent = entry_par;
    entry_par->AddRegion(region);
  }

  // Re-root the entry function body into the primary function, and link in the
  // extracted stuff into the entry body.
  entry_proc->body->parent = primary_proc;
  primary_proc->body.Swap(entry_proc->body);
  entry_proc->body.Emplace(entry_proc, entry_seq);

  // Now, go figure out which vectors are logically read and written by the
  // two procedures, so we can split them up. Our goal is to build up the
  // list of arguments that we need to pass into the primary function from
  // the entry function.
  std::set<VECTOR *, CompareVectors> read_by_entry;
  std::set<VECTOR *, CompareVectors> written_by_entry;
  std::set<VECTOR *, CompareVectors> read_by_primary;
  std::set<VECTOR *, CompareVectors> written_by_primary;

  for (auto vec : entry_proc->vectors) {
    vec->ForEachUse<REGION>([&] (REGION *region, VECTOR *) {
      auto region_proc = region->Ancestor()->AsProcedure();
      assert(region_proc != nullptr);

      if (region_proc == entry_proc) {
        ClassifyVector(vec, region, read_by_entry, written_by_entry);

      } else if (region_proc == primary_proc) {
        ClassifyVector(vec, region, read_by_primary, written_by_primary);

      } else {
        assert(false);
      }
    });
  }

  std::vector<VECTOR *> primary_params;

  // The parameters we need are written by `entry` and `read` by `primary`.
  std::set_intersection(written_by_entry.begin(), written_by_entry.end(),
                        read_by_primary.begin(), read_by_primary.end(),
                        std::back_inserter(primary_params), CompareVectors());

  // Create the mapping between the vectors that need to be updated in the
  // primary data flow function that still point at the old function.
  std::unordered_map<VECTOR *, VECTOR *> replacements;

  for (auto vec : primary_params) {
    replacements[vec] = primary_proc->input_vecs.Create(vec);
  }

  for (auto vec : read_by_primary) {
    if (!replacements.count(vec)) {
      replacements[vec] = primary_proc->vectors.Create(vec);
    }
  }

  for (auto vec : written_by_entry) {
    if (!replacements.count(vec)) {
      replacements[vec] = primary_proc->vectors.Create(vec);
    }
  }

  for (auto [old_vec, new_vec] : replacements) {
    old_vec->ReplaceUsesWithIf<REGION>(new_vec, [=] (REGION *user, VECTOR *) {
      return user->Ancestor() == primary_proc;
    });
  }

  // Garbage collect the unneeded vectors from the entry proc.
  entry_proc->vectors.RemoveUnused();

  // Call the dataflow proc from the entry proc.
  auto call = impl->operation_regions.CreateDerived<CALL>(
      impl->next_id++, entry_seq, primary_proc,
      ProgramOperation::kCallProcedure);
  entry_seq->AddRegion(call);

  for (auto vec : primary_params) {
    call->arg_vecs.AddUse(vec);
  }

  // Terminate the entry proc.
  entry_seq->AddRegion(impl->operation_regions.CreateDerived<RETURN>(
      entry_seq, ProgramOperation::kReturnFalseFromProcedure));
}

}  // namespace

// Build the primary and entry data flow procedures.
void BuildEagerProcedure(ProgramImpl *impl, Context &context,
                         Query query) {

  assert(context.work_list.empty());
  assert(context.view_to_work_item.empty());

  const auto proc = impl->procedure_regions.Create(
      impl->next_id++, ProcedureKind::kEntryDataFlowFunc);

  context.work_list.clear();

  //  context.view_to_work_item.clear();
  //  context.view_to_induction.clear();
  //  context.product_vector.clear();

  const auto proc_par = impl->parallel_regions.Create(proc);

  for (auto io : query.IOs()) {
    const auto par = impl->parallel_regions.Create(proc);
    proc->body.Emplace(proc, par);
    ExtendEagerProcedure(impl, io, context, proc, par);

    auto curr_body = proc->body.get();
    proc->body.Clear();
    curr_body->parent = proc_par;
    proc_par->AddRegion(curr_body);
  }

  // TODO(pag): I think I have half-fixed the bug described below. Basically,
  //            I think I've "fixed" it for the first "level" of inductions,
  //            but none of the subsequent levels of inductions. It's possible
  //            that we'll need to break out work lists to separate joins and
  //            such, so that I can do this type of fixing up in phases.
  //
  // TODO(pag): Possible future bug lies here. So, right now we group everything
  //            into one PARALLEL, `par`, then build out from there. But maybe
  //            the right approach is to place them into independent parallel
  //            nodes, then somehow merge them. I think this will be critical
  //            when there are more than one message being received. Comment
  //            below, kept for posterity, relates to my thinking on this
  //            subject.
  //
  // This is subtle. We can't group all messages into a single PARALLEL node,
  // otherwise some messages will get "sucked into" an induction region reached
  // by a possibly unrelated message, and thus the logical ordering of
  // inductions will get totally screwed up. For example, one induction A might
  // be embedded in another induction B's init region, but B's cycle/output
  // regions will append to A's induction vector!
  //
  // Really, we need to pretend that all of messages are treated completely
  // independently at first, and then allow `CompleteProcedure` and the work
  // list, which partially uses depth for ordering, to figure the proper order
  // for regions. This is tricky because we need to place anything we find,
  // in terms of.
  proc->body.Emplace(proc, proc_par);

  CompleteProcedure(impl, proc, context);

  ExtractPrimaryProcedure(impl, proc, context);

  for (auto io : query.IOs()) {
    BuildIOProcedure(impl, query, io, context, proc);
  }
}

}  // namespace hyde
