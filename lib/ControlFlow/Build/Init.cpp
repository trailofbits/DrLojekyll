// Copyright 2020, Trail of Bits. All rights reserved.

#include "Build.h"

namespace hyde {

// Builds an initialization function which does any work that depends purely
// on constants.
void BuildInitProcedure(ProgramImpl *impl, Context &context, Query query) {

  // Make sure that the first procedure is the init procedure.
  const auto init_proc = context.init_proc;
  assert(init_proc != nullptr);
  assert(!init_proc->body.get());

  const auto entry_proc = context.entry_proc;
  assert(entry_proc != nullptr);

  const auto seq = impl->series_regions.Create(init_proc);
  init_proc->body.Emplace(init_proc, seq);

  auto call = impl->operation_regions.CreateDerived<CALL>(
      impl->next_id++, init_proc, entry_proc);
  init_proc->body.Emplace(init_proc, call);

  for (auto other_io : query.IOs()) {
    const auto receives = other_io.Receives();
    if (receives.empty()) {
      continue;
    }

    // Pass in the empty vector once or twice for other messages.
    const auto empty_vec =
        init_proc->VectorFor(impl, VectorKind::kEmpty, receives[0].Columns());
    call->arg_vecs.AddUse(empty_vec);
    if (receives[0].CanReceiveDeletions()) {
      call->arg_vecs.AddUse(empty_vec);
    }
  }

  assert(call->arg_vecs.Size() == entry_proc->input_vecs.Size());

  CompleteProcedure(impl, init_proc, context);
}

}  // namespace hyde
