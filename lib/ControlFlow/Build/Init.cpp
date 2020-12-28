// Copyright 2020, Trail of Bits. All rights reserved.

#include "Build.h"

namespace hyde {

// Builds an initialization function which does any work that depends purely
// on constants.
void BuildInitProcedure(ProgramImpl *impl, Context &context) {

  // Make sure that the first procedure is the init procedure.
  assert(impl->procedure_regions.Empty());
  const auto init_proc = impl->procedure_regions.Create(
      impl->next_id++, ProcedureKind::kInitializer);

  const auto seq = impl->series_regions.Create(init_proc);
  init_proc->body.Emplace(init_proc, seq);

  const auto par = impl->parallel_regions.Create(seq);
  seq->AddRegion(par);

  // Go find all TUPLEs whose inputs are constants. We ignore constant refs,
  // as those are dataflow dependent.
  //
  // NOTE(pag): The dataflow builder ensures that TUPLEs are the only node types
  //            that can take all constants.
  for (auto view : impl->query.Tuples()) {
    bool all_const = true;
    for (auto in_col : view.InputColumns()) {
      if (!in_col.IsConstant()) {
        all_const = false;
      }
    }
    if (!all_const) {
      continue;
    }

    const auto let = impl->operation_regions.CreateDerived<LET>(par);
    par->AddRegion(let);

    // Add variable mappings.
    view.ForEachUse([&](QueryColumn in_col, InputColumnRole,
                        std::optional<QueryColumn> out_col) {
      const auto const_var = let->VariableFor(impl, in_col);
      if (out_col) {
        let->col_id_to_var[out_col->Id()] = const_var;
      }
    });

    BuildEagerRegion(impl, view, view, context, let, nullptr);
  }

  CompleteProcedure(impl, init_proc, context);
}

}  // namespace hyde
