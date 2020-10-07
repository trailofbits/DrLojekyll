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
  const auto par = impl->parallel_regions.Create(init_proc);
  UseRef<REGION>(init_proc, par).Swap(init_proc->body);

  // Go find all TUPLEs whose inputs are constants. We ignore constant refs,
  // as those are dataflow dependent.
  for (auto tuple : impl->query.Tuples()) {
    bool all_const = true;
    for (auto in_col : tuple.InputColumns()) {
      if (!in_col.IsConstant()) {
        all_const = false;
      }
    }
    if (!all_const) {
      continue;
    }

    const auto parent = impl->operation_regions.CreateDerived<LET>(par);
    parent->ExecuteAlongside(impl, par);

    // Add variable mappings.
    tuple.ForEachUse([&](QueryColumn in_col, InputColumnRole,
                         std::optional<QueryColumn> out_col) {
      auto in_var = impl->const_to_var[QueryConstant::From(in_col)];
      parent->col_id_to_var.emplace(in_col.Id(), in_var);
      if (out_col) {
        parent->col_id_to_var.emplace(out_col->Id(), in_var);
      }
    });

    BuildEagerRegion(impl, tuple, tuple, context, parent, nullptr);
  }

  CompleteProcedure(impl, init_proc, context);
}

}  // namespace hyde
