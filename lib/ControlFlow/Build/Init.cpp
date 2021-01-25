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

  const auto uncond_inserts_var = impl->global_vars.Create(
      impl->next_id++, VariableRole::kConditionRefCount);

  // Test that we haven't yet done an initialization.
  const auto test_and_set = impl->operation_regions.CreateDerived<ASSERT>(
      seq, ProgramOperation::kTestAndAdd);
  seq->regions.AddUse(test_and_set);

  test_and_set->cond_vars.AddUse(uncond_inserts_var);

  const auto cond_par = impl->parallel_regions.Create(test_and_set);
  test_and_set->body.Emplace(test_and_set, cond_par);

  // Go find all TUPLEs whose inputs are constants. We ignore constant refs,
  // as those are dataflow dependent.
  //
  // NOTE(pag): The dataflow builder ensures that TUPLEs are the only node types
  //            that can take all constants.
  for (auto tuple : impl->query.Tuples()) {
    const QueryView view(tuple);
    bool all_const = true;
    for (auto in_col : tuple.InputColumns()) {
      if (!in_col.IsConstant()) {
        all_const = false;
      }
    }

    if (!all_const) {
      continue;
    }

    const auto let = impl->operation_regions.CreateDerived<LET>(cond_par);
    cond_par->AddRegion(let);

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

  // Okay, now that we've processed all constant tuples, we want to look through
  // all views, and if any of them are conditional, then we want to go and
  // do any processing to insert their data, or possibly remove their data.

  // TODO(pag): For each condition, create two boolean flags:
  //              1) cross from 0 to 1+
  //              2) crossed from 1 to 0
  //
  //            - Have a mapping of all VIEWs which depend on the boolean flag
  //            - For each such view, re-evaluate the condition, and if it fails

  // TODO(pag): For each unique string of (+conds, -conds), create a twoboolean
  //            variable that tells us if we've crossed a threshold.

  CompleteProcedure(impl, init_proc, context);
}

}  // namespace hyde
