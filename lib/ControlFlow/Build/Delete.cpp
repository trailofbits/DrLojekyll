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

  auto par = impl->parallel_regions.Create(parent);
  parent->body.Emplace(parent, par);

  for (auto succ_view : view.Successors()) {
    const auto called_proc = GetOrCreateBottomUpRemover(
        impl, context, view, succ_view);
    const auto call = impl->operation_regions.CreateDerived<CALL>(
        impl->next_id++, par, called_proc);
    par->regions.AddUse(call);

    auto i = 0u;
    for (auto col : view.Columns()) {
      const auto var = parent->VariableFor(impl, col);
      assert(var != nullptr);
      call->arg_vars.AddUse(var);
      const auto param = called_proc->input_vars[i++];
      assert(var->Type() == param->Type());
      (void) param;
    }
  }
}

// The interesting thing with DELETEs is that they don't have a data model;
// whereas an INSERT might share its data model with its corresponding SELECTs,
// as well as with the node feeding it, a DELETE is more a signal saying "my
// successor must delete this data from /its/ model."
void CreateBottomUpDeleteRemover(ProgramImpl *impl, Context &context,
                                 QueryView view, PROC *proc) {

  auto del = QueryDelete::From(view);
  for (auto succ_view : view.Successors()) {
    assert(succ_view.IsMerge());

    const auto called_proc = GetOrCreateBottomUpRemover(
        impl, context, view, succ_view);
    const auto call = impl->operation_regions.CreateDerived<CALL>(
        impl->next_id++, proc, called_proc);

    auto i = 0u;
    for (auto col : del.Columns()) {
      const auto var = proc->VariableFor(impl, col);
      assert(var != nullptr);
      call->arg_vars.AddUse(var);
      const auto param = called_proc->input_vars[i++];
      assert(var->Type() == param->Type());
      (void) param;
    }

    call->ExecuteAlongside(impl, proc);
  }

  auto ret = impl->operation_regions.CreateDerived<RETURN>(
      proc, ProgramOperation::kReturnFalseFromProcedure);
  ret->ExecuteAfter(impl, proc);
}

}  // namespace hyde
