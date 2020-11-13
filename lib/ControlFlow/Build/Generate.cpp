// Copyright 2020, Trail of Bits. All rights reserved.

#include "Build.h"

namespace hyde {

// Build an eager region for a `QueryMap`.
void BuildEagerGenerateRegion(ProgramImpl *impl, QueryMap view,
                              Context &context, OP *parent) {
  const auto functor = view.Functor();
  assert(functor.IsPure());

  std::vector<QueryColumn> input_cols;
  std::vector<QueryColumn> output_cols;

  auto gen = impl->operation_regions.CreateDerived<GENERATOR>(parent, functor);
  UseRef<REGION>(parent, gen).Swap(parent->body);
  auto i = 0u;

  // Deal with the functor inputs and outputs.
  for (auto j = 0u, num_cols = functor.Arity(); i < num_cols; ++i) {
    const auto out_col = view.NthColumn(i);

    // Outputs correspond to `free`-attributed parameters.
    if (functor.NthParameter(i).Binding() == ParameterBinding::kFree) {
      const auto out_var = gen->defined_vars.Create(
          impl->next_id++, VariableRole::kFunctorOutput);
      out_var->query_column = out_col;
      gen->col_id_to_var.emplace(out_col.Id(), out_var);

    // Inputs correspond to `bound`-attributed parameters.
    } else {
      const auto in_col = view.NthInputColumn(j++);
      const auto in_var = parent->VariableFor(impl, in_col);
      gen->col_id_to_var.emplace(out_col.Id(), in_var);
      gen->used_vars.AddUse(in_var);
      if (!in_var->query_column) {
        in_var->query_column = in_col;
      }
      if (in_col.IsConstantOrConstantRef() && !in_var->query_const) {
        in_var->query_const = QueryConstant::From(in_col);
      }
    }
  }

  // Deal with the copied/attached columns, which emulate lexical scope. Here
  // we turn them back into actual lexical scope :-D
  for (auto j = 0u, num_cols = view.Columns().size(); i < num_cols; ++i, ++j) {
    auto out_col = view.NthCopiedColumn(j);
    auto in_col = view.NthInputCopiedColumn(j);
    const auto in_var = parent->VariableFor(impl, in_col);
    if (!in_var->query_column) {
      in_var->query_column = in_col;
    }
    if (in_col.IsConstantOrConstantRef() && !in_var->query_const) {
      in_var->query_const = QueryConstant::From(in_col);
    }

    gen->col_id_to_var.emplace(in_col.Id(), in_var);
    gen->col_id_to_var.emplace(out_col.Id(), in_var);
  }

  BuildEagerSuccessorRegions(impl, view, context, gen,
                             QueryView(view).Successors(), nullptr);
}

}  // namespace hyde
