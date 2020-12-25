// Copyright 2020, Trail of Bits. All rights reserved.

#include "Build.h"

namespace hyde {
namespace {

static GENERATOR *CreateGeneratorCall(ProgramImpl *impl, QueryMap view,
                                      ParsedFunctor functor, Context &context,
                                      REGION *parent, bool bottom_up) {
  std::vector<QueryColumn> input_cols;
  std::vector<QueryColumn> output_cols;

  auto gen = impl->operation_regions.CreateDerived<GENERATOR>(
      parent, functor, impl->next_id++, view.IsPositive());
  auto i = 0u;

  // Deal with the functor inputs and outputs.
  for (auto j = 0u, num_cols = functor.Arity(); i < num_cols; ++i) {
    const auto out_col = view.NthColumn(i);

    // Outputs correspond to `free`-attributed parameters.
    if (functor.NthParameter(i).Binding() == ParameterBinding::kFree) {
      const auto out_var = gen->defined_vars.Create(
          impl->next_id++, VariableRole::kFunctorOutput);
      out_var->query_column = out_col;
      gen->col_id_to_var[out_col.Id()] = out_var;

    // Inputs correspond to `bound`-attributed parameters.
    } else {
      assert(functor.NthParameter(i).Binding() == ParameterBinding::kBound);

      const auto in_col = view.NthInputColumn(j++);

      VAR *in_var = nullptr;
      if (bottom_up) {
        in_var = parent->VariableFor(impl, in_col);
        gen->col_id_to_var[out_col.Id()] = in_var;
      } else {
        in_var = parent->VariableFor(impl, out_col);
        gen->col_id_to_var[in_col.Id()] = in_var;
      }

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
    VAR *in_var = nullptr;

    if (bottom_up) {
      in_var = parent->VariableFor(impl, in_col);
      gen->col_id_to_var[out_col.Id()] = in_var;
    } else {
      in_var = parent->VariableFor(impl, out_col);
      gen->col_id_to_var[in_col.Id()] = in_var;
    }

    if (!in_var->query_column) {
      in_var->query_column = in_col;
    }
    if (in_col.IsConstantOrConstantRef() && !in_var->query_const) {
      in_var->query_const = QueryConstant::From(in_col);
    }
  }

  return gen;
}

}  // namespace

// Build an eager region for a `QueryMap`.
void BuildEagerGenerateRegion(ProgramImpl *impl, QueryMap map,
                              Context &context, OP *parent) {
  const QueryView view(map);

  const auto functor = map.Functor();
  assert(functor.IsPure());

  // TODO(pag): Think about requiring persistence of the predecessor, so that
  //            we always have the inputs persisted.

  const auto gen = CreateGeneratorCall(
      impl, map, functor, context, parent, true);
  parent->body.Emplace(parent, gen);
  parent = gen;

  // If we can receive deletions, and if we're in a path where we haven't
  // actually inserted into a view, then we need to go and do a differential
  // insert/update/check.
  DataModel * const model = impl->view_to_model[view]->FindAs<DataModel>();
  TABLE * const table = model->table;
  if (table) {
    parent = BuildInsertCheck(impl, view, context, parent, table,
                              view.CanReceiveDeletions(), view.Columns());
  }

  BuildEagerSuccessorRegions(
      impl, view, context, parent, view.Successors(), table);
}

// Build a bottom-up remover for generator calls.
void CreateBottomUpGenerateRemover(ProgramImpl *impl, Context &context,
                                   QueryMap map, ParsedFunctor functor,
                                   PROC *proc, TABLE *already_checked) {
  QueryView view(map);
  const auto gen = CreateGeneratorCall(
      impl, map, functor, context, proc, true);
  proc->body.Emplace(proc, gen);

  auto parent = impl->parallel_regions.Create(gen);
  gen->body.Emplace(gen, parent);

  const auto model = impl->view_to_model[view]->FindAs<DataModel>();
  if (model->table) {

    // The caller didn't already do a state transition, so we can do it.
    if (already_checked != model->table) {
      parent->regions.AddUse(BuildBottomUpTryMarkUnknown(
          impl, model->table, parent, view.Columns(),
          [&](PARALLEL *par) { parent = par; }));
    }
  }

  for (auto succ_view : view.Successors()) {

    assert(!succ_view.IsMerge());  // TODO(pag): I don't recall why.

    const auto called_proc = GetOrCreateBottomUpRemover(
        impl, context, view, succ_view, model->table);
    const auto call = impl->operation_regions.CreateDerived<CALL>(
        impl->next_id++, parent, called_proc);

    auto i = 0u;
    for (auto col : view.Columns()) {
      const auto var = call->VariableFor(impl, col);
      assert(var != nullptr);
      call->arg_vars.AddUse(var);

      const auto param = called_proc->input_vars[i++];
      assert(var->Type() == param->Type());
      (void) param;
    }

    parent->regions.AddUse(call);
  }

  auto ret = impl->operation_regions.CreateDerived<RETURN>(
      proc, ProgramOperation::kReturnFalseFromProcedure);
  ret->ExecuteAfter(impl, proc);
}

// Build a top-down checker on a map / generator.
void BuildTopDownGeneratorChecker(ProgramImpl *impl, Context &context,
                                  PROC *proc, QueryMap gen,
                                  std::vector<QueryColumn> &view_cols,
                                  TABLE *already_checked) {
  const auto functor = gen.Functor();
  assert(functor.IsPure());

  const QueryView view(gen);
  const QueryView pred_view = view.Predecessors()[0];
  const auto model = impl->view_to_model[view]->FindAs<DataModel>();
  const auto pred_model = impl->view_to_model[pred_view]->FindAs<DataModel>();

  auto series = impl->series_regions.Create(proc);
  proc->body.Emplace(proc, series);

  // This map was persisted, we're not going to re-execute the functor because
  // it is pure. We'll do a partial or complete scan to recover the inputs
  // needed by the predecessor to check the predecessor.
  if (model->table) {

    TABLE *table_to_update = model->table;

    // Maps are conditional, i.e. they might admit fewer tuples through
    // than they are fed, so they never share the data model with their
    // predecessors.
    assert(model->table != pred_model->table);

    auto call_pred = [&](PARALLEL *par) {
      par->regions.AddUse(ReturnTrueWithUpdateIfPredecessorCallSucceeds(
          impl, context, par, view, view_cols, table_to_update, pred_view,
          already_checked));
    };

    auto if_unknown = [&](ProgramImpl *, REGION *parent) -> REGION * {
      return BuildTopDownTryMarkAbsent(impl, model->table, parent,
                                       view_cols, call_pred);
    };

    series->regions.AddUse(BuildMaybeScanPartial(
        impl, view, view_cols, model->table, series,
        [&](REGION *parent) -> REGION * {
          if (already_checked != model->table) {
            already_checked = model->table;
            return BuildTopDownCheckerStateCheck(
                impl, parent, model->table, view_cols,
                BuildStateCheckCaseReturnTrue, BuildStateCheckCaseNothing,
                if_unknown);

          } else {
            table_to_update = nullptr;

            return ReturnTrueWithUpdateIfPredecessorCallSucceeds(
                impl, context, parent, view, view_cols, nullptr, pred_view,
                already_checked);
          }
        }));

  // The predecessor has a table; this is good because we can scan it, and
  // then re-check the condition with the found variables.
  } else if (pred_model->table) {

    std::vector<QueryColumn> pred_view_cols;

    // Get a list of output columns of the predecessor that we have.
    gen.ForEachUse(
        [&](QueryColumn in_col, InputColumnRole, std::optional<QueryColumn>) {
          if (auto in_var = proc->col_id_to_var[in_col.Id()];
              in_var && QueryView::Containing(in_col) == pred_view) {
            pred_view_cols.push_back(in_col);
          }
        });

    // This sucks; we don't really have any of the predecessor columns
    // available :-/
    if (pred_view_cols.empty()) {
      goto handle_worst_case;
    }

    series->regions.AddUse(BuildMaybeScanPartial(
        impl, pred_view, pred_view_cols, pred_model->table, series,
        [&](REGION *parent) -> REGION * {
          const auto call = CreateGeneratorCall(
              impl, gen, gen.Functor(), context, parent, false);
          call->body.Emplace(
              call, ReturnTrueWithUpdateIfPredecessorCallSucceeds(
                        impl, context, call, view, view_cols, nullptr,
                        pred_view, nullptr));
          return call;
        }));

  // This generator doesn't have persistent backing, nor does its predecessor,
  // so we have to call down to its predecessor. If we've already done the
  // check and the predecessor call returns `true` then we can return `true`.
  // Otherwise we have bigger problems :-(
  } else {
  handle_worst_case:

    std::vector<QueryColumn> bound_cols;
    for (auto col : gen.MappedColumns()) {
      if (functor.NthParameter(*(col.Index())).Binding() ==
          ParameterBinding::kBound) {
        bound_cols.push_back(col);
      }
    }

    // Try to figure out if we have sufficient columns to be able to invoke
    // the generator.
    bool done_check = true;
    for (auto out_of_in_col : bound_cols) {
      if (out_of_in_col.IsConstantRef()) {
        continue;  // The input column is constant, so we have it.

      // We don't have an input variable associated with the output column
      // which maps up with the input column associated with a `bound`-
      // attributed parameter of the functor.
      } else if (!proc->col_id_to_var.count(out_of_in_col.Id())) {
        done_check = false;
        break;
      }
    }

    // If we can do the check because we have enough input columns then we'll
    // trust things.
    if (done_check) {

      // Map in the variables.
      for (auto out_of_in_col : bound_cols) {
        auto out_var = series->VariableFor(impl, out_of_in_col);
        auto in_col = gen.NthInputColumn(*(out_of_in_col.Index()));
        series->col_id_to_var[in_col.Id()] = out_var;
      }

      const auto call = CreateGeneratorCall(
          impl, gen, gen.Functor(), context, series, false);
      series->regions.AddUse(call);

      call->body.Emplace(call, ReturnTrueWithUpdateIfPredecessorCallSucceeds(
          impl, context, call, view, view_cols, nullptr, pred_view, nullptr));

    // The issue here is that our codegen model of top-down checking treats
    // predecessors as black boxes. We really need to recover the columns from
    // the predecessor that are used for comparison, so that we can apply the
    // check to them, but we don't (yet) have a way of doing this. This is
    // also kind of a problem for
    } else {
      assert(false &&
             "TODO(pag): Handle worst case of top-down generator checker");
      series->regions.AddUse(BuildStateCheckCaseReturnFalse(impl, series));
    }
  }
}


}  // namespace hyde
