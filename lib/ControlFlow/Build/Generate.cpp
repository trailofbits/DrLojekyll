// Copyright 2020, Trail of Bits. All rights reserved.

#include "Build.h"

namespace hyde {
namespace {

static GENERATOR *CreateGeneratorCall(ProgramImpl *impl, QueryMap view,
                                      ParsedFunctor functor, Context &context,
                                      REGION *parent, bool bottom_up) {
  std::vector<QueryColumn> input_cols;
  std::vector<QueryColumn> output_cols;

  auto gen = impl->operation_regions.CreateDerived<GENERATOR>(parent, functor,
                                                              impl->next_id++);
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
      if (bottom_up && !in_var->query_const &&
          in_col.IsConstantOrConstantRef()) {
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
void BuildEagerGenerateRegion(ProgramImpl *impl, QueryView pred_view,
                              QueryMap map, Context &context, OP *parent_,
                              TABLE *last_table_) {
  const QueryView view(map);

  const auto functor = map.Functor();
  assert(functor.IsPure());

  auto [parent, pred_table, _] =
      InTryInsert(impl, context, pred_view, parent_, last_table_);

  // TODO(pag): Think about requiring persistence of the predecessor, so that
  //            we always have the inputs persisted.

  const auto gen =
      CreateGeneratorCall(impl, map, functor, context, parent, true);
  parent->body.Emplace(parent, gen);

  // If we're dealing with a negated generator, then make sure that children
  // end up in the `empty_body`.
  if (!map.IsPositive()) {
    parent = impl->operation_regions.CreateDerived<LET>(gen);
    gen->empty_body.Emplace(gen, parent);

  // In the positive case, child nodes will put themselves into `parent->body`.
  } else {
    parent = gen;
  }

  // NOTE(pag): A generator will never share the data model of its predecessor,
  //            otherwise it would be too accepting.
  BuildEagerInsertionRegions(impl, view, context, parent, view.Successors(),
                             nullptr);
}

// Build a bottom-up remover for generator calls.
void CreateBottomUpGenerateRemover(ProgramImpl *impl, Context &context,
                                   QueryMap map, ParsedFunctor functor,
                                   OP *parent, TABLE *already_checked) {
  const QueryView view(map);
  LET *let = nullptr;

  // If we do have a data model, then scan for the outputs and remove them
  // that way.
  //
  // TODO(pag): Have some sort of smarter decision, e.g. allowing people to
  //            mark functors as cheap or expensive (choose one).
  const auto model = impl->view_to_model[view]->FindAs<DataModel>();
  if (model->table) {

    const auto seq = impl->series_regions.Create(parent);
    parent->body.Emplace(parent, seq);

    // NOTE(pag): MAPs never share their data models with their predecessors.
    assert(model->table != already_checked);

    // We have input columns but we need to translate them to output columns
    // for the sake of the `BuildMaybeScanPartial`.
    std::vector<QueryColumn> view_cols;
    map.ForEachUse([&](QueryColumn in_col, InputColumnRole role,
                       std::optional<QueryColumn> out_col) {
      if (InputColumnRole::kFunctorInput == role) {
        const auto in_var = parent->VariableFor(impl, in_col);
        parent->col_id_to_var[out_col->Id()] = in_var;
        view_cols.push_back(*out_col);
      }
    });

    // Scan over the index.
    (void) BuildMaybeScanPartial(
        impl, view, view_cols, model->table, seq,
        [&](REGION *in_scan, bool) -> REGION * {
          assert(!let);
          let = impl->operation_regions.CreateDerived<LET>(in_scan);
          return let;
        });

  // If we don't have a data model then repeat the call to the generator.
  } else {
    const auto gen =
        CreateGeneratorCall(impl, map, functor, context, parent, true);
    parent->body.Emplace(parent, gen);

    // If this is a positive use then children go on the positive side;
    // otherwise they go in the 'empty' side.
    let = impl->operation_regions.CreateDerived<LET>(gen);
    if (map.IsPositive()) {
      gen->body.Emplace(gen, let);
    } else {
      gen->empty_body.Emplace(gen, let);
    }
  }

  // NOTE(pag): We'll let `BuildEagerRemovalRegions` mark the removal
  //            for us.
  BuildEagerRemovalRegions(impl, view, context, let, view.Successors(),
                           nullptr /* already_removed */);
}

// Build a top-down checker on a map / generator.
REGION *BuildTopDownGeneratorChecker(ProgramImpl *impl, Context &context,
                                     REGION *proc, QueryMap map,
                                     std::vector<QueryColumn> &view_cols,
                                     TABLE *already_checked) {
  const auto functor = map.Functor();
  assert(functor.IsPure());

  const QueryView view(map);

  // So, we have a tuple of data, we know it was part of this model, but this
  // model may be shared with another table. Our goal is this:
  //
  // First, figure out if this is a plausible tuple for this node. This means
  // invoking the functor, and checking if the outputs of the functor match
  // what we have in `view_cols`.
  //
  // If the outputs match, then we have a plausible tuple, and we then want to
  // take the inputs and copied columns and call down to our predecessor and
  // ask our predecessor if the plausible data was indeed provided.


  // Save the variables associated with the view; the var/id mapping may get
  // clobbered by `CreateGeneratorCall`.
  std::vector<VAR *> view_vars;
  for (auto col : view_cols) {
    view_vars.push_back(proc->VariableFor(impl, col));
  }

  const auto gen = CreateGeneratorCall(impl, map, functor, context, proc,
                                       false /* bottom_up */);

  // If nothing is generated, then it wasn't plausible!
  gen->empty_body.Emplace(gen, BuildStateCheckCaseReturnFalse(impl, gen));

  OP *parent = gen;

  // Outputs correspond to `free`-attributed parameters, and this functor
  // has at least one `free`-attributed parameter, which therefore must be
  // compared against what we have in `view_vars`.
  if (!functor.IsFilter()) {
    const auto cmp = impl->operation_regions.CreateDerived<TUPLECMP>(
        gen, ComparisonOperator::kEqual);
    gen->body.Emplace(gen, cmp);

    // Deal with the comparison failing.
    switch (functor.Range()) {

      // If we can get more outputs from this generator, then keep generating
      // until we fall through the procedure. Higher level code will inject
      // a terminating `return-false` for us.
      case FunctorRange::kOneOrMore:
      case FunctorRange::kZeroOrMore: break;

      // Emit a `return-false` if the comparison failed and we can never get
      // more than one set of outputs from this generator.
      case FunctorRange::kZeroOrOne:
      case FunctorRange::kOneToOne:
        cmp->false_body.Emplace(gen, BuildStateCheckCaseReturnFalse(impl, gen));
        break;
    }

    // `free`-attributed parameters are the outputs of the functor.
    for (auto i = 0u; i < functor.Arity(); ++i) {
      if (functor.NthParameter(i).Binding() == ParameterBinding::kFree) {
        cmp->lhs_vars.AddUse(view_vars[i]);
        cmp->rhs_vars.AddUse(gen->VariableFor(impl, map.NthColumn(i)));
      }
    }

    parent = cmp;
  }

  // We now have a plausible tuple. Go call the checker for our predecessor.
  // In the case that the predecessor checker returns false, we know that we
  // can return false because we've already checked that the output of the
  // functor matches our arguments.
  const QueryView pred_view = view.Predecessors()[0];
  parent->body.Emplace(
      parent,
      CallTopDownChecker(
          impl, context, parent, view, view_cols, pred_view, already_checked,
          [=](REGION *parent_if_true) -> REGION * {
            return BuildStateCheckCaseReturnTrue(impl, parent_if_true);
          },
          [=](REGION *parent_if_false) -> REGION * {
            return BuildStateCheckCaseReturnFalse(impl, parent_if_false);
          }));

  return gen;
}


}  // namespace hyde
