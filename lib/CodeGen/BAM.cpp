// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/CodeGen/BAM.h>

#include <bitset>
#include <cassert>
#include <functional>
#include <map>
#include <memory>
#include <ostream>
#include <set>
#include <sstream>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <iostream>

#include <drlojekyll/DataFlow/Format.h>
#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Lex/Format.h>
#include <drlojekyll/Parse/Format.h>
#include <drlojekyll/Parse/Parse.h>
#include <drlojekyll/Util/Compiler.h>
#include <drlojekyll/Util/Node.h>

// TODO(pag):
//    When calling operator() of some dest thing, inspect its incoming columns,
//    and fill in any constants / generators, i.e. run the generator on the
//    producer-side.
//
//    Pass in a worker ID to each operator() ? Will that help?
//
//    Add a `bool` argument to every function that tells us if we're adding or
//    removing the tuple.
//
//    Look for opportunities to co-locate data?
//        -> infer a new data model?
//
//    Try to create specialized JOINs? E.g. if we can push an operator all the
//    way to a join, we should:
//        1)  if the operator applies to one of the non-pivot columns, push to
//            down to one of the sources below the join
//        2)  if it applies to the pivot, push it down to all sources below
//            the join
//        3)  if the join is a cross-product, and if the operator applies to
//            two differently-sourced columns, then specialize the join by
//            that operator
//
//    On a multi-way join, e.g. X=Y=Z, and pushing an X, generate code for:
//      if count(Y=X) > count(Z=X):
//        for ... where Z=X:
//          for ... where Y=X:
//            <code>
//      else:
//        ...
//          ...
//            <code>
//
//    Where <code> is duplicated on each side, so that if <code> pushes to
//    a constraint, then the compiler can possibly hoist some of the constraints
//    out of the loop.
//
//    Check for mutability of parameters on the query representation, using
//    tainting, and doing so across INSERTs. The key here is that we should use
//    mutability as a requirement to decide where tuple DIFFs are even possible.
//
//    Analyze the how `bound` columns in queries relate to other columns, and
//    figure out what JOINs need to be eager, and which ones can be
//    demand-based.
//
//    Analyze how columns that are outputted from a join are used, and for each
//    user, possibly create an index that gives them easy query-based access if
//    we will have the join not push its results forward.
//
//    Have a way of marking some queries as `materialized` or `populate`, to
//    say that we should eagerly fill them up.
//
//    Taint nodes in terms of whether or not they can produce removals.
//    Propagate this taint. Use this to make the tuple processor more
//    state-machine like rather than going and redoing all the things.
//
//    For things like MAPs, FILTERs, and AGGREGATEs, we may want a column-
//    oriented input representation.
//
//      - Might be able to create a kind of symbolic arity estimator, where we
//        can then go and reason about the number of entries in a given column,
//        or leader columns.
//
//      - Think of things like: if a MAP goes an MAPs some columns, it would
//        so something like: iterate over the columns, producing a new set of
//        columns that could index into the old ones (for the pass-through'd)
//        columns, and tell us which to include or not.
//
//      - Ditto for filters.
//
//      - Think about whether or not it makes sense to store columns of things
//        in memory in a compressed representation always.
//
//    Try:
//      Consider breaking some of the control-flow in the MAPs, KVINDEXs,
//      AGGREGATEs to operate in batches. E.g. first run through a batch that is
//      approximately cache-sized and make sure every aggregate is initialized,
//      then re-go over the batch and do the aggregating. Goal is to minimize
//      the amount of branching in the inner loop by breaking it out into
//      multiple loops, eaach doing part of the control flow.
//
//    Check:
//      Is it possible have a deletion be received before its corresponding
//      addition?
//
//      I think it is possible to have deletions processed before recipients
//      see insertions.
//
//    Try:
//      Make it so that MERGEs don't do source tracking if they can't produce
//      deletions.
//
//    TODO:
//      Other specializations related to stuff not needing to support removal?
//
//    TODO:
//      Custom zero-arg specialization of a generator that increments a number
//      for yielding. I.e. `__gen.Emit()` increments the loop count.
//
//    TODO:
//      Code generation that has a notion of "regions" and uses C++ templates
//      and lambdas to recover nested for loop iteration.
//
//    TODO!!!??
//      If a JOIN is conditional on something then should it store up results
//      until the condition becomes true, then "release" all previously
//      unpublished tuples????
//
//      What actually is the right way of handling conditions... Should things
//      be put into wait lists? Are the conditions properly differential?
//
//      Can we sink them all the way down to messages, then remove the non-
//      message ones, so that we can put only the "key" things into a waitlist?
namespace hyde {
extern OutputStream *gOut;

namespace {

using ViewCaseMap = std::map<std::tuple<uint64_t, uint64_t, bool>, unsigned>;

// For aggregates and k/v indices, to assign them to a worker.
static unsigned gNextUnhomedInbox = 1;

// static unsigned TypeSize(TypeLoc loc) {
//  switch (loc.Kind()) {
//    case TypeKind::kSigned8:
//    case TypeKind::kUnsigned8:
//      return 1;
//    case TypeKind::kSigned16:
//    case TypeKind::kUnsigned16:
//      return 2;
//    case TypeKind::kSigned32:
//    case TypeKind::kUnsigned32:
//    case TypeKind::kFloat:
//      return 4;
//    case TypeKind::kSigned64:
//    case TypeKind::kUnsigned64:
//    case TypeKind::kDouble:
//    case TypeKind::kString:
//      return 8;
//    case TypeKind::kUUID:
//      return 16;
//    default:
//      assert(false);
//      return 0;
//  }
//}
//
//
//// Should this view use an inbox? Inboxes let us accumulate pending tuples to
//// apply in bulk. They also permit us to share tuples across threads.
// static bool UseInbox(QueryView view) {
//  return view.IsAggregate() ||
//         view.IsKVIndex() ||
//         view.IsMap() ||
//         view.IsJoin() ||
//         view.IsSelect();
//}

static const char *TypeName(TypeLoc loc) {
  switch (loc.Kind()) {
    case TypeKind::kSigned8:
      return "int8_t";
    case TypeKind::kSigned16:
      return "int16_t";
    case TypeKind::kSigned32:
      return "int32_t";
    case TypeKind::kSigned64:
      return "int64_t";
    case TypeKind::kUnsigned8:
      return "uint8_t";
    case TypeKind::kUnsigned16:
      return "uint16_t";
    case TypeKind::kUnsigned32:
      return "uint32_t";
    case TypeKind::kUnsigned64:
      return "uint64_t";
    case TypeKind::kFloat:
      return "float";
    case TypeKind::kDouble:
      return "double";
    case TypeKind::kBytes:
      return "::hyde::rt::Bytes";
    case TypeKind::kASCII:
      return "::hyde::rt::ASCII";
    case TypeKind::kUTF8:
      return "::hyde::rt::UTF8";
    case TypeKind::kUUID:
      return "::hyde::rt::UUID";
    default:
      assert(false);
      return "void";
  }
}

static const char *TypeName(QueryColumn col) {
  return TypeName(col.Type());
}

// Comment containing a variable's name, if any, to be placed after one of
// our column variable names.
static OutputStream &CommentOnVar(OutputStream &os, ParsedVariable var) {
  if (!var.IsUnnamed()) {
    os << "  /* " << var << " */";
  }
  return os;
}

// Comment containing a variable's name, if any, to be placed after one of
// our column variable names.
static OutputStream &CommentOnParam(OutputStream &os, ParsedParameter var) {
  if (!var.IsUnnamed()) {
    os << "  /* " << var << " */";
  }
  return os;
}

// Comment containing a variable's name, if any, to be placed after one of
// our column variable names.
static OutputStream &CommentOnCol(OutputStream &os, QueryColumn col) {
  return CommentOnVar(os, col.Variable());
}

// Return a binding string for a functor.
static std::string BindingPattern(ParsedFunctor decl) {
  const auto pattern = ParsedDeclaration(decl).BindingPattern();
  return std::string(pattern.begin(), pattern.end());
}

// We sometimes deal in terms of input columns, and so there may actually be
// repeats. We need to make sure that we name them all uniquely, and not in
// terms of their unique IDs, which may re-occur, hence the indexing scheme.
template <typename... Vecs>
static std::pair<std::unordered_map<QueryColumn, unsigned>,
                 std::unordered_map<unsigned, QueryColumn>>
ArgumentList(OutputStream &os, Vecs &... vecs) {
  auto i = 0u;
  std::unordered_map<QueryColumn, unsigned> col_to_index;
  std::unordered_map<unsigned, QueryColumn> index_to_col;

  auto sep = "\n    ";
  auto do_arg_list = [&](std::vector<QueryColumn> &cols) -> void {
    for (auto col : cols) {
      col_to_index.emplace(col, i);
      index_to_col.emplace(i, col);

      os << sep << TypeName(col) << " I" << i << CommentOnCol(os, col);
      sep = ",\n    ";
      ++i;
    }
  };

  int force[] = {(do_arg_list(vecs), 0)...};
  (void)force;

  return {col_to_index, index_to_col};
}

// Fill up a container, e.g. a vector of columns, given a range of elements.
template <typename T, typename Range>
static void FillContainerFromRange(T &container, Range range) {
  container.insert(container.end(), range.begin(), range.end());
}

// We keep track of a bitset of 0/1 reference counters for each view, so that
// we can know for each (source, dest) pairing, if source produced a given tuple
// to dest.
static void DefineMergeSources(OutputStream &os, Query query) {
  std::unordered_map<QueryView, std::vector<QueryView>> to_from;

  query.ForEachView([&](QueryView view) {
    view.ForEachUser(
        [&](QueryView target_view) { to_from[target_view].push_back(view); });
  });

  for (const auto &[to_view, from_views] : to_from) {
    if (!to_view.IsMerge() || !to_view.CanReceiveDeletions()) {
      continue;
    }

    auto i = 0u;

    const auto id = to_view.UniqueId();

    // The refcount type for a given node.
    os << "using RC" << id << " = std::bitset<" << from_views.size() << ">;\n";

    // The refcount that `from_view` will contribute to `to_view`.
    for (auto from_view : from_views) {
      const auto from_id = from_view.UniqueId();
      os << "static constexpr auto SOURCE_" << from_id << "_" << id << " = RC"
         << id << ".set(" << (i++) << ");\n";
    }
  }
  os << "\n";
}

// static void AddTuple(OutputStream &os, QueryView view, const char *indent,
//                     const char *stage, const char *wid="__wid") {
//  os << indent << "__stages[" << wid << " + " << stage << "]."
//     << view.UniqueId()
//     << ".emplace_back(\n" << indent << "    ";
//
//  auto sep = "";
//  for (const auto col : view.Columns()) {
//    os << sep << 'C' << col.UniqueId();
//    sep = ", ";
//  }
//
//  if (view.IsMerge()) {
//    os << sep << "__rc";
//  }
//
//  os << ");\n";
//}

// Figure out a specification for the input columns.
static std::vector<QueryColumn> InputColumnSpec(QueryView source_view,
                                                QueryView target_view) {
  std::vector<QueryColumn> input_cols;

  // Pass down the columns to a merge.
  if (target_view.IsMerge()) {
    for (auto merged_view : QueryMerge::From(target_view).MergedViews()) {
      if (merged_view == source_view) {
        FillContainerFromRange(input_cols, source_view.Columns());
        break;
      }
    }

    assert(!input_cols.empty());

    // Pass down the columns to a join.
  } else if (target_view.IsJoin()) {
    auto target_join = QueryJoin::From(target_view);

    std::vector<unsigned> join_col_to_view_col;
    join_col_to_view_col.resize(target_view.Columns().size());

    // Filter out the pivot columns.
    for (auto i = 0u; i < target_join.NumPivotColumns(); ++i) {
      for (auto pivot_col : target_join.NthInputPivotSet(i)) {
        assert(!pivot_col.IsConstant());

        if (QueryView::Containing(pivot_col) == source_view) {
          input_cols.push_back(pivot_col);
        }
      }
    }

    // Filter out the proposed values.
    for (auto i = 0u, max_i = target_join.NumMergedColumns(); i < max_i; ++i) {
      auto in_col = target_join.NthInputMergedColumn(i);
      assert(!in_col.IsConstant());
      if (QueryView::Containing(in_col) == source_view) {
        input_cols.push_back(in_col);
      }
    }

    // Tuple, mostly just passes things along.
  } else if (target_view.IsTuple()) {
    auto target_tuple = QueryTuple::From(target_view);
    FillContainerFromRange(input_cols, target_tuple.InputColumns());

    // Filter / constraints.
  } else if (target_view.IsConstraint()) {
    auto target_filter = QueryConstraint::From(target_view);
    input_cols.push_back(target_filter.InputLHS());
    input_cols.push_back(target_filter.InputRHS());
    FillContainerFromRange(input_cols, target_filter.InputCopiedColumns());

    // Mapping function.
  } else if (target_view.IsMap()) {
    auto target_map = QueryMap::From(target_view);

    FillContainerFromRange(input_cols, target_map.InputColumns());
    FillContainerFromRange(input_cols, target_map.InputCopiedColumns());

    // Aggregate function.
  } else if (target_view.IsAggregate()) {
    auto target_agg = QueryAggregate::From(target_view);

    FillContainerFromRange(input_cols, target_agg.InputGroupColumns());
    FillContainerFromRange(input_cols, target_agg.InputConfigurationColumns());
    FillContainerFromRange(input_cols, target_agg.InputAggregatedColumns());

  } else if (target_view.IsSelect()) {
    FillContainerFromRange(input_cols, target_view.Columns());
  }

  return input_cols;
}

// Send the output columns in `view` to all users.
static void CallUsers(OutputStream &os, QueryView view, ViewCaseMap &case_map,
                      const char *indent, const char *added, bool is_add) {
  if (!is_add) {
    assert(view.CanProduceDeletions());
  }

  view.ForEachUser([&](QueryView target_view) {
    auto case_key =
        std::make_tuple(view.UniqueId(), target_view.UniqueId(), is_add);
    assert(case_map.count(case_key));
    auto case_id = case_map[case_key];

    auto input_cols = InputColumnSpec(view, target_view);

    os << indent << "__stages[__wid].depth_" << target_view.Depth()
       << ".EmplaceBack(";

    auto sep = "";
    for (auto in_col : input_cols) {
      os << sep << '\n' << indent << "    ";
      if (in_col.IsConstant()) {
        os << "/* TODO constant */";  // TODO(pag): Handle constants!

      } else if (QueryView::Containing(in_col) == view) {
        os << "C" << in_col.UniqueId();

      } else {
        assert(false);
      }
      sep = ",";
    }
    os << ", " << case_id << ");\n";
  });
}

// Declare the function that will do aggregate some results.
static void DeclareAggregate(
    OutputStream &os, QueryAggregate agg,
    std::set<std::pair<uint64_t, bool>> &seen_functors) {
  const auto functor = agg.Functor();
  const std::pair<uint64_t, bool> key(
      functor.UniqueId(), QueryView::From(agg).CanReceiveDeletions());

  if (seen_functors.count(key)) {
    return;
  }
  seen_functors.insert(key);

  const auto binding_pattern = BindingPattern(functor);

  os << "#indef AGGREGATOR_" << functor.Name() << '_' << binding_pattern << '\n'
     << "#define AGGREGATOR_" << functor.Name() << '_' << binding_pattern
     << '\n'
     << "// Aggregator object (will collect results). This is a default\n"
     << "// implementation that should really be replaced via specialization\n"
     << "// via user code.\n"
     << "struct " << functor.Name() << '_' << binding_pattern << "_config {\n";

  for (auto param : functor.Parameters()) {
    os << "  " << TypeName(param.Type()) << ' ' << param.Name() << ';'
       << CommentOnParam(os, param) << '\n';
  }

  os << "};\n\n"
     << "// Return type when we ask for the summaries from an aggregate..\n"
     << "using " << functor.Name() << '_' << binding_pattern
     << "_result = std::tuple<";

  auto sep = "";
  for (auto param : functor.Parameters()) {
    switch (param.Binding()) {
      case ParameterBinding::kImplicit:
      case ParameterBinding::kMutable:
      case ParameterBinding::kFree:
        assert(false);
        break;

      case ParameterBinding::kSummary:
        os << sep << TypeName(param.Type());
        sep = ", ";
        break;

      case ParameterBinding::kBound:
      case ParameterBinding::kAggregate:
        break;
    }
  }

  // Forward declare the aggregator as returning the above structure.
  os << ">;\n"
     << "#endif\n\n";
}

// Generate code associated with an aggregate.
static void DefineAggregate(OutputStream &os, QueryAggregate agg,
                            ViewCaseMap case_map) {
  const auto view = QueryView::From(agg);
  assert(view.CanProduceDeletions());

  const auto id = agg.UniqueId();
  const auto summarizer = agg.Functor();
  const auto binding_pattern = BindingPattern(summarizer);

  os << "/* Aggregate: " << ParsedDeclaration(summarizer) << " \n";

  std::vector<QueryColumn> index_to_col;
  auto num_group_and_config_cols = 0u;

  for (auto col : agg.InputGroupColumns()) {
    os << " * Group var: " << col.Variable() << "\n";
    index_to_col.push_back(col);
    ++num_group_and_config_cols;
  }
  for (auto col : agg.InputConfigurationColumns()) {
    os << " * Config var: " << col.Variable() << "\n";
    index_to_col.push_back(col);
    ++num_group_and_config_cols;
  }
  for (auto col : agg.InputAggregatedColumns()) {
    os << " * Aggregating var: " << col.Variable() << "\n";
    index_to_col.push_back(col);
  }
  for (auto col : agg.SummaryColumns()) {
    os << " * Summary var: " << col.Variable() << "\n";
  }
  os << " */\n\n"
     << "// Initializer function for the aggregate configuration.\n"
     << "extern \"C\" void " << summarizer.Name() << '_' << binding_pattern
     << "_init(hyde_rt_";

  if (view.CanReceiveDeletions()) {
    os << "Differential";
  }

  os << "AggregateState<" << summarizer.Name() << '_' << binding_pattern
     << "_config> &";

  for (auto param : summarizer.Parameters()) {
    switch (param.Binding()) {
      case ParameterBinding::kBound:
        os << ", " << TypeName(param.Type());
        break;
      default:
        break;
    }
  }

  os << ");\n\n"
     << "// Function that adds a value from the aggregate.\n"
     << "extern \"C\" void " << summarizer.Name() << '_' << binding_pattern
     << "_add(hyde_rt_";

  if (view.CanReceiveDeletions()) {
    os << "Differential";
  }

  os << "AggregateState<" << summarizer.Name() << '_' << binding_pattern
     << "_config> &";

  for (auto param : summarizer.Parameters()) {
    switch (param.Binding()) {
      case ParameterBinding::kBound:
      case ParameterBinding::kAggregate:
        os << ", " << TypeName(param.Type());
        break;
      default:
        break;
    }
  }

  os << ");\n\n";

  if (view.CanReceiveDeletions()) {
    os << "// Function that removes a value from the aggregate.\n"
       << "extern \"C\" void " << summarizer.Name() << '_' << binding_pattern
       << "_remove(hyde_rt_DifferentialAggregateState<" << summarizer.Name()
       << '_' << binding_pattern << "_config> &";

    for (auto param : summarizer.Parameters()) {
      switch (param.Binding()) {
        case ParameterBinding::kBound:
        case ParameterBinding::kAggregate:
          os << ", " << TypeName(param.Type());
          break;
        default:
          break;
      }
    }

    os << ");\n\n";
  }

  // This means that we are grouping, and within each group, we need an
  // instance of a configured map.
  os << "// Maps config/group vars to aggregators.\n"
     << "static ::hyde::rt::Aggregate<\n    hyde_rt_";
  if (view.CanReceiveDeletions()) {
    os << "Differetial";
  }
  os << "AggregateState<" << summarizer.Name() << '_' << binding_pattern
     << "_config>  /* Configured aggregator type */";

  if (!agg.NumGroupColumns()) {
    os << ",\n    ::hyde::rt::NoGroupVars";
  } else {
    auto sep = ",\n    ::hyde::rt::GroupVars<";
    for (const auto col : agg.InputGroupColumns()) {
      os << sep << TypeName(col) << CommentOnCol(os, col);
      sep = ",\n               ";
    }
    os << ">";
  }

  if (!agg.NumConfigurationColumns()) {
    os << ",\n    ::hyde::rt::NoConfigVars";
  } else {
    auto sep = ",\n    ::hyde::rt::ConfigVars<";
    for (const auto col : agg.InputConfigurationColumns()) {
      os << sep << TypeName(col) << CommentOnCol(os, col);
      sep = ",\n               ";
    }
    os << ">";
  }

  //
  //  auto [col_to_index, index_to_col] = ArgumentList(
  //      os, group_cols, config_cols, agg_cols);

  os << "> MA" << id << ";\n\n"
     << "template <bool __added, typename Tuple>\n"
     << "void V" << agg.UniqueId() << "("
     << "\n    Stage *__stages, unsigned __wid, unsigned __wm,"
     << "\n    const Tuple &__tuple) noexcept {\n"
     << "  const auto [";

  auto sep = "";
  auto i = 0u;
  for (auto col : agg.GroupColumns()) {
    (void)col;
    os << 'G' << (i++);
    sep = ", ";
  }

  i = 0u;
  for (auto col : agg.ConfigurationColumns()) {
    os << sep << 'C' << col.UniqueId() << CommentOnCol(os, col);
    sep = ", ";
  }

  std::unordered_map<QueryColumn, unsigned> col_to_id;

  i = 0u;
  for (auto col : agg.InputAggregatedColumns()) {
    os << sep << 'A' << i;
    col_to_id.emplace(col, i++);
  }

  os << "] = __tuple;\n";

  for (auto [col, id] : col_to_id) {
    os << "  const auto C" << col.UniqueId() << " = A" << id << ';'
       << CommentOnCol(os, col) << '\n';
  }

  i = 0u;
  for (auto col : agg.GroupColumns()) {
    os << "  const auto C" << col.UniqueId() << " = G" << (i++) << ';'
       << CommentOnCol(os, col) << '\n';
  }

  i = 0u;
  for (auto col : agg.ConfigurationColumns()) {
    os << "  const auto C" << col.UniqueId() << " = C" << (i++) << ';'
       << CommentOnCol(os, col) << '\n';
  }

  sep = "";
  os << "\n  // Make sure this tuple is on the right worker.\n"
     << "  if (1 < __wm) {\n"
     << "    const auto __hash = ::hyde::Hash<";

  for (auto col : agg.GroupColumns()) {
    os << sep << TypeName(col);
    sep = ", ";
  }

  for (auto col : agg.ConfigurationColumns()) {
    os << sep << TypeName(col);
    sep = ", ";
  }

  os << ">(";

  i = 0u;
  sep = "";
  for (auto col : agg.GroupColumns()) {
    (void)col;
    os << sep << 'G' << (i++);
    sep = ", ";
  }

  i = 0u;
  for (auto col : agg.ConfigurationColumns()) {
    (void)col;
    os << sep << 'C' << (i++);
    sep = ", ";
  }

  os << ");\n"
     << "    if (auto __owid = __hash & __wm; __owid != __wid) {\n"
     << "      __stages[__owid].depth_" << view.Depth() << ".EmplaceBack(";

  i = 0u;
  sep = "";
  for (auto col : agg.GroupColumns()) {
    (void)col;
    os << sep << 'G' << (i++);
    sep = ", ";
  }

  i = 0u;
  for (auto col : agg.ConfigurationColumns()) {
    (void)col;
    os << sep << 'C' << (i++);
    sep = ", ";
  }

  i = 0u;
  for (auto col : agg.InputAggregatedColumns()) {
    (void)col;
    os << sep << 'A' << (i++);
    sep = ", ";
  }

  assert(case_map.count(std::make_tuple(id, id, true)));
  auto add_case_id = case_map[std::make_tuple(id, id, true)];

  if (view.CanReceiveDeletions()) {
    assert(case_map.count(std::make_tuple(id, id, false)));
    auto rem_case_id = case_map[std::make_tuple(id, id, false)];
    os << sep << "(__added ? " << add_case_id << " : " << rem_case_id
       << "));\n";
  } else {
    os << sep << "add_case_id);\n";
  }

  os << "      return 1u;\n"
     << "    }\n"
     << "  }\n\n"
     << "  const auto __agg = MA" << id << ".Get(";
  sep = "";
  i = 0u;
  for (auto col : agg.GroupColumns()) {
    (void)col;
    os << sep << 'G' << (i++);
    sep = ", ";
  }

  i = 0u;
  for (auto col : agg.ConfigurationColumns()) {
    (void)col;
    os << sep << 'C' << (i++);
    sep = ", ";
  }

  os << ");\n\n"
     << "  // If the aggregate isn't initialize it then this will be the "
        "first\n"
     << "  // result that it produces.\n"
     << "  if (!__agg->IsInitialized()) {\n"
     << "    if constexpr (!__added) {\n"
     << "      return 0u;  // Can't remove from an uninitialized aggregate.\n"
     << "    } else {\n"
     << "      " << summarizer.Name() << '_' << binding_pattern
     << "_init(__agg";

  i = 0u;
  for (auto col : agg.ConfigurationColumns()) {
    (void)col;
    os << ", C" << (i++);
  }

  os << ");\n"
     << "      const auto [";

  i = 0u;
  sep = "";
  for (auto col : agg.SummaryColumns()) {
    (void)col;
    os << sep << "S" << (i++);
    sep = ", ";
  }

  os << "] = __agg->Summarize();\n";

  i = 0u;
  for (auto col : agg.SummaryColumns()) {
    os << "      const auto C" << col.UniqueId() << " = S" << (i++) << ';'
       << CommentOnCol(os, col) << '\n';
  }

  CallUsers(os, view, case_map, "      ", "true", true);

  os << "    }\n\n"
     << "  // This aggregate has been initialized, so we need to get the old\n"
     << "  // summaries, then update the aggregate's state, then see if we "
        "need\n"
     << "  // to retract the old summary before sending the new summary "
        "values.\n"
     << "  } else {\n"
     << "    const auto [";

  i = 0u;
  sep = "";
  for (auto col : agg.SummaryColumns()) {
    (void)col;
    os << sep << "prev_S" << (i++);
    sep = ", ";
  }

  os << "] = __agg->Summarize();\n";

  if (view.CanReceiveDeletions()) {
    os << "    constexpr auto __update = __added ? " << summarizer.Name() << '_'
       << binding_pattern << "_add"
       << " : " << summarizer.Name() << '_' << binding_pattern << "_remove;\n"
       << "    __update(*__agg";
  } else {
    os << "    static_assert(__added);\n"
       << "    " << summarizer.Name() << '_' << binding_pattern
       << "_add(*__agg";
  }

  auto c = 0u;
  auto a = 0u;
  for (auto param : summarizer.Parameters()) {
    switch (param.Binding()) {
      case ParameterBinding::kBound:
        os << ", C" << (c++);
        break;
      case ParameterBinding::kAggregate:
        os << ", A" << (a++);
        break;
      default:
        break;
    }
  }

  os << ");\n"
     << "    const auto [";

  i = 0u;
  sep = "";
  for (auto col : agg.SummaryColumns()) {
    (void)col;
    os << sep << "S" << (i++);
    sep = ", ";
  }

  os << "] = __agg->Summarize();\n"
     << "    if (";

  i = 0u;
  sep = "";
  for (auto col : agg.SummaryColumns()) {
    (void)col;
    os << sep << "prev_S" << i << " == S" << i;
    sep = " && ";
    ++i;
  }

  os << ") {\n"
     << "      return 0u;  // No change in the summary value.\n"
     << "    }\n\n";

  i = 0u;
  for (auto col : agg.SummaryColumns()) {
    os << "    auto C" << col.UniqueId() << " = S" << (i++) << ';'
       << CommentOnCol(os, col) << '\n';
  }

  os << "\n    // Insertions (processed after deletions due to `PopTuple`).\n";

  // Do the adds first, because we are adding to a work list, and so we will
  // pop off the removals before processing the adds.
  CallUsers(os, view, case_map, "    ", "true", true);

  i = 0u;
  for (auto col : agg.SummaryColumns()) {
    os << "    C" << col.UniqueId() << " = prev_S" << (i++) << ';'
       << CommentOnCol(os, col) << '\n';
  }

  os << "\n    // Removals (processed before insertions).\n";

  CallUsers(os, view, case_map, "    ", "false", false);

  os << "  }\n"
     << "  return 0u;\n"
     << "}\n\n";
}

static void DeclareView(OutputStream &os, QueryView view) {
  os << "template <bool __added, typename Tuple>\n"
     << "static unsigned V" << view.UniqueId() << "("
     << "\n    Stage *__stages, unsigned __wid, unsigned __wm,"
     << "\n    const Tuple &__tuple";

  if (view.IsMerge() && view.CanReceiveDeletions()) {
    os << ", RC" << view.UniqueId();
  }

  os << ") noexcept;\n\n";
}

static void DefineTuple(OutputStream &os, QueryTuple tuple,
                        ViewCaseMap &case_map) {
  const auto view = QueryView::From(tuple);

  os << "/* Tuple; just forwards stuff to users. */\n"
     << "template <bool __added, typename Tuple>\n"
     << "unsigned V" << tuple.UniqueId() << "("
     << "\n    Stage *__stages, unsigned __wid, unsigned /* __wm */,"
     << "\n    const Tuple &__tuple) noexcept {\n"
     << "  const auto [";

  auto sep = "";
  for (auto col : tuple.Columns()) {
    os << sep << 'C' << col.UniqueId() << CommentOnCol(os, col);
    sep = ",\n              ";
  }

  os << "] = __tuple;\n";

  if (view.CanReceiveDeletions()) {
    assert(view.CanProduceDeletions());

    os << "  if constexpr (__added) {\n";
    CallUsers(os, QueryView::From(tuple), case_map, "    ", "true", true);
    os << "  } else {\n";
    CallUsers(os, QueryView::From(tuple), case_map, "    ", "false", false);
    os << "  }\n";

  } else {
    os << "  static_assert(__added);\n";
    CallUsers(os, QueryView::From(tuple), case_map, "  ", "true", true);
  }

  os << "  return 0u;\n"
     << "}\n\n";
}

// Define a KV Index that has no keys, and so just updates values.
static void DefineGlobalVarTail(OutputStream &os, QueryKVIndex view) {
  auto sep = "";
  const auto id = view.UniqueId();
  os << "  auto &__stage = __stages[(__wid * 2) + __added];\n"
     << "  if (1 < __wm) {\n"
     << "\n"
     << "    // These tuples are on the wrong worker, move them over.\n"
     << "    if (const auto __owid = " << (gNextUnhomedInbox++)
     << "u & __wm; __owid != __wid) {\n"
     << "      auto &__vec = __stage.V" << id << "_inbox;\n"
     << "      auto &__ovec = __stages[(__owid * 2) + __added].V" << id
     << "_inbox;\n"
     << "      if (__ovec.empty()) {\n"
     << "        __ovec.swap(__vec);\n"
     << "      } else {\n"
     << "        if (__ovec.size() < __vec.size()) {\n"
     << "          __ovec.swap(__vec);\n"
     << "        }\n"
     << "        __ovec.insert(__ovec.end(), __vec.begin(), __vec.end());\n"
     << "      }\n"
     << "      return;\n"
     << "    }\n"
     << "  }\n\n"
     << "  // We're in the right worker, go get the current state of the\n"
     << "  // global variable.\n"
     << "  const auto [";

  sep = "";
  for (auto col : view.ValueColumns()) {
    os << sep << "old_C" << col.UniqueId();
    sep = ", ";
  }
  os << ", __initialized] = KV" << id << ".Get();\n"
     << "  auto __present = __initialized;\n";

  for (auto col : view.ValueColumns()) {
    os << "  auto prev_C" << col.UniqueId() << " = old_C" << col.UniqueId()
       << ';' << CommentOnCol(os, col) << '\n';
  }
  os << '\n' << "  for (auto [";
  sep = "";
  for (auto col : view.ValueColumns()) {
    os << sep << "proposed_C" << col.UniqueId();
    sep = ", ";
  }
  os << "] : __stage.V" << id << "_inbox) {\n\n"
     << "    // If the values aren't yet initialized then take the proposed\n"
     << "    // values as the initial values.\n"
     << "    if (!__present) {\n"
     << "      __present = true;\n";
  for (auto col : view.ValueColumns()) {
    os << "      prev_C" << col.UniqueId() << " = proposed_C" << col.UniqueId()
       << ';' << CommentOnCol(os, col) << '\n';
  }

  os << "\n"
     << "    // We have prior values that we need to merge with.\n"
     << "    } else {\n";

  auto i = 0u;
  for (auto col : view.ValueColumns()) {
    os << "      prev_C" << col.UniqueId() << " = "
       << view.NthValueMergeFunctor(i++).Name() << "_merge(prev_C"
       << col.UniqueId() << ", proposed_C" << col.UniqueId() << ");\n";
  }

  os << "    }\n"
     << "  }\n\n"
     << "  // Check if we need to forward along the new state of the tuple.\n"
     << "  if (!__initialized";
  for (auto col : view.ValueColumns()) {
    os << " || old_C" << col.UniqueId() << " != prev_C" << col.UniqueId();
  }
  os << ") {\n"
     << "    __stage.V" << id << ".emplace_back(";
  sep = "";
  for (auto col : view.ValueColumns()) {
    os << sep << "prev_C" << col.UniqueId();
    sep = ", ";
  }

  os << ");\n"
     << "  }\n"
     << "}\n\n";
}

static void DefineKVIndex(OutputStream &os, QueryKVIndex kv,
                          ViewCaseMap case_map) {
  const auto view = QueryView::From(kv);

  auto i = 0u;
  for (auto col : kv.ValueColumns()) {
    auto functor = kv.NthValueMergeFunctor(i++);
    os << "extern \"C\" " << TypeName(col) << ' ' << functor.Name() << "_merge("
       << TypeName(col) << ", " << TypeName(col) << ");\n\n";
  }

  os << "// Mapping for maintaining key/value tuples.\n";

  if (view.CanReceiveDeletions()) {
    os << "static ::hyde::rt::DifferentialMap<\n    ";
  } else {
    os << "static ::hyde::rt::Map<\n    ";
  }

  const auto id = kv.UniqueId();

  auto sep = "";
  if (kv.NumKeyColumns()) {
    os << "::hyde::rt::KeyVars<";
    for (const auto col : kv.KeyColumns()) {
      os << sep << TypeName(col) << CommentOnCol(os, col);
      sep = ",\n                    ";
    }
    os << ">";
  } else {
    os << "::hyde::rt::EmptyKeyVars";
  }

  os << ",\n    ::hyde::ValueVars<";
  sep = "";
  for (const auto col : kv.ValueColumns()) {
    os << sep << TypeName(col) << CommentOnCol(os, col);
    sep = ",\n                      ";
  }
  os << ">> KV" << id << ";\n\n"
     << "/* Key/value mapping. */\n"
     << "template <bool __added>\n"
     << "static void V" << id << "(\n    Stage *__stages"
     << ",\n    unsigned __wid"
     << ",\n    unsigned __wm) noexcept {\n";

  if (!kv.NumKeyColumns()) {
    DefineGlobalVarTail(os, kv);
    return;
  }

  // We have key columns, so this will end up behaving similar to an aggregate.

  os << "  auto __has_prev = false;\n"
     << "  auto __is_first = false;\n"
     << "  auto &__vec = __stages[(__wid * 2) + __added];\n\n"
     << "  // Sort the inbox by the keys, maintaining the order of values.\n"
     << "  std::stable_sort(__vec->begin(), __vec->end(), "
     << "[] (auto __lhs, auto __rhs) -> bool {\n"
     << "    const auto __lhs_keys = std::make_tuple(";

  i = 0u;
  for (auto col : kv.KeyColumns()) {
    (void)col;
    os << sep << "std::get<" << (i++) << ">(__lhs)";
    sep = ", ";
  }
  os << ");\n"
     << "    const auto __rhs_keys = std::make_tuple(";

  sep = "";

  i = 0u;
  for (auto col : kv.KeyColumns()) {
    (void)col;
    os << sep << "std::get<" << (i++) << ">(__rhs)";
    sep = ", ";
  }
  os << ");\n"
     << "    return __lhs_keys < __rhs_keys;\n"
     << "  };\n\n";

  for (auto col : kv.Columns()) {
    os << "  " << TypeName(col) << " prev_" << col.UniqueId() << ", first_C"
       << col.UniqueId() << ';' << CommentOnCol(os, col) << '\n';
  }

  os << '\n'
     << "  const auto __update_prev = [=] (void) -> void {\n"
     << '\n'
     << "    // Only store this tuple if it's new\n"
     << "    if constexpr (__added) {\n"
     << "      if (__is_first || (";

  sep = "";
  for (auto col : kv.ValueColumns()) {
    os << sep << "first_C" << col.UniqueId() << " != prev_C" << col.UniqueId();
    sep = " && ";
  }

  os << '\n'
     << "        // We have a prior value for this tuple, so add a removal "
        "entry\n"
     << "        if (!__is_first) {\n"
     << "          __stages[(__wid * 2) + false].V" << id
     << "_inbox.emplace_back(";

  sep = "\n              ";
  for (auto col : kv.Columns()) {
    os << sep << "first_C" << col.UniqueId();
    sep = ",\n              ";
  }

  os << ");\n"
     << "        }\n"
     << "        KV" << id << ".Put(";

  sep = "";
  for (auto col : kv.Columns()) {
    os << sep << "prev_C" << col.UniqueId();
    sep = ", ";
  }

  os << ");\n"
     << "        __stages[(__wid * 2) + true].V" << id << ".emplace_back(";
  sep = "\n            ";
  for (auto col : kv.Columns()) {
    os << sep << "prev_C" << col.UniqueId();
    sep = ",\n            ";
  }

  os << ");\n"
     << "      }\n\n"
     << "    // Can't remove a tuple that doesn't exist.\n"
     << "    } else if (__is_first) {\n"
     << "      return;\n\n"
     << "    // Remove the tuple if the requests value for deletion matches "
        "the\n"
     << "    // present value of the tuple.\n"
     << "    } else if (";

  sep = "";
  for (auto col : kv.ValueColumns()) {
    os << sep << "first_C" << col.UniqueId() << " == prev_C" << col.UniqueId();
    sep = " && ";
  }

  os << ") {\n"
     << "      KV" << id << ".Erase(";

  sep = "\n          ";
  for (auto col : kv.KeyColumns()) {
    os << sep << "prev_C" << col.UniqueId();
    sep = ",\n          ";
  }

  os << ");\n"
     << "      __stages[(__wid * 2) + false].V" << id << ".emplace_back(";
  sep = "\n          ";
  for (auto col : kv.Columns()) {
    os << sep << "prev_C" << col.UniqueId();
    sep = ",\n          ";
  }

  os << ");\n"
     << "    }\n"
     << "  }\n\n"
     << "  for (const auto [";

  sep = "";
  for (auto col : kv.Columns()) {
    os << sep << "proposed_C" << col.UniqueId();
    sep = ", ";
  }

  os << "] : __vec) {\n\n"
     << "    // Check to see if we need to send this tuple to another worker.\n"
     << "    if (1 < __wm) {\n"
     << "      const auto __hash = Hash<";
  sep = "";
  for (auto col : kv.KeyColumns()) {
    os << sep << TypeName(col);
    sep = ", ";
  }
  os << ">::Compute(";
  sep = "";
  for (auto col : kv.KeyColumns()) {
    os << sep << "proposed_C" << col.UniqueId();
    sep = ", ";
  }
  os << ");\n"
     << "      if (const auto __owid = __hash & __wm; __owid != __wid) {\n"
     << "        __stages[(__owid * 2) + __added].emplace_back(";

  sep = "\n            ";
  for (auto col : kv.Columns()) {
    os << sep << "proposed_C" << col.UniqueId() << CommentOnCol(os, col);
    sep = ",\n            ";
  }

  os << ");\n"
     << "      }\n"
     << "    }\n\n"
     << "    if (__has_prev) {\n\n"
     << "      // The last processed tuple doesn't share the same key columns\n"
     << "      // as this tuple.\n"
     << "      if (";

  sep = "";
  for (auto col : kv.KeyColumns()) {
    os << sep << "prev_C" << col.UniqueId() << " != proposed_C"
       << col.UniqueId();
    sep = " || ";
  }
  os << ") {\n"
     << "        __update_prev();\n"
     << "        __has_prev = false;\n\n"
     << "      // Merge with the prior values. Only do this if we're adding.\n"
     << "      } else if contexpr (__added) {\n";

  i = 0u;
  for (auto col : kv.ValueColumns()) {
    os << "        prev_C" << col.UniqueId() << " = "
       << kv.NthValueMergeFunctor(i++).Name() << "_merge(\n"
       << "            prev_C" << col.UniqueId() << ", proposed_C"
       << col.UniqueId() << ");\n";
  }

  // TODO(pag): Think about whether k/v removal should remove by key matching
  //            alone (the usual semantics, or by key+value matching (what we
  //            do here).
  os << "        continue;\n\n"
     << "      // We're trying to remove the K/V mapping. We only process "
        "removals\n"
     << "      // where the values match with the first values.\n"
     << "      } else if (__is_first || (";

  sep = "";
  for (auto col : kv.ValueColumns()) {
    os << sep << "proposed_C" << col.UniqueId() << " != first_C"
       << col.UniqueId();
    sep = " || ";
  }

  os << ")) {\n"
     << "        continue;\n"
     << "      }\n"
     << "    }\n\n"
     << "    __has_prev = true;\n"
     << "    const auto [";

  sep = "";
  for (auto col : kv.ValueColumns()) {
    os << sep << "curr_C" << col.UniqueId();
    sep = ", ";
  }

  os << sep << "__initialized] =\n        KV" << id << ".Get(";
  sep = "";
  for (auto col : kv.KeyColumns()) {
    os << sep << "proposed_C" << col.UniqueId();
    sep = ", ";
  }
  os << ");\n"
     << "    __is_first = !__initialized;\n\n"
     << "    if (__initialized) {\n"
     << "      if constexpr (__added) {\n";

  i = 0u;
  for (auto col : kv.ValueColumns()) {
    os << "        first_C" << col.UniqueId() << " = curr_C" << col.UniqueId()
       << ';' << CommentOnCol(os, col) << '\n'
       << "        prev_C" << col.UniqueId() << " = "
       << kv.NthValueMergeFunctor(i++).Name() << "_merge(curr_C"
       << col.UniqueId() << ", proposed_C" << col.UniqueId() << ");\n";
  }
  os << "      } else {\n";
  for (auto col : kv.ValueColumns()) {
    os << "        first_C" << col.UniqueId() << " = proposed_C"
       << col.UniqueId() << ';' << CommentOnCol(os, col) << '\n'
       << "        prev_C" << col.UniqueId() << " = proposed_C"
       << col.UniqueId() << ";\n";
  }

  os << "      }\n"
     << "    } else {\n";
  for (auto col : kv.ValueColumns()) {
    os << "      first_C" << col.UniqueId() << " = proposed_C" << col.UniqueId()
       << ';' << CommentOnCol(os, col) << '\n'
       << "      prev_C" << col.UniqueId() << " = proposed_C" << col.UniqueId()
       << ";\n";
  }

  os << "    }\n"
     << "  }\n"
     << "  if (__has_prev) {\n"
     << "    __update_prev();\n"
     << "  }\n"
     << "}\n\n";
}

static void DefineMerge(OutputStream &os, QueryMerge view) {
  (void)os;
  (void)view;
  //
  //  const auto id = view.UniqueId();
  //
  //  os << "// Set for tracking unique elements in the merge.\n"
  //     << "static ::hyde::rt::Set<RC" << id;
  //
  //  for (const auto col : view.Columns()) {
  //    os << ",\n                       " << TypeName(col)
  //       << CommentOnCol(os, col);
  //  }
  //
  //  os << "> S" << id << ";\n\n"
  //     << "/* Merge; forwards unique tuples to users, and reference counts\n"
  //     << " * tuples in terms of their source. */\n"
  //     << "template <bool __added>\n"
  //     << "static void V" << view.UniqueId() << '(';
  //
  //  std::vector<QueryColumn> cols;
  //  FillContainerFromRange(cols, view.Columns());
  //
  //  auto [col_to_id, id_to_col] = ArgumentList(os, cols);
  //
  //  os << ",\n    Stage *__stages"
  //     << ",\n    unsigned __wid"
  //     << ",\n    unsigned __wm) noexcept {\n";
  //
  //  auto sep = "\n    ";
  //  for (auto col : view.Columns()) {
  //    os << sep << TypeName(col) << " C" << col.UniqueId()
  //       << CommentOnCol(os, col);
  //    sep = ",\n    ";
  //  }
  //
  //  os << sep << "RC" << id << " __source"
  //     << "\n, Stage *__stages"
  //     << ",\n    unsigned __wid"
  //     << ",\n    unsigned __wm) noexcept {\n"
  //     << "  if (1 < __wm) {\n"
  //     << "    const auto __hash = Hash<";
  //
  //  sep = "";
  //  for (auto col : view.Columns()) {
  //    os << sep << TypeName(col);
  //    sep = ", ";
  //  }
  //
  //  os << ">::Update(";
  //  sep = "\n        ";
  //  for (auto col : view.Columns()) {
  //    os << sep << 'C' << col.UniqueId() << CommentOnCol(os, col);
  //    sep = ",\n        ";
  //  }
  //
  //  os << ");\n"
  //     << "    // Send this tuple to another worker.\n"
  //     << "    if (const auto __owid = __hash & __wm; __owid != __wid) {\n";
  //
  //  AddTuple(os, QueryView::From(view), "      ", "__added", "__owid");
  //
  //  os << "      return;\n"
  //     << "    }\n"
  //     << "  }\n"
  //     << "  if constexpr (__added) {\n"
  //     << "    if (!S" << id << ".Add(";
  //
  //  sep = "";
  //  for (auto col : view.Columns()) {
  //    os << sep << 'C' << col.UniqueId();
  //    sep = ", ";
  //  }
  //
  //  os << sep << "__source)) {\n"
  //     << "      return;  // Already added.\n"
  //     << "    }\n"
  //     << "  } else {\n"
  //     << "    if (!S" << id << ".Remove(";
  //  sep = "";
  //  for (auto col : view.Columns()) {
  //    os << sep << 'C' << col.UniqueId();
  //    sep = ", ";
  //  }
  //  os << sep << "__source)) {\n"
  //     << "      return;  // Not fully removed.\n"
  //     << "    }\n"
  //     << "  }\n";
  //  AddTuple(os, QueryView::From(view), "  ", "__added");
  //  os << "}\n\n";
}

static void DeclareGenerator(OutputStream &os, QueryMap map,
                             std::unordered_set<uint64_t> &seen_functors) {
  const auto functor = map.Functor();
  if (seen_functors.count(functor.UniqueId())) {
    return;
  }
  seen_functors.insert(functor.UniqueId());

  const auto binding_pattern = BindingPattern(functor);

  // Declare the tuple type as a structure.
  os << "\n"
     << "#ifndef FUNCTOR_" << functor.Name() << '_' << binding_pattern << '\n'
     << "#define FUNCTOR_" << functor.Name() << '_' << binding_pattern << '\n'
     << "struct " << functor.Name() << '_' << binding_pattern << "_tag {};\n";

  if (!map.IsFilterLike()) {
    os << "using " << functor.Name() << '_' << binding_pattern
       << "_generator = ::hyde::rt::Generator<";

    auto sep = "";
    for (auto param : functor.Parameters()) {
      switch (param.Binding()) {
        case ParameterBinding::kImplicit:
        case ParameterBinding::kMutable:
        case ParameterBinding::kSummary:
        case ParameterBinding::kAggregate:
          assert(false);
          break;

        case ParameterBinding::kBound:
          break;

        case ParameterBinding::kFree:
          os << sep << TypeName(param.Type());
          sep = ", ";
          break;
      }
    }

    // Forward declare the aggregator as returning a generator of the above
    // structure type.
    os << ">;\n";
  }

  os << "extern \"C\" ";
  if (map.IsFilterLike()) {
    os << "bool ";
  } else {
    os << "void ";
  }
  os << functor.Name() << '_' << binding_pattern << "(";

  auto sep = "\n    ";

  if (!map.IsFilterLike()) {
    os << sep << functor.Name() << '_' << binding_pattern << "_generator &";
  }

  for (auto param : functor.Parameters()) {
    if (param.Binding() == ParameterBinding::kBound) {
      os << sep << TypeName(param.Type()) << CommentOnParam(os, param);
      sep = ",\n    ";
    }
  }

  os << ");\n"
     << "#endif\n\n";
}

static void DefineMap(OutputStream &os, QueryMap map, ViewCaseMap &case_map) {
  const auto view = QueryView::From(map);
  const auto id = map.UniqueId();
  const auto functor = map.Functor();
  const auto binding_pattern = BindingPattern(functor);

  os << "/* Map: " << ParsedDeclaration(map.Functor()) << "\n";
  for (auto col : map.InputColumns()) {
    os << " * Input column: " << col.Variable().Name() << '\n';
  }
  for (auto col : map.InputCopiedColumns()) {
    os << " * Input copied column: " << col.Variable().Name() << '\n';
  }
  for (auto col : map.MappedColumns()) {
    os << " * Output column: " << col.Variable().Name() << '\n';
  }
  for (auto col : map.CopiedColumns()) {
    os << " * Output copied column: " << col.Variable().Name() << '\n';
  }

  os << " */\n\n";

  if (!functor.IsPure()) {
    os << "#ifndef FUNCTOR_KV_" << functor.Name() << '_' << binding_pattern
       << '\n'
       << "#define FUNCTOR_KV_" << functor.Name() << '_' << binding_pattern
       << '\n';
    if (map.IsFilterLike()) {
      os << "// The functor isn't pure, and it has no free parameters, so it\n"
         << "// behaves like a filter\n";

      if (view.CanReceiveDeletions()) {
        os << "static ::hyde::rt::DifferentialMap<\n    ";
      } else {
        os << "static ::hyde::rt::Map<\n    ";
      }

    } else {
      os << "// The functor isn't pure, thus we need to record the outputs\n"
         << "// of each execution so that upon re-executing, we can send\n"
         << "// differential updates about what has changed since the last\n"
         << "// execution.\n";

      if (view.CanReceiveDeletions()) {
        os << "static ::hyde::rt::DifferentialMultiMap<\n    ";
      } else {
        os << "static ::hyde::rt::MultiMap<\n    ";
      }
    }

    auto sep = "";
    os << "::hyde::rt::KeyVars<";
    for (const auto param : functor.Parameters()) {
      if (ParameterBinding::kBound == param.Binding()) {
        os << sep << TypeName(param.Type()) << CommentOnParam(os, param);
        sep = ",\n                    ";
      }
    }

    os << ">,\n    ::hyde::ValueVars<";
    if (map.IsFilterLike()) {
      os << "bool";

    } else {
      sep = "";
      for (const auto param : functor.Parameters()) {
        if (ParameterBinding::kFree == param.Binding()) {
          os << sep << TypeName(param.Type()) << CommentOnParam(os, param);
          sep = ",\n                      ";
        }
      }
    }
    os << ">> KV_" << functor.Name() << '_' << binding_pattern << ";\n";
    os << "#endif\n\n";
  }

  os << "template <bool __added, typename Tuple>\n"
     << "void V" << id << "("
     << "\n    Stage *__stages, unsigned __wid, unsigned,"
     << "\n    const Tuple &__tuple) noexcept {\n";

  std::vector<QueryColumn> input_cols;
  std::vector<QueryColumn> bound_cols;
  std::vector<QueryColumn> free_cols;
  std::unordered_map<QueryColumn, QueryColumn> out_to_in;
  std::unordered_map<QueryColumn, unsigned> col_to_id;

  auto i = 0u;
  auto o = 0u;
  auto b = 0u;

  for (auto param : functor.Parameters()) {
    switch (param.Binding()) {
      case ParameterBinding::kImplicit:
      case ParameterBinding::kMutable:
      case ParameterBinding::kAggregate:
      case ParameterBinding::kSummary:
        assert(false);
        break;

      case ParameterBinding::kBound:
        input_cols.push_back(map.NthInputColumn(b++));
        col_to_id.emplace(input_cols.back(), i++);
        bound_cols.push_back(map.Columns()[o++]);
        out_to_in.emplace(bound_cols.back(), input_cols.back());
        break;

      case ParameterBinding::kFree:
        free_cols.push_back(map.Columns()[o++]);
        break;
    }
  }

  for (auto in_copied_col : map.InputCopiedColumns()) {
    input_cols.push_back(in_copied_col);
    out_to_in.emplace(map.Columns()[o++], in_copied_col);
    col_to_id.emplace(in_copied_col, i++);
  }

  i = 0u;
  auto sep = "";
  os << "  const auto [";
  for (auto col : input_cols) {
    col_to_id.emplace(col, i);
    os << sep << 'I' << col_to_id[col];
    sep = ", ";
  }
  os << "] = __tuple;\n";

  for (auto [out_col, in_col] : out_to_in) {
    os << "  const auto C" << out_col.UniqueId() << " = I" << col_to_id[in_col]
       << ';' << CommentOnCol(os, out_col) << '\n';
  }

  // If this function isn't pure then that means that we can produce
  // differential updates that revoke prior results.
  if (!functor.IsPure()) {
    os << '\n'
       << "  auto &__kv = KV_" << functor.Name() << '_' << binding_pattern
       << ";\n";

    // It's a predicate that isn't pure.
    if (map.IsFilterLike()) {
      sep = "";
      os << "  const auto [";

      if (map.IsFilterLike()) {
        os << "__prev_res";
        sep = ", ";
      } else {
        for (auto col : free_cols) {
          os << sep << "prev_C" << col.UniqueId();
          sep = ", ";
        }
      }

      os << sep << "__has_prev] = __kv.Get(";

      sep = "\n      ";
      for (auto col : bound_cols) {
        os << sep << "C" << col.UniqueId() << CommentOnCol(os, col);
        sep = ",\n      ";
      }

      os << ");\n\n";

      // If we have received a deletion request on an impure functor then we
      // don't need need to invoke the functor; instead we just need to delete
      // any prior saved values. In the case of a predicate/filter-like functor,
      // we only need to forward the deletion to readers if the last invocation
      // returned `true`, i.e. if the users of the columns even received bound
      // columns.
      if (view.CanReceiveDeletions()) {
        os << "  // If we're deleting things then go delete the prior record.\n"
           << "  if constexpr (!__added) {\n"
           << "    if (!__has_prev) {\n"
           << "      return;  // Nothing to delete.\n\n"
           << "    } else if (__prev_res) {\n";

        CallUsers(os, QueryView::From(map), case_map, "      ", "false", false);

        os << "    }\n"
           << "    __kv.Erase(";

        sep = "";
        for (auto col : bound_cols) {
          os << sep << "C" << col.UniqueId();
          sep = ", ";
        }

        os << ");\n"
           << "    return;\n"
           << "  }\n\n";
      }

      // It's a mapping function.
    } else {
      os << "  auto &__prev_gen = __stages[__wid].prev_G" << id << ";\n"
         << "  __prev_gen.Clear();\n"
         << "  auto __has_prev = __kv.Get(";

      sep = "\n      ";
      for (auto col : bound_cols) {
        os << sep << "C" << col.UniqueId() << CommentOnCol(os, col);
        sep = ",\n      ";
      }

      os << sep << "__prev_gen);\n"
         << "  __prev_gen.Sort();\n"
         << "  auto __prev_it = __prev_gen.begin();\n"
         << "  auto __prev_end = __prev_gen.end();\n\n";

      // If we have received a deletion request on an impure functor then we
      // don't need need to invoke the functor; instead we just need to delete
      // any prior saved values.
      if (view.CanReceiveDeletions()) {
        os << "  // If we're deleting things then go delete all prior "
              "records.\n"
           << "  if constexpr (!__added) {\n"
           << "    if (!__has_prev) {\n"
           << "      return;  // Nothing to delete.\n"
           << "    }\n"
           << "    __kv.Erase(";

        sep = "";
        for (auto col : bound_cols) {
          os << sep << "C" << col.UniqueId();
          sep = ", ";
        }
        os << ");\n"
           << "    for (auto [";

        sep = "";
        for (auto col : free_cols) {
          os << sep << "C" << col.UniqueId();
          sep = ", ";
        }

        os << "] : __prev_gen) {\n";
        CallUsers(os, QueryView::From(map), case_map, "      ", "false", false);
        os << "    }\n"
           << "    return;\n"
           << "  }\n\n";
      }
    }
  }

  // This function is going to behave like a filter, i.e. it's going to
  // tell us whether or not to admit the tuple along.
  if (map.IsFilterLike()) {
    os << "  const auto __res = ::hyde::rt::InlineDefinition<" << functor.Name()
       << '_' << binding_pattern << "_tag>(" << functor.Name() << '_'
       << binding_pattern << ")(\n";

    sep = "      ";
    for (auto col : bound_cols) {
      os << sep << col.UniqueId();
      sep = ", ";
    }

    os << ");\n\n";

    if (functor.IsPure()) {
      os << "  if (__res) {\n";

      // Not pure; we need to manage the differentials.
    } else {
      // We can receive deletions.
      if (view.CanReceiveDeletions()) {
        os << "  if constexpr (!__added) {\n"
           << "    if (!__has_prev || !__prev_res) {\n"
           << "      return;  // Nothing to remove, or removal would be "
              "redundant.\n"
           << "    }\n"
           << "  } else ";

        // We can't receive any deletions, so we can detect in advance when
        // nothing has changed.
      } else {
        os << "  ";
      }
      os << "if ((!__has_prev && !__res) ||\n"
         << "      (__has_prev && __prev_res == __res)) {\n"
         << "    return;  // Nothing to do or change.\n"
         << "  }\n\n";

      os << "  if (__res) {\n";
    }

    // Just a normal mapping function.
  } else {
    os << "  auto &__gen = __stages[__wid].G" << id << ";\n"
       << "  __gen.Clear();\n"
       << "  ::hyde::rt::InlineDefinition<" << functor.Name() << '_'
       << binding_pattern << "_tag>(" << functor.Name() << '_'
       << binding_pattern << ")(\n      __gen";

    for (auto col : bound_cols) {
      os << ", " << col.UniqueId();
    }

    os << ");\n"
       << "  __gen.Sort();\n\n"
       << "  // Loop for each produced tuple.\n"
       << "  for (const auto __tuple : __gen) {\n";
  }

  // If this is an actual mapping function, i.e. it produces many tuples, and
  // if it's not pure, and if previously produced outputs, then only process
  // the outputs that
  if (!map.IsFilterLike() && !functor.IsPure()) {
    os << '\n'
       << "    // Don't process any tuples that are shared with the prior "
          "execution.\n"
       << "    while (__prev_it < __prev_end) {\n"
       << "      const auto __prev_tuple = *__prev_it;\n"
       << "      if (__prev_tuple == __tuple) {\n"
       << "        ++__prev_it;\n"
       << "        goto __next_iter;\n"
       << "      } else if (__prev_tuple < __tuple) {\n"
       << "        ++prev_it;\n"
       << "      } else {\n"
       << "        goto __process_tuple;\n"
       << "      }\n"
       << "    }\n\n"
       << "  __process_tuple:\n";
  }

  sep = "";
  os << "    const auto [";
  for (auto col : free_cols) {
    os << sep << 'C' << col.UniqueId();
    sep = ", ";
  }
  os << "] = __tuple;\n";

  if (view.CanProduceDeletions()) {
    os << "    if constexpr (__added) {\n";
    CallUsers(os, QueryView::From(map), case_map, "      ", "true", true);
    os << "    } else {\n";
    CallUsers(os, QueryView::From(map), case_map, "      ", "false", false);
    os << "    }\n";
  } else {
    os << "    static_assert(__added);\n";
    CallUsers(os, QueryView::From(map), case_map, "    ", "true", true);
  }

  os << "  __next_iter:\n"
     << "    continue;\n"
     << "  }\n";

  // We've now processed all additions, now we need to process removals
  // where the prior values differ. We process them after because they are
  // pushed onto the back of the work list, and thus will be processed
  // first.
  if (!map.IsFilterLike() && !functor.IsPure()) {
    os << '\n'
       << "  // Remove tuples that weren't produced by the most recent "
          "execution.\n"
       << "  __prev_gen.Erase(__gen);\n"
       << "  for (const auto [";
    sep = "";
    for (auto col : free_cols) {
      os << sep << 'C' << col.UniqueId();
      sep = ", ";
    }
    os << "] : __prev_gen) {\n";
    CallUsers(os, QueryView::From(map), case_map, "    ", "false", false);
    os << "  }\n";
  }

  os << "  return 0u;\n"
     << "}\n\n";
}

static void DefineConstraint(OutputStream &os, QueryConstraint filter,
                             ViewCaseMap &case_map) {
  const auto view = QueryView::From(filter);

  os << "/* Constraint; conditionally forwards stuff to users. */\n"
     << "template <bool __added, typename Tuple>\n"
     << "void V" << filter.UniqueId() << "("
     << "\n    Stage *__stages, unsigned __wid, unsigned __wm,"
     << "\n    const Tuple &__tuple) noexcept {\n"
     << "  const auto [__lhs, __rhs";

  const auto lhs = filter.LHS();
  const auto rhs = filter.RHS();

  for (auto col : filter.CopiedColumns()) {
    os << ", C" << col.UniqueId();
  }

  os << "] = __tuple;\n"
     << "  if (__lhs";
  switch (filter.Operator()) {
    case ComparisonOperator::kEqual:
      os << " == ";
      break;
    case ComparisonOperator::kNotEqual:
      os << " != ";
      break;
    case ComparisonOperator::kLessThan:
      os << " < ";
      break;
    case ComparisonOperator::kGreaterThan:
      os << " > ";
      break;
  }

  os << "__rhs)) {\n"
     << "    const auto C" << lhs.UniqueId() << " = __lhs;\n";
  if (lhs != rhs) {
    os << "    const auto C" << rhs.UniqueId() << " = __rhs;\n";
  }

  if (view.CanReceiveDeletions()) {
    os << "    if constexpr (__added) {\n";
    CallUsers(os, QueryView::From(filter), case_map, "      ", "true", true);
    os << "    } else {\n";
    CallUsers(os, QueryView::From(filter), case_map, "      ", "false", false);
    os << "    }\n";
  } else {
    os << "    static_assert(__added);\n";
    CallUsers(os, QueryView::From(filter), case_map, "      ", "true", true);
  }

  os << "  }\n"
     << "  return 0u;\n"
     << "}\n\n";
}

static void DeclareEquiJoin(OutputStream &os, QueryView from_view,
                            QueryJoin join) {}

static void DeclareJoin(OutputStream &os, QueryJoin join) {
  //  const auto id = join.UniqueId();

  // Cross-product.
  if (!join.NumPivotColumns()) {
    assert(false && "TODO: Cross-products.");

    // Equi-join.
  } else {
    for (const auto col : join.NthInputPivotSet(0)) {
      DeclareEquiJoin(os, QueryView::Containing(col), join);
    }
  }
}

static void DefineStage(OutputStream &os, Query query) {
  os << "// State associated with a stage of execution. We track one work "
        "list\n"
     << "// for each depth of node in the dataflow graph.\n"
     << "struct Stage {\n";

  std::vector<unsigned> num_cases_at_depth;
  query.ForEachView([&](QueryView view) {
    if (view.IsMap()) {
      const auto map = QueryMap::From(view);
      if (map.IsFilterLike()) {
        return;
      }
      const auto functor = map.Functor();
      const auto binding_pattern = BindingPattern(functor);
      os << "  " << functor.Name() << '_' << binding_pattern << "_generator G"
         << view.UniqueId();
      if (!functor.IsPure()) {
        os << ", prev_G" << view.UniqueId();
      }
      os << ";\n";
    }

    const auto depth = view.Depth();
    num_cases_at_depth.resize(std::max(num_cases_at_depth.size(), depth + 1ul));

    auto can_remove_scale = view.CanReceiveDeletions() ? 2u : 1u;
    auto num_cases = 1u;

    if (view.IsJoin()) {
      num_cases = QueryJoin::From(view).NumJoinedViews();

    } else if (view.IsMerge()) {
      num_cases = QueryMerge::From(view).NumMergedViews();
    }

    num_cases_at_depth[depth] += num_cases * can_remove_scale;
  });

  for (auto i = 0u; i < num_cases_at_depth.size(); ++i) {
    if (const auto num_cases = num_cases_at_depth[i]; num_cases) {
      os << "  ::hyde::WorkList<" << num_cases << "> depth_" << i << ";\n";
    }
  }

  os << "};\n\n";
}

static void DefineStep(OutputStream &os, Query query, ViewCaseMap &case_map) {
  std::vector<std::vector<QueryView>> views_by_depth;

  query.ForEachView([&](QueryView view) {
    const auto depth = view.Depth();
    views_by_depth.resize(std::max(views_by_depth.size(), depth + 1ul));
    views_by_depth[depth].push_back(view);
  });

  // Topological sort of the views. On a given level, it orders merges/joins
  // latest, and before those, aggregates/kvindices.
  for (auto &views : views_by_depth) {
    std::sort(views.begin(), views.end(), [](QueryView a, QueryView b) {
      const auto a_is_merge = a.IsMerge() || a.IsJoin();
      const auto b_is_merge = b.IsMerge() || b.IsJoin();

      if (a_is_merge && b_is_merge) {
        return a.UniqueId() < b.UniqueId();
      } else if (b_is_merge) {
        return true;
      } else if (a_is_merge) {
        return false;
      }

      const auto a_is_agg = a.IsAggregate() || a.IsKVIndex();
      const auto b_is_agg = b.IsAggregate() || b.IsKVIndex();

      if (a_is_agg && b_is_agg) {
        return a.UniqueId() < b.UniqueId();
      } else if (b_is_agg) {
        return true;
      } else if (a_is_agg) {
        return false;
      } else {
        return a.UniqueId() < b.UniqueId();
      }
    });
  }

  os << "// Stepping function for advancing execution one step. This function\n"
     << "// takes in the current worker id `__wid` and a mask `__wm` that is\n"
     << "// `2**num_workers - 1`. `__stages` is an `__wm` element array, "
        "containing\n"
     << "// work lists for each depth of the dataflow graph.\n"
     << "//\n"
     << "// The return value of this function is the number of tuples that it\n"
     << "// placed into the work lists of other workers.\n"
     << "extern \"C\" uint64_t Step(Stage *__stages, unsigned __wid, "
     << "unsigned __wm) {\n"
     << "  auto &__stage = __stages[__wid];\n"
     << "  bool __changed = false;"
     << "  uint64_t __num_moved_tuples = 0;\n\n";

  auto depth = 0u;

  for (const auto &views : views_by_depth) {
    if (views.empty()) {
      ++depth;
      continue;
    }

    os << "__depth_" << depth << ":\n"
       << "  __changed = false;\n"
       << "  while (auto __case = __stage.depth_" << depth << ".PopCase()) {\n"
       << "    switch (__case) {\n";

    auto num_cases = 0u;

    for (auto view : views) {
      const auto id = view.UniqueId();

      for (const auto is_add_stage : {false, true}) {
        if (!is_add_stage && !view.CanReceiveDeletions()) {
          continue;
        }

        auto do_case = [&](QueryView source_view, bool has_source_view) {
          auto cols = InputColumnSpec(source_view, view);

          os << "      // " << (is_add_stage ? "Add " : "Remove ")
             << view.KindName();

          for (auto col : view.Columns()) {
            os << ' ' << col.Variable();
          }

          os << "\n"
             << "      case " << num_cases << ": {\n"
             << "        __changed = true;\n"
             << "        auto &__tuple = __stage.depth_" << depth
             << ".PopTuple<";

          auto sep = "";
          for (auto col : cols) {
            os << sep << TypeName(col);
            sep = ", ";
          }

          os << ">();\n"
             << "        __num_moved_tuples += V" << id;

          if (has_source_view) {
            os << '_' << source_view.UniqueId();
          }

          os << '<' << (is_add_stage ? "true" : "false")
             << ">(__stages, __wid, __wm, __tuple";

          if (view.IsMerge()) {
            os << ", SOURCE_" << source_view.UniqueId() << "_" << id;
          }

          os << ");\n"
             << "        break;\n"
             << "      }\n";
          ++num_cases;
        };

        query.ForEachView([&](QueryView source_view) {
          case_map.emplace(std::make_tuple(source_view.UniqueId(),
                                           view.UniqueId(), is_add_stage),
                           num_cases);
        });

        if (view.IsJoin()) {
          for (auto source_view : QueryJoin::From(view).JoinedViews()) {
            do_case(source_view, true);
          }
        } else if (view.IsMerge()) {
          for (auto source_view : QueryMerge::From(view).MergedViews()) {
            do_case(source_view,
                    false /* because we will pass through a refmask */);
          }
        } else {
          do_case(view, false);
        }
      }
    }

    os << "      case " << num_cases << ":\n"
       << "        __stage.depth_" << depth << ".Clear();\n";

    auto next_depth = depth;
    for (auto view : views) {
      view.ForEachUser([&](QueryView target_view) {
        next_depth = std::min<unsigned>(target_view.Depth(), next_depth);
      });
    }

    if (next_depth < depth) {
      os << "        if (__changed) {\n"
         << "          goto __depth_" << next_depth << ";\n"
         << "        } else {\n"
         << "          goto __depth_" << (depth + 1u) << ";\n"
         << "        }\n";

    } else {
      os << "        goto __depth_" << (depth + 1u) << ";\n";
    }

    os << "    }\n"  // End of `switch`.
       << "  }\n";   // End of `while`.

    ++depth;
  }

  os << "__depth_" << depth << ":\n"
     << "  return __num_moved_tuples;\n"
     << "}\n\n";
}

}  // namespace

// Generates BAM-like code following the push method of pipelined bottom-up
// execution of Datalog.
void GenerateCode(const ParsedModule &module, const Query &query,
                  OutputStream &os) {
  std::unordered_set<uint64_t> seen_generators;
  //  std::set<std::pair<ParsedFunctor, bool>> seen_functors;
  std::set<std::pair<uint64_t, bool>> seen_aggregators;

  os << "struct Stage;\n";

  DefineMergeSources(os, query);

  for (auto view : query.Maps()) {
    DeclareGenerator(os, view, seen_generators);
    DeclareView(os, QueryView::From(view));
  }

  for (auto view : query.Aggregates()) {
    DeclareAggregate(os, view, seen_aggregators);
    DeclareView(os, QueryView::From(view));
  }

  for (auto view : query.Joins()) {
    DeclareJoin(os, view);
  }

  for (auto view : query.Tuples()) {
    DeclareView(os, QueryView::From(view));
  }

  for (auto view : query.Constraints()) {
    DeclareView(os, QueryView::From(view));
  }

  for (auto view : query.Merges()) {
    DeclareView(os, QueryView::From(view));
  }

  for (auto view : query.KVIndices()) {
    DeclareView(os, QueryView::From(view));
  }

  os << '\n';

  DefineStage(os, query);

  for (auto include : module.Includes()) {
    if (include.IsSystemInclude()) {
      os << include << '\n';
    }
  }

  for (auto include : module.Includes()) {
    if (!include.IsSystemInclude()) {
      os << include << '\n';
    }
  }

  for (auto code : module.Inlines()) {
    os << code.CodeToInline() << '\n';
  }

  os << '\n';

  ViewCaseMap case_map;

  DefineStep(os, query, case_map);

  for (auto view : query.Tuples()) {
    DefineTuple(os, view, case_map);
  }

  for (auto view : query.Maps()) {
    DefineMap(os, view, case_map);
  }

  for (auto view : query.Constraints()) {
    DefineConstraint(os, view, case_map);
  }

  for (auto view : query.Aggregates()) {
    DefineAggregate(os, view, case_map);
  }

  for (auto view : query.KVIndices()) {
    DefineKVIndex(os, view, case_map);
  }

  if (false) {
    for (auto view : query.Merges()) {
      DefineMerge(os, view);
    }
  }

  (void)CallUsers;
}

}  // namespace hyde
