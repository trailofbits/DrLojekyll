// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/CodeGen/BAM.h>

#include <bitset>
#include <functional>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Lex/Format.h>
#include <drlojekyll/Parse/Format.h>
#include <drlojekyll/Parse/Parse.h>
#include <drlojekyll/Rel/Format.h>
#include <drlojekyll/Sema/BottomUpAnalysis.h>
#include <drlojekyll/Sema/SIPSAnalysis.h>
#include <drlojekyll/Sema/SIPSScore.h>
#include <drlojekyll/Transforms/CombineModules.h>
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
//    figure out what JOINs need to be eager, and which ones can be demand-based.
//
//    Analyze how columns that are outputted from a join are used, and for each
//    user, possibly create an index that gives them easy query-based access if
//    we will have the join not push its results forward.
//
//    Make each function take in a TupleVector
//      - If that function is currently active in the call stack, append the
//        tuple vector to the active function and return
//      - - Adds one tuple vector as a kind next tuple vector in a tuple vector
//          list.
//      - If the function is not active, set the active tuple vector to be
//        the passed in tuple vector
//
//    Another approach:
//      - One tuple vector per tuple kind
//      - Every time a message comes in, it is added to the corresponding tuple
//        vector for the next time step t
//      - At each time step:
//      - - Iterate over all tuple vector lists, starting at the deepest one in
//          the graph
//        - For each tuple vector, run the step function
//        - That function will add entries to other vectors, maybe deeper ones
//        - Return back to iteration phase for deepest non-empty tuple vector
//        - When everything is done, time step t is done, advance to t+1
//        - - Take a rocksdb snapshot when time step `t` is done, so that
//            queries can concurrently operate on that time step while other
//            ongoing writes can advance the time.

namespace hyde {
namespace {

static unsigned gNextAggregate = 0;

//static unsigned TypeSize(TypeLoc loc) {
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
  std::stringstream ss;
  for (auto param : decl.Parameters()) {
    switch (param.Binding()) {
      case ParameterBinding::kImplicit:
        assert(false);
        break;
      case ParameterBinding::kMutable:
        ss << 'm';
        break;
      case ParameterBinding::kFree:
        ss << 'f';
        break;

      case ParameterBinding::kBound:
        ss << 'b';
        break;

      case ParameterBinding::kSummary:
        ss << 's';
        break;

      case ParameterBinding::kAggregate:
        ss << 'a';
        break;
    }
  }

  return ss.str();
}

// We sometimes deal in terms of input columns, and so there may actually be
// repeats. We need to make sure that we name them all uniquely, and not in
// terms of their unique IDs, which may re-occur, hence the indexing scheme.
template <typename... Vecs>
static std::pair<std::unordered_map<QueryColumn, unsigned>,
                 std::unordered_map<unsigned, QueryColumn>>
ArgumentList(OutputStream &os, Vecs&...vecs) {
  auto i = 0u;
  std::unordered_map<QueryColumn, unsigned> col_to_index;
  std::unordered_map<unsigned, QueryColumn> index_to_col;

  auto sep = "\n    ";
  auto do_arg_list = [&] (std::vector<QueryColumn> &cols) -> void {
    for (auto col : cols) {
      col_to_index.emplace(col, i);
      index_to_col.emplace(i, col);

      os << sep << TypeName(col) << " I" << i
         << CommentOnCol(os, col);
      sep = ",\n    ";
      ++i;
    }
  };

  int force[] = {(do_arg_list(vecs), 0)...};
  (void) force;

  return {col_to_index, index_to_col};
}


// Fill up a container, e.g. a vector of columns, given a range of elements.
template <typename T, typename Range>
static void FillContainerFromRange(T &container, Range range) {
  container.insert(container.end(), range.begin(), range.end());
}

// Apply a callback `cb` to each view targeted (i.e. that uses one or more of
// the columns produced) by `view`.
template <typename T>
void ForEachUser(QueryView view, T cb) {
  std::unordered_set<QueryView> target_views;
  for (QueryColumn col : view.Columns()) {
    col.ForEachUser([&target_views] (QueryView user_view) {
      target_views.insert(user_view);
    });
  }

  // Sort the views by depth. We want a consistent topological ordering of the
  // nodes, so that we always send new information to the shallowest node
  // first.
  std::vector<QueryView> ordered_views;
  ordered_views.reserve(target_views.size());
  ordered_views.insert(ordered_views.end(), target_views.begin(),
                       target_views.end());

  std::sort(ordered_views.begin(), ordered_views.end(),
            [] (QueryView a, QueryView b) {
              if (a.Depth() < b.Depth()) {
                return true;
              } else if (a.Depth() > b.Depth()) {
                return false;
              } else {
                return a.UniqueId() < b.UniqueId();  // A bit arbitrary :-(
              }
            });

  for (auto target_view : ordered_views) {
    cb(target_view);
  }
}

// We keep track of a bitset of 0/1 reference counters for each view, so that
// we can know for each (source, dest) pairing, if source produced a given tuple
// to dest.
static void DefineMergeSources(OutputStream &os, Query query) {
  std::unordered_map<QueryView, std::vector<QueryView>> to_from;

  query.ForEachView([&] (QueryView view) {
    ForEachUser(view, [&] (QueryView target_view) {
      to_from[target_view].push_back(view);
    });
  });

  for (const auto &[to_view, from_views] : to_from) {
    if (!to_view.IsMerge()) {
      continue;
    }

    auto i = 0u;

    const auto id = to_view.UniqueId();

    // The refcount type for a given node.
    os << "using RC" << id << " = std::bitset<" << from_views.size() << ">;\n";

    // The refcount that `from_view` will contribute to `to_view`.
    for (auto from_view : from_views) {
      const auto from_id = from_view.UniqueId();
      os << "static constexpr auto SOURCE_" << from_id << "_" << id
         << " = RC" << id << ".set(" << (i++) << ");\n";
    }
  }
  os << "\n";
}

static void AddTuple(OutputStream &os, QueryView view, const char *indent,
                     const char *stage, const char *wid="__wid") {
  os << indent << "__stages[(2u * " << wid << ") + " << stage << "].V"
     << view.UniqueId()
     << ".emplace_back(\n" << indent << "    ";

  auto sep = "";
  for (const auto col : view.Columns()) {
    os << sep << 'C' << col.UniqueId();
    sep = ", ";
  }

  if (view.IsMerge()) {
    os << sep << "__rc";
  }

  os << ");\n";
}

// Send the output columns in `view` to all users.
static void CallUsers(OutputStream &os, QueryView view, const char *indent,
                      const char *added) {
  std::vector<QueryColumn> input_cols;

  ForEachUser(view, [&] (QueryView target_view) {
    os << indent << "V" << target_view.UniqueId();

    input_cols.clear();

    // Pass down the columns to a merge.
    if (target_view.IsMerge()) {
      FillContainerFromRange(input_cols, view.Columns());

    // Pass down the columns to a join.
    } else if (target_view.IsJoin()) {
      auto target_join = QueryJoin::From(target_view);

      std::vector<unsigned> join_col_to_view_col;
      join_col_to_view_col.resize(target_view.Columns().size());

      // Make the call target of the JOIN specific to this source view.
      os << "_V" << view.UniqueId();

      // Filter out the pivot columns.
      for (auto i = 0u; i < target_join.NumPivotColumns(); ++i) {
        for (auto pivot_col : target_join.NthInputPivotSet(i)) {
          assert(!pivot_col.IsConstant());

          if (QueryView::Containing(pivot_col) == view) {
            input_cols.push_back(pivot_col);
          }
        }
      }

      // Filter out the proposed values.
      for (auto i = 0u, max_i = target_join.NumMergedColumns(); i < max_i; ++i) {
        auto in_col = target_join.NthInputMergedColumn(i);
        assert(!in_col.IsConstant());
        if (QueryView::Containing(in_col) == view) {
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
      assert(false);
    }

    os << '<' << added << ">(\n" << indent << "    ";
    auto sep = "";
    for (auto in_col : input_cols) {
      if (in_col.IsConstant()) {
        os << sep << "/* TODO constant */";  // TODO(pag): Handle constants/generators!

      } else if (in_col.IsGenerator()) {
        os << sep << "/* TODO generator */";  // TODO(pag): Handle this!

      } else if (QueryView::Containing(in_col) == view) {
        os << sep << "C" << in_col.UniqueId();

      } else {
        assert(false);
      }
      sep = ", ";
    }

    if (target_view.IsMerge()) {
      os << sep << "SOURCE_" << view.UniqueId() << '_'
         << target_view.UniqueId();
      sep = ", ";
    }

    os << sep << "\n    " << indent << "__stages, __wid, __nw);\n";
  });
}

// Declare the function that will do aggregate some results.
static void DeclareAggregate(
    OutputStream &os, QueryAggregate agg,
    std::unordered_set<ParsedFunctor> &seen_functors) {

  auto functor = agg.Functor();
  if (!seen_functors.count(functor)) {
    seen_functors.insert(functor);

    const auto binding_pattern = BindingPattern(functor);

    os << "// Aggregator object (will collect results); extends the empty\n"
       << "// to ensure a minimum size, but uses empty base class optimization\n"
       << "// to place config vars inside of the aggregator.\n"
       << "struct " << functor.Name() << '_' << binding_pattern << "_config"
       << " : public ::hyde::rt::AggregateConfiguration {\n";

    for (auto param : functor.Parameters()) {
      os << "  " << TypeName(param.Type()) << ' ' << param.Name()
         << ';' << CommentOnParam(os, param) << '\n';
    }

    os << "};\n\n"
       << "// Return type of the aggregator.\n"
       << "struct " << functor.Name() << '_' << binding_pattern
       << "_result {\n";

    for (auto param : functor.Parameters()) {
      switch (param.Binding()) {
        case ParameterBinding::kImplicit:
        case ParameterBinding::kMutable:
        case ParameterBinding::kFree:
          assert(false);
          break;

        case ParameterBinding::kSummary:
          os << "  " << TypeName(param.Type()) << ' ' << param.Name()
             << ';' << CommentOnParam(os, param) << '\n';
          break;

        case ParameterBinding::kBound:
        case ParameterBinding::kAggregate:
          break;
      }
    }

    // Forward declare the aggregator as returning the above structure.
    os << "};\n\n"
       << "// Initializer function for the aggregate configuration.\n"
       << "extern \"C\" void " << functor.Name() << '_' << binding_pattern
       << "_init(";
    auto sep = "";
    for (auto param : functor.Parameters()) {
      switch (param.Binding()) {
        case ParameterBinding::kBound:
          os << sep << TypeName(param.Type()) << ' ' << param.Name();
          sep = ", ";
          break;
        default:
          break;
      }
    }

    os << sep << functor.Name() << '_' << binding_pattern
       << "_config &__agg);\n\n"
       << "// Update function that adds/removes a value from the aggregate.\n"
       << "extern \"C\" void " << functor.Name() << '_' << binding_pattern
       << "_update(";
    sep = "";
    for (auto param : functor.Parameters()) {
      switch (param.Binding()) {
        case ParameterBinding::kAggregate:
          os << sep << TypeName(param.Type()) << ' ' << param.Name();
          sep = ", ";
          break;
        default:
          break;
      }
    }

    os << sep << functor.Name() << '_' << binding_pattern
       << "_config &__agg, bool __added);\n\n";
  }
  os << "template <bool __added>\n"
     << "static DR_INLINE void V" << agg.UniqueId() << '(';
  auto sep = "";
  for (auto col : agg.InputGroupColumns()) {
    os << sep << TypeName(col) << CommentOnCol(os, col);
    sep = ", ";
  }

  for (auto col : agg.InputConfigurationColumns()) {
    os << sep << TypeName(col) << CommentOnCol(os, col);
    sep = ", ";
  }

  for (auto col : agg.InputAggregatedColumns()) {
    os << sep << TypeName(col) << CommentOnCol(os, col);
  }

  os << ") noexcept;\n";
}


// Generate code associated with an aggregate.
static void DefineAggregate(OutputStream &os, QueryAggregate agg) {
  const auto id = agg.UniqueId();
  const auto summarizer = agg.Functor();
  const auto binding_pattern = BindingPattern(summarizer);

  std::vector<QueryColumn> group_cols;
  std::vector<QueryColumn> config_cols;
  std::vector<QueryColumn> agg_cols;
  std::vector<QueryColumn> summary_cols;

  FillContainerFromRange(group_cols, agg.InputGroupColumns());
  FillContainerFromRange(config_cols, agg.InputConfigurationColumns());
  FillContainerFromRange(agg_cols, agg.InputAggregatedColumns());

  FillContainerFromRange(summary_cols, agg.SummaryColumns());

  std::vector<ParsedParameter> summary_params;
  for (auto param : summarizer.Parameters()) {
    switch (param.Binding()) {
      case ParameterBinding::kImplicit:
      case ParameterBinding::kMutable:
      case ParameterBinding::kFree:
        assert(false);
        break;

      case ParameterBinding::kBound:
      case ParameterBinding::kAggregate:
        break;

      case ParameterBinding::kSummary:
        summary_params.push_back(param);
        break;
    }
  }

  os << "/* Aggregate: " << ParsedDeclaration(summarizer) << " \n";
  for (auto col : group_cols) {
    os << " * Group var: " << col.Variable() << "\n";
  }
  for (auto col : config_cols) {
    os << " * Config var: " << col.Variable() << "\n";
  }
  for (auto col : agg_cols) {
    os << " * Aggregating var: " << col.Variable() << "\n";
  }
  for (auto col : summary_cols) {
    os << " * Summary var: " << col.Variable() << "\n";
  }
  os << " */\n\n";

  // This means that we are grouping, and within each group, we need an
  // instance of a configured map.
  os << "// Maps config/group vars to aggregators.\n"
     << "static ::hyde::rt::Aggregate<"
     << "\n    " << summarizer.Name() << '_' << binding_pattern
     << "_config  /* Configured aggregator type */";

  if (group_cols.empty()) {
    os << ",\n    ::hyde::rt::NoGroupVars";
  } else {
    auto sep = ",\n    ::hyde::rt::GroupVars<";
    for (const auto col : group_cols) {
      os << sep << TypeName(col) << CommentOnCol(os, col);
      sep = ",\n               ";
    }
    os << ">";
  }

  if (config_cols.empty()) {
    os << ",\n    ::hyde::rt::NoConfigVars";
  } else {
    auto sep = ",\n    ::hyde::rt::ConfigVars<";
    for (const auto col : config_cols) {
      os << sep << TypeName(col) << CommentOnCol(os, col);
      sep = ",\n               ";
    }
    os << ">";
  }

  os << "> MA" << id << ";\n\n";


  os << "template <bool __added>\n"
     << "static DR_INLINE void V" << id << '(';

  auto [col_to_index, index_to_col] = ArgumentList(
      os, group_cols, config_cols, agg_cols);

  os << ",\n    Stage *__stages"
     << ",\n    unsigned __wid"
     << ",\n    unsigned __nw) noexcept {\n";

  // Now name the column variables.
  for (auto [col, index] : col_to_index) {
    os << "  const auto C" << col.UniqueId() << " = I" << index << ";"
       << CommentOnCol(os, col) << '\n';
  }

  // If there are group/config columns then we can spread work out across
  // workers.
  os << "\n  if (1 < __nw) {\n";
  if (!group_cols.empty() || !config_cols.empty()) {
    os << "    const auto __hash = Hash<";

    auto sep = "";
    for (auto col : group_cols) {
      os << sep << TypeName(col);
      sep = ", ";
    }
    for (auto col : config_cols) {
      os << sep << TypeName(col);
      sep = ", ";
    }

    os << ">::Update(";
    sep = "";
    for (auto col : group_cols) {
      os << sep << 'C' << col.UniqueId();
      sep = ", ";
    }
    for (auto col : config_cols) {
      os << sep << 'C' << col.UniqueId();
      sep = ", ";
    }
    os << ");\n";

  // If there are no group/config cols, then do a fixed assignment of the
  // aggregate to a worker.
  } else {
    os << "    const auto __hash = " << (gNextAggregate++) << "u;\n";
  }

  os << "    // Send this tuple to another worker.\n"
     << "    if (const auto __owid = __hash % __nw; __owid != __wid) {\n";

  AddTuple(os, QueryView::From(agg), "      ", "__added", "__owid");

  os << "      return;\n"
     << "    }\n"
     << "  }\n\n"
     << "  auto &__agg = MA" << id << ".Get(";

  auto sep = "";
  for (const auto col : group_cols) {
    os << sep << "C" << col.UniqueId();
    sep = ", ";
  }

  for (const auto col : config_cols) {
    os << sep << col.UniqueId();
    sep = ", ";
  }

  os << ");\n\n"
     << "  const auto __has_current = __agg.IsInitialized();\n"
     << "  if (!__has_current) {\n"
     << "    if constexpr (!__added) {\n"
     << "      return;\n"
     << "    }\n"
     << "    " << summarizer.Name() << '_' << binding_pattern << "_init(";

  auto b = 0u;
  sep = "\n        ";

  // Pass them in the order that they appear in the functor declaration.
  for (auto param : summarizer.Parameters()) {
    switch (param.Binding()) {
      case ParameterBinding::kImplicit:
      case ParameterBinding::kMutable:
      case ParameterBinding::kFree:
        assert(false);
        break;

      case ParameterBinding::kBound:
        os << sep << "C" << config_cols[b].UniqueId()
           << CommentOnCol(os, config_cols[b]);
        ++b;
        sep = ",\n        ";
        break;

      case ParameterBinding::kAggregate:
      case ParameterBinding::kSummary:
        break;
    }
  }

  os << sep << "__agg);\n"
     << "  }\n\n"
     << "  // Get the old summary values, update the aggregate, and get the "
     << "  // new summary values.\n"
     << "  const auto [";

  sep = "";
  for (const auto col : summary_cols) {
    os << sep << "old_C" << col.UniqueId();
    sep = ", ";
  }

  os << "] = __agg.Summarize();\n";

  // Cal the summarizer function.
  os << "  " << summarizer.Name() << '_' << binding_pattern << "_update(";
  auto a = 0u;
  sep = "\n      ";

  // Pass them in the order that they appear in the functor declaration.
  for (auto param : summarizer.Parameters()) {
    switch (param.Binding()) {
      case ParameterBinding::kImplicit:
      case ParameterBinding::kMutable:
      case ParameterBinding::kFree:
        assert(false);
        break;

      case ParameterBinding::kAggregate:
        os << sep << "C" << agg_cols[a].UniqueId()
           << CommentOnCol(os, agg_cols[a]);
        ++a;
        sep = ",\n      ";
        break;

      case ParameterBinding::kBound:
      case ParameterBinding::kSummary:
        break;
    }
  }

  os << sep << "__agg, __added);\n"
     << "  const auto [";

  sep = "";
  for (const auto col : summary_cols) {
    os << sep << "new_C" << col.UniqueId();
    sep = ", ";
  }
  os << "] = __agg.Summarize();\n\n"
     << "  if (__has_current) {\n"
     << "    if (";

  sep = "";
  for (const auto col : summary_cols) {
    os << sep << "old_C" << col.UniqueId() << " != new_C" << col.UniqueId();
    sep = "||\n      ";
  }

  os << ") {\n";

  // Remove the old version.
  os << "      // Remove the old summary values.\n";
  for (const auto col : summary_cols) {
    os << "      const auto C" << col.UniqueId() << " = old_C" << col.UniqueId()
       << ';' << CommentOnCol(os, col) << '\n';
  }

  AddTuple(os, QueryView::From(agg), "      ", "0");

  os << "    } else {\n"
     << "      return;  // Nothing changed.\n"
     << "    }\n"
     << "  }\n\n";

  // Add the new version.
  os << "  // Add the new summary values.\n";
  for (const auto col : summary_cols) {
    os << "  const auto C" << col.UniqueId() << " = new_C" << col.UniqueId()
       << ';' << CommentOnCol(os, col) << '\n';
  }

  AddTuple(os, QueryView::From(agg), "  ", "1");

  os << "}\n\n";
}

static void DefineTuple(OutputStream &os, QueryTuple view) {
  os << "/* Tuple; just forwards stuff to users. */\n"
     << "template <bool __added>\n"
     << "static DR_INLINE void V" << view.UniqueId() << '(';
  auto sep = "\n    ";
  for (auto col : view.Columns()) {
    os << sep << TypeName(col) << " C" << col.UniqueId()
       << CommentOnCol(os, col);
    sep = ",\n    ";
  }
  os << sep << "Stage *__stages"
     << ",\n    unsigned __wid"
     << ",\n    unsigned __nw) noexcept {\n";

  AddTuple(os, QueryView::From(view), "  ", "__added");

  os << "}\n\n";
}

static void DefineKVIndex(OutputStream &os, QueryKVIndex view) {
  auto i = 0u;
  for (auto col : view.ValueColumns()) {
    auto functor = view.NthValueMergeFunctor(i++);
    os << "extern \"C\" " << TypeName(col) << ' ' << functor.Name() << "_merge("
       << TypeName(col) << ", " << TypeName(col) << ");\n";
  }

  os << "// Mapping for maintaining key/value tuples.\n"
     << "static ::hyde::rt::Map<\n    ";

  const auto id = view.UniqueId();

  auto sep = "";
  if (view.NumKeyColumns()) {
    os << "::hyde::rt::KeyVars<";
    for (const auto col : view.KeyColumns()) {
      os << sep << TypeName(col) << CommentOnCol(os, col);
      sep = ",\n                    ";
    }
    os << ">";
  } else {
    os << "::hyde::rt::EmptyKeyVars";
  }

  os << ",\n    ::hyde::ValueVars<";
  sep = "";
  for (const auto col : view.ValueColumns()) {
    os << sep << TypeName(col) << CommentOnCol(os, col);
    sep = ",\n                      ";
  }
  os << ">> KV" << id << ";\n\n"
     << "/* Key/value mapping. */\n"
     << "template <bool __added>\n"
     << "static DR_INLINE void V" << view.UniqueId() << '(';

  sep = "\n    ";
  for (auto col : view.KeyColumns()) {
    os << sep << TypeName(col) << " C" << col.UniqueId()
       << CommentOnCol(os, col);
    sep = ",\n    ";
  }
  for (auto col : view.ValueColumns()) {
    os << sep << TypeName(col) << " proposed_C" << col.UniqueId()
       << CommentOnCol(os, col);
    sep = ",\n    ";
  }
  os << sep << "Stage *__stages"
     << ",\n    unsigned __wid"
     << ",\n    unsigned __nw) noexcept {\n";

  // If there are no key columns then it's basically a global var.
  if (view.NumKeyColumns()) {
    os << "  const auto __hash = ::hyde::rt::Hash<";

    sep = "";
    for (auto col : view.KeyColumns()) {
      os << sep << TypeName(col);
      sep = ", ";
    }

    os << ">::Update(" << QueryView::From(view).Hash();
    for (auto col : view.KeyColumns()) {
      os << ", C" << col.UniqueId();
    }

    os << ");\n";
  }


  for (auto col : view.ValueColumns()) {
    os << "  " << TypeName(col) << " C" << col.UniqueId()
       << CommentOnCol(os, col) << ";\n";
  }

  os << "  if (auto [";
  sep = "";
  for (auto col : view.ValueColumns()) {
    os << sep << "old_C" << col.UniqueId();
    sep = ", ";
  }
  os << sep << "__present] = KV" << id << ".Get(";
  sep = "__hash";
  for (auto col : view.KeyColumns()) {
    os << sep << 'C' << col.UniqueId();
    sep = ", ";
  }
  os << "); __present) {\n"
     << "    if constexpr (__added) {\n"
     << "      if (";
  sep = "";
  for (auto col : view.ValueColumns()) {
    os << sep << "old_C" << col.UniqueId() << " != proposed_C" << col.UniqueId();
    sep = " ||\n          ";
  }

  os << ") {\n";

  i = 0u;
  for (auto col : view.ValueColumns()) {
    os << "        auto new_C" << col.UniqueId() << " = "
       << view.NthValueMergeFunctor(i++).Name() << "_merge(old_C"
       << col.UniqueId() << ", proposed_C" << col.UniqueId() << ");\n";
  }

  os << "        if (";
  sep = "";
  for (auto col : view.ValueColumns()) {
    os << sep << "old_C" << col.UniqueId() << " != new_C" << col.UniqueId();
    sep = " ||\n            ";
  }

  os << ") {\n";

  auto call_kv_method = [&] (const char *indent, const char *func, bool vals) {
    os << indent << "KV" << id << "." << func << "(";
    if (view.NumKeyColumns()) {
      os << "__hash";
      for (auto col : view.KeyColumns()) {
        os << ", C" << col.UniqueId();
      }
      if (vals) {
        for (auto col : view.ValueColumns()) {
          os << ", C" << col.UniqueId();
        }
      }
    } else if (vals) {
      sep = "";
      for (auto col : view.ValueColumns()) {
        os << sep << "C" << col.UniqueId();
        sep = ", ";
      }
    }

    os << ");\n";
  };

  for (auto col : view.ValueColumns()) {
    os << "          C" << col.UniqueId()
       << " = new_C" << col.UniqueId()
       << ';' << CommentOnCol(os, col) << '\n';
  }

  call_kv_method("          ", "Update", true);


  os << "\n"
     << "          // Tell users we're removing the old version.\n";
  for (auto col : view.ValueColumns()) {
    os << "          C" << col.UniqueId()
       << " = old_C" << col.UniqueId()
       << ';' << CommentOnCol(os, col) << '\n';
  }
  AddTuple(os, QueryView::From(view), "          ", "0");

  os << "\n"
     << "          // Prepare to send the new versions of the values through.\n";
  for (auto col : view.ValueColumns()) {
    os << "          C" << col.UniqueId()
       << " = new_C" << col.UniqueId()
       << ';' << CommentOnCol(os, col) << '\n';
  }

  os << "        } else {\n"
     << "          return;  // No new info.\n"
     << "        }\n"
     << "      } else {\n"
     << "        return;  // No new info.\n"
     << "      }\n"
     << "    } else {\n";

  for (auto col : view.ValueColumns()) {
    os << "      C" << col.UniqueId()
       << " = old_C" << col.UniqueId()
       << ';' << CommentOnCol(os, col) << '\n';
  }

  // Remove case.
  call_kv_method("      ", "Erase", false);

  os << "    }\n"
     << "  } else if constexpr (!__added) {\n"
     << "    return;  // Can't remove what we don't have.\n"
     << "  } else {\n";

  for (auto col : view.ValueColumns()) {
    os << "    C" << col.UniqueId() << " = proposed_C"
       << col.UniqueId() << ';' << CommentOnCol(os, col) << '\n';
  }

  // Insert case.
  call_kv_method("    ", "Insert", true);
  os << "  }\n\n"
     << "  // Add or remove this tuple.\n";

  AddTuple(os, QueryView::From(view), "  ", "__added");

  os << "}\n\n";
}

static void DefineMerge(OutputStream &os, QueryMerge view) {
  const auto id = view.UniqueId();

  os << "// Set for tracking unique elements in the merge.\n"
     << "static ::hyde::rt::Set<RC" << id;

  for (const auto col : view.Columns()) {
    os << ",\n                       " << TypeName(col)
       << CommentOnCol(os, col);
  }

  os << "> S" << id << ";\n\n"
     << "/* Merge; forwards unique tuples to users, and reference counts\n"
     << " * tuples in terms of their source. */\n"
     << "template <bool __added>\n"
     << "static DR_INLINE void V" << view.UniqueId() << '(';
  auto sep = "\n    ";
  for (auto col : view.Columns()) {
    os << sep << TypeName(col) << " C" << col.UniqueId()
       << CommentOnCol(os, col);
    sep = ",\n    ";
  }

  os << sep << "RC" << id << " __source"
     << "\n, Stage *__stages"
     << ",\n    unsigned __wid"
     << ",\n    unsigned __nw) noexcept {\n"
     << "  if (1 < __nw) {\n"
     << "    const auto __hash = Hash<";

  sep = "";
  for (auto col : view.Columns()) {
    os << sep << TypeName(col);
    sep = ", ";
  }

  os << ">::Update(";
  sep = "";
  for (auto col : view.Columns()) {
    os << sep << 'C' << col.UniqueId();
    sep = ", ";
  }

  os << ");\n"
     << "    // Send this tuple to another worker.\n"
     << "    if (const auto __owid = __hash % __nw; __owid != __wid) {\n";

  AddTuple(os, QueryView::From(view), "      ", "__added", "__owid");

  os << "      return;\n"
     << "    }\n"
     << "  }\n"
     << "  if constexpr (__added) {\n"
     << "    if (!S" << id << ".Add(";

  sep = "";
  for (auto col : view.Columns()) {
    os << sep << 'C' << col.UniqueId();
    sep = ", ";
  }

  os << sep << "__source)) {\n"
     << "      return;  // Already added.\n"
     << "    }\n"
     << "  } else {\n"
     << "    if (!S" << id << ".Remove(";
  sep = "";
  for (auto col : view.Columns()) {
    os << sep << 'C' << col.UniqueId();
    sep = ", ";
  }
  os << sep << "__source)) {\n"
     << "      return;  // Not fully removed.\n"
     << "    }\n"
     << "  }\n";
  AddTuple(os, QueryView::From(view), "  ", "__added");
  os << "}\n\n";
}

static void DeclareMap(OutputStream &os, QueryMap map,
                       std::unordered_set<ParsedFunctor> &seen_functors) {
  const auto functor = map.Functor();
  if (!seen_functors.count(functor)) {
    seen_functors.insert(functor);

    const auto binding_pattern = BindingPattern(functor);

    // Declare the tuple type as a structure.
    os << "\n"
       << "struct " << functor.Name() << '_' << binding_pattern
       << "_result {\n";

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
          os << "  " << TypeName(param.Type()) << ' ' << param.Name() << ";\n";
          break;
      }
    }

    // Forward declare the aggregator as returning a generator of the above
    // structure type.
    os << "  bool __added;\n"
       << "};\n\n"
       << "extern \"C\" ::hyde::rt::Generator<" <<  functor.Name()
       << '_' << binding_pattern << "_result> "
       << functor.Name() << '_' << binding_pattern << '(';

    auto sep = "";
    for (auto param : functor.Parameters()) {
      if (param.Binding() == ParameterBinding::kBound) {
        os << sep << TypeName(param.Type()) << CommentOnParam(os, param);
        sep = ", ";
      }
    }

    os << ");\n\n";
  }
}

static void DefineMap(OutputStream &os, QueryMap map) {
  const auto functor = map.Functor();
  const auto binding_pattern = BindingPattern(functor);

  os << "/* Map: " << ParsedDeclaration(map.Functor()) << "\n";
  for (auto col : map.InputColumns()) {
    os << " * Input column: " << col.Variable().Name() << '\n';
  }
  for (auto col : map.InputCopiedColumns()) {
    os << " * Input copied column: " << col.Variable().Name() << '\n';
  }
  for (auto col : map.Columns()) {
    os << " * Output column: " << col.Variable().Name() << '\n';
  }
  for (auto col : map.CopiedColumns()) {
    os << " * Output copied column: " << col.Variable().Name() << '\n';
  }

  os << " */\n"
     << "template <bool __added>\n"
     << "static DR_INLINE void V" << map.UniqueId() << '(';

  std::vector<QueryColumn> cols;
  FillContainerFromRange(cols, map.InputColumns());
  FillContainerFromRange(cols, map.InputCopiedColumns());

  auto [col_to_id, id_to_col] = ArgumentList(os, cols);

  os << ",\n    Stage *__stages"
     << ",\n    unsigned __wid"
     << ",\n    unsigned __nw) noexcept {\n";

  for (auto [col, id] : col_to_id) {
    os << "  const auto C" << col.UniqueId() << " = I" << id << ';'
       << CommentOnCol(os, col) << '\n';
  }

  if (map.NumCopiedColumns()) {
    os << "\n  // Copied columns.\n";
    auto i = 0u;
    for (auto col : map.InputCopiedColumns()) {
      auto out_col = map.NthCopiedColumn(i++);
      os << "  const auto C" << out_col.UniqueId() << " = C"
         << col.UniqueId() << ";"
         << CommentOnCol(os, out_col) << '\n';
    }
  }

  os << '\n'
     << "  // Loop for each produced tuple (may produce removals).\n"
     << "  for (auto [";

  auto sep = "";
  auto f = 0u;

  for (auto param : functor.Parameters()) {
    switch (param.Binding()) {
      case ParameterBinding::kImplicit:
      case ParameterBinding::kMutable:
      case ParameterBinding::kAggregate:
      case ParameterBinding::kSummary:
        assert(false);
        break;

      case ParameterBinding::kBound:
        ++f;
        break;

      case ParameterBinding::kFree:
        os << sep << "C" << map.NthColumn(f++).UniqueId();
        sep = ", ";
        break;
    }
  }

  os << "] : " << functor.Name() << '_' << binding_pattern
     << "(";

  auto b = 0u;

  sep = "";
  // Pass them in the order that they appear in the functor declaration.
  for (auto param : functor.Parameters()) {
    switch (param.Binding()) {
      case ParameterBinding::kImplicit:
      case ParameterBinding::kMutable:
      case ParameterBinding::kAggregate:
      case ParameterBinding::kSummary:
        assert(false);
        break;

      case ParameterBinding::kFree:
        ++b;
        break;

      case ParameterBinding::kBound:
        os << sep << "C" << map.NthInputColumn(b++).UniqueId();
        sep = ", ";
        break;
    }
  }

  os << ")) {\n";

  AddTuple(os, QueryView::From(map), "    ", "__added");

  os << "  }\n}\n\n";
}

static void DefineConstraint(OutputStream &os, QueryConstraint filter) {
  const auto lhs = filter.LHS();
  const auto rhs = filter.RHS();

  std::vector<QueryColumn> cols;
  cols.push_back(lhs);
  cols.push_back(rhs);
  FillContainerFromRange(cols, filter.InputCopiedColumns());

  os << "template <bool __added>\n"
     << "static DR_INLINE void V" << filter.UniqueId() << '(';

  auto [col_to_id, id_to_col] = ArgumentList(os, cols);

  os << ",\n    Stage *__stages"
     << ",\n    unsigned __wid"
     << ",\n    unsigned __nw) noexcept {\n";

  for (auto [col, id] : col_to_id) {
    os << "  const auto C" << col.UniqueId() << " = I" << id << ';'
       << CommentOnCol(os, col) << '\n';
  }

  if (filter.NumCopiedColumns()) {
    os << "\n  // Copied columns.\n";
    auto i = 0u;
    for (auto col : filter.InputCopiedColumns()) {
      auto out_col = filter.NthCopiedColumn(i++);
      os << "  const auto C" << out_col.UniqueId() << " = C"
         << col.UniqueId() << ";"
         << CommentOnCol(os, out_col) << '\n';
    }
  }

  os << "  if (C" << lhs.UniqueId();
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

  os << "C" << rhs.UniqueId() << ")) {\n";

  AddTuple(os, QueryView::From(filter), "    ", "__added");

  os << "  }\n}\n\n";
}

static void DeclareEquiJoin(OutputStream &os, QueryView from_view,
                            QueryJoin join) {

}

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
  os << "// State associated with a stage of execution. Index `0` is for\n"
     << "// removals, and index `1` is for additions.\n"
     << "struct Stage {\n";
  query.ForEachView([&] (QueryView view) {
    auto sep = "";
    const auto id = view.UniqueId();
    if (view.IsJoin()) {

    } else {
      os << "  std::vector<std::tuple<";
      for (const auto col : view.Columns()) {
        os << sep << TypeName(col);
        sep = ", ";
      }
      if (view.IsMerge()) {
        os << sep << "RC" << id;
      }
      os << ">> V" << id << ";\n";
    }
  });
  os << "};\n\n";
}

static void DefineStep(OutputStream &os, Query query) {
  std::vector<std::vector<QueryView>> views_by_depth;

  query.ForEachView([&] (QueryView view) {
    const auto depth = view.Depth();
    views_by_depth.resize(depth + 1);
    views_by_depth[depth].push_back(view);
  });

  // Topological sort of the views. On a given level, it orders merges/joins
  // latest, and before those, aggregates/kvindices.
  for (auto &views : views_by_depth) {
    std::sort(views.begin(), views.end(), [] (QueryView a, QueryView b) {
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

  os << '\n'
     << "// Stepping function for advancing execution one step. This function\n"
     << "// takes in the current worker id `__wid` and the total number of\n"
     << "// workers `__nw`. `__stages` is an `__nw * 2` element array, where\n"
     << "// there are two entries per worker: the first is for removals, the\n"
     << "// second is for insertions.\n"
     << "extern \"C\" void Step(Stage *__stages, unsigned __wid,"
     << "unsigned __nw) {\n"
     << "  Stage *__stage = nullptr;\n"
     << "  bool __changed = false;\n"
     << "__restart:\n"
     << "  __changed = false;\n";

  auto i = 0;
  for (auto added : {"false", "true"}) {
    os << "  __stage = &(__stages[(2u * __wid) + " << (i++) << "]);\n";

    for (const auto &views : views_by_depth) {
      if (views.empty()) {
        continue;
      }

      for (auto view : views) {
        const auto id = view.UniqueId();
        auto sep = "";
        if (view.IsJoin()) {

        } else {
          os << "  if (auto __vec = &(__stage->V" << id << "); !__vec->empty()) {\n"
             << "    __changed = true;\n"
             << "    ([=] (void) {\n"
             << "      std::sort(__vec->begin(), __vec->end());\n"
             << "      std::reverse(__vec->begin(), __vec->end());\n"
             << "      do {\n"
             << "        auto [";
          for (const auto col : view.Columns()) {
            os << sep << "C" << col.UniqueId() << CommentOnCol(os, col);
            sep = ",\n              ";
          }
          os << "] = __vec->back();\n"
             << "        __vec->pop_back();\n";
          CallUsers(os, view, "        ", added);
          os << "      } while (!__vec->empty());\n"
             << "    })();\n"
             << "  }\n";
        }
      }

      // We let ourselves process all additions in a given level without
      // undoing.
      //
      // TODO(pag): Is this only well-defined for removals? Is this well-defined
      //            at all?
      if (i) {
        os << "  if (__changed) {\n"
           << "    goto __restart;\n"
           << "  }\n";
      }
    }

    // We let ourselves process all removals at a time.
    //
    // TODO(pag): Is this well-defined?
    if (!i) {
      os << "  if (__changed) {\n"
         << "    goto __restart;\n"
         << "  }\n";
    }
  }

  os << "}\n";
}

}  // namespace

// Generates BAM-like code following the push method of pipelined bottom-up
// execution of Datalog.
void GenerateCode(const ParsedModule &module, const Query &query,
                  OutputStream &os) {

  std::unordered_set<ParsedFunctor> seen_functors;

  os << "struct Stage;\n";

  DefineMergeSources(os, query);

  for (auto view : query.Maps()) {
    DeclareMap(os, view, seen_functors);
  }

  for (auto view : query.Aggregates()) {
    DeclareAggregate(os, view, seen_functors);
  }

  for (auto view : query.Joins()) {
    DeclareJoin(os, view);
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

  for (auto view : query.Tuples()) {
    DefineTuple(os, view);
  }

  for (auto view : query.KVIndices()) {
    DefineKVIndex(os, view);
  }

  for (auto view : query.Merges()) {
    DefineMerge(os, view);
  }

  for (auto view : query.Maps()) {
    DefineMap(os, view);
  }

  for (auto view : query.Constraints()) {
    DefineConstraint(os, view);
  }

  for (auto view : query.Aggregates()) {
    DefineAggregate(os, view);
  }

  DefineStep(os, query);
}

}  // namespace hyde
