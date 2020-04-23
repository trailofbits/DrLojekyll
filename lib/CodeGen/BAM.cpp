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
#include <drlojekyll/Rel/Format.h>
#include <drlojekyll/Sema/BottomUpAnalysis.h>
#include <drlojekyll/Sema/SIPSAnalysis.h>
#include <drlojekyll/Sema/SIPSScore.h>
#include <drlojekyll/Transforms/CombineModules.h>
#include <drlojekyll/Util/Compiler.h>

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

namespace hyde {
namespace {

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
    case TypeKind::kString:
      return "::hyde::rt::String";
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

template <typename T>
static std::string Spelling(const DisplayManager &dm, T named_thing) {
  std::string_view data;
  auto found_data = dm.TryReadData(named_thing.Name().SpellingRange(), &data);
  assert(found_data);
  (void) found_data;
  return std::string(data);
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

  auto do_arg_list = [&] (std::vector<QueryColumn> &cols) -> void {
    for (auto col : cols) {
      col_to_index.emplace(col, i);
      index_to_col.emplace(i, col);

      os << ",\n    " << TypeName(col) << " I" << i
         << CommentOnCol(os, col);

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

// Send the output columns in `view` to all users.
static void CallUsers(const DisplayManager &dm, OutputStream &os,
                      QueryView view, const char *indent) {
  std::unordered_set<QueryView> target_views;

  // Go find all views that we need to call.
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

  std::vector<QueryColumn> input_cols;

  for (auto target_view : ordered_views) {
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
      for (auto i = 0u; i < target_join.NumPivots(); ++i) {
        for (auto pivot_col : target_join.NthPivotSet(i)) {
          assert(!pivot_col.IsConstant());

          if (QueryView::Containing(pivot_col) == view) {
            input_cols.push_back(pivot_col);
          }
        }
      }

      // Filter out the proposed values.
      for (auto i = 0u, max_i = target_join.NumOutputColumns(); i < max_i; ++i) {
        auto in_col = target_join.NthInputColumn(i);
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

    os << "(__added";
    for (auto in_col : input_cols) {
      if (in_col.IsConstant()) {
        os << ", /* TODO constant */";  // TODO(pag): Handle constants/generators!

      } else if (in_col.IsGenerator()) {
        os << ", /* TODO generator */";  // TODO(pag): Handle this!

      } else if (QueryView::Containing(in_col) == view) {
        os << ", C" << in_col.UniqueId();

      } else {
        assert(false);
      }
    }

    os << ");\n";
  }
}

static void DeclareAggregate(
    OutputStream &os, QueryAggregate agg,
    std::unordered_set<ParsedFunctor> &seen_functors) {

  auto functor = agg.Functor();
  if (!seen_functors.count(functor)) {
    seen_functors.insert(functor);

    const auto binding_pattern = BindingPattern(functor);

    // Declare the tuple type as a structure.
    os << "\n"
       << "struct " << functor.Name() << '_' << binding_pattern << "_result {\n"
       << "  unsigned added;\n";

    for (auto param : functor.Parameters()) {
      switch (param.Binding()) {
        case ParameterBinding::kImplicit:
        case ParameterBinding::kMutable:
        case ParameterBinding::kFree:
          assert(false);
          break;

        case ParameterBinding::kSummary:
          os << "  " << TypeName(param.Type()) << ' ' << param.Name() << ";\n";
          break;

        case ParameterBinding::kBound:
        case ParameterBinding::kAggregate:
          break;
      }
    }

    // Forward declare the aggregator as returning a generator of the above
    // structure type.
    os << "};\n\n"
       << "extern \"C\" ::hyde::rt::Generator<"
       << functor.Name() << '_' << binding_pattern << "_result> "
       << functor.Name() << '_' << binding_pattern << "("
       << functor.Name() << "_config &__aggregator, "
       << "unsigned __added";

    for (auto param : functor.Parameters()) {
      switch (param.Binding()) {
        case ParameterBinding::kImplicit:
        case ParameterBinding::kMutable:
        case ParameterBinding::kFree:
          assert(false);
          break;

        case ParameterBinding::kBound:
        case ParameterBinding::kAggregate:
          os << ", " << TypeName(param.Type()) << ' ' << param.Name();
          break;

        case ParameterBinding::kSummary:
          break;
      }
    }

    os << ");\n\n";

    // If we have configuration variables, then we need a configuration
    // structure that embeds those variables.
    if (agg.NumConfigColumns()) {

      os << "// Aggregator object (will collect results); extends the empty\n"
         << "// to ensure a minimum size, but uses empty base class optimization\n"
         << "// to place config vars inside of the aggregator.\n"
         << "struct " << functor.Name() << '_' << binding_pattern << "_config"
         << " : public ::hyde::rt::AggregateConfiguration {\n";

      for (auto param : functor.Parameters()) {
        if (param.Binding() == ParameterBinding::kBound) {
          os << "  " << TypeName(param.Type()) << ' ' << param.Name() << ";\n";
        }
      }

      os << "};\n\n";

    // There are no configuration variables. Alias the empty configuration; the
    // aggregating functor will operate on it.
    } else {
      os << "// Aggregator object (will collect results); defaults to empty\n"
         << "// due to there being no config/group variables.\n"
         << "using " << functor.Name() << '_' << binding_pattern
         << "_config = ::hyde::rt::AggregateConfiguration;\n\n";
    }
  }

  os << "static void V" << agg.UniqueId() << "(unsigned __added";

  for (auto col : agg.InputGroupColumns()) {
    os << ", " << TypeName(col) << CommentOnCol(os, col);
  }

  for (auto col : agg.InputConfigurationColumns()) {
    os << ", " << TypeName(col) << CommentOnCol(os, col);
  }

  for (auto col : agg.InputAggregatedColumns()) {
    os << ", " << TypeName(col) << CommentOnCol(os, col);
  }

  os << ") noexcept;\n";
}


// Generate code associated with an aggregate.
static void DefineAggregate(
    const DisplayManager &dm, OutputStream &os, QueryAggregate agg) {
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

  const auto has_map = !group_cols.empty() || !config_cols.empty();

  // This means that we are grouping, and within each group, we need an
  // instance of a configured map.
  if (has_map) {
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

  } else {
    os << "// Aggregator without config or group vars.\n"
       << "static " << summarizer.Name() << '_' << binding_pattern
       << "_config A" << id << ";\n\n";
  }

  os << "void V" << id << "(\n    unsigned __input_added";

  auto [col_to_index, index_to_col] = ArgumentList(
      os, group_cols, config_cols, agg_cols);

  os << ") noexcept {\n";

  // Now name the column variables.
  for (auto [col, index] : col_to_index) {
    os << "  const auto C" << col.UniqueId() << " = I" << index << ";"
       << CommentOnCol(os, col) << '\n';
  }

  os << '\n';

  auto comma = "";
  if (has_map) {
    os << "  // Hash the group and config vars on behalf of the aggregate.\n"
       << "  const auto __hash = ::hyde::rt::Hash<";

    comma = "";
    for (auto col : group_cols) {
      os << comma << TypeName(col);
      comma = ", ";
    }

    for (auto col : config_cols) {
      os << comma << TypeName(col);
      comma = ", ";
    }

    os << ">::Update(" << QueryView::From(agg).Hash();
    for (auto col : group_cols) {
      os << ", C" << col.UniqueId();
    }

    for (auto col : config_cols) {
      os << ", C" << col.UniqueId();
    }

    os << ");\n\n";

    os << "  // Get (and possibly configure) the aggregator for this grouping.\n"
       << "  auto &__aggregator = MA" << id << "(__hash";
    for (const auto col : group_cols) {
      os << ", C" << col.UniqueId();
    }

    for (const auto col : config_cols) {
      os << ", "<< col.UniqueId();
    }

    os << ");\n\n";

  } else {
    os << "  const uint64_t __hash = " << QueryView::From(agg).Hash() << ";\n"
       << "  auto &__aggregator = A" << id << ";  // Ungrouped aggregator.\n\n";
  }

  if (!group_cols.empty()) {
    auto i = 0u;
    os << "  // Output group columns.\n";
    for (auto out_col : agg.GroupColumns()) {
      os << "  const auto C" << out_col.UniqueId() << " = C"
         << group_cols[i++].UniqueId() << ';'
         << CommentOnCol(os, out_col) << '\n';
    }
    os << '\n';
  }

  if (!config_cols.empty()) {
    auto i = 0u;
    os << "  // Output config columns.\n";
    for (auto out_col : agg.ConfigurationColumns()) {
      os << "  const auto C" << out_col.UniqueId() << " = C"
         << config_cols[i++].UniqueId() << ';'
         << CommentOnCol(os, out_col) << '\n';
    }
    os << '\n';
  }

  os << "  // Loop for each produced tuple (may produce removals).\n"
     << "  for (auto [__added";

  auto b = 0u;
  auto s = 0u;

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
        os << ", C" << agg.NthSummarizedColumn(s++).UniqueId();
        break;
    }
  }

  os << "] : " << summarizer.Name() << '_' << binding_pattern
     << "(__aggregator, __input_added";

  b = 0u;
  auto a = 0u;

  // Pass them in the order that they appear in the functor declaration.
  for (auto param : summarizer.Parameters()) {
    switch (param.Binding()) {
      case ParameterBinding::kImplicit:
      case ParameterBinding::kMutable:
      case ParameterBinding::kFree:
        assert(false);
        break;

      case ParameterBinding::kBound:
        os << ", C" << config_cols[b++].UniqueId();
        break;

      case ParameterBinding::kAggregate:
        os << ", C" << agg_cols[a++].UniqueId();
        break;

      case ParameterBinding::kSummary:
        break;
    }
  }

  os << ")) {\n";

  CallUsers(dm, os, QueryView::From(agg), "    ");

  os << "  }\n}\n\n";
}

static void DeclareTuple(OutputStream &os, QueryTuple tuple) {
  os << "static void V" << tuple.UniqueId() << "(unsigned __added";
  for (auto col : tuple.InputColumns()) {
    os << ", " << TypeName(col) << CommentOnCol(os, col);
  }
  os << ") noexcept;\n";
}

static void DefineTuple(const DisplayManager &dm, OutputStream &os,
                        QueryTuple view) {

  os << "/* Tuple; just forwards stuff to users. */\n"
     << "void V" << view.UniqueId() << "(\n    unsigned __added";
  for (auto col : view.Columns()) {
    os << ",\n    " << TypeName(col) << " C " << col.UniqueId()
       << CommentOnCol(os, col);
  }
  os << ") noexcept {\n";
  CallUsers(dm, os, QueryView::From(view), "  ");
  os << "}\n\n";
}

static void DeclareMerge(OutputStream &os, QueryMerge view) {
  os << "static void V" << view.UniqueId() << "(unsigned __added";
  for (auto col : view.Columns()) {
    os << ", " << TypeName(col) << CommentOnCol(os, col);
  }
  os << ") noexcept;\n";
}

static void DefineMerge(const DisplayManager &dm, OutputStream &os,
                        QueryMerge view) {

  os << "/* Merge; just forwards stuff to users. */\n"
     << "void V" << view.UniqueId() << "(\n    unsigned __added";
  for (auto col : view.Columns()) {
    os << ",\n    " << TypeName(col) << " C " << col.UniqueId()
       << CommentOnCol(os, col);
  }
  os << ") noexcept {\n";
  CallUsers(dm, os, QueryView::From(view), "  ");
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
       << "struct " << functor.Name() << '_' << binding_pattern << "_result {\n"
       << "  unsigned added;\n";

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
    os << "};\n\n"
       << "extern \"C\" ::hyde::rt::Generator<" <<  functor.Name()
       << '_' << binding_pattern << "_result> "
       << functor.Name() << '_' << binding_pattern << "(unsigned __added";

    for (auto param : functor.Parameters()) {
      if (param.Binding() == ParameterBinding::kBound) {
        os << ", " << TypeName(param.Type()) << " /* " << param.Name() << " */";
      }
    }

    os << ");\n\n";
  }

  os << "static void V" << map.UniqueId() << "(unsigned __added";
  for (auto col : map.InputColumns()) {
    os << ", " << TypeName(col) << CommentOnCol(os, col);
  }
  for (auto col : map.InputCopiedColumns()) {
    os << ", " << TypeName(col) << CommentOnCol(os, col);
  }
  os << ") noexcept;\n";
}

static void DefineMap(const DisplayManager &dm, OutputStream &os, QueryMap map) {
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
     << "static void V" << map.UniqueId() << "(\n    unsigned __input_added";

  std::vector<QueryColumn> cols;
  FillContainerFromRange(cols, map.InputColumns());
  FillContainerFromRange(cols, map.InputCopiedColumns());

  auto [col_to_id, id_to_col] = ArgumentList(os, cols);

  os << ") noexcept {\n";

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
     << "  for (auto [__added";

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
        os << ", C" << map.NthColumn(f++).UniqueId();
        break;
    }
  }


  os << "] : " << functor.Name() << '_' << binding_pattern
     << "(__input_added";

  auto b = 0u;

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
        os << ", C" << map.NthInputColumn(b++).UniqueId();
        break;
    }
  }

  os << ")) {\n";

  CallUsers(dm, os, QueryView::From(map), "    ");

  os << "  }\n}\n\n";
}

static void DeclareConstraint(OutputStream &os, QueryConstraint filter) {
  const auto lhs = filter.LHS();
  const auto rhs = filter.RHS();
  os << "static void V" << filter.UniqueId() << "(unsigned __added, "
     << TypeName(lhs) << CommentOnCol(os, lhs) << ", "
     << TypeName(rhs) << CommentOnCol(os, rhs);

  for (auto col : filter.InputCopiedColumns()) {
    os << ", " << TypeName(col) << CommentOnCol(os, col);
  }
  os << ") noexcept;\n";
}

}  // namespace

// Generates BAM-like code following the push method of pipelined bottom-up
// execution of Datalog.
void GenerateCode(
    const DisplayManager &dm, const ParsedModule &module, const Query &query,
    std::ostream &cxx_os) {

  OutputStream os(dm, cxx_os);
  std::unordered_set<ParsedFunctor> seen_functors;

  for (auto view : query.Tuples()) {
    DeclareTuple(os, view);
  }

  for (auto view : query.Merges()) {
    DeclareMerge(os, view);
  }

  for (auto view : query.Maps()) {
    DeclareMap(os, view, seen_functors);
  }

  for (auto view : query.Constraints()) {
    DeclareConstraint(os, view);
  }

  for (auto view : query.Aggregates()) {
    DeclareAggregate(os, view, seen_functors);
  }

  os << '\n';

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

  os << '\n';

  for (auto view : query.Tuples()) {
    DefineTuple(dm, os, view);
  }

  for (auto view : query.Merges()) {
    DefineMerge(dm, os, view);
  }

  for (auto view : query.Maps()) {
    DefineMap(dm, os, view);
  }

  for (auto view : query.Aggregates()) {
    DefineAggregate(dm, os, view);
  }

}

}  // namespace hyde
