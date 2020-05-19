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
//    Have a way of marking some queries as `materialized` or `populate`, to
//    say that we should eagerly fill them up.

namespace hyde {
namespace {

// For aggregates and k/v indices, to assign them to a worker.
static unsigned gNextUnhomedInbox = 1;

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


// Should this view use an inbox? Inboxes let us accumulate pending tuples to
// apply in bulk. They also permit us to share tuples across threads.
static bool UseInbox(QueryView view) {
  return view.IsAggregate() ||
         view.IsKVIndex() ||
         view.IsMap() ||
         view.IsJoin() ||
         view.IsSelect();
}

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

    const auto use_inbox = UseInbox(target_view);

    if (use_inbox) {
      os << indent << "__stages[(__wid * 2) + " << added << "].V"
         << target_view.UniqueId();
    } else {
      os << indent << "V" << target_view.UniqueId();
    }

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

    if (use_inbox) {
      os << "_inbox.emplace_back(";
    } else {
      os << '<' << added << ">(";
    }

    auto sep = "";
    for (auto in_col : input_cols) {
      os << sep << '\n' << indent  << "    ";
      if (in_col.IsConstant()) {
        os << "/* TODO constant */";  // TODO(pag): Handle constants/generators!

      } else if (in_col.IsGenerator()) {
        os << "/* TODO generator */";  // TODO(pag): Handle this!

      } else if (QueryView::Containing(in_col) == view) {
        os << "C" << in_col.UniqueId();

      } else {
        assert(false);
      }
      sep = ",";
    }

    if (target_view.IsMerge()) {
      os << sep << '\n' << indent  << "    " << "SOURCE_" << view.UniqueId() << '_'
         << target_view.UniqueId();
      sep = ",";
    }

    if (!use_inbox) {
      os << sep << '\n' << indent  << "    __stages, __wid, __nw";
    }

    os << ");\n";
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
}

// Generate code associated with an aggregate.
static void DefineAggregate(OutputStream &os, QueryAggregate agg) {
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
  os << " */\n\n";

  // This means that we are grouping, and within each group, we need an
  // instance of a configured map.
  os << "// Maps config/group vars to aggregators.\n"
     << "static ::hyde::rt::Aggregate<"
     << "\n    " << summarizer.Name() << '_' << binding_pattern
     << "_config  /* Configured aggregator type */";

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

  os << "> MA" << id << ";\n\n";

  //
  //  auto [col_to_index, index_to_col] = ArgumentList(
  //      os, group_cols, config_cols, agg_cols);

  os << "template <bool __added>\n"
     << "static void V" << id << "(\n    Stage *__stages"
     << ",\n    unsigned __wid"
     << ",\n    unsigned __nw) noexcept {\n"
     << "  " << summarizer.Name() << '_' << binding_pattern
     << " *__prev_agg = nullptr;\n"
     << "  auto __has_current = false;\n";

  auto sep = "";

  for (auto col : agg.GroupColumns()) {
    os << "  " << TypeName(col) << " prev_C" << col.UniqueId() << ", first_C"
       << col.UniqueId() << ';' << CommentOnCol(os, col) << '\n';
  }
  for (auto col : agg.ConfigurationColumns()) {
    os << "  " << TypeName(col) << " prev_C" << col.UniqueId() << ", first_C"
       << col.UniqueId() << ';' << CommentOnCol(os, col) << '\n';
  }
  for (auto col : agg.SummaryColumns()) {
    os << "  " << TypeName(col) << " prev_C" << col.UniqueId() << ", first_C"
       << col.UniqueId() << ';' << CommentOnCol(os, col) << '\n';
  }

  os << '\n'
     << "  auto &__vec = __stages[(__wid * 2) + __added].V" << id << "_inbox;\n"
     << "  std::sort(__vec->begin(), __vec->end());\n\n"
     << "  auto __update_prev = [=] (void) -> void {\n"
     << "    auto [";

  sep = "";
  for (const auto col : agg.SummaryColumns()) {
    os << sep << "new_C" << col.UniqueId();
    sep = ", ";
  }

  os << "] = __prev_agg.Summarize();\n"
     << "    if (__has_current && (";

  sep = "";
  for (const auto col : agg.SummaryColumns()) {
    os << sep << "new_C" << col.UniqueId() << " != " << "prev_C"
       << col.UniqueId();
    sep = " || ";
  }

  os << ")) {\n\n"
     << "      // Remove the old summary values.\n";

  for (const auto col : agg.GroupColumns()) {
    os << "      const auto C" << col.UniqueId() << " = prev_C"
       << col.UniqueId() << ';' << CommentOnCol(os, col) << '\n';
  }
  for (const auto col : agg.ConfigurationColumns()) {
    os << "      const auto C" << col.UniqueId() << " = prev_C"
       << col.UniqueId() << ';' << CommentOnCol(os, col) << '\n';
  }
  for (const auto col : agg.SummaryColumns()) {
    os << "      auto C" << col.UniqueId() << " = prev_C" << col.UniqueId()
       << ';' << CommentOnCol(os, col) << '\n';
  }

  AddTuple(os, QueryView::From(agg), "      ", "false");

  os << "\n"
     << "      // Send the new summary values.\n";

  for (const auto col : agg.SummaryColumns()) {
    os << "      C" << col.UniqueId() << " = new_C" << col.UniqueId() << ';'
       << CommentOnCol(os, col) << '\n';
  }

  AddTuple(os, QueryView::From(agg), "      ", "true");

  os << "\n"
     << "    // First tuple for this aggregate.\n"
     << "    } else if (__added && !__has_current) {\n";

  for (const auto col : agg.GroupColumns()) {
    os << "      const auto C" << col.UniqueId() << " = prev_C"
       << col.UniqueId() << ';' << CommentOnCol(os, col) << '\n';
  }
  for (const auto col : agg.ConfigurationColumns()) {
    os << "      const auto C" << col.UniqueId() << " = prev_C"
       << col.UniqueId() << ';' << CommentOnCol(os, col) << '\n';
  }

  for (const auto col : agg.SummaryColumns()) {
    os << "      C" << col.UniqueId() << " = new_C" << col.UniqueId() << ';'
       << CommentOnCol(os, col) << '\n';
  }

  AddTuple(os, QueryView::From(agg), "      ", "true");

  os << "    }\n"
     << "  };\n\n";

  // There aren't group/config columns, so we can
  if (!num_group_and_config_cols) {
    os << "  // There are no group/config columns to hash, so we can check\n"
       << "  // the entire range to see if it's on the right worker.\n"
       << "  if (1 < __nw) {\n"
       << "    if (auto __owid = " << (gNextUnhomedInbox++) << " % __nw; "
       << "__owid != __wid) {\n"
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
       << "    }\n"
       << "  }\n\n";
  }

  os << "  for (auto [";

  auto i = 0u;
  sep = "";
  std::unordered_map<QueryColumn, unsigned> col_to_index;
  for (auto col : index_to_col) {
    os << sep << 'I' << i;
    col_to_index.emplace(col, i++);
    sep = ", ";
  }

  os << "] : __vec) {\n";

  // Now name the column variables.
  for (auto [col, index] : col_to_index) {
    os << "    const auto C" << col.UniqueId() << " = I" << index << ";"
       << CommentOnCol(os, col) << '\n';
  }

  // If there are group/config columns then we can spread work out across
  // workers.
  if (num_group_and_config_cols) {
    os << "    if (1 < __nw) {\n"
       << "      const auto __hash = Hash<";

    sep = "";
    for (i = 0u; i < num_group_and_config_cols; ++i) {
      const auto col = index_to_col[i];
      os << sep << TypeName(col);
      sep = ", ";
    }

    os << ">::Update(";
    sep = "";
    for (i = 0u; i < num_group_and_config_cols; ++i) {
      os << sep << 'I' << i;
      sep = ", ";
    }
    os << ");\n\n"
       << "      // Send this tuple to another worker.\n"
       << "      if (const auto __owid = __hash % __nw; __owid != __wid) {\n"
       << "        if (__prev_agg) {\n"
       << "          __update_prev();\n"
       << "          __prev_agg = nullptr;\n"
       << "        }\n"
       << "        __stages[(2u * __owid) + __added].V" << agg.UniqueId()
       << "_inbox.emplace_back(";

    sep = "\n            ";
    for (auto j = 0u; j < i; ++j) {
      os << sep << 'I' << j << CommentOnCol(os, index_to_col[j]);
      sep = ",\n            ";
    }

    os << ");\n"
       << "        continue;\n"
       << "      }\n"
       << "    }\n\n";
  }

  os << "    if (__prev_agg) {\n\n";

  if (num_group_and_config_cols) {
    os << "      // This tuple belongs to a differently-configured aggregate.\n"
       << "      // Commit changes to that prior aggregate.\n"
       << "      if (";
    sep = "";
    for (i = 0; i < num_group_and_config_cols; ++i) {
      const auto col = QueryView::From(agg).Columns()[i];
      os << sep << 'I' << i << " != prev_C" << col.UniqueId();
      sep = " ||\n          ";
    }
    os << ") {\n"
       << "        __update_prev();\n"
       << "        __prev_agg = nullptr;\n\n"
       << "      // This tuple will be summarized into the same aggregate as the\n"
       << "      // previous tuple.\n"
       << "      } else {\n";

  } else {
    os << "      // We have the previous aggregator, and there are no group or\n"
       << "      // configuration columns that need to be checked, so unconditionally\n"
       << "      // update the aggregate.\n"
       << "      if constexpr (true) {\n";
  }

  os << "        " << summarizer.Name() << '_' << binding_pattern << "_update("
     << "__prev_agg, __added";
  for (i = num_group_and_config_cols; i < index_to_col.size(); ++i) {
    os << ", I" << i;
  }

  os << ");\n"
     << "        continue;\n"
     << "      }\n"
     << "    }\n\n"
     << "    __prev_agg = MA" << id << ".Get(";

  sep = "";
  for (i = 0u; i < num_group_and_config_cols; ++i) {
    os << sep << 'I' << i;
    sep = ", ";
  }
  os << ");\n\n"
     << "    // Check if we should initialize the aggregate. If so, pass in any\n"
     << "    // columns that are specified as `bound` to the functor; these are\n"
     << "    // so-called configuration columns.\n"
     << "    __has_current = __agg->IsInitialized();\n"
     << "    if (!__has_current) {\n"
     << "      if constexpr (!__added) {\n"
     << "        continue;  // Don't initialize if we're removing.\n"
     << "      }\n"
     << "      " << summarizer.Name() << '_' << binding_pattern << "_init(\n"
     << "          __prev_agg";

  for (const auto col : agg.InputConfigurationColumns()) {
    os << ",\n          C" << col.UniqueId() << CommentOnCol(os, col);
  }

  os << ");\n"
     << "    }\n\n"
     << "    // Update the current aggregate.\n";

  i = 0u;
  for (auto col : agg.GroupColumns()) {
    os << "    prev_C" << col.UniqueId() << " = I" << (i++) << ';'
       << CommentOnCol(os, col) << '\n';
  }
  for (auto col : agg.ConfigurationColumns()) {
    os << "    prev_C" << col.UniqueId() << " = I" << (i++) << ';'
       << CommentOnCol(os, col) << '\n';
  }

  os << "    " << summarizer.Name() << '_' << binding_pattern
     << "_update(__prev_agg, __added";
  for (auto i = num_group_and_config_cols; i < index_to_col.size(); ++i) {
    os << ", I"<< i;
  }
  os << ");\n";

  os << "  }\n\n"
     << "  // If we updated an aggregate then commit the changes.\n"
     << "  if (__prev_agg) {\n"
     << "    __update_prev();\n"
     << "  }\n"
     << "}\n\n";
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

// Define a KV Index that has no keys, and so just updates values.
static void DefineGlobalVarTail(OutputStream &os, QueryKVIndex view) {
  auto sep = "";
  const auto id = view.UniqueId();
  os << "  auto &__stage = __stages[(__wid * 2) + __added];\n"
     << "  if (1 < __nw) {\n"
     << "\n"
     << "    // These tuples are on the wrong worker, move them over.\n"
     << "    if (const auto __owid = " << (gNextUnhomedInbox++)
     << "u % __nw; __owid != __wid) {\n"
     << "      auto &__vec = __stage.V" << id << "_inbox;\n"
     << "      auto &__ovec = __stages[(__owid * 2) + __added].V"
     << id << "_inbox;\n"
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
  os << '\n'
     << "  for (auto [";
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
     << "static void V" << id
     << "(\n    Stage *__stages"
     << ",\n    unsigned __wid"
     << ",\n    unsigned __nw) noexcept {\n";

  if (!view.NumKeyColumns()) {
    DefineGlobalVarTail(os, view);
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
  for (auto col : view.KeyColumns()) {
    (void) col;
    os << sep << "std::get<" << (i++) << ">(__lhs)";
    sep = ", ";
  }
  os << ");\n"
     << "    const auto __rhs_keys = std::make_tuple(";

  sep = "";

  i = 0u;
  for (auto col : view.KeyColumns()) {
    (void) col;
    os << sep << "std::get<" << (i++) << ">(__rhs)";
    sep = ", ";
  }
  os << ");\n"
     << "    return __lhs_keys < __rhs_keys;\n"
     << "  };\n\n";

  for (auto col : view.Columns()) {
    os << "  " << TypeName(col) << " prev_" << col.UniqueId()
       << ", first_C" << col.UniqueId() << ';' << CommentOnCol(os, col) << '\n';
  }

  os << '\n'
     << "  const auto __update_prev = [=] (void) -> void {\n"
     << '\n'
     << "    // Only store this tuple if it's new\n"
     << "    if constexpr (__added) {\n"
     << "      if (__is_first || (";

  sep = "";
  for (auto col : view.ValueColumns()) {
    os << sep << "first_C" << col.UniqueId() << " != prev_C" << col.UniqueId();
    sep = " && ";
  }

  os << '\n'
     << "        // We have a prior value for this tuple, so add a removal entry\n"
     << "        if (!__is_first) {\n"
     << "          __stages[(__wid * 2) + false].V" << id << "_inbox.emplace_back(";

  sep = "\n              ";
  for (auto col : view.Columns()) {
    os << sep << "first_C" << col.UniqueId();
    sep = ",\n              ";
  }

  os << ");\n"
     << "        }\n"
     << "        KV" << id << ".Put(";

  sep = "";
  for (auto col : view.Columns()) {
    os << sep << "prev_C" << col.UniqueId();
    sep = ", ";
  }

  os << ");\n"
     << "        __stages[(__wid * 2) + true].V" << id << ".emplace_back(";
  sep = "\n            ";
  for (auto col : view.Columns()) {
    os << sep << "prev_C" << col.UniqueId();
    sep = ",\n            ";
  }

  os << ");\n"
     << "      }\n\n"
     << "    // Can't remove a tuple that doesn't exist.\n"
     << "    } else if (__is_first) {\n"
     << "      return;\n\n"
     << "    // Remove the tuple if the requests value for deletion matches the\n"
     << "    // present value of the tuple.\n"
     << "    } else if (";

  sep = "";
  for (auto col : view.ValueColumns()) {
    os << sep << "first_C" << col.UniqueId() << " == prev_C" << col.UniqueId();
    sep = " && ";
  }

  os << ") {\n"
     << "      KV" << id << ".Erase(";

  sep = "\n          ";
  for (auto col : view.KeyColumns()) {
    os << sep << "prev_C" << col.UniqueId();
    sep = ",\n          ";
  }

  os << ");\n"
     << "      __stages[(__wid * 2) + false].V" << id << ".emplace_back(";
  sep = "\n          ";
  for (auto col : view.Columns()) {
    os << sep << "prev_C" << col.UniqueId();
    sep = ",\n          ";
  }

  os << ");\n"
     << "    }\n"
     << "  }\n\n"
     << "  for (const auto [";

  sep = "";
  for (auto col : view.Columns()) {
    os << sep << "proposed_C" << col.UniqueId();
    sep = ", ";
  }

  os << "] : __vec) {\n\n"
     << "    // Check to see if we need to send this tuple to another worker.\n"
     << "    if (1 < __nw) {\n"
     << "      const auto __hash = Hash<";
  sep = "";
  for (auto col : view.KeyColumns()) {
    os << sep << TypeName(col);
    sep = ", ";
  }
  os << ">::Compute(";
  sep = "";
  for (auto col : view.KeyColumns()) {
    os << sep << "proposed_C" << col.UniqueId();
    sep = ", ";
  }
  os << ");\n"
     << "      if (const auto __owid = __hash % __nw; __owid != __wid) {\n"
     << "        __stages[(__owid * 2) + __added].emplace_back(";

  sep = "\n            ";
  for (auto col : view.Columns()) {
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
  for (auto col : view.KeyColumns()) {
    os << sep << "prev_C" << col.UniqueId() << " != proposed_C" << col.UniqueId();
    sep = " || ";
  }
  os << ") {\n"
     << "        __update_prev();\n"
     << "        __has_prev = false;\n\n"
     << "      // Merge with the prior values. Only do this if we're adding.\n"
     << "      } else if contexpr (__added) {\n";

  i = 0u;
  for (auto col : view.ValueColumns()) {
    os << "        prev_C" << col.UniqueId() << " = "
       << view.NthValueMergeFunctor(i++).Name() << "_merge(\n"
       << "            prev_C"
       << col.UniqueId() << ", proposed_C" << col.UniqueId() << ");\n";
  }

  // TODO(pag): Think about whether k/v removal should remove by key matching
  //            alone (the usual semantics, or by key+value matching (what we
  //            do here).
  os << "        continue;\n\n"
     << "      // We're trying to remove the K/V mapping. We only process removals\n"
     << "      // where the values match with the first values.\n"
     << "      } else if (__is_first || (";

  sep = "";
  for (auto col : view.ValueColumns()) {
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
  for (auto col : view.ValueColumns()) {
    os << sep << "curr_C" << col.UniqueId();
    sep = ", ";
  }

  os << sep << "__initialized] =\n        KV" << id << ".Get(";
  sep = "";
  for (auto col : view.KeyColumns()) {
    os << sep << "proposed_C" << col.UniqueId();
    sep = ", ";
  }
  os << ");\n"
     << "    __is_first = !__initialized;\n\n"
     << "    if (__initialized) {\n"
     << "      if constexpr (__added) {\n";

  i = 0u;
  for (auto col : view.ValueColumns()) {
    os << "        first_C" << col.UniqueId() << " = curr_C"
       << col.UniqueId() << ';' << CommentOnCol(os, col) << '\n'
       << "        prev_C" << col.UniqueId() << " = "
       << view.NthValueMergeFunctor(i++).Name() << "_merge(curr_C"
       << col.UniqueId() << ", proposed_C" << col.UniqueId() << ");\n";
  }
  os << "      } else {\n";
  for (auto col : view.ValueColumns()) {
    os << "        first_C" << col.UniqueId() << " = proposed_C"
       << col.UniqueId() << ';' << CommentOnCol(os, col) << '\n'
       << "        prev_C" << col.UniqueId() << " = proposed_C"
       << col.UniqueId() << ";\n";
  }

  os << "      }\n"
     << "    } else {\n";
  for (auto col : view.ValueColumns()) {
    os << "      first_C" << col.UniqueId() << " = proposed_C"
       << col.UniqueId() << ';' << CommentOnCol(os, col) << '\n'
       << "      prev_C" << col.UniqueId() << " = proposed_C"
       << col.UniqueId() << ";\n";
  }

  os << "    }\n"
     << "  }\n"
     << "  if (__has_prev) {\n"
     << "    __update_prev();\n"
     << "  }\n"
     << "}\n\n";
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
     << "static void V" << view.UniqueId() << '(';
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

    // For aggregates, we need to have a holding area for their inputs to
    // enable sharding the work across multiple threads. It also means that
    // we can have aggregates process all of their work in sequence. This
    // lets us compress out removals :-D
    if (view.IsAggregate()) {
      auto agg = QueryAggregate::From(view);
      os << "  std::vector<std::tuple<";
      for (const auto col : agg.InputGroupColumns()) {
        os << sep << TypeName(col);
        sep = ", ";
      }
      for (const auto col : agg.InputConfigurationColumns()) {
        os << sep << TypeName(col);
        sep = ", ";
      }
      for (const auto col : agg.InputAggregatedColumns()) {
        os << sep << TypeName(col);
        sep = ", ";
      }
      os << ">> V" << id << "_inbox;\n";

    // Joins have multiple inboxes.
    } else if (view.IsJoin()) {

    // For other things that need an inbox, e.g. merge, kvindex, and select,
    // we can more easily define their inboxes as having the same types as their
    // output columns.
    } else if (UseInbox(view)) {
      os << "  std::vector<std::tuple<";
      for (const auto col : view.Columns()) {
        os << sep << TypeName(col);
        sep = ", ";
      }
      os << ">> V" << id << "_inbox;\n";
    }

    sep = "";
    os << "  std::vector<std::tuple<";
    for (const auto col : view.Columns()) {
      os << sep << TypeName(col);
      sep = ", ";
    }
    if (view.IsMerge()) {
      os << sep << "RC" << id;
    }
    os << ">> V" << id << ";\n";
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

  os << "// Stepping function for advancing execution one step. This function\n"
     << "// takes in the current worker id `__wid` and the total number of\n"
     << "// workers `__nw`. `__stages` is an `__nw * 2` element array, where\n"
     << "// there are two entries per worker: the first is for removals, the\n"
     << "// second is for insertions.\n"
     << "extern \"C\" void Step(Stage *__stages, unsigned __wid, "
     << "unsigned __nw) {\n"
     << "  Stage *__stage = nullptr;\n"
     << "  bool __changed = false;\n\n";

  auto sep = "";

  os << "__restart:\n"
     << "  __changed = false;\n";

  auto i = 0;
  for (auto added : {"false", "true"}) {
    if (!i) {
      os << "\n"
         << "  // The first stage of dataflow execution is to process removals.\n"
         << "  // Removals will be processed until none are present.\n\n";
    } else {
      os << "\n"
         << "  // The second stage of dataflow execution is to process insertions,\n"
         << "  // which are processed in grouped, batched by the depth of the node\n"
         << "  // in the dataflow graph. If anything is changed, then we jump back\n"
         << "  // to the first stage to process removals.\n\n";
    }
    os << "  __stage = &(__stages[(2u * __wid) + " << (i++) << "]);\n";

    for (const auto &views : views_by_depth) {
      if (views.empty()) {
        continue;
      }

      for (auto view : views) {
        const auto id = view.UniqueId();
        const auto use_inbox = UseInbox(view);
        sep = "";

        if (view.IsJoin()) {

        // Process the aggregate inbox first.
        //
        // NOTE(pag): We don't need to reverse the order because there is no
        //            risk that the inbox will change while we're processing it.
        } else if (use_inbox) {
          os << "  // Process the inbox.\n"
             << "  if (auto __vec = &(__stage->V" << id
             << "_inbox); !__vec->empty()) {\n"
             << "    __changed = true;\n"
             << "    V" << id << '<' << added << ">(__stages, __wid, __nw);\n"
             << "    __vec->clear();\n"
             << "  }\n\n";
        }

        os << "  if (auto __vec = &(__stage->V" << id << "); !__vec->empty()) {\n"
           << "    __changed = true;\n"
           << "    ([=] (void) {\n"
           << "      std::reverse(__vec->begin(), __vec->end());\n"
           << "      do {\n"
           << "        auto [";

        sep = "";
        for (const auto col : view.Columns()) {
          os << sep << "C" << col.UniqueId() << CommentOnCol(os, col);
          sep = ",\n              ";
        }
        os << "] = __vec->back();\n"
           << "        __vec->pop_back();\n";
        CallUsers(os, view, "        ", added);
        os << "      } while (!__vec->empty());\n"
           << "    })();\n"
           << "  }\n\n";
      }

      // We let ourselves process all additions in a given level without
      // undoing.
      //
      // TODO(pag): Is this only well-defined for removals? Is this well-defined
      //            at all?
      if (i) {
        os << "  if (__changed) {\n"
           << "    goto __restart;\n"
           << "  }\n\n";
      }
    }

    // We let ourselves process all removals at a time.
    //
    // TODO(pag): Is this well-defined?
    if (!i) {
      os << "  if (__changed) {\n"
         << "    goto __restart;\n"
         << "  }\n\n";
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
