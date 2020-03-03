// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/CodeGen/BAM.h>

#include <bitset>
#include <functional>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>
#include <tuple>
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
//    and fille in any constants / generators, i.e. run the generator on the
//    producer-side.
//
//    Pass in a worker ID to each operator() ? Will that help?
#include <set>
namespace hyde {
namespace {

static const std::less<std::string> kStringComparator;

using Variable = std::pair<TypeLoc, std::string>;

static unsigned TypeSize(TypeLoc loc) {
  switch (loc.Kind()) {
    case TypeKind::kSigned8:
    case TypeKind::kUnsigned8:
      return 1;
    case TypeKind::kSigned16:
    case TypeKind::kUnsigned16:
      return 2;
    case TypeKind::kSigned32:
    case TypeKind::kUnsigned32:
    case TypeKind::kFloat:
      return 4;
    case TypeKind::kSigned64:
    case TypeKind::kUnsigned64:
    case TypeKind::kDouble:
    case TypeKind::kString:
      return 8;
    case TypeKind::kUUID:
      return 16;
    default:
      assert(false);
      return 0;
  }
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
    case TypeKind::kString:
      return "::hyde::rt::String";
    case TypeKind::kUUID:
      return "::hyde::rt::UUID";
    default:
      assert(false);
      return "void";
  }
}

template <typename T>
static std::string Spelling(const DisplayManager &dm, T named_thing) {
  std::string_view data;
  auto found_data = dm.TryReadData(named_thing.Name().SpellingRange(), &data);
  assert(found_data);
  (void) found_data;
  return std::string(data);
}

// Order larger-sized parameters first, so that we can minimize the amount
// of padding in a structure. When two variables have the same size,
// order them lexicographically.
std::vector<Variable> SizeOrderedVariables(const std::vector<Variable> &vars) {
  auto sorted_vars = vars;
  std::sort(sorted_vars.begin(), sorted_vars.end(),
            [] (const Variable &lhs, const Variable &rhs) {
              auto lhs_size = TypeSize(lhs.first);
              auto rhs_size = TypeSize(rhs.first);
              if (lhs_size > rhs_size) {
                return true;
              } else if (lhs_size < rhs_size) {
                return false;
              } else {
                return kStringComparator(lhs.second, rhs.second);
              }
            });
  return sorted_vars;
}

// Send the output columns in `view` to all users.
static void CallUsers(const DisplayManager &dm, OutputStream &os,
                      QueryView view, const char *indent) {
  std::unordered_set<QueryView> target_views;
  for (QueryColumn col : view.Columns()) {
    col.ForEachUser([&target_views] (QueryView user_view) {
      target_views.insert(user_view);
    });
  }

  for (auto target_view : target_views) {
    if (target_view.IsMerge()) {
      os << indent << "Local" << target_view.UniqueId() << '(';
      auto sep = "";
      for (auto col : view.Columns()) {
        os << sep << col.Variable();
        sep = ", ";
      }
      os << ");\n";

    } else if (target_view.IsJoin()) {
      auto target_join = QueryJoin::From(target_view);

      std::vector<unsigned> join_col_to_view_col;
      join_col_to_view_col.resize(target_view.Columns().size());

      for (auto i = 0u; i < target_join.NumPivots(); ++i) {
        for (auto pivot_col : target_join.NthPivotSet(i)) {
          if (QueryView::Containing(pivot_col) == view) {
//            join_col_to_view_col[i] =
          } else {

          }
        }
      }

    } else {

    }
  }
}

// Generate code associated with an aggregate.
static void GenerateAggregate(
    const DisplayManager &dm, OutputStream &os, QueryAggregate agg) {
  const auto id = agg.UniqueId();
  const auto summarizer = agg.Functor();

  std::vector<Variable> group_vars;
  std::vector<Variable> config_vars;
  std::vector<Variable> agg_vars;
  std::vector<Variable> output_vars;

  for (auto param : summarizer.Parameters()) {
    switch (param.Binding()) {
      case ParameterBinding::kBound:
        config_vars.emplace_back(param.Type(), Spelling(dm, param));
        HYDE_FALLTHROUGH;
      case ParameterBinding::kSummary:
        output_vars.emplace_back(param.Type(), Spelling(dm, param));
        break;
      case ParameterBinding::kAggregate:
        agg_vars.emplace_back(param.Type(), Spelling(dm, param));
        break;
      default:
        assert(false);
        break;
    }
  }

  for (auto col : agg.GroupColumns()) {
    const auto var = col.Variable();
    group_vars.emplace_back(var.Type(), Spelling(dm, var));
  }

  // If we have configuration variables, then we need a configuration
  // structure that embeds those variables.
  if (!config_vars.empty()) {
    os << "  struct C" << id
       << " : public ::hyde::rt::AggregateConfiguration {\n";

    for (const auto &[type, name] : SizeOrderedVariables(config_vars)) {
      os << "    " << TypeName(type) << ' ' << name << ";\n";
    }
    os << "  };\n";

  // There are no configuration variables. Alias the empty configuration; the
  // aggregating functor will operate on it.
  } else {
    os << "  using C" << id << " = ::hyde::rt::AggregateConfiguration;\n";
  }

  const auto has_map = !group_vars.empty() || !config_vars.empty();

  // This means that we are grouping, and within each group, we need an
  // instance of a configured
  if (has_map) {
    os << "  ::hyde::rt::Map<C" << id;
    for (const auto [type, name] : group_vars) {
      os << ", " << TypeName(type);
    }

    for (const auto [type, name] : config_vars) {
      os << ", " << TypeName(type);
    }
    os << "> MM" << id << ";\n";

  } else {
    os << "  C" << id << " MM" << id << ";\n";
  }

  os << "  inline C" << id << " &GetC" << id << "(uint64_t __hash";
  for (const auto [type, name] : group_vars) {
    os << ", " << TypeName(type) << ' ' << name;
  }

  for (const auto [type, name] : config_vars) {
    os << ", " << TypeName(type) << ' ' << name;
  }

  os << ") noexcept {\n";
  if (has_map) {
    os << "    return MM" << id << "(__hash";
    for (const auto [type, name] : group_vars) {
      os << ", " << name;
    }

    for (const auto [type, name] : config_vars) {
      os << ", "  << name;
    }

    os << ");\n";

  } else {
    os << "    return MM" << id << ";\n";
  }

  os << "  }\n"
     << "  void Local" << id << "(uint64_t __version";

  for (auto [type, name] : group_vars) {
    os << ", " << TypeName(type) << ' ' << name;
  }
  for (auto [type, name] : config_vars) {
    os << ", " << TypeName(type) << ' ' << name;
  }
  for (auto [type, name] : agg_vars) {
    os << ", " << TypeName(type) << ' ' << name;
  }

  os << ") noexcept {\n";

  if (has_map) {
    os << "    const auto __hash = this->Hash(__version";
    for (auto [type, name] : group_vars) {
      os << ", " << name;
    }
    for (auto [type, name] : config_vars) {
      os << ", " << name;
    }
    os << ");\n"
       << "    const auto __dest_worker_id = __hash & __num_workers_mask;\n"
       << "    if (__worker_id != __dest_worker_id) {\n"
       << "      return __workers[__dest_worker_id]->Remote" << id << "(__version";

    for (auto [type, name] : group_vars) {
      os << ", " << name;
    }
    for (auto [type, name] : config_vars) {
      os << ", " << name;
    }
    for (auto [type, name] : agg_vars) {
      os << ", " << name;
    }

    os << ");\n"
       << "    }\n";
  } else {
    os << "    const uint64_t __hash = 0;\n";
  }

  os << "    if (" << summarizer.Name() << "(";

  os << "GetC" << id << "(__hash";
  for (const auto [type, name] : group_vars) {
    os << ", " << name;
  }

  for (const auto [type, name] : config_vars) {
    os << ", "  << name;
  }

  os << "), __version";

  for (auto [type, name] : config_vars) {
    os << ", " << name;
  }
  for (auto [type, name] : agg_vars) {
    os << ", " << name;
  }
  os << ")) {\n";

  CallUsers(dm, os, QueryView::From(agg), "      ");

  os << "    } else {\n";


  os << "    }\n"
     << "  }\n";
}

}  // namespace

// Generates BAM-like code following the push method of pipelined bottom-up
// execution of Datalog.
void GenerateCode(
    const DisplayManager &dm, const Query &query, std::ostream &cxx_os) {

  OutputStream os(dm, cxx_os);
  os << "class Program final : public ::hyde::rt::Program<::Program> {\n"
     << " public:\n"
     << "  ~Program(void) = default;\n"
     << "  void Step(unsigned selector, unsigned step, void *data) noexcept override {\n";

  os << "  }\n"
     << "  void Init(void) noexcept override {\n"
     << "  }\n"
     << " private:\n";
  for (auto agg : query.Aggregates()) {
    GenerateAggregate(dm, os, agg);
  }

  os << "};\n";
}

}  // namespace hyde
