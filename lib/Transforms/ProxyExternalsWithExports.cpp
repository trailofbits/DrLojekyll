// Copyright 2020, Trail of Bits, Inc. All rights reserved.

#include <drlojekyll/Transforms/ProxyExternalsWithExports.h>

#include <cassert>
#include <sstream>
#include <unordered_set>
#include <drlojekyll/Display/DisplayConfiguration.h>
#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Lex/Format.h>
#include <drlojekyll/Parse/ErrorLog.h>
#include <drlojekyll/Parse/Format.h>
#include <drlojekyll/Parse/Parser.h>
#include <drlojekyll/Transforms/CombineModules.h>

namespace hyde {
namespace {

static bool IsUsed(ParsedDeclaration decl) {
  for (auto use : decl.PositiveUses()) {
    (void) use;
    return true;
  }
  for (auto use : decl.NegativeUses()) {
    (void) use;
    return true;
  }
  return false;
}

static void ProxyExternal(
    hyde::OutputStream &os, ParsedDeclaration decl, const char *declarator) {
  os << decl << "\n";

  // Create the proxy rule.
  os << "#export " << decl.Name() << "_proxy(";
  auto comma = "";
  for (auto param : decl.Parameters()) {
    os << comma << param.Type() << " " << param.Name();
    comma = ", ";
  }

  // End the proxy declaration, and make a rule for the query that
  // directly defers to the proxy.
  os << ")\n" << decl.Name();
  comma = "(";
  for (auto param : decl.Parameters()) {
    os << comma << param.Name();
    comma = ", ";
  }
  os << ") : " << decl.Name();
  comma = "_proxy(";
  for (auto param : decl.Parameters()) {
    os << comma << param.Name();
    comma = ", ";
  }
  os << ").\n";
}

}  // namespace

// Transforms `module` so that all queries are rewritten to be messages,
// possibly pairs of input and output messages.
ParsedModule ProxyExternalsWithExports(DisplayManager &display_manager,
                                       ParsedModule module) {
  std::stringstream ss;
  hyde::OutputStream os(display_manager, ss);

  std::unordered_set<ParsedDeclaration> proxied_decls;

  if (module != module.RootModule()) {
    module = CombineModules(display_manager, module.RootModule());
  }

  for (auto _ : module.Imports()) {
    (void) _;
    module = CombineModules(display_manager, module.RootModule());
    break;
  }

  for (auto include : module.Includes()) {
    if (include.IsSystemInclude()) {
      os << include << "\n";
    }
  }

  for (auto include : module.Includes()) {
    if (!include.IsSystemInclude()) {
      os << include << "\n";
    }
  }

  for (auto decl : module.Messages()) {
    os << ParsedDeclaration(decl) << "\n";
  }

  for (auto decl : module.Functors()) {
    os << ParsedDeclaration(decl) << "\n";
  }

  for (auto decl : module.Exports()) {
    os << ParsedDeclaration(decl) << "\n";
  }

  for (auto decl : module.Locals()) {
    os << ParsedDeclaration(decl) << "\n";
  }

  for (auto query : module.Queries()) {
    const auto decl = ParsedDeclaration(query);
    bool has_bound_params = false;
    for (auto param : decl.Parameters()) {
      if (param.Binding() == ParameterBinding::kBound) {
        has_bound_params = true;
      }
    }

    if (has_bound_params || IsUsed(decl)) {
      proxied_decls.insert(decl);
      ProxyExternal(os, decl, "#query");
    } else {
      os << decl << "\n";
    }
  }

  for (auto clause : module.Clauses()) {
    auto decl = ParsedDeclaration::Of(clause);
    auto comma = "";
    if (proxied_decls.find(decl) != proxied_decls.end()) {
      os << decl.Name() << "_proxy(";
      for (auto var : clause.Parameters()) {
        os << comma << var;
        comma = ", ";
      }
      os << ")";

    } else {
      os << ParsedClauseHead(clause);
    }

    comma = " : ";
    for (auto assign : clause.Assignments()) {
      os << comma << assign;
      comma = ", ";
    }

    for (auto compare : clause.Comparisons()) {
      os << comma << compare;
      comma = ", ";
    }

    auto do_pred = [&] (ParsedPredicate pred) {
      decl = ParsedDeclaration::Of(pred);
      if (proxied_decls.find(decl) != proxied_decls.end()) {
        os << comma;
        if (pred.IsNegated()) {
          os << "!";
        }
        os << decl.Name();
        comma = "_proxy(";
        for (auto arg : pred.Arguments()) {
          os << comma << arg.Name();
          comma = ", ";
        }
        os << ")";
      } else {
        os << comma << pred;
      }
      comma = ", ";
    };

    for (auto pred : clause.PositivePredicates()) {
      do_pred(pred);
    }

    for (auto pred : clause.NegatedPredicates()) {
      do_pred(pred);
    }

    for (auto agg : clause.Aggregates()) {
      os << comma << agg.Functor();
      comma = " over ";
      do_pred(agg.Predicate());
    }

    os << ".\n";
  }

  DisplayConfiguration config;
  config.name = "<proxy-externals>";

  hyde::ErrorLog error_log;
  hyde::Parser parser(display_manager, error_log);
  auto transformed_module = parser.ParseStream(ss, config);
  assert(error_log.IsEmpty());
  return transformed_module;
}

}  // namespace hyde
