// Copyright 2020, Trail of Bits, Inc. All rights reserved.

#include <drlojekyll/Transforms/ConvertQueriesToMessages.h>

#include <cassert>
#include <sstream>
#include <utility>
#include <vector>

#include <drlojekyll/Display/DisplayConfiguration.h>
#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Lex/Format.h>
#include <drlojekyll/Parse/ErrorLog.h>
#include <drlojekyll/Parse/Format.h>
#include <drlojekyll/Parse/Parser.h>
#include <drlojekyll/Transforms/CombineModules.h>

namespace hyde {

// Transforms `module` so that all queries are rewritten to be messages,
// possibly pairs of input and output messages.
ParsedModule ConvertQueriesToMessages(DisplayManager &display_manager,
                                      ParsedModule module) {
  std::stringstream ss;
  hyde::OutputStream os(display_manager, ss);

  std::vector<std::pair<unsigned, ParsedParameter>> bound_params;
  std::vector<std::pair<unsigned, ParsedParameter>> free_params;

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

  for (auto code : module.Inlines()) {
    os << code << "\n";
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
    auto decl = ParsedDeclaration(query);

    bound_params.clear();
    free_params.clear();

    auto i = 0u;
    for (auto param : decl.Parameters()) {
      if (param.Binding() == ParameterBinding::kBound) {
        bound_params.emplace_back(i++, param);
      } else {
        free_params.emplace_back(i++, param);
      }
    }

    if (free_params.empty()) {
      continue;
    }

    if (!bound_params.empty()) {
      // Queries with bound parameters are treated converted into requests with
      // a globally unique ID, so that we can use bottom-up execution to satisfy
      // the queries, and so that the code doing the querying can listen to
      // messages in order to see the results of their query.
      os << "#message request_" << query.Name();
      auto comma = "(";
      for (auto param : bound_params) {
        os << comma << param.second.Type() << " " << param.second.Name();
        comma = ", ";
      }
      os << comma << "@uuid _RequestID)\n";


      // TODO(pag): Have a message that can remove a request. Parser support
      //            is needed.

      // Have a local that stores the query request persistently.
      os << "#local requested_" << query.Name();
      comma = "(";
      for (auto param : bound_params) {
        os << comma << param.second.Type() << " " << param.second.Name();
        comma = ", ";
      }

      os << comma << "@uuid _RequestID)\n";

      // Define the clause for `requested_<name>` to simply persist the
      // data associated with `request_<name>` for the query `<name>`.
      os << "requested_" << query.Name();
      comma = "(";
      for (auto param : bound_params) {
        os << comma << param.second.Name();
        comma = ", ";
      }
      os << comma << "_RequestID) : request_" << query.Name();
      comma = "(";
      for (auto param : bound_params) {
        os << comma << param.second.Name();
        comma = ", ";
      }
      os << comma << "_RequestID).\n";
    }

    // The response message is what query-ers will subscribe to (behind the
    // scenes).
    os << "#message response_" << query.Name();
    auto comma = "(";
    for (auto param : free_params) {
      os << comma << param.second.Type() << " " << param.second.Name();
      comma = ", ";
    }
    os << ")\n";

    // Convert the query into a local that depends on the `requested_` local,
    // which is generated from the `request_` message.
    os << "#local " << query.Name();
    comma = "(";
    for (auto param : decl.Parameters()) {
      os << comma << param.Type() << " " << param.Name();
      comma = ", ";
    }
    os << ")\n";

    for (auto clause : query.Clauses()) {
      os << ParsedClauseHead(clause) << " : ";

      // Start with the requested data as a left corner; we'll try to sink it
      // later.
      if (!bound_params.empty()) {
        os << "requested_" << query.Name();
        comma = "(";
        for (auto bound_param : bound_params) {
          os << comma << clause.NthParameter(bound_param.first);
          comma = ", ";
        }
        os << comma << "_RequestID), ";
      }

      os << ParsedClauseBody(clause) << ".\n";
    }
  }

  for (auto clause : module.Clauses()) {
    os << clause << "\n";
  }

  // TODO(pag): Have the local `<name>` publish to the message `response_<name>`
  //            Parser support is needed.

  DisplayConfiguration config;
  config.name = "<remove-query>";

  hyde::ErrorLog error_log;
  hyde::Parser parser(display_manager, error_log);
  auto transformed_module = parser.ParseStream(ss, config);
  assert(error_log.IsEmpty());
  return transformed_module;
}

}  // namespace hyde
