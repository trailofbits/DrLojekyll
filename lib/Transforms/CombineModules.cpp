// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include <drlojekyll/Transforms/CombineModules.h>

#include <sstream>
#include <cassert>

#include <drlojekyll/Display/DisplayConfiguration.h>
#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Parse/ErrorLog.h>
#include <drlojekyll/Parse/Format.h>
#include <drlojekyll/Parse/Parser.h>
#include <drlojekyll/Sema/ModuleIterator.h>

namespace hyde {

// Combines `root_module` and everything it includes into a new module that
// contains all definitions and declarations.
std::optional<ParsedModule> CombineModules(
    const DisplayManager &display_manager, const ErrorLog &error_log,
    ParsedModule root_module) {
  auto has_import = false;
  for (auto import : root_module.Imports()) {
    has_import = true;
    (void) import;
  }

  if (!has_import) {
    return root_module;
  }

  std::stringstream ss;
  OutputStream os(display_manager, ss);
  os.SetKeepImports(false);
  os.SetRenameLocals(true);
  for (auto module : ParsedModuleIterator(root_module)) {
    os << module;
  }

  DisplayConfiguration config;
  config.name = "<amalgamation>";

  hyde::Parser parser(display_manager, error_log);
  return parser.ParseStream(ss, config);
}

}  // namespace hyde
