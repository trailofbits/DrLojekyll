// Copyright 2019, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/Parse/Parse.h>
#include <optional>

namespace hyde {

class DisplayManager;
class ErrorLog;

// Combines `root_module` and everything it includes into a new module that
// contains all definitions and declarations.
std::optional<ParsedModule> CombineModules(
    const DisplayManager &display_manager, const ErrorLog &log,
    ParsedModule root_module);

}  // namespace hyde
