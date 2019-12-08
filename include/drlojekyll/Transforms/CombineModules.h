// Copyright 2019, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/Parse/Parse.h>

namespace hyde {

class DisplayManager;

// Combines `root_module` and everything it includes into a new module that
// contains all definitions and declarations.
ParsedModule CombineModules(
    DisplayManager &display_manager, ParsedModule root_module);

}  // namespace hyde
