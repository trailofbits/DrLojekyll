// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/Parse/Parse.h>

namespace hyde {

class DisplayManager;

// Transforms `module` so that all queries and messages that are used become
// "shims" around an export.
//
// This gives us more flexibility in terms of writing rules that use `#query`is,
// because their binding constraints don't need to be made, and it lets us make
// explicit the binding constraints in the relational form in terms of concrete
// INPUTs against which stuff must be joined.
ParsedModule ProxyExternalsWithExports(
    DisplayManager &display_manager, ParsedModule module);

}  // namespace hyde
