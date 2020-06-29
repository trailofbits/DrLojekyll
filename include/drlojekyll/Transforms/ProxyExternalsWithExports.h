// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/Parse/Parse.h>
#include <optional>

namespace hyde {

class DisplayManager;
class ErrorLog;

// Transforms `module` so that all queries and messages that are used become
// "shims" around an export.
//
// This gives us more flexibility in terms of writing rules that use `#query`is,
// because their binding constraints don't need to be made, and it lets us make
// explicit the binding constraints in the relational form in terms of concrete
// INPUTs against which stuff must be joined.
std::optional<ParsedModule> ProxyExternalsWithExports(
    const DisplayManager &display_manager, const ErrorLog &error_log,
    ParsedModule module);

}  // namespace hyde
