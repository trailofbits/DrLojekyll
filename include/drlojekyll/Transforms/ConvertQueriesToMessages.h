// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/Parse/Parse.h>
#include <optional>

namespace hyde {

class DisplayManager;
class ErrorLog;

// Transforms `module` so that all queries are rewritten to be messages,
// possibly pairs of input and output messages.
std::optional<ParsedModule> ConvertQueriesToMessages(
    const DisplayManager &display_manager, const ErrorLog &error_log,
    ParsedModule module);

}  // namespace hyde
