// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/Parse/Parse.h>

namespace hyde {

class DisplayManager;

// Transforms `module` so that all queries are rewritten to be messages,
// possibly pairs of input and output messages.
ParsedModule ConvertQueriesToMessages(
    DisplayManager &display_manager, ParsedModule module);

}  // namespace hyde
