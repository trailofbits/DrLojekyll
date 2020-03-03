// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

#include <iosfwd>

#include <drlojekyll/Parse/Parse.h>
#include <drlojekyll/Util/Node.h>

namespace hyde {

class DisplayManager;
class Query;

// Generates BAM-like code following the push method of pipelined bottom-up
// execution of Datalog.
void GenerateCode(
    const DisplayManager &display_manager, const Query &query,
    std::ostream &cxx_os);

}  // namespace hyde
