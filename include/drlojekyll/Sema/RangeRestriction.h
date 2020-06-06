// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

namespace hyde {

class DisplayManager;
class ErrorLog;
class ParsedModule;

// Ensures that all variables used in the heads of clauses are used in their
// bodies.
void CheckForRangeRestrictionErrors(const DisplayManager &dm,
                                    const ParsedModule &module,
                                    const ErrorLog &log);

}  // namespace hyde
