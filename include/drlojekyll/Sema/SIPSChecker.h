// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

namespace hyde {

class DisplayManager;
class ErrorLog;
class ParsedModule;

// Check for generic errors in clauses.
void CheckForErrors(const DisplayManager &dm, const ParsedModule &module,
                    const ErrorLog &log);

}  // namespace hyde