// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/ControlFlow/Program.h>
#include <drlojekyll/Display/Format.h>

namespace hyde {

// Emits transpiled C++ code for the given program to `os`.
void GenerateCode(Program &module, OutputStream &os);

}  // namespace hyde
