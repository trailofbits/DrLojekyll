// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

namespace hyde {

class Program;
class OutputStream;

// Emits transpiled C++ code for the given program to `os`.
void GenerateCode(Program &module, OutputStream &os);

}  // namespace hyde
