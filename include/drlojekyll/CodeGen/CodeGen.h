// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

namespace hyde {

class Program;
class OutputStream;

// Emits C++ code for the given program to `os`.
void GenerateCxxCode(const Program &module, OutputStream &os);

// Emits Python code for the given program to `os`.
void GeneratePythonCode(const Program &module, OutputStream &os);

}  // namespace hyde
