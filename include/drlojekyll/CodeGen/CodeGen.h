// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

namespace hyde {

class Program;
class OutputStream;

// Emits C++ code for the given program to `os`.
void GenerateCxxDatabaseCode(const Program &module, OutputStream &os);

// Emits Python code for the given program to `os`.
void GeneratePythonDatabaseCode(const Program &module, OutputStream &os);

// Emits Python code to build up and collect messages to send to a database,
// or to collect messages published by the database and aggregate them into
// a single object.
void GeneratePythonInterfaceCode(const Program &program, OutputStream &os);

}  // namespace hyde
