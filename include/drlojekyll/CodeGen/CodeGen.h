// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

namespace hyde {

class Program;
class OutputStream;

namespace cxx {

// Emits C++ code for the given program to `os`.
void GenerateDatabaseCode(const Program &module, OutputStream &os);

// Emits C++ code to build up and collect messages to send to a database,
// or to collect messages published by the database and aggregate them into
// a single object.
void GenerateInterfaceCode(const Program &program, OutputStream &os);

}  // namespace cxx
namespace python {

// Emits Python code for the given program to `os`.
void GenerateDatabaseCode(const Program &module, OutputStream &os);

// Emits Python code to build up and collect messages to send to a database,
// or to collect messages published by the database and aggregate them into
// a single object.
void GenerateInterfaceCode(const Program &program, OutputStream &os);

}  // namespace python
}  // namespace hyde
