// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/Parse/Parse.h>

namespace hyde {

class Program;
class OutputStream;
enum class Language : unsigned;

std::vector<ParsedFunctor> Functors(ParsedModule module);
std::vector<ParsedQuery> Queries(ParsedModule module);
std::vector<ParsedMessage> Messages(ParsedModule module);
std::vector<ParsedInline> Inlines(ParsedModule module, Language lang);

namespace cxx {

// Emits C++ RPC code for the given program to `os`.
void GenerateServerCode(const Program &module, OutputStream &os);

// Emits C++ RPC code for the given program to `header_os` and `impl_os`.
void GenerateClientCode(const Program &module, OutputStream &header_os,
                        OutputStream &impl_os);

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
namespace flat {

// Emits Google FlatBuffer code that represents the actual serialization
// representations.
void GenerateInterfaceCode(const Program &program, OutputStream &os);

}  // namespace flat
}  // namespace hyde
