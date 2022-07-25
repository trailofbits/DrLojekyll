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

}  // namespace python
}  // namespace hyde
