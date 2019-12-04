// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include <cstdlib>
#include <cstring>
#include <cassert>
#include <iostream>
#include <sstream>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <vector>

#include <drlojekyll/Display/DisplayConfiguration.h>
#include <drlojekyll/Display/DisplayManager.h>
#include <drlojekyll/Parse/ErrorLog.h>
#include <drlojekyll/Parse/Parser.h>

#pragma once

namespace hyde {

// Wrapper around a `std::ostream` that lets us stream out `Token`s and
// `DisplayRange`s.
class OutputStream {
 public:
  ~OutputStream(void);

  OutputStream(DisplayManager &display_manager_, std::ostream &os_);

  OutputStream &operator<<(Token tok);

  OutputStream &operator<<(DisplayRange range);

  template <typename T>
  OutputStream &operator<<(T val);

 private:
  DisplayManager display_manager;
  std::ostream &os;
};

void FormatDecl(OutputStream &os, ParsedDeclaration decl);

void FormatPredicate(OutputStream &os, ParsedPredicate pred);

void FormatAggregate(OutputStream &os, ParsedAggregate aggregate);

void FormatClause(OutputStream &os, ParsedClause clause);

void FormatModule(OutputStream &os, ParsedModule module);

}  // namespace hyde
