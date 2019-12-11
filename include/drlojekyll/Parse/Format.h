// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include <ostream>
#include <drlojekyll/Display/DisplayManager.h>
#include <drlojekyll/Parse/Parse.h>

#pragma once

namespace hyde {

// Wrapper around a `std::ostream` that lets us stream out `Token`s and
// `DisplayRange`s.
class OutputStream {
 public:
  ~OutputStream(void);

  inline OutputStream(DisplayManager &display_manager_, std::ostream &os_)
      : display_manager(display_manager_),
        os(os_) {}

  OutputStream &operator<<(Token tok);

  OutputStream &operator<<(DisplayRange range);

  OutputStream &operator<<(ParsedDeclaration decl);
  
  OutputStream &operator<<(ParsedPredicate pred);
  
  OutputStream &operator<<(ParsedAggregate aggregate);
  
  OutputStream &operator<<(ParsedClause clause);
  
  OutputStream &operator<<(ParsedModule module);

  template <typename T>
  OutputStream &operator<<(T val) {
    os << val;
    return *this;
  }

  inline void SetKeepImports(bool state) {
    include_imports = state;
  }

  inline void SetRenameLocals(bool state) {
    rename_locals = state;
  }

 private:
  DisplayManager &display_manager;
  std::ostream &os;
  bool include_imports{true};
  bool rename_locals{false};
};

}  // namespace hyde
