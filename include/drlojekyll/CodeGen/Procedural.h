// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <memory>

namespace hyde {

class Program {
 public:
  class Impl;

 private:
  std::shared_ptr<Impl> impl;
};

// Represents a procedure. A procedure operates on concrete arguments.
class Procedure {
 public:
  class Impl;

 private:
  Impl * const impl;
};

// A generator is similar to a procedure. It may operate on one or more
// concrete arguments, and on each invocation, yields a tuple of values.
//
// A generator is an abstraction over functors and queries.
class Generator {

};

// Represents a set of of tuples.
class Set {
 public:
  class Impl;

 private:
  Impl * const impl;
};

}  // namespace hyde
