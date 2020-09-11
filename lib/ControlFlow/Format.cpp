// Copyright 2020, Trail of Bits. All rights reserved.

#include "Program.h"

#include <drlojekyll/DataFlow/Format.h>
#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Lex/Format.h>
#include <drlojekyll/Parse/Format.h>

namespace hyde {

OutputStream &operator<<(OutputStream &os, Program program) {
  (void) program;
  return os;
}

}  // namespace hyde
