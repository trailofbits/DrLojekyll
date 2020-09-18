// Copyright 2019, Trail of Bits. All rights reserved.

#include <drlojekyll/Display.h>
#include <drlojekyll/Lex.h>

namespace hyde {

OutputStream &operator<<(OutputStream &os, Token tok) {
  os << tok.SpellingRange();
  return os;
}

}  // namespace hyde
