// Copyright 2019, Trail of Bits. All rights reserved.

#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Lex/Format.h>

namespace hyde {

OutputStream &operator<<(OutputStream &os, Token tok) {
  os << tok.SpellingRange();
  return os;
}

}  // namespace hyde
