// Copyright 2019, Trail of Bits. All rights reserved.

#include <drlojekyll/Lex/Format.h>
#include <drlojekyll/Display/Format.h>

namespace hyde {

OutputStream &operator<<(OutputStream &os, Token tok) {
  os << tok.SpellingRange();
  return os;
}

}  // namespace hyde
