// Copyright 2019, Trail of Bits. All rights reserved.

#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Lex/Format.h>

namespace hyde {

OutputStream &operator<<(OutputStream &os, Token tok) {
  switch (tok.Lexeme()) {
    case Lexeme::kLiteralTrue: os << "true"; break;
    case Lexeme::kLiteralFalse: os << "false"; break;
    default: os << tok.SpellingRange(); break;
  }
  return os;
}

}  // namespace hyde
