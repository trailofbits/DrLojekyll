// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <memory>

namespace hyde {

class DisplayReader;
class StringPool;
class Token;

class LexerImpl;

// Reads one or more characters from a display, and then
class Lexer {
 public:
  ~Lexer(void);

  Lexer(void) = default;

  // Read tokens from `reader`.
  void ReadFromDisplay(const DisplayReader &reader);

  // Try to read the next token from the lexer. If successful, returns `true`
  // and updates `*tok_out`. If no more tokens can be produced, then `false` is
  // returned. Error conditions are signalled via special lexemes.
  bool TryGetNextToken(const StringPool &string_pool, Token *tok_out);

 private:
  std::shared_ptr<LexerImpl> impl;
};

}  // namespace hyde
