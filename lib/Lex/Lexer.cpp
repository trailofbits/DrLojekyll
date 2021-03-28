// Copyright 2019, Trail of Bits. All rights reserved.

#include <drlojekyll/Display/Display.h>
#include <drlojekyll/Display/DisplayManager.h>
#include <drlojekyll/Display/DisplayReader.h>
#include <drlojekyll/Lex/Lexer.h>
#include <drlojekyll/Lex/StringPool.h>

#include <cassert>
#include <cctype>
#include <string>

#include "Token.h"

namespace hyde {
namespace {

static const std::string kStopChars =
    " \r\t\n,.()!#{}[]<>:;=-+*/?\\|&^$%\"'~`@_";

}  // namespace

class LexerImpl {
 public:
  inline explicit LexerImpl(DisplayReader reader_) : reader(reader_) {}

  DisplayReader reader;

  // Data being read.
  std::string data;
};

Lexer::~Lexer(void) {}

// Enter into a display.
void Lexer::ReadFromDisplay(const hyde::DisplayReader &reader) {
  std::make_shared<LexerImpl>(reader).swap(impl);
}

// Try to read the next token from the lexer. If successful, returns `true`
// and updates `*tok_out`. If no more tokens can be produced, then `false` is
// returned. Error conditions are signalled via special lexemes.
bool Lexer::TryGetNextToken(const StringPool &string_pool, Token *tok_out) {
  if (!impl) {
    return false;
  }

  impl->data.clear();

  char ch = '\0';

  // Accumulator for adding characters to `impl->data`.
  auto accumulate_if = [=](auto accept) {
    char acc_ch = '\0';
    while (impl->reader.TryReadChar(&acc_ch)) {
      if (!accept(acc_ch)) {
        impl->reader.UnreadChar();
        break;
      } else {
        impl->data.push_back(acc_ch);
      }
    }
  };

  // Update the output token with our current position.
  Token temp;
  Token &ret = tok_out ? *tok_out : temp;

  ret = {};
  ret.position = impl->reader.CurrentPosition();

  if (!impl->reader.TryReadChar(&ch)) {

    // There was an error condition when reading characters. This could be due
    // to the stream underlying the display, or due to there being an invalid
    // character in the display.
    if (impl->reader.TryGetErrorMessage(nullptr)) {
      auto &error = ret.As<lex::ErrorToken>();
      error.Store<Lexeme>(Lexeme::kInvalidStreamOrDisplay);
      error.Store<char>('\0');
      error.Store<lex::ErrorIndexDisp>(0u);
      error.Store<lex::ErrorLineDisp>(0u);
      error.Store<lex::ErrorColumn>(ret.position.Column());
      error.Store<lex::IndexDisp>(0u);
      error.Store<lex::LineDisp>(0u);
      error.Store<lex::Column>(ret.position.Column());

    // We've reached the end of this display, yield a token and pop this
    // display off the stack.
    } else {
      auto &basic = ret.As<lex::BasicToken>();
      basic.Store<Lexeme>(Lexeme::kEndOfFile);
    }

    impl.reset();
    return true;
  }

  impl->data.push_back(ch);
  auto tentative_lexeme = Lexeme::kInvalid;
  auto tentative_type_kind = TypeKind::kInvalid;
  auto tentative_has_decimal_point = false;

  switch (ch) {

    // Accumulate whitespace into a single token, then summarizes the
    // whitespace in terms of its total displacement relative to the next
    // token.
    case '\n':
    case ' ': {
      accumulate_if(
          [](char next_ch) { return next_ch == ' ' || next_ch == '\n'; });

      uint32_t num_leading_spaces = 0;
      uint32_t num_leading_lines = 0;
      for (auto wch : impl->data) {
        if (' ' == wch) {
          num_leading_spaces += 1;
        } else {
          num_leading_spaces = 0;
          num_leading_lines += 1;
        }
      }

      (void) num_leading_lines;

      const auto after_pos = impl->reader.CurrentPosition();
      auto &ws = ret.As<lex::WhitespaceToken>();
      ws.Store<Lexeme>(Lexeme::kWhitespace);
      ws.Store<lex::SpellingWidth>(num_leading_spaces);
      ws.Store<lex::LineDisp>(after_pos.Line() - ret.position.Line());
      ws.Store<lex::IndexDisp>(after_pos.Index() - ret.position.Index());
      ws.Store<lex::Column>(after_pos.Column());
      return true;
    }

    case '(': {
      auto &basic = ret.As<lex::BasicToken>();
      basic.Store<Lexeme>(Lexeme::kPuncOpenParen);
      basic.Store<lex::SpellingWidth>(1);
      return true;
    }

    case ')': {
      auto &basic = ret.As<lex::BasicToken>();
      basic.Store<Lexeme>(Lexeme::kPuncCloseParen);
      basic.Store<lex::SpellingWidth>(1);
      return true;
    }

    case '{': {
      auto &basic = ret.As<lex::BasicToken>();
      basic.Store<Lexeme>(Lexeme::kPuncOpenBrace);
      basic.Store<lex::SpellingWidth>(1);
      return true;
    }

    case '}': {
      auto &basic = ret.As<lex::BasicToken>();
      basic.Store<Lexeme>(Lexeme::kPuncCloseBrace);
      basic.Store<lex::SpellingWidth>(1);
      return true;
    }

    case '.': {
      auto &basic = ret.As<lex::BasicToken>();
      basic.Store<Lexeme>(Lexeme::kPuncPeriod);
      basic.Store<lex::SpellingWidth>(1);
      return true;
    }

    case ',': {
      auto &basic = ret.As<lex::BasicToken>();
      basic.Store<Lexeme>(Lexeme::kPuncComma);
      basic.Store<lex::SpellingWidth>(1);
      return true;
    }

    case '=': {
      auto &basic = ret.As<lex::BasicToken>();
      basic.Store<Lexeme>(Lexeme::kPuncEqual);
      basic.Store<lex::SpellingWidth>(1);
      return true;
    }

    case '?': {
      auto &basic = ret.As<lex::BasicToken>();
      basic.Store<Lexeme>(Lexeme::kPuncQuestion);
      basic.Store<lex::SpellingWidth>(1);
      return true;
    }

    case '*': {
      auto &basic = ret.As<lex::BasicToken>();
      basic.Store<Lexeme>(Lexeme::kPuncStar);
      basic.Store<lex::SpellingWidth>(1);
      return true;
    }

    case '+': {
      auto &basic = ret.As<lex::BasicToken>();
      basic.Store<Lexeme>(Lexeme::kPuncPlus);
      basic.Store<lex::SpellingWidth>(1);
      return true;
    }

    case '!': {
      auto &basic = ret.As<lex::BasicToken>();

      if (impl->reader.TryReadChar(&ch)) {
        if (ch == '=') {
          basic.Store<Lexeme>(Lexeme::kPuncNotEqual);
          basic.Store<lex::SpellingWidth>(2);

        } else {
          impl->reader.UnreadChar();
          basic.Store<Lexeme>(Lexeme::kPuncExclaim);
          basic.Store<lex::SpellingWidth>(1);
        }
      } else {
        basic.Store<Lexeme>(Lexeme::kPuncExclaim);
        basic.Store<lex::SpellingWidth>(1);
      }

      return true;
    }

    case '<': {
      auto &basic = ret.As<lex::BasicToken>();
      basic.Store<Lexeme>(Lexeme::kPuncLess);
      basic.Store<lex::SpellingWidth>(1);
      return true;
    }

    case '`':
      if (!impl->reader.TryReadChar(&ch) || ch != '`' ||
          !impl->reader.TryReadChar(&ch) || ch != '`') {
      return_invalid_code:
        auto &error = ret.As<lex::ErrorToken>();
        const auto error_pos = impl->reader.CurrentPosition();
        error.Store<Lexeme>(Lexeme::kInvalidUnterminatedCode);
        error.Store<lex::ErrorIndexDisp>(error_pos.Index() -
                                         ret.position.Index());
        error.Store<lex::ErrorLineDisp>(error_pos.Line() - ret.position.Line());
        error.Store<lex::ErrorColumn>(error_pos.Column());
        error.Store<lex::IndexDisp>(error_pos.Index() - ret.position.Index());
        error.Store<lex::LineDisp>(error_pos.Line() - ret.position.Line());
        error.Store<lex::Column>(error_pos.Column());
        error.Store<char>(ch);
        return true;

      } else {
        impl->data.clear();
        while (impl->reader.TryReadChar(&ch)) {
          if (ch != '`') {
            impl->data.push_back(ch);

          } else if (!impl->reader.TryReadChar(&ch)) {
            goto return_invalid_code;

          } else if (ch != '`') {
            impl->data.push_back(ch);

          } else if (!impl->reader.TryReadChar(&ch)) {
            goto return_invalid_code;

          } else if (ch != '`') {
            impl->data.push_back(ch);

          } else if (!impl->data.find("c++")) {
            impl->data.erase(impl->data.begin(), impl->data.begin() + 3u);
            const auto after_pos = impl->reader.CurrentPosition();

            auto &literal = ret.As<lex::CodeLiteralToken>();
            literal.Store<Lexeme>(Lexeme::kLiteralCxxCode);
            literal.Store<lex::Id>(string_pool.InternCode(impl->data));
            literal.Store<lex::ReprWidth>(impl->data.size());
            literal.Store<lex::IndexDisp>(after_pos.Index() -
                                          ret.position.Index());
            literal.Store<lex::LineDisp>(after_pos.Line() -
                                         ret.position.Line());
            literal.Store<lex::Column>(after_pos.Column());
            return true;

          } else if (!impl->data.find("python")) {
            impl->data.erase(impl->data.begin(), impl->data.begin() + 6u);
            const auto after_pos = impl->reader.CurrentPosition();

            auto &literal = ret.As<lex::CodeLiteralToken>();
            literal.Store<Lexeme>(Lexeme::kLiteralPythonCode);
            literal.Store<lex::Id>(string_pool.InternCode(impl->data));
            literal.Store<lex::ReprWidth>(impl->data.size());
            literal.Store<lex::IndexDisp>(after_pos.Index() -
                                          ret.position.Index());
            literal.Store<lex::LineDisp>(after_pos.Line() -
                                         ret.position.Line());
            literal.Store<lex::Column>(after_pos.Column());
            return true;

          } else {
            const auto after_pos = impl->reader.CurrentPosition();
            auto &literal = ret.As<lex::CodeLiteralToken>();
            literal.Store<Lexeme>(Lexeme::kLiteralCode);
            literal.Store<lex::Id>(string_pool.InternCode(impl->data));
            literal.Store<lex::ReprWidth>(impl->data.size());
            literal.Store<lex::IndexDisp>(after_pos.Index() -
                                          ret.position.Index());
            literal.Store<lex::LineDisp>(after_pos.Line() -
                                         ret.position.Line());
            literal.Store<lex::Column>(after_pos.Column());
            return true;
          }
        }
        goto return_invalid_code;
      }

    case '>': {
      auto &basic = ret.As<lex::BasicToken>();
      basic.Store<Lexeme>(Lexeme::kPuncGreater);
      basic.Store<lex::SpellingWidth>(1);
      return true;
    }

    case ':': {
      auto &basic = ret.As<lex::BasicToken>();
      basic.Store<Lexeme>(Lexeme::kPuncColon);
      basic.Store<lex::SpellingWidth>(1);
      return true;
    }

    // String literal.
    case '"': {
      auto spelling_width = 1u;
      auto error_offset = 1u;
      auto prev_was_escape = false;
      auto is_invalid = false;
      auto is_finished = false;
      impl->data.clear();

      // Go collect all the characters of the string, and render them
      // into `impl->data`.
      while (impl->reader.TryReadChar(&ch)) {
        spelling_width += 1u;
        error_offset += 1u;

        if ('\n' == ch || '\r' == ch) {
          if (!is_invalid) {
            const auto error_pos = impl->reader.CurrentPosition();
            auto &error = ret.As<lex::ErrorToken>();
            error.Store<Lexeme>(Lexeme::kInvalidNewLineInString);
            error.Store<lex::ErrorIndexDisp>(error_pos.Index() -
                                             ret.position.Index());
            error.Store<lex::ErrorLineDisp>(error_pos.Line() -
                                            ret.position.Line());
            error.Store<lex::ErrorColumn>(error_pos.Column());
            error.Store<char>(ch);
            is_invalid = true;
          }

        } else if ('\\' == ch) {
          if (prev_was_escape) {
            prev_was_escape = false;
            if (!is_invalid) {
              impl->data.push_back('\\');
            }

          } else {
            prev_was_escape = true;
          }
        } else if (prev_was_escape) {
          if (!is_invalid) {
            switch (ch) {
              case 'a':  // Alert/beep.
                impl->data.push_back('\a');
                break;
              case 'e':  // Escape.
                impl->data.push_back('\033');
                break;
              case 'n':  // New line.
                impl->data.push_back('\n');
                break;
              case 'r':  // Carriage return.
                impl->data.push_back('\r');
                break;
              case 't':  // Tab.
                impl->data.push_back('\t');
                break;
              case '0':  // NUL.
                impl->data.push_back('\0');
                break;
              case '"': impl->data.push_back('"'); break;
              default:
                if (!is_invalid) {
                  const auto error_pos = impl->reader.CurrentPosition();
                  auto &error = ret.As<lex::ErrorToken>();
                  error.Store<Lexeme>(Lexeme::kInvalidEscapeInString);
                  error.Store<lex::ErrorIndexDisp>(error_pos.Index() -
                                                   ret.position.Index());
                  error.Store<lex::ErrorLineDisp>(error_pos.Line() -
                                                  ret.position.Line());
                  error.Store<lex::ErrorColumn>(error_pos.Column());
                  error.Store<char>(ch);
                  is_invalid = true;
                }
                break;
            }
          }
          prev_was_escape = false;

        } else if (ch == '"') {
          is_finished = true;
          break;

        } else if (!is_invalid) {
          impl->data.push_back(ch);
        }
      }

      if (!is_invalid) {
        if (is_finished) {
          auto &literal = ret.As<lex::StringLiteralToken>();
          literal.Store<Lexeme>(Lexeme::kLiteralString);
          literal.Store<lex::SpellingWidth>(spelling_width);
          literal.Store<lex::Id>(string_pool.InternString(impl->data));
          literal.Store<lex::ReprWidth>(impl->data.size());

        } else {
          const auto error_pos = impl->reader.CurrentPosition();
          auto &error = ret.As<lex::ErrorToken>();
          error.Store<Lexeme>(Lexeme::kInvalidUnterminatedString);
          error.Store<lex::ErrorIndexDisp>(error_pos.Index() -
                                           ret.position.Index());
          error.Store<lex::ErrorLineDisp>(error_pos.Line() -
                                          ret.position.Line());
          error.Store<lex::ErrorColumn>(error_pos.Column());
          error.Store<char>(ch);
          is_invalid = true;
        }
      }

      if (is_invalid) {
        const auto after_pos = impl->reader.CurrentPosition();
        auto &error = ret.As<lex::ErrorToken>();
        assert(error.Load<Lexeme>() != Lexeme::kInvalid);
        error.Store<lex::IndexDisp>(after_pos.Index() - ret.position.Index());
        error.Store<lex::LineDisp>(after_pos.Line() - ret.position.Line());
        error.Store<lex::Column>(after_pos.Column());
      }

      return true;
    }

    // Pragmas.
    case '@': {
      accumulate_if(
          [](char next_ch) { return kStopChars.find(next_ch) == std::string::npos; });
      if (impl->data == "@highlight") {
        auto &basic = ret.As<lex::BasicToken>();
        basic.Store<Lexeme>(Lexeme::kPragmaDebugHighlight);
        basic.Store<lex::SpellingWidth>(impl->data.size());

      } else if (impl->data == "@product") {
        auto &basic = ret.As<lex::BasicToken>();
        basic.Store<Lexeme>(Lexeme::kPragmaPerfProduct);
        basic.Store<lex::SpellingWidth>(impl->data.size());

      } else if (impl->data == "@impure") {
        auto &basic = ret.As<lex::BasicToken>();
        basic.Store<Lexeme>(Lexeme::kPragmaHintImpure);
        basic.Store<lex::SpellingWidth>(impl->data.size());

      } else if (impl->data == "@range") {
        auto &basic = ret.As<lex::BasicToken>();
        basic.Store<Lexeme>(Lexeme::kPragmaPerfRange);
        basic.Store<lex::SpellingWidth>(impl->data.size());

      } else if (impl->data == "@inline") {
        auto &basic = ret.As<lex::BasicToken>();
        basic.Store<Lexeme>(Lexeme::kPragmaPerfInline);
        basic.Store<lex::SpellingWidth>(impl->data.size());

      } else if (impl->data == "@transparent") {
        auto &basic = ret.As<lex::BasicToken>();
        basic.Store<Lexeme>(Lexeme::kPragmaPerfTransparent);
        basic.Store<lex::SpellingWidth>(impl->data.size());

      } else if (impl->data == "@differential") {
        auto &basic = ret.As<lex::BasicToken>();
        basic.Store<Lexeme>(Lexeme::kPragmaDifferential);
        basic.Store<lex::SpellingWidth>(impl->data.size());

      } else {
        auto &error = ret.As<lex::ErrorToken>();
        error.Store<Lexeme>(Lexeme::kInvalidPragma);
        error.Store<char>(impl->data.size() == 1u ? impl->data[1] : impl->data[0]);
        error.Store<lex::ErrorIndexDisp>(1u);
        error.Store<lex::ErrorLineDisp>(0);
        error.Store<lex::ErrorColumn>(ret.position.Column() + 1u);

        const auto next_pos = impl->reader.CurrentPosition();
        error.Store<lex::IndexDisp>(next_pos.Index() - ret.position.Index());
        error.Store<lex::LineDisp>(next_pos.Line() - ret.position.Line());
        error.Store<lex::Column>(next_pos.Column());
      }
      return true;
    }

    // Directives.
    case '#':
      tentative_lexeme = Lexeme::kInvalidDirective;
      goto accumulate_directive_atom_keyword_variable;

    // Atoms, keywords.
    case 'a':
    case 'b':
    case 'c':
    case 'd':
    case 'e':
    case 'f':
    case 'g':
    case 'h':
    case 'i':
    case 'j':
    case 'k':
    case 'l':
    case 'm':
    case 'n':
    case 'o':
    case 'p':
    case 'q':
    case 'r':
    case 's':
    case 't':
    case 'u':
    case 'v':
    case 'w':
    case 'x':
    case 'y':
    case 'z':
      tentative_lexeme = Lexeme::kIdentifierAtom;
      goto accumulate_directive_atom_keyword_variable;

    // Variables.
    case '_':
      tentative_lexeme = Lexeme::kIdentifierUnnamedVariable;
      goto accumulate_directive_atom_keyword_variable;
    case 'A':
    case 'B':
    case 'C':
    case 'D':
    case 'E':
    case 'F':
    case 'G':
    case 'H':
    case 'I':
    case 'J':
    case 'K':
    case 'L':
    case 'M':
    case 'N':
    case 'O':
    case 'P':
    case 'Q':
    case 'R':
    case 'S':
    case 'T':
    case 'U':
    case 'V':
    case 'W':
    case 'X':
    case 'Y':
    case 'Z':
      tentative_lexeme = Lexeme::kIdentifierVariable;
      goto accumulate_directive_atom_keyword_variable;

    accumulate_directive_atom_keyword_variable:
      accumulate_if(
          [](char next_ch) { return next_ch == '_' || std::isalnum(next_ch); });

      if (impl->data[0] == '_') {
        assert(tentative_lexeme == Lexeme::kIdentifierUnnamedVariable);
      }

      switch (impl->data.size()) {
        case 1:
          break;

        case 2:
          if (impl->data == "i8") {
            tentative_lexeme = Lexeme::kTypeIn;
            tentative_type_kind = TypeKind::kSigned8;

          } else if (impl->data == "u8") {
            tentative_lexeme = Lexeme::kTypeUn;
            tentative_type_kind = TypeKind::kUnsigned8;
          }
          break;

        case 3:
          if ('f' == impl->data[0]) {
            if (impl->data == "f32") {
              tentative_lexeme = Lexeme::kTypeFn;
              tentative_type_kind = TypeKind::kFloat;

            } else if (impl->data == "f64") {
              tentative_lexeme = Lexeme::kTypeFn;
              tentative_type_kind = TypeKind::kDouble;
            }
          } else if ('i' == impl->data[0]) {
            if (impl->data == "i16") {
              tentative_lexeme = Lexeme::kTypeIn;
              tentative_type_kind = TypeKind::kSigned16;

            } else if (impl->data == "i32") {
              tentative_lexeme = Lexeme::kTypeIn;
              tentative_type_kind = TypeKind::kSigned32;

            } else if (impl->data == "i64") {
              tentative_lexeme = Lexeme::kTypeIn;
              tentative_type_kind = TypeKind::kSigned64;
            }
          } else if ('u' == impl->data[0]) {
            if (impl->data == "u16") {
              tentative_lexeme = Lexeme::kTypeUn;
              tentative_type_kind = TypeKind::kUnsigned16;

            } else if (impl->data == "u32") {
              tentative_lexeme = Lexeme::kTypeUn;
              tentative_type_kind = TypeKind::kUnsigned32;

            } else if (impl->data == "u64") {
              tentative_lexeme = Lexeme::kTypeUn;
              tentative_type_kind = TypeKind::kUnsigned64;
            }
          }
          break;
        case 4:
          if (impl->data == "free") {
            tentative_lexeme = Lexeme::kKeywordFree;

          } else if (impl->data == "over") {
            tentative_lexeme = Lexeme::kKeywordOver;

          } else if (impl->data == "uuid") {
            tentative_lexeme = Lexeme::kTypeUUID;
            tentative_type_kind = TypeKind::kUUID;

          } else if (impl->data == "utf8") {
            tentative_lexeme = Lexeme::kTypeUTF8;
            tentative_type_kind = TypeKind::kUTF8;

          } else if (impl->data == "bool") {
            tentative_lexeme = Lexeme::kTypeBoolean;
            tentative_type_kind = TypeKind::kBoolean;

          } else if (impl->data == "true") {
            auto &ident = ret.As<lex::IdentifierToken>();
            ident.Store<Lexeme>(Lexeme::kLiteralTrue);
            ident.Store<lex::SpellingWidth>(4u);
            ident.Store<lex::Id>(string_pool.LiteralTrueId());
            ident.Store<TypeKind>(TypeKind::kBoolean);
            return true;
          }
          break;
        case 5:
          if (impl->data == "bound") {
            tentative_lexeme = Lexeme::kKeywordBound;

          } else if (impl->data == "ascii") {
            tentative_lexeme = Lexeme::kTypeASCII;
            tentative_type_kind = TypeKind::kASCII;

          } else if (impl->data == "bytes") {
            tentative_lexeme = Lexeme::kTypeBytes;
            tentative_type_kind = TypeKind::kBytes;

          } else if (impl->data == "false") {
            auto &ident = ret.As<lex::IdentifierToken>();
            ident.Store<Lexeme>(Lexeme::kLiteralFalse);
            ident.Store<lex::SpellingWidth>(5u);
            ident.Store<lex::Id>(string_pool.LiteralFalseId());
            ident.Store<TypeKind>(TypeKind::kBoolean);
            return true;
          }
          break;
        case 6:
          if (impl->data == "#query") {
            tentative_lexeme = Lexeme::kHashQueryDecl;

          } else if (impl->data == "#local") {
            tentative_lexeme = Lexeme::kHashLocalDecl;
          }
          break;
        case 7:
          if (impl->data[0] == '#') {
            if (impl->data == "#export") {
              tentative_lexeme = Lexeme::kHashExportDecl;

            } else if (impl->data == "#import") {
              tentative_lexeme = Lexeme::kHashImportModuleStmt;
            }
          } else if (impl->data == "summary") {
            tentative_lexeme = Lexeme::kKeywordSummary;

          } else if (impl->data == "mutable") {
            tentative_lexeme = Lexeme::kKeywordMutable;
          }
          break;

        case 8:
          if (impl->data[0] == '#') {
            if (impl->data == "#message") {
              tentative_lexeme = Lexeme::kHashMessageDecl;

            } else if (impl->data == "#functor") {
              tentative_lexeme = Lexeme::kHashFunctorDecl;

            } else if (impl->data == "#foreign") {
              tentative_lexeme = Lexeme::kHashForeignTypeDecl;
            }
          }
          break;

        case 9:
          if (impl->data == "aggregate") {
            tentative_lexeme = Lexeme::kKeywordAggregate;
          } else if (impl->data == "#prologue") {
            tentative_lexeme = Lexeme::kHashInlinePrologueStmt;

          } else if (impl->data == "#epilogue") {
            tentative_lexeme = Lexeme::kHashInlineEpilogueStmt;

          } else if (impl->data == "#constant") {
            tentative_lexeme = Lexeme::kHashForeignConstantDecl;
          }
          break;

        default: break;
      }

      if (tentative_lexeme == Lexeme::kInvalidDirective ||
          tentative_lexeme == Lexeme::kInvalid) {
        auto &error = ret.As<lex::ErrorToken>();
        error.Store<Lexeme>(tentative_lexeme);
        error.Store<char>(ch);
        error.Store<lex::ErrorIndexDisp>(1u);
        error.Store<lex::ErrorLineDisp>(0);
        error.Store<lex::ErrorColumn>(ret.position.Column() + 1u);

        const auto after_pos = impl->reader.CurrentPosition();
        error.Store<lex::IndexDisp>(after_pos.Index() - ret.position.Index());
        error.Store<lex::LineDisp>(after_pos.Line() - ret.position.Line());
        error.Store<lex::Column>(after_pos.Column());

      // It's a type name.
      } else if (tentative_type_kind != TypeKind::kInvalid) {
        auto &type = ret.As<lex::TypeToken>();
        type.Store<Lexeme>(tentative_lexeme);
        type.Store<lex::SpellingWidth>(impl->data.size());
        type.Store<TypeKind>(tentative_type_kind);

      // It's a directive.
      } else if ('#' == impl->data[0]) {
        auto &basic = ret.As<lex::BasicToken>();
        basic.Store<Lexeme>(tentative_lexeme);
        basic.Store<lex::SpellingWidth>(impl->data.size());

      // If we've found an identifier, then intern it.
      } else if (tentative_lexeme == Lexeme::kIdentifierAtom ||
                 tentative_lexeme == Lexeme::kIdentifierVariable) {
        auto &ident = ret.As<lex::IdentifierToken>();
        ident.Store<Lexeme>(tentative_lexeme);
        ident.Store<lex::SpellingWidth>(impl->data.size());
        ident.Store<lex::Id>(string_pool.InternString(impl->data));

      } else if (tentative_lexeme == Lexeme::kIdentifierUnnamedVariable) {
        auto &ident = ret.As<lex::IdentifierToken>();
        ident.Store<Lexeme>(tentative_lexeme);

        // A normal unnamed variable.
        if (1u == impl->data.size()) {
          ident.Store<lex::SpellingWidth>(1u);  // Length of `_`.
          ident.Store<lex::Id>(string_pool.UnderscoreId());

        // A "named" unnamed variable, e.g. `_foo`. Re-uses of the same unnamed
        // variable, even within the same clause, are treated as distinct.
        } else {
          ident.Store<lex::SpellingWidth>(impl->data.size());
          ident.Store<lex::Id>(string_pool.InternString(impl->data));
        }

      } else {
        auto &basic = ret.As<lex::BasicToken>();
        basic.Store<Lexeme>(tentative_lexeme);
        basic.Store<lex::SpellingWidth>(impl->data.size());
      }

      return true;

    // Number literals. We have to deal with a variety of formats here :-(
    case '0':
    case '1':
    case '2':
    case '3':
    case '4':
    case '5':
    case '6':
    case '7':
    case '8':
    case '9': {
      auto is_all_decimal = true;
      accumulate_if([&is_all_decimal](char next_ch) {
        switch (next_ch) {
          case '0':
          case '1':
          case '2':
          case '3':
          case '4':
          case '5':
          case '6':
          case '7':
          case '8':
          case '9': return true;
          case 'a':
          case 'b':
          case 'c':
          case 'd':
          case 'e':
          case 'f':
          case 'A':
          case 'B':
          case 'C':
          case 'D':
          case 'E':
          case 'F':
          case 'x':
          case 'X': is_all_decimal = false; return true;
          default: return false;
        }
      });

      // We read a number, but it was attached to another token.
      if (impl->reader.TryReadChar(&ch)) {
        impl->reader.UnreadChar();
        if (kStopChars.find(ch) == std::string::npos) {
          auto &error = ret.As<lex::ErrorToken>();
          const auto error_pos = impl->reader.CurrentPosition();
          error.Store<Lexeme>(Lexeme::kInvalidNumber);
          error.Store<lex::ErrorIndexDisp>(error_pos.Index() - ret.position.Index());
          error.Store<lex::ErrorLineDisp>(error_pos.Line() - ret.position.Line());
          error.Store<lex::ErrorColumn>(error_pos.Column());

          accumulate_if([](char next_ch) {
            return kStopChars.find(next_ch) == std::string::npos;
          });

          const auto after_pos = impl->reader.CurrentPosition();
          error.Store<lex::IndexDisp>(after_pos.Index() - ret.position.Index());
          error.Store<lex::LineDisp>(after_pos.Line() - ret.position.Line());
          error.Store<lex::Column>(after_pos.Column());
          error.Store<char>(ch);
          return true;
        }
      }

      if ('0' == impl->data[0]) {

        // Zero.
        if (impl->data.size() == 1) {
          auto &literal = ret.As<lex::NumberLiteralToken>();
          literal.Store<Lexeme>(Lexeme::kLiteralNumber);
          literal.Store<lex::SpellingWidth>(1);
          literal.Store<lex::NumberSpellingKind>(
              lex::NumberSpellingKind::kDecimal);
          literal.Store<bool>(false);  // No decimal point.
          literal.Store<lex::PrefixWidth>(0u);
          return true;

        // Either a single digit octal literal, or something invalid.
        } else if (impl->data.size() == 2) {
          if ('1' <= impl->data[1] && impl->data[1] <= '7') {
            auto &literal = ret.As<lex::NumberLiteralToken>();
            literal.Store<Lexeme>(Lexeme::kLiteralNumber);
            literal.Store<lex::SpellingWidth>(2u);
            literal.Store<lex::NumberSpellingKind>(
                lex::NumberSpellingKind::kOctal);
            literal.Store<bool>(false);  // No decimal point.
            literal.Store<lex::PrefixWidth>(1u);  // `0` prefix for octal.
            return true;

          // Invalid octal digit.
          } else {
            auto &error = ret.As<lex::ErrorToken>();
            error.Store<Lexeme>(Lexeme::kInvalidOctalNumber);
            error.Store<char>(impl->data[1]);
            error.Store<lex::ErrorIndexDisp>(1u);
            error.Store<lex::ErrorLineDisp>(0u);
            error.Store<lex::ErrorColumn>(ret.position.Column() + 1u);

            error.Store<lex::IndexDisp>(2u);
            error.Store<lex::LineDisp>(0u);
            error.Store<lex::Column>(ret.position.Column() + 2u);
            return true;
          }

        // Looks like a hexadecimal literal.
        } else if ('x' == impl->data[1] || 'X' == impl->data[1]) {
          auto all_hex = true;
          auto i = 2u;
          while (all_hex && i < impl->data.size()) {
            switch (impl->data[i]) {
              case '0':
              case '1':
              case '2':
              case '3':
              case '4':
              case '5':
              case '6':
              case '7':
              case '8':
              case '9':
              case 'a':
              case 'b':
              case 'c':
              case 'd':
              case 'e':
              case 'f':
              case 'A':
              case 'B':
              case 'C':
              case 'D':
              case 'E':
              case 'F': ++i; continue;
              default: all_hex = false; break;
            }
          }

          if (all_hex) {
            auto &literal = ret.As<lex::NumberLiteralToken>();
            literal.Store<Lexeme>(Lexeme::kLiteralNumber);
            literal.Store<lex::SpellingWidth>(impl->data.size());
            literal.Store<lex::NumberSpellingKind>(
                lex::NumberSpellingKind::kHexadecimal);
            literal.Store<bool>(false);
            literal.Store<lex::PrefixWidth>(2u);  // `0x` or `0X` prefix length.
            return true;

          } else {
            auto &error = ret.As<lex::ErrorToken>();
            error.Store<Lexeme>(Lexeme::kInvalidHexadecimalNumber);
            error.Store<char>(impl->data[i]);
            error.Store<lex::ErrorIndexDisp>(i);
            error.Store<lex::ErrorLineDisp>(0u);
            error.Store<lex::ErrorColumn>(ret.position.Column() + i);

            const auto after_pos = impl->reader.CurrentPosition();
            error.Store<lex::IndexDisp>(after_pos.Index() -
                                        ret.position.Index());
            error.Store<lex::LineDisp>(after_pos.Line() - ret.position.Line());
            error.Store<lex::Column>(after_pos.Column());
            return true;
          }

        // Looks like a binary literal.
        } else if ('b' == impl->data[1] || 'B' == impl->data[1]) {
          auto all_bin = true;
          auto i = 2u;
          while (all_bin && i < impl->data.size()) {
            switch (impl->data[i]) {
              case '0':
              case '1': ++i; continue;
              default: all_bin = false; break;
            }
          }

          if (all_bin) {
            auto &literal = ret.As<lex::NumberLiteralToken>();
            literal.Store<Lexeme>(Lexeme::kLiteralNumber);
            literal.Store<lex::SpellingWidth>(impl->data.size());
            literal.Store<lex::NumberSpellingKind>(
                lex::NumberSpellingKind::kBinary);
            literal.Store<bool>(false);
            literal.Store<lex::PrefixWidth>(2u);  // `0b` or `0B` prefix length.
            return true;

          } else {
            auto &error = ret.As<lex::ErrorToken>();
            error.Store<Lexeme>(Lexeme::kInvalidBinaryNumber);
            error.Store<char>(impl->data[i]);
            error.Store<lex::ErrorIndexDisp>(i);
            error.Store<lex::ErrorLineDisp>(0u);
            error.Store<lex::ErrorColumn>(ret.position.Column() + i);

            const auto after_pos = impl->reader.CurrentPosition();
            error.Store<lex::IndexDisp>(after_pos.Index() -
                                        ret.position.Index());
            error.Store<lex::LineDisp>(after_pos.Line() - ret.position.Line());
            error.Store<lex::Column>(after_pos.Column());
            return true;
          }

        // Looks like an octal literal.
        } else {
          auto all_octal = true;
          auto has_leading_zeros = true;
          auto i = 1u;
          while (all_octal && i < impl->data.size()) {
            switch (impl->data[i]) {
              case '0':
                if (has_leading_zeros) {
                  all_octal = false;
                } else {
                  ++i;
                }
                break;
              case '1':
              case '2':
              case '3':
              case '4':
              case '5':
              case '6':
              case '7':
                has_leading_zeros = false;
                ++i;
                continue;
              default: all_octal = false; break;
            }
          }

          if (all_octal) {
            auto &literal = ret.As<lex::NumberLiteralToken>();
            literal.Store<Lexeme>(Lexeme::kLiteralNumber);
            literal.Store<lex::SpellingWidth>(impl->data.size());
            literal.Store<lex::NumberSpellingKind>(
                lex::NumberSpellingKind::kOctal);
            literal.Store<bool>(false);
            literal.Store<lex::PrefixWidth>(1u);  // `0` prefix.
            return true;

          } else {
            auto &error = ret.As<lex::ErrorToken>();
            error.Store<Lexeme>(Lexeme::kInvalidOctalNumber);
            error.Store<char>(impl->data[i]);
            error.Store<lex::ErrorIndexDisp>(i);
            error.Store<lex::ErrorLineDisp>(0u);
            error.Store<lex::ErrorColumn>(ret.position.Column() + i);

            const auto after_pos = impl->reader.CurrentPosition();
            error.Store<lex::IndexDisp>(after_pos.Index() -
                                        ret.position.Index());
            error.Store<lex::LineDisp>(after_pos.Line() - ret.position.Line());
            error.Store<lex::Column>(after_pos.Column());
            return true;
          }
        }

      // Looks like decimal.
      } else if (is_all_decimal) {

        // Go look for decimal points, might be floating point.
        if (impl->reader.TryReadChar(&ch)) {
          if (ch == '.') {
            if (impl->reader.TryReadChar(&ch)) {
              if (std::isdigit(ch)) {
                impl->data.push_back('.');
                impl->data.push_back(ch);
                accumulate_if(
                    [](char next_ch) { return std::isdigit(next_ch); });

                tentative_has_decimal_point = true;

                // We read a number, but it was attached to another token.
                if (impl->reader.TryReadChar(&ch)) {
                  impl->reader.UnreadChar();
                  const auto error_pos = impl->reader.CurrentPosition();
                  if (kStopChars.find(ch) == std::string::npos) {
                    auto &error = ret.As<lex::ErrorToken>();
                    error.Store<Lexeme>(Lexeme::kInvalidNumber);
                    error.Store<char>(ch);
                    error.Store<lex::ErrorIndexDisp>(impl->data.size());
                    error.Store<lex::ErrorLineDisp>(0u);
                    error.Store<lex::ErrorColumn>(error_pos.Column());

                    accumulate_if([](char next_ch) {
                      return kStopChars.find(next_ch) == std::string::npos;
                    });

                    const auto after_pos = impl->reader.CurrentPosition();
                    error.Store<lex::IndexDisp>(after_pos.Index() -
                                                ret.position.Index());
                    error.Store<lex::LineDisp>(after_pos.Line() -
                                               ret.position.Line());
                    error.Store<lex::Column>(after_pos.Column());
                    return true;
                  }
                }

              } else {
                impl->reader.UnreadChar();  // Non-digit.
                impl->reader.UnreadChar();  // Period.
              }
            } else {
              impl->reader.UnreadChar();
            }
          } else {
            impl->reader.UnreadChar();
          }
        }

        auto &literal = ret.As<lex::NumberLiteralToken>();
        literal.Store<Lexeme>(Lexeme::kLiteralNumber);
        literal.Store<lex::SpellingWidth>(impl->data.size());
        literal.Store<lex::NumberSpellingKind>(
            lex::NumberSpellingKind::kDecimal);
        literal.Store<bool>(tentative_has_decimal_point);
        literal.Store<lex::PrefixWidth>(0u);
        return true;

      // Some weird mix.
      } else {
        auto i = 0u;
        is_all_decimal = true;
        while (is_all_decimal && i < impl->data.size()) {
          switch (impl->data[i]) {
            case '0':
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9': ++i; continue;
            default: is_all_decimal = false; break;
          }
        }

        auto &error = ret.As<lex::ErrorToken>();
        error.Store<Lexeme>(Lexeme::kInvalidNumber);
        error.Store<char>(impl->data[i]);
        error.Store<lex::ErrorIndexDisp>(i);
        error.Store<lex::ErrorLineDisp>(0u);
        error.Store<lex::ErrorColumn>(ret.position.Column() + i);

        const auto after_pos = impl->reader.CurrentPosition();
        error.Store<lex::IndexDisp>(after_pos.Index() - ret.position.Index());
        error.Store<lex::LineDisp>(after_pos.Line() - ret.position.Line());
        error.Store<lex::Column>(after_pos.Column());
        return true;
      }
    }

    // Single line comment.
    case ';': {
      accumulate_if([](char next_ch) { return next_ch != '\n'; });
      auto &basic = ret.As<lex::BasicToken>();
      basic.Store<Lexeme>(Lexeme::kComment);
      basic.Store<lex::SpellingWidth>(impl->data.size());
      return true;
    }

    // Unknown, accumulate character up until a stop character.
    default: {
      auto &error = ret.As<lex::ErrorToken>();
      error.Store<Lexeme>(Lexeme::kInvalidUnknown);
      error.Store<lex::ErrorIndexDisp>(0);
      error.Store<lex::ErrorLineDisp>(0u);
      error.Store<lex::ErrorColumn>(ret.position.Column());
      error.Store<char>(ch);

      if (kStopChars.find(ch) == std::string::npos) {
        accumulate_if([](char next_ch) {
          return kStopChars.find(next_ch) == std::string::npos;
        });
      }

      const auto next_pos = impl->reader.CurrentPosition();
      error.Store<lex::IndexDisp>(next_pos.Index() - ret.position.Index());
      error.Store<lex::LineDisp>(next_pos.Line() - ret.position.Line());
      error.Store<lex::Column>(next_pos.Column());

      return true;
    }
  }
}

}  // namespace hyde
