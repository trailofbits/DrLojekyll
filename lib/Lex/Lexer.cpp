// Copyright 2019, Trail of Bits. All rights reserved.

#include <drlojekyll/Lex/Lexer.h>

#include <cctype>
#include <string>

#include <drlojekyll/Display/Display.h>
#include <drlojekyll/Display/DisplayManager.h>
#include <drlojekyll/Display/DisplayReader.h>

#include <drlojekyll/Lex/StringPool.h>

#include "Token.h"

namespace hyde {
namespace {

static const std::string kStopChars = " \n,.:()!#{}[];=-+*/&^$%'~`";

}  // namespace

class LexerImpl {
 public:
  inline explicit LexerImpl(DisplayReader reader_)
      : reader(reader_) {}

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

  Token temp = {};
  lex::TokenInterpreter interpreter = {};

  impl->data.clear();

  char ch = '\0';

  // Accumulator for adding characters to `impl->data`.
  auto accumulate_if = [=] (auto accept) {
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
  auto &ret = tok_out ? *tok_out : temp;
  ret.position = impl->reader.CurrentPosition();

  if (!impl->reader.TryReadChar(&ch)) {

    // There was an error condition when reading characters. This could be due
    // to the stream underlying the display, or due to there being an invalid
    // character in the display.
    if (impl->reader.TryGetErrorMessage(nullptr)) {
      interpreter.basic.lexeme = static_cast<uint8_t>(
          Lexeme::kInvalidStreamOrDisplay);

    // We've reached the end of this display, yield a token and pop this
    // display off the stack.
    } else {
      interpreter.basic.lexeme = static_cast<uint8_t>(Lexeme::kEndOfFile);
    }

    impl.reset();

    ret.opaque_data = interpreter.flat;
    return true;
  }

  impl->data.push_back(ch);

  switch (ch) {

    // Accumulate whitespace into a single token, then summarizes the
    // whitespace in terms of its total displacement relative to the next
    // token.
    case '\n':
    case ' ': {
      accumulate_if([] (char next_ch) {
        return next_ch == ' ' || next_ch == '\n';
      });

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

      interpreter.basic.lexeme = static_cast<uint8_t>(Lexeme::kWhitespace);
      interpreter.whitespace.num_leading_spaces = num_leading_spaces;
      interpreter.whitespace.num_leading_newlines = num_leading_lines;
      interpreter.whitespace.spelling_width =
          static_cast<uint16_t>(impl->data.size());
      ret.opaque_data = interpreter.flat;
      return true;
    }

    case '(':
      interpreter.basic.lexeme = static_cast<uint8_t>(Lexeme::kPuncOpenParen);
      interpreter.basic.spelling_width = 1;
      ret.opaque_data = interpreter.flat;
      return true;

    case ')':
      interpreter.basic.lexeme = static_cast<uint8_t>(Lexeme::kPuncCloseParen);
      interpreter.basic.spelling_width = 1;
      ret.opaque_data = interpreter.flat;
      return true;

    case '{':
      interpreter.basic.lexeme = static_cast<uint8_t>(Lexeme::kPuncOpenBrace);
      interpreter.basic.spelling_width = 1;
      ret.opaque_data = interpreter.flat;
      return true;

    case '}':
      interpreter.basic.lexeme = static_cast<uint8_t>(Lexeme::kPuncCloseBrace);
      interpreter.basic.spelling_width = 1;
      ret.opaque_data = interpreter.flat;
      return true;

    case '.':
      interpreter.basic.lexeme = static_cast<uint8_t>(Lexeme::kPuncPeriod);
      interpreter.basic.spelling_width = 1;
      ret.opaque_data = interpreter.flat;
      return true;

    case ',':
      interpreter.basic.lexeme = static_cast<uint8_t>(Lexeme::kPuncComma);
      interpreter.basic.spelling_width = 1;
      ret.opaque_data = interpreter.flat;
      return true;

    case '=':
      interpreter.basic.lexeme = static_cast<uint8_t>(Lexeme::kPuncEqual);
      interpreter.basic.spelling_width = 1;
      ret.opaque_data = interpreter.flat;
      return true;

    case '!':
      interpreter.basic.lexeme = static_cast<uint8_t>(Lexeme::kPuncExclaim);
      interpreter.basic.spelling_width = 1;

      if (impl->reader.TryReadChar(&ch)) {
        if (ch == '=') {
          interpreter.basic.lexeme = static_cast<uint8_t>(Lexeme::kPuncNotEqual);
          interpreter.basic.spelling_width = 2;

        // Read it as an inline code literal.
        } else if (ch == '<') {

          auto done = false;
          auto seen_exclaim = false;
          auto error_offset = 2u;
          impl->data.clear();

          while (impl->reader.TryReadChar(&ch)) {
            ++error_offset;
            if ('!' == ch) {
              if (seen_exclaim) {
                impl->data.push_back('!');
              } else {
                seen_exclaim = true;
              }
            } else if ('>' == ch) {
              if (seen_exclaim) {
                done = true;
                break;
              } else {
                impl->data.push_back('!');
                impl->data.push_back(ch);
                seen_exclaim = false;
              }
            } else {
              impl->data.push_back(ch);
            }
          }

          const auto after_pos = impl->reader.CurrentPosition();

          if (!done) {
            interpreter.error.error_offset = error_offset;
            interpreter.error.error_col = after_pos.Column();
            interpreter.error.disp_lines = after_pos.Line() - ret.position.Line();
            interpreter.error.lexeme =
                static_cast<uint8_t>(Lexeme::kInvalidUnterminatedCode);

          } else {
            interpreter.code.num_bytes = impl->data.size() + 4;
            interpreter.code.next_cols = after_pos.Column();
            interpreter.code.disp_lines = after_pos.Line() - ret.position.Line();
            interpreter.code.id = string_pool.InternCode(impl->data);
            interpreter.code.lexeme = static_cast<uint8_t>(Lexeme::kLiteralCode);
          }
        } else {
          impl->reader.UnreadChar();
        }
      }

      ret.opaque_data = interpreter.flat;
      return true;

    case '<':
      interpreter.basic.lexeme = static_cast<uint8_t>(Lexeme::kPuncLess);
      interpreter.basic.spelling_width = 1;
      ret.opaque_data = interpreter.flat;
      return true;

    case '>':
      interpreter.basic.lexeme = static_cast<uint8_t>(Lexeme::kPuncGreater);
      interpreter.basic.spelling_width = 1;
      ret.opaque_data = interpreter.flat;
      return true;

    case ':':
      interpreter.basic.lexeme = static_cast<uint8_t>(Lexeme::kPuncColon);
      interpreter.basic.spelling_width = 1;
      ret.opaque_data = interpreter.flat;
      return true;

    // String literal.
    case '"': {
      interpreter.basic.lexeme = static_cast<uint8_t>(Lexeme::kLiteralString);

      uint16_t spelling_width = 1;
      auto prev_was_escape = false;
      auto is_invalid = false;
      auto is_finished = false;
      auto error_offset = 1u;
      impl->data.clear();

      // Go collect all the characters of the string, and render them
      // into `impl->data`.
      while (impl->reader.TryReadChar(&ch)) {
        spelling_width += 1;
        error_offset += 1;

        if ('\n' == ch || '\r' == ch) {
          const auto after_pos = impl->reader.CurrentPosition();
          interpreter.error.error_offset = error_offset;
          interpreter.error.error_col = after_pos.Column();
          interpreter.error.disp_lines = after_pos.Line() - ret.position.Line();
          interpreter.error.invalid_char = ch;
          interpreter.error.lexeme =
              static_cast<uint8_t>(Lexeme::kInvalidNewLineInString);
          is_invalid = true;

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
              case '"':impl->data.push_back('"');
                break;
              default:
                if (!is_invalid) {
                  const auto after_pos = impl->reader.CurrentPosition();
                  interpreter.error.error_offset = error_offset;
                  interpreter.error.error_col = after_pos.Column();
                  interpreter.error.disp_lines = after_pos.Line() - ret.position.Line();
                  interpreter.error.invalid_char = ch;
                  interpreter.error.lexeme =
                      static_cast<uint8_t>(Lexeme::kInvalidEscapeInString);
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
          interpreter.string.index = string_pool.InternString(impl->data);
          interpreter.string.num_bytes =
              static_cast<uint16_t>(impl->data.size());
          interpreter.string.spelling_width = spelling_width;

        } else {
          const auto after_pos = impl->reader.CurrentPosition();
          interpreter.error.error_offset = error_offset;
          interpreter.error.error_col = after_pos.Column();
          interpreter.error.disp_lines = after_pos.Line() - ret.position.Line();
          interpreter.error.invalid_char = ch;
          interpreter.error.lexeme =
              static_cast<uint8_t>(Lexeme::kInvalidUnterminatedString);
        }
      }

      ret.opaque_data = interpreter.flat;
      return true;
    }

    // Directives.
    case '#':
      interpreter.basic.lexeme = static_cast<uint8_t>(Lexeme::kInvalidDirective);
      goto accumulate_directive_atom_keyword_variable;

    // Atoms, keywords.
    case 'a': case 'b': case 'c': case 'd': case 'e': case 'f': case 'g':
    case 'h': case 'i': case 'j': case 'k': case 'l': case 'm': case 'n':
    case 'o': case 'p': case 'q': case 'r': case 's': case 't': case 'u':
    case 'v': case 'w': case 'x': case 'y': case 'z':
      interpreter.basic.lexeme = static_cast<uint8_t>(Lexeme::kIdentifierAtom);
      goto accumulate_directive_atom_keyword_variable;

    // Variables.
    case '_':
    case 'A': case 'B': case 'C': case 'D': case 'E': case 'F': case 'G':
    case 'H': case 'I': case 'J': case 'K': case 'L': case 'M': case 'N':
    case 'O': case 'P': case 'Q': case 'R': case 'S': case 'T': case 'U':
    case 'V': case 'W': case 'X': case 'Y': case 'Z':
      interpreter.basic.lexeme = static_cast<uint8_t>(Lexeme::kIdentifierVariable);
      goto accumulate_directive_atom_keyword_variable;

    accumulate_directive_atom_keyword_variable:
      accumulate_if([] (char next_ch) {
        return next_ch == '_' || std::isalnum(next_ch);
      });

      interpreter.basic.spelling_width =
          static_cast<uint16_t>(impl->data.size());

      switch (impl->data.size()) {
        case 1:
          if (impl->data[0] == '_') {
            interpreter.basic.lexeme = static_cast<uint8_t>(
                Lexeme::kIdentifierUnnamedVariable);
            interpreter.identifier.spelling_width = 1;
            interpreter.identifier.index = 1;  // See `DisplayManager::Impl::Impl`.
          }
          break;

        case 2:
          if (impl->data == "i8") {
            interpreter.basic.lexeme = static_cast<uint8_t>(Lexeme::kTypeIn);
            interpreter.type.spelling_width = 2;
            interpreter.type.type_width = 8;

          } else if (impl->data == "u8") {
            interpreter.basic.lexeme = static_cast<uint8_t>(Lexeme::kTypeUn);
            interpreter.type.spelling_width = 2;
            interpreter.type.type_width = 8;
          }
          break;

        case 3:
          if ('f' == impl->data[0]) {
            if (impl->data == "f32") {
              interpreter.basic.lexeme = static_cast<uint8_t>(Lexeme::kTypeFn);
              interpreter.type.spelling_width = 3;
              interpreter.type.type_width = 32;

            } else if (impl->data == "f64") {
              interpreter.basic.lexeme = static_cast<uint8_t>(Lexeme::kTypeFn);
              interpreter.type.spelling_width = 3;
              interpreter.type.type_width = 64;
            }
          } else if ('i' == impl->data[0]) {
            if (impl->data == "i16") {
              interpreter.basic.lexeme = static_cast<uint8_t>(Lexeme::kTypeIn);
              interpreter.type.spelling_width = 3;
              interpreter.type.type_width = 16;

            } else if (impl->data == "i32") {
              interpreter.basic.lexeme = static_cast<uint8_t>(Lexeme::kTypeIn);
              interpreter.type.spelling_width = 3;
              interpreter.type.type_width = 32;

            } else if (impl->data == "i64") {
              interpreter.basic.lexeme = static_cast<uint8_t>(Lexeme::kTypeIn);
              interpreter.type.spelling_width = 3;
              interpreter.type.type_width = 64;
            }
          } else if ('u' == impl->data[0]) {
            if (impl->data == "u16") {
              interpreter.basic.lexeme = static_cast<uint8_t>(Lexeme::kTypeUn);
              interpreter.type.spelling_width = 3;
              interpreter.type.type_width = 16;

            } else if (impl->data == "u32") {
              interpreter.basic.lexeme = static_cast<uint8_t>(Lexeme::kTypeUn);
              interpreter.type.spelling_width = 3;
              interpreter.type.type_width = 32;

            } else if (impl->data == "u64") {
              interpreter.basic.lexeme = static_cast<uint8_t>(Lexeme::kTypeUn);
              interpreter.type.spelling_width = 3;
              interpreter.type.type_width = 64;
            }
          }
          break;
        case 4:
          if (impl->data == "free") {
            interpreter.basic.lexeme = static_cast<uint8_t>(Lexeme::kKeywordFree);

          } else if (impl->data == "over") {
            interpreter.basic.lexeme = static_cast<uint8_t>(Lexeme::kKeywordOver);

          } else if (impl->data == "uuid") {
            interpreter.basic.lexeme = static_cast<uint8_t>(Lexeme::kTypeUUID);
            interpreter.type.spelling_width = 4;
            interpreter.type.type_width = 128;

          } else if (impl->data == "utf8") {
            interpreter.basic.lexeme = static_cast<uint8_t>(Lexeme::kTypeUTF8);
            interpreter.type.spelling_width = 4;
            interpreter.type.type_width = 128;
          }
          break;
        case 5:
          if (impl->data == "bound") {
            interpreter.basic.spelling_width = 5;
            interpreter.basic.lexeme =
                static_cast<uint8_t>(Lexeme::kKeywordBound);

          } else if (impl->data == "ascii") {
            interpreter.basic.lexeme = static_cast<uint8_t>(Lexeme::kTypeASCII);
            interpreter.type.spelling_width = 5;
            interpreter.type.type_width = 128;

          } else if (impl->data == "bytes") {
            interpreter.basic.lexeme = static_cast<uint8_t>(Lexeme::kTypeASCII);
            interpreter.type.spelling_width = 5;
            interpreter.type.type_width = 128;
          }
          break;
        case 6:
          if (impl->data == "#query") {
            interpreter.basic.spelling_width = 6;
            interpreter.basic.lexeme =
                static_cast<uint8_t>(Lexeme::kHashQueryDecl);

          } else if (impl->data == "#local") {
            interpreter.basic.spelling_width = 6;
            interpreter.basic.lexeme =
                static_cast<uint8_t>(Lexeme::kHashLocalDecl);

          } else if (impl->data == "inline") {
            interpreter.basic.spelling_width = 6;
            interpreter.basic.lexeme =
                static_cast<uint8_t>(Lexeme::kKeywordInline);
          }
          break;
        case 7:
          if (impl->data[0] == '#') {
            if (impl->data == "#export") {
              interpreter.basic.spelling_width = 7;
              interpreter.basic.lexeme =
                  static_cast<uint8_t>(Lexeme::kHashExportDecl);

            } else if (impl->data == "#import") {
              interpreter.basic.spelling_width = 7;
              interpreter.basic.lexeme =
                  static_cast<uint8_t>(Lexeme::kHashImportModuleStmt);

            } else if (impl->data == "#inline") {
              interpreter.basic.spelling_width = 7;
              interpreter.basic.lexeme =
                  static_cast<uint8_t>(Lexeme::kHashInlineStmt);
            }
          } else if (impl->data == "summary") {
            interpreter.basic.spelling_width = 7;
            interpreter.basic.lexeme =
                static_cast<uint8_t>(Lexeme::kKeywordSummary);

          } else if (impl->data == "mutable") {
            interpreter.basic.spelling_width = 7;
            interpreter.basic.lexeme =
                static_cast<uint8_t>(Lexeme::kKeywordMutable);
          }
          break;

        case 8:
          if (impl->data[0] == '#') {
            if (impl->data == "#message") {
              interpreter.basic.spelling_width = 8;
              interpreter.basic.lexeme =
                  static_cast<uint8_t>(Lexeme::kHashMessageDecl);

            } else if (impl->data == "#functor") {
              interpreter.basic.spelling_width = 8;
              interpreter.basic.lexeme =
                  static_cast<uint8_t>(Lexeme::kHashFunctorDecl);

            } else if (impl->data == "#include") {
              interpreter.basic.spelling_width = 8;
              interpreter.basic.lexeme =
                  static_cast<uint8_t>(Lexeme::kHashIncludeStmt);
            }
          }
          break;

        case 9:
          if (impl->data == "aggregate") {
            interpreter.basic.spelling_width = 9;
            interpreter.basic.lexeme =
                static_cast<uint8_t>(Lexeme::kKeywordAggregate);

          } else if (impl->data == "unordered") {
            interpreter.basic.spelling_width = 9;
            interpreter.basic.lexeme =
                static_cast<uint8_t>(Lexeme::kKeywordUnordered);
          }
          break;

        default:
          break;
      }

      // If we've found an identifier, then intern it.
      if ((interpreter.basic.lexeme ==
           static_cast<uint8_t>(Lexeme::kIdentifierAtom)) ||
          (interpreter.basic.lexeme ==
           static_cast<uint8_t>(Lexeme::kIdentifierVariable))) {
        interpreter.identifier.index = string_pool.InternString(impl->data);
        interpreter.identifier.spelling_width =
            static_cast<uint16_t>(impl->data.size());
      }

      ret.opaque_data = interpreter.flat;
      return true;

    // Number literals. We have to deal with a variety of formats here :-(
    case '0': case '1': case '2': case '3': case '4': case '5':
    case '6': case '7': case '8': case '9': {
      auto is_all_decimal = true;
      accumulate_if([&is_all_decimal] (char next_ch) {
        if (!std::isdigit(next_ch)) {
          is_all_decimal = false;
        }
        return std::isalnum(next_ch);
      });

      // Go look for decimal points.
      if (is_all_decimal) {
        if (impl->reader.TryReadChar(&ch)) {
          if (ch == '.') {
            if (impl->reader.TryReadChar(&ch)) {
              if (std::isdigit(ch)) {
                impl->data.push_back('.');
                impl->data.push_back(ch);
                accumulate_if([](char next_ch) {
                  return std::isdigit(next_ch);
                });

                interpreter.number.has_decimal_point = true;
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
      }

      if ('0' == impl->data[0]) {

        // Zero.
        if (impl->data.size() == 1) {
          interpreter.number.lexeme = Lexeme::kLiteralNumber;
          interpreter.number.spelling_kind = lex::NumberSpellingKind::kDecimal;
          interpreter.number.spelling_width = 1;

        // Either a single digit octal literal, or something invalid.
        } else if (impl->data.size() == 2) {
          if ('1' <= impl->data[1] && impl->data[1] <= '7') {
            interpreter.number.lexeme = Lexeme::kLiteralNumber;
            interpreter.number.spelling_kind = lex::NumberSpellingKind::kOctal;
            interpreter.number.prefix_width = 1;  // `0`.
            interpreter.number.spelling_width = 2;
          } else {
            interpreter.error.error_offset = 1;
            interpreter.error.error_col = ret.position.Column() + 2;
            interpreter.error.disp_lines = 0;
            interpreter.error.invalid_char = impl->data[1];
            interpreter.error.lexeme =
                static_cast<uint8_t>(Lexeme::kInvalidNumber);
          }

        // Looks like a hexadecimal literal.
        } else if ('x' == impl->data[1] || 'X' == impl->data[1]) {
          auto all_hex = true;
          auto i = 2u;
          while (all_hex && i < impl->data.size()) {
            switch (impl->data[i]) {
              case '0': case '1': case '2': case '3': case '4': case '5':
              case '6': case '7': case '8': case '9':
              case 'a': case 'b': case 'c': case 'd': case 'e': case 'f':
              case 'A': case 'B': case 'C': case 'D': case 'E': case 'F':
                ++i;
                continue;
              default:
                all_hex = false;
                break;
            }
          }

          if (all_hex) {
            interpreter.number.lexeme = Lexeme::kLiteralNumber;
            interpreter.number.spelling_kind =
                lex::NumberSpellingKind::kHexadecimal;
            interpreter.number.prefix_width = 2;  // `0x` or `0X`.
            interpreter.number.spelling_width =
                static_cast<uint16_t>(impl->data.size());

          } else {
            interpreter.error.error_offset = i;
            interpreter.error.error_col = ret.position.Column() + i;
            interpreter.error.disp_lines = 0;
            interpreter.error.invalid_char = impl->data[i];
            interpreter.error.lexeme =
                static_cast<uint8_t>(Lexeme::kInvalidNumber);
          }

        // Looks like a binary literal.
        } else if ('b' == impl->data[1] || 'B' == impl->data[1]) {
          auto all_bin = true;
          auto i = 2u;
          while (all_bin && i < impl->data.size()) {
            switch (impl->data[i]) {
              case '0': case '1':
                ++i;
                continue;
              default:
                all_bin = false;
                break;
            }
          }

          if (all_bin) {
            interpreter.number.lexeme = Lexeme::kLiteralNumber;
            interpreter.number.spelling_kind =
                lex::NumberSpellingKind::kBinary;
            interpreter.number.prefix_width = 2;  // `0b` or `0B`.
            interpreter.number.spelling_width =
                static_cast<uint16_t>(impl->data.size());

          } else {
            interpreter.error.error_offset = i;
            interpreter.error.error_col = ret.position.Column() + i;
            interpreter.error.disp_lines = 0;
            interpreter.error.invalid_char = impl->data[i];
            interpreter.error.lexeme =
                static_cast<uint8_t>(Lexeme::kInvalidNumber);
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
              case '1': case '2': case '3': case '4': case '5':
              case '6': case '7':
                has_leading_zeros = false;
                ++i;
                continue;
              default:
                all_octal = false;
                break;
            }
          }
          if (all_octal) {
            interpreter.number.lexeme = Lexeme::kLiteralNumber;
            interpreter.number.spelling_kind =
                lex::NumberSpellingKind::kBinary;
            interpreter.number.prefix_width = 1;  // `0`.
            interpreter.number.spelling_width =
                static_cast<uint16_t>(impl->data.size());

          } else {
            interpreter.error.error_offset = i;
            interpreter.error.error_col = ret.position.Column() + i;
            interpreter.error.disp_lines = 0;
            interpreter.error.invalid_char = impl->data[i];
            interpreter.error.lexeme =
                static_cast<uint8_t>(Lexeme::kInvalidNumber);
          }
        }

      // Looks like decimal.
      } else {
        auto all_decimal = true;
        auto i = 0u;
        while (all_decimal && i < impl->data.size()) {
          switch (impl->data[i]) {
            case '0': case '1': case '2': case '3': case '4': case '5':
            case '6': case '7': case '8': case '9':
              ++i;
              continue;
            default:
              all_decimal = false;
              break;
          }
        }

        if (all_decimal) {
          interpreter.number.lexeme = Lexeme::kLiteralNumber;
          interpreter.number.spelling_kind =
              lex::NumberSpellingKind::kDecimal;
          interpreter.number.spelling_width =
              static_cast<uint16_t>(impl->data.size());

        } else {
          interpreter.error.error_offset = i;
          interpreter.error.error_col = ret.position.Column() + i;
          interpreter.error.disp_lines = 0;
          interpreter.error.invalid_char = impl->data[i];
          interpreter.error.lexeme =
              static_cast<uint8_t>(Lexeme::kInvalidNumber);
        }
      }

      ret.opaque_data = interpreter.flat;
      return true;
    }

    // Single line comment.
    case ';':
      accumulate_if([] (char next_ch) {
        return next_ch != '\n';
      });
      interpreter.basic.lexeme = static_cast<uint8_t>(Lexeme::kComment);
      interpreter.basic.spelling_width =
          static_cast<uint16_t>(impl->data.size());
      ret.opaque_data = interpreter.flat;
      return true;

    // Unknown, accumulate character up until a stop character.
    default:
      interpreter.error.invalid_char = ch;

      if (kStopChars.find(ch) == std::string::npos) {
        accumulate_if([] (char next_ch) {
          return kStopChars.find(next_ch) == std::string::npos;
        });
      }

      interpreter.error.disp_lines = 0;
      interpreter.error.error_col = ret.position.Column();
      interpreter.error.error_offset = 0;
      interpreter.error.lexeme = static_cast<uint8_t>(Lexeme::kInvalidUnknown);
      return true;
  }
}

}  // namespace hyde
