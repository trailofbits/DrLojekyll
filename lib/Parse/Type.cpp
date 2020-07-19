// Copyright 2020, Trail of Bits, Inc. All rights reserved.

#include <drlojekyll/Parse/Type.h>

#include <drlojekyll/Lex/Token.h>

namespace hyde {
namespace {

static TypeKind TokToTypeKind(Token tok) noexcept {
  switch (tok.Lexeme()) {
    case Lexeme::kTypeBytes: return TypeKind::kBytes;
    case Lexeme::kTypeASCII: return TypeKind::kASCII;
    case Lexeme::kTypeUTF8: return TypeKind::kUTF8;
    case Lexeme::kTypeUUID: return TypeKind::kUUID;
    case Lexeme::kTypeIn:
      switch (tok.TypeSizeInBytes()) {
        case 1: return TypeKind::kSigned8;
        case 2: return TypeKind::kSigned16;
        case 4: return TypeKind::kSigned32;
        case 8: return TypeKind::kSigned64;
        default: return TypeKind::kInvalid;
      }
    case Lexeme::kTypeUn:
      switch (tok.TypeSizeInBytes()) {
        case 1: return TypeKind::kUnsigned8;
        case 2: return TypeKind::kUnsigned16;
        case 4: return TypeKind::kUnsigned32;
        case 8: return TypeKind::kUnsigned64;
        default: return TypeKind::kInvalid;
      }
    case Lexeme::kTypeFn:
      switch (tok.TypeSizeInBytes()) {
        case 4: return TypeKind::kFloat;
        case 8: return TypeKind::kDouble;
        default: return TypeKind::kInvalid;
      }
    default: return TypeKind::kInvalid;
  }
}

}  // namespace

unsigned SizeInBits(TypeKind kind) noexcept {
  return SizeInBytes(kind) * 8u;
}

unsigned SizeInBytes(TypeKind kind) noexcept {
  switch (kind) {
    case TypeKind::kInvalid: break;
    case TypeKind::kSigned8: return 1u;
    case TypeKind::kSigned16: return 2u;
    case TypeKind::kSigned32: return 4u;
    case TypeKind::kSigned64: return 8u;
    case TypeKind::kUnsigned8: return 1u;
    case TypeKind::kUnsigned16: return 2u;
    case TypeKind::kUnsigned32: return 4u;
    case TypeKind::kUnsigned64: return 8u;
    case TypeKind::kFloat: return 4u;
    case TypeKind::kDouble: return 8u;
    case TypeKind::kBytes:
    case TypeKind::kASCII:
    case TypeKind::kUTF8:
    case TypeKind::kUUID:
      return 16u;
  }
  return 0u;
}

TypeLoc::TypeLoc(const Token &tok)
    : kind(TokToTypeKind(tok)),
      range(tok.SpellingRange()) {}

TypeLoc::TypeLoc(TypeKind kind_, const DisplayRange &range_)
    : kind(kind_),
      range(range_) {}

TypeLoc &TypeLoc::operator=(const Token &tok) noexcept {
  kind = TokToTypeKind(tok);
  range = tok.SpellingRange();
  return *this;
}

const char *Spelling(TypeKind kind) noexcept {
  switch (kind) {
    case TypeKind::kInvalid: break;
    case TypeKind::kSigned8: return "i8";
    case TypeKind::kSigned16: return "i16";
    case TypeKind::kSigned32: return "i32";
    case TypeKind::kSigned64: return "i64";
    case TypeKind::kUnsigned8: return "u8";
    case TypeKind::kUnsigned16: return "u16";
    case TypeKind::kUnsigned32: return "u32";
    case TypeKind::kUnsigned64: return "u64";
    case TypeKind::kFloat: return "f32";
    case TypeKind::kDouble: return "f64";
    case TypeKind::kBytes: return "bytes";
    case TypeKind::kASCII: return "ascii";
    case TypeKind::kUTF8: return "utf8";
    case TypeKind::kUUID: return "uuid";
  }
  return "<invalid>";
}

}  // namespace hyde
