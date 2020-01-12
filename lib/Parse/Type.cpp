// Copyright 2020, Trail of Bits, Inc. All rights reserved.

#include <drlojekyll/Parse/Type.h>

#include <drlojekyll/Lex/Token.h>

namespace hyde {
namespace {

static TypeKind TokToTypeKind(Token tok) noexcept {
  switch (tok.Lexeme()) {
    case Lexeme::kTypeString: return TypeKind::kString;
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
    case TypeKind::kInvalid: return 0;
    case TypeKind::kSigned8: return 1;
    case TypeKind::kSigned16: return 2;
    case TypeKind::kSigned32: return 4;
    case TypeKind::kSigned64: return 8;
    case TypeKind::kUnsigned8: return 1;
    case TypeKind::kUnsigned16: return 2;
    case TypeKind::kUnsigned32: return 4;
    case TypeKind::kUnsigned64: return 8;
    case TypeKind::kFloat: return 4;
    case TypeKind::kDouble: return 8;
    case TypeKind::kString: return 0;
    case TypeKind::kUUID: return 16;
  }
}

TypeLoc::TypeLoc(const Token &tok)
    : kind(TokToTypeKind(tok)),
      range(tok.SpellingRange()) {}

TypeLoc &TypeLoc::operator=(const Token &tok) noexcept {
  kind = TokToTypeKind(tok);
  range = tok.SpellingRange();
  return *this;
}

}  // namespace hyde
