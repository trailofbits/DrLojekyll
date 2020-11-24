// Copyright 2020, Trail of Bits, Inc. All rights reserved.

#include <drlojekyll/Lex/Token.h>
#include <drlojekyll/Parse/Type.h>

#include <cassert>

namespace hyde {

TypeLoc::TypeLoc(const Token &tok)
    : kind(tok.TypeKind()),
      range(tok.SpellingRange()) {}

TypeLoc::TypeLoc(TypeKind kind_, const DisplayRange &range_)
    : kind(kind_),
      range(range_) {}

TypeLoc::TypeLoc(TypeKind kind_, uint32_t foreign_id_,
                 const DisplayRange &range_)
    : kind(kind_ == TypeKind::kForeignType
               ? static_cast<TypeKind>((foreign_id_ << 8u) |
                                       static_cast<uint32_t>(kind_))
               : kind_),
      range(range_) {
  if (kind_ == TypeKind::kForeignType) {
    assert(0u < foreign_id_);
  } else {
    assert(!foreign_id_);
  }
}

TypeLoc &TypeLoc::operator=(const Token &tok) noexcept {
  kind = tok.TypeKind();
  range = tok.SpellingRange();
  return *this;
}

const char *Spelling(TypeKind kind) noexcept {
  switch (static_cast<TypeKind>(static_cast<uint8_t>(kind))) {
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
    case TypeKind::kForeignType: return "<foreign>";
  }
  return "<invalid>";
}

}  // namespace hyde
