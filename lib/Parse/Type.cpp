// Copyright 2020, Trail of Bits, Inc. All rights reserved.

#include <drlojekyll/Lex/Token.h>
#include <drlojekyll/Parse/Type.h>
#include <drlojekyll/Parse/Parse.h>

#include <cassert>

namespace hyde {

TypeLoc::TypeLoc(const Token &tok)
    : kind(tok.TypeKind()),
      range(tok.SpellingRange()) {}

TypeLoc::TypeLoc(const ParsedForeignType &ft)
    : TypeLoc(ft.Name()) {}

TypeLoc::TypeLoc(TypeKind kind_) : kind(kind_) {}

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
    case TypeKind::kBoolean: return "bool";
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
    case TypeKind::kForeignType: return "<foreign>";
  }
  return "<invalid>";
}

// Does equality imply identity?
bool TypeLoc::IsReferentiallyTransparent(const ParsedModule &module,
                                         Language lang) const noexcept {
  switch (UnderlyingKind()) {
    case TypeKind::kInvalid: return false;
    case TypeKind::kBoolean:
    case TypeKind::kSigned8:
    case TypeKind::kSigned16:
    case TypeKind::kSigned32:
    case TypeKind::kSigned64:
    case TypeKind::kUnsigned8:
    case TypeKind::kUnsigned16:
    case TypeKind::kUnsigned32:
    case TypeKind::kUnsigned64:
    case TypeKind::kFloat:
    case TypeKind::kDouble:
      return true;
    case TypeKind::kForeignType: {
      if (auto type = module.ForeignType(Kind())) {
        return type->IsReferentiallyTransparent(lang);
      } else {
        assert(false);
        return false;
      }
    }
  }
  return false;
}

// Does the value type admit a designated null value, or do we need to wrap
// it in something else, e.g. `std::optional`.
bool TypeLoc::IsNullable(const ParsedModule &module,
                         Language lang) const noexcept {
  switch (UnderlyingKind()) {
    case TypeKind::kInvalid: return false;
    case TypeKind::kBoolean:
    case TypeKind::kSigned8:
    case TypeKind::kSigned16:
    case TypeKind::kSigned32:
    case TypeKind::kSigned64:
    case TypeKind::kUnsigned8:
    case TypeKind::kUnsigned16:
    case TypeKind::kUnsigned32:
    case TypeKind::kUnsigned64:
    case TypeKind::kFloat:
    case TypeKind::kDouble:
      return false;
    case TypeKind::kForeignType: {
      if (auto type = module.ForeignType(Kind())) {
        return type->IsNullable(lang);
      } else {
        assert(false);
        return false;
      }
    }
  }
  return false;
}

std::string_view TypeLoc::Spelling(const ParsedModule &module) const {
  if (auto ty = module.ForeignType(kind)) {
    return ty->NameAsString();
  } else {
    return ::hyde::Spelling(kind);
  }
}

}  // namespace hyde
