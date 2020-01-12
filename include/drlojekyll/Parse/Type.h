// Copyright 2019, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/Display/DisplayPosition.h>

namespace hyde {

class ParsedVariable;
class ParsedParameter;
class ParsedLiteral;
class Token;

enum class TypeKind {
  kInvalid,
  kSigned8,
  kSigned16,
  kSigned32,
  kSigned64,
  kUnsigned8,
  kUnsigned16,
  kUnsigned32,
  kUnsigned64,
  kFloat,
  kDouble,
  kString,
  kUUID
};

unsigned SizeInBits(TypeKind kind) noexcept;
unsigned SizeInBytes(TypeKind kind) noexcept;

// Type and location of that type.
class TypeLoc {
 public:
  TypeLoc(const Token &tok);
  TypeLoc &operator=(const Token &tok) noexcept;

  inline TypeLoc(void)
      : kind(TypeKind::kInvalid) {}

  inline TypeKind Kind(void) const noexcept {
    return kind;
  }

  inline DisplayPosition Position(void) const noexcept {
    return range.From();
  }

  inline DisplayRange SpellingRange(void) const noexcept {
    return range;
  }

  inline bool IsValid(void) const noexcept {
    return kind != TypeKind::kInvalid;
  }

  inline bool IsInvalid(void) const noexcept {
    return kind == TypeKind::kInvalid;
  }

 private:
  TypeKind kind;
  DisplayRange range;
};

}  // namespace hyde
