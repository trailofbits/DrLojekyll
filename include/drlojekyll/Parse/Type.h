// Copyright 2019, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/Display/DisplayPosition.h>

#include <optional>
#include <string_view>

namespace hyde {

class ParsedVariable;
class ParsedParameter;
class ParsedLiteral;
class Token;

enum class TypeKind : uint32_t {
  kInvalid,
  kBoolean,
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
  kBytes,

  // If it's a foreign type type then we embed extra data in the high
  // 24 bits.
  kForeignType
};

const char *Spelling(TypeKind kind) noexcept;

class ParsedModule;
class ParsedForeignType;
enum class Language : unsigned;

// Type and location of that type.
class TypeLoc {
 public:
  inline TypeLoc(void) : kind(TypeKind::kInvalid) {}

  TypeLoc(const Token &tok);
  TypeLoc(const ParsedForeignType &ft);
  TypeLoc(TypeKind kind_);
  TypeLoc(TypeKind kind_, const DisplayRange &range_);
  TypeLoc(TypeKind kind_, uint32_t foreign_id_, const DisplayRange &range_);
  TypeLoc &operator=(const Token &tok) noexcept;

  inline TypeKind Kind(void) const noexcept {
    return kind;
  }

  inline TypeKind UnderlyingKind(void) const noexcept {
    return static_cast<TypeKind>(static_cast<uint8_t>(kind));
  }

  inline std::optional<uint32_t> ForeignTypeId(void) const noexcept {
    if (Kind() == TypeKind::kForeignType) {
      return static_cast<uint32_t>(kind) >> 8u;
    } else {
      return std::nullopt;
    }
  }

  inline DisplayPosition Position(void) const noexcept {
    return range.From();
  }

  inline DisplayRange SpellingRange(void) const noexcept {
    return range;
  }

  // Does equality imply identity?
  bool IsReferentiallyTransparent(const ParsedModule &module,
                                  Language lang) const noexcept;

  inline bool IsForeign(void) const noexcept {
    return UnderlyingKind() == TypeKind::kForeignType;
  }

  inline bool IsBuiltIn(void) const noexcept {
    switch (UnderlyingKind()) {
      case TypeKind::kInvalid:
      case TypeKind::kForeignType: return false;
      default: return true;
    }
  }

  inline bool IsValid(void) const noexcept {
    return kind != TypeKind::kInvalid;
  }

  inline bool IsInvalid(void) const noexcept {
    return kind == TypeKind::kInvalid;
  }

  inline bool operator==(TypeLoc that) const noexcept {
    return kind == that.kind;
  }

  inline bool operator!=(TypeLoc that) const noexcept {
    return kind != that.kind;
  }

  inline const char *Spelling(void) const noexcept {
    return ::hyde::Spelling(kind);
  }

  std::string_view Spelling(const ParsedModule &module) const;

 private:
  template <typename, typename>
  friend class Node;

  TypeKind kind;
  DisplayRange range;
};

}  // namespace hyde
