// Copyright 2019, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/Display/DisplayPosition.h>

namespace hyde {

class ParsedVariable;
class ParsedParameter;
class ParsedLiteral;

// Type of a variable or literal.
class Type {
 public:
  enum Kind {
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

  inline ::hyde::Type::Kind Kind(void) const {
    return kind;
  }

  unsigned SizeInBits(void) const;
  unsigned SizeInBytes(void) const;

 private:
  Kind kind;
  DisplayPosition position;
};

}  // namespace hyde
