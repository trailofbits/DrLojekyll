// Copyright 2020, Trail of Bits. All rights reserved.

#include "Query.h"

namespace hyde {

QueryIOImpl::~QueryIOImpl(void) {}

QueryIOImpl::QueryIOImpl(ParsedDeclaration declaration_)
    : User(this),
      declaration(declaration_),
      transmits(this),
      receives(this) {}

QueryIOImpl *QueryIOImpl::AsIO(void) noexcept {
  return this;
}

const char *QueryIOImpl::KindName(void) const noexcept {
  return "I/O";
}

}  // namespace hyde
