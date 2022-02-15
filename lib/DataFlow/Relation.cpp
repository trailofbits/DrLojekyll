// Copyright 2022, Trail of Bits. All rights reserved.

#include "Query.h"

namespace hyde {

QueryRelationImpl::~QueryRelationImpl(void) {}

QueryRelationImpl::QueryRelationImpl(ParsedDeclaration decl_)
    : Def<QueryRelationImpl>(this),
      User(this),
      declaration(decl_),
      inserts(this),
      selects(this) {}

}  // namespace hyde
