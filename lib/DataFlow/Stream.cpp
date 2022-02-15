// Copyright 2020, Trail of Bits. All rights reserved.

#include "Query.h"

namespace hyde {

QueryStreamImpl::~QueryStreamImpl(void) {}

QueryStreamImpl::QueryStreamImpl(void) : Def<QueryStreamImpl>(this) {}

QueryConstantImpl *QueryStreamImpl::AsConstant(void) noexcept {
  return nullptr;
}

QueryTagImpl *QueryStreamImpl::AsTag(void) noexcept {
  return nullptr;
}

QueryIOImpl *QueryStreamImpl::AsIO(void) noexcept {
  return nullptr;
}

}  // namespace hyde
