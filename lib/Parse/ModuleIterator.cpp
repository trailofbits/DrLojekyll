// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include <drlojekyll/Parse/ModuleIterator.h>

#include "Parse.h"

namespace hyde {

ParsedModule ParsedModuleIterator::Iterator::operator*(void) const {
  return ParsedModule(
      impl.impl->root_module->all_modules[index]->shared_from_this());
}

ParsedModuleIterator::ParsedModuleIterator(const ParsedModule &module)
    : impl(module) {}

ParsedModuleIterator::Iterator ParsedModuleIterator::begin(void) const {
  return Iterator(impl, 0);
}

ParsedModuleIterator::Iterator ParsedModuleIterator::end(void) const {
  return Iterator(
      impl, static_cast<unsigned>(impl.impl->root_module->all_modules.size()));
}

}  // namespace hyde
