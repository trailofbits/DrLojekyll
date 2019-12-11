// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include <drlojekyll/Sema/ModuleIterator.h>

#include <vector>
#include <unordered_set>

#include <drlojekyll/Parse/Parse.h>

namespace hyde {

class ParsedModuleIterator::Impl {
 public:
  Impl(const ParsedModule &root_module) {
    std::unordered_set<ParsedModule> seen_modules;
    AddModule(root_module.RootModule(), seen_modules);
  }

  // Adds the modules so that all dependencies are properly ordered.
  void AddModule(const ParsedModule &module,
                 std::unordered_set<ParsedModule> &seen_modules) {
    if (seen_modules.count(module)) {
      return;
    }
    seen_modules.insert(module);
    for (auto import : module.Imports()) {
      AddModule(import.ImportedModule(), seen_modules);
    }
    modules.push_back(module);
  }

  std::vector<ParsedModule> modules;
};

const ParsedModule &ParsedModuleIterator::Iterator::operator*(void) const {
  return impl->modules[index];
}

ParsedModuleIterator::ParsedModuleIterator(const ParsedModule &module)
    : impl(std::make_unique<Impl>(module)) {}

ParsedModuleIterator::Iterator ParsedModuleIterator::begin(void) const {
  return Iterator(impl, 0);
}

ParsedModuleIterator::Iterator ParsedModuleIterator::end(void) const {
  return Iterator(impl, static_cast<unsigned>(impl->modules.size()));
}

}  // namespace hyde
