// Copyright 2020, Trail of Bits. All rights reserved.

#include "Program.h"

#include <set>
#include <unordered_map>

namespace hyde {
namespace {

// Return the set of all views that contribute data to `view`. This includes
// things like conditions.
std::set<QueryView> DependenciesOf(QueryView view) {
  std::set<QueryView> dependencies;
  for (auto changed = true; changed; ) {
    changed = false;

  }
}

}  // namespace

// Build a program from a query.
std::optional<Program> Program::Build(
    const Query &query, const ErrorLog &) {

  std::unordered_map<uint64_t, std::set<QueryView>> eager_from;

  return std::nullopt;
}

}  // namespace hyde
