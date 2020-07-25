// Copyright 2020, Trail of Bits. All rights reserved.

#include <set>
#include <unordered_map>

#include "Program.h"

namespace hyde {
namespace {

// Return the set of all views that contribute data to `view`. This includes
// things like conditions.
std::set<QueryView> DependenciesOf(QueryView output) {
  std::set<QueryView> dependencies;
  std::vector<QueryView> frontier;
  frontier.push_back(output);

  while (!frontier.empty()) {
    const auto view = frontier.back();
    frontier.pop_back();
    view.ForEachUse(
        [&](QueryColumn col, InputColumnRole, std::optional<QueryColumn>) {
          auto dep_view = QueryView::Containing(col);
          if (!dependencies.count(dep_view)) {
            dependencies.insert(dep_view);
            frontier.push_back(dep_view);
          }
        });
  }

  return dependencies;
}

// Return the set of all views that are transitively derived from `input`.
std::set<QueryView> DependentsOf(QueryView input) {
  std::set<QueryView> dependents;
  std::vector<QueryView> frontier;
  frontier.push_back(input);

  while (!frontier.empty()) {
    const auto view = frontier.back();
    frontier.pop_back();
    view.ForEachUser([&](QueryView user) {
      if (!dependents.count(user)) {
        dependents.insert(user);
        frontier.push_back(user);
      }
    });
  }

  return dependents;
}

}  // namespace

// Build a program from a query.
std::optional<Program> Program::Build(const Query &query, const ErrorLog &) {

  std::set<QueryView> eager;
  std::set<QueryView> lazy;
  std::unordered_map<QueryView, std::set<QueryView>> eager_from;
  std::unordered_map<QueryView, std::set<QueryView>> lazy_to;

  for (auto cond : query.Conditions()) {
    for (auto setter : cond.Setters()) {
      auto deps = DependenciesOf(setter);
      eager.insert(deps.begin(), deps.end());
      eager_from.emplace(setter, std::move(deps));
    }
  }

  for (auto io : query.IOs()) {
    for (auto transmit : io.Transmits()) {
      auto deps = DependenciesOf(transmit);
      eager.insert(deps.begin(), deps.end());
    }
    for (auto receive : io.Receives()) {
      eager_from.emplace(transmit, std::move(deps));
    }
  }

  for (auto insert : query.Inserts()) {
    if (insert.IsRelation()) {
      auto deps = DependenciesOf(insert);
      lazy.insert(deps.begin(), deps.end());
      lazy_to.emplace(insert, std::move(deps));
    }
  }

  lazy.erase(eager.begin(), eager.end());

  return std::nullopt;
}

}  // namespace hyde
