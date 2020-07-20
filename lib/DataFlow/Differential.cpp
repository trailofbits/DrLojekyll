// Copyright 2020, Trail of Bits. All rights reserved.

#include "Query.h"

namespace hyde {

// Identify which data flows can receive and produce deletions.
void QueryImpl::TrackDifferentialUpdates(void) const {

  std::unordered_map<ParsedDeclaration, std::vector<SELECT *>>
      decl_to_selects;

  std::unordered_map<INSERT *, std::vector<SELECT *>>
      insert_to_selects;

  for (auto select : selects) {
    if (auto rel = select->relation.get(); rel) {
      decl_to_selects[rel->declaration].push_back(select);
    } else if (auto stream = select->stream.get(); stream) {
      if (auto input = stream->AsIO(); input) {
        decl_to_selects[input->declaration].push_back(select);
      }
    }
  }

  for (auto insert : inserts) {
    for (auto select : decl_to_selects[insert->declaration]) {
      insert_to_selects[insert].push_back(select);
    }
  }

  for (auto changed = true; changed; ) {
    changed = false;
    ForEachView([&] (VIEW *view) {
      if (view->can_receive_deletions && !view->can_produce_deletions) {
        view->can_produce_deletions = true;
        changed = true;
      }

      if (auto insert = view->AsInsert();
          insert && insert->can_receive_deletions) {
        for (auto select : insert_to_selects[insert]) {
          if (!select->can_receive_deletions) {
            changed = true;
            select->can_receive_deletions = true;
          }
        }
      }

      if (!view->can_produce_deletions) {
        return;
      }

      for (COL *col : view->columns) {
        col->ForEachUser([&] (VIEW *user_view) {
          if (!user_view->can_receive_deletions) {
            user_view->can_receive_deletions = true;
            changed = true;
          }
        });
      }
    });
  }
}

}  // namespace hyde
