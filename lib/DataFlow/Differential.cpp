// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/Parse/ErrorLog.h>

#include "Query.h"

namespace hyde {

// Identify which data flows can receive and produce deletions.
void QueryImpl::TrackDifferentialUpdates(const ErrorLog &log,
                                         bool check_conds) const {

  std::unordered_map<ParsedDeclaration, std::vector<SELECT *>> decl_to_selects;

  std::unordered_map<INSERT *, std::vector<SELECT *>> insert_to_selects;

  const_cast<const QueryImpl *>(this)->ForEachView([](VIEW *v) {
    v->can_receive_deletions = false;
    v->can_produce_deletions = false;
  });

  for (auto select : selects) {
    if (auto rel = select->relation.get(); rel) {
      decl_to_selects[rel->declaration].push_back(select);
    } else if (auto stream = select->stream.get(); stream) {
      if (auto input = stream->AsIO(); input) {
        if (ParsedMessage::From(input->declaration).IsDifferential()) {
          select->can_receive_deletions = true;
          select->can_produce_deletions = true;
        }
        decl_to_selects[input->declaration].push_back(select);
      }
    }
  }

  for (auto insert : inserts) {
    for (auto select : decl_to_selects[insert->declaration]) {
      insert_to_selects[insert].push_back(select);
    }
  }

  for (auto changed = true; changed;) {
    changed = false;
    ForEachView([&](VIEW *view) {
      // If a node is conditional then we treat it as differential.
      if (check_conds && !view->can_produce_deletions &&
          (!view->positive_conditions.Empty() ||
           !view->negative_conditions.Empty())) {
        view->can_produce_deletions = true;
        changed = true;
      }

      if (!view->can_produce_deletions && view->AsNegate()) {
        view->can_produce_deletions = true;
        changed = true;
      }

      if (view->can_receive_deletions && !view->can_produce_deletions) {
        view->can_produce_deletions = true;
        changed = true;
      }

      if (auto insert = view->AsInsert();
          insert && insert->can_produce_deletions) {
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
        col->ForEachUser([&](VIEW *user_view) {
          if (!user_view->can_receive_deletions) {
            user_view->can_receive_deletions = true;
            changed = true;
          }
        });
      }
    });
  }

  // Conditions introduce additional deletions, so only error check when we
  // propagate based on them.
  if (!check_conds) {
    return;
  }

  for (auto view : inserts) {
    if (!view->stream) {
      continue;
    }

    const auto io = view->stream->AsIO();
    if (!io) {
      continue;
    }

    const auto message = ParsedMessage::From(io->declaration);
    const auto range = message.SpellingRange();

    // Require that the source code be faithful to the data flow in terms of
    // what messages can receive and produce differentials.

    if (message.IsDifferential()) {
      if (!view->can_produce_deletions) {
        assert(!view->can_receive_deletions);

        //        log.Append(range, message.Differential().SpellingRange())
        //            << "Message '" << message.Name() << '/' << message.Arity()
        //            << "' is marked with the '@differential' attribute but cannot "
        //            << "produce deletions";
      }

    } else if (view->can_produce_deletions) {
      log.Append(range, range.To())
          << "Message '" << message.Name() << '/' << message.Arity()
          << "' can produce deletions but is not marked with the "
          << "'@differential' attribute";
    }
  }
}

}  // namespace hyde
