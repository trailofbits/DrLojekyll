// Copyright 2020, Trail of Bits. All rights reserved.

#include <unordered_set>
#include <vector>

#include "Query.h"

namespace hyde {

namespace {
bool TaintWithCol(
    COL *col, COL *taint_col,
    std::vector<std::shared_ptr<std::unordered_set<COL*>>> &col_taints,
    bool is_backward = false) {
  auto old_size = col_taints[col->id]->size();
  auto index = taint_col->id;
  if (is_backward) {
    col_taints[col->id]->insert(taint_col);
  }
  col_taints[col->id]->insert(col_taints[index]->begin(),
                             col_taints[index]->end());
  return old_size != col_taints[col->id]->size();
}
} // namespace

// Taint all columns with the insert columns they are derived from
void QueryImpl::RunForwardsTaintAnalysis(void) {
  forwards_col_taints.clear();
  forwards_col_taints.emplace_back(
      std::make_shared<std::unordered_set<COL*>>());

  std::vector<VIEW *> sorted_views;
  ForEachViewInReverseDepthOrder([&](VIEW *view) {
    sorted_views.push_back(view);
    for (auto c : view->columns) {
      forwards_col_taints.emplace_back(
          std::make_shared<std::unordered_set<COL*>>());
      c->forwards_col_taints.reset();
    }
  });

  ForEachView([&](VIEW *v) {
   for (auto c : v->columns) {
     c->forwards_col_taints = forwards_col_taints[c->id];
   }
  });

  for (auto insert : inserts) {
    for (auto col : insert->input_columns) {
      forwards_col_taints[col->id]->insert(col);
    }
  }

  for (auto changed = true; changed;) {
    changed = false;
    for (auto view : sorted_views) {
      QueryView(view).ForEachUse([&](QueryColumn in_col, InputColumnRole role,
                                     std::optional<QueryColumn> out_col) {
        switch (role) {
          case InputColumnRole::kJoinPivot: {

            // TODO(sonya): Check this for robustness. It might recursion or another loop
            for (auto in : in_col.impl->view->input_columns) {
                  if (in_col.impl->view->in_to_out.find(in)
                        != in_col.impl->view->in_to_out.end()
                      && in_col.impl->view->in_to_out.find(in)->second
                        == in_col.impl) {
                auto res = TaintWithCol(in_col.impl, in, forwards_col_taints);
                changed = changed || res;
                res = TaintWithCol(in, in_col.impl, forwards_col_taints);
                changed = changed || res;
              }
            }
//            auto res = TaintWithCol(out_col->impl, in_col.impl,
//                                    forwards_col_taints);
//            changed = changed || res;
            auto res = TaintWithCol(in_col.impl, out_col->impl,
                                    forwards_col_taints);
            changed = changed || res;
           break;
          }
          case InputColumnRole::kNegated:
          case InputColumnRole::kCopied:
          case InputColumnRole::kAggregateConfig:
          case InputColumnRole::kAggregateGroup:
          case InputColumnRole::kCompareLHS:
          case InputColumnRole::kCompareRHS:
          case InputColumnRole::kIndexKey:
          case InputColumnRole::kFunctorInput:
          case InputColumnRole::kJoinNonPivot:
          case InputColumnRole::kMergedColumn: {
//            auto res = TaintWithCol(out_col->impl, in_col.impl,
//                                    forwards_col_taints);
//            changed = changed || res;
            auto res = TaintWithCol(in_col.impl, out_col->impl,
                               forwards_col_taints);
            changed = changed || res;
            break;
          }
          case InputColumnRole::kAggregatedColumn:
          case InputColumnRole::kIndexValue:
          case InputColumnRole::kPublished: break;
          case InputColumnRole::kMaterialized:
            if (out_col) {
//              auto res = TaintWithCol(out_col->impl, in_col.impl,
//                                      forwards_col_taints);
//              changed = changed || res;
              auto res = TaintWithCol(in_col.impl, out_col->impl,
                                      forwards_col_taints);
              changed = changed || res;
            }
            break;
        }
      });
    }
  }
}

// Taint all columns with the list of columns which their outputs effect
void QueryImpl::RunBackwardsTaintAnalysis(void) {
  std::vector<std::shared_ptr<std::unordered_set<COL *>>> col_taints;
  backwards_col_taints.clear();

  col_taints.emplace_back(nullptr);
  backwards_col_taints.emplace_back(nullptr);

  std::vector<VIEW *> sorted_views;

  ForEachViewInReverseDepthOrder([&](VIEW *view) {
    sorted_views.push_back(view);
    for (auto c : view->columns) {
      col_taints.emplace_back(
            std::make_shared<std::unordered_set<COL*>>());
      backwards_col_taints.emplace_back(nullptr);
      c->backwards_col_taints.reset(new UseList<COL>(view));
    }
  });

  for (auto changed = true; changed;) {
    changed = false;
    for (auto view : sorted_views) {
      QueryView(view).ForEachUse([&](QueryColumn in_col, InputColumnRole role,
                                     std::optional<QueryColumn> out_col) {
        switch (role) {
          case InputColumnRole::kAggregateConfig:
          case InputColumnRole::kAggregateGroup:
          case InputColumnRole::kCompareLHS:
          case InputColumnRole::kCompareRHS:
          case InputColumnRole::kFunctorInput:
          case InputColumnRole::kIndexKey:
          case InputColumnRole::kNegated:
          case InputColumnRole::kCopied:
          case InputColumnRole::kMergedColumn:
          case InputColumnRole::kJoinNonPivot: {
            auto res = TaintWithCol(out_col->impl, in_col.impl,
                                    col_taints, true);
            changed = changed || res;
            assert(!col_taints[out_col->impl->id]->empty());
            break;
          }
          case InputColumnRole::kJoinPivot: {
            auto res = TaintWithCol(out_col->impl, in_col.impl,
                                    col_taints, true);
            changed = changed || res;
            assert(!col_taints[out_col->impl->id]->empty());
           break;
          }
          case InputColumnRole::kAggregatedColumn:
          case InputColumnRole::kIndexValue:
          case InputColumnRole::kPublished: break;
          case InputColumnRole::kMaterialized:
            if (out_col) {
              auto res = TaintWithCol(out_col->impl, in_col.impl,
                                      col_taints, true);
              changed = changed || res;
              assert(!col_taints[out_col->impl->id]->empty());
            }
            break;
        }
      });
    }
  }

  ForEachView([&](VIEW *v) {
   for (auto c : v->columns) {
     backwards_col_taints[c->id] = c->backwards_col_taints;
       for (auto taint : *(col_taints[c->id])) {
         backwards_col_taints[c->id]->AddUse(taint);
       }
     }
  });

}

//std::unordered_set<COL *>
//QueryImpl::GetForwardsTaintsFromColId(unsigned col_id) {
//  if (forwards_col_taints.empty() || !forwards_col_taints[col_id]) {
//    return std::unordered_set<COL *>();
//  }
//  assert(forwards_col_taints.size() > col_id);
//  return *forwards_col_taints[col_id];
//}
//
UsedNodeRange<QueryColumn>
QueryImpl::GetBackwardsTaintsFromColId(unsigned col_id) {
  if (backwards_col_taints.empty() || !backwards_col_taints[col_id]) {
    return {};
  } else {
    return {UsedNodeIterator<QueryColumn>(backwards_col_taints[col_id]->begin()),
            UsedNodeIterator<QueryColumn>(backwards_col_taints[col_id]->end())};
  }
}

}  // namespace hyde
