// Copyright 2020, Trail of Bits. All rights reserved.

#include <unordered_set>
#include <vector>

#include "Query.h"

//#include <iostream>
#include <sstream>


namespace hyde {

namespace {

bool TaintWithCol(COL *col, COL *taint_col, std::vector<std::shared_ptr<std::unordered_set<COL *>>> &col_taints, bool is_backward = false) {
  auto old_size = col_taints[col->id]->size();
  auto index = taint_col->id;
  if (is_backward) {
    col_taints[col->id]->insert(taint_col);
  }
  col_taints[col->id]->insert(col_taints[index]->begin(),
                             col_taints[index]->end());
  return old_size != col_taints[col->id]->size();
}

const std::unordered_set<COL *> &Taints(COL *col, std::vector<std::shared_ptr<std::unordered_set<COL *>>> &col_taints) {
  return *col_taints[col->id];
}

const std::unordered_set<COL *> &Taints(unsigned col_id, std::vector<std::shared_ptr<std::unordered_set<COL *>>> &col_taints) {
  return *col_taints[col_id];
}

//const std::string TaintsAsString(unsigned col_id, std::vector<std::shared_ptr<std::unordered_set<COL *>>> &col_taints) {
//  if (col_taints[col_id]->empty()) {
//    return "";
//  }
//
//  std::stringstream ss;
//  for (auto col : *(col_taints[col_id])) {
//    ss << col->id << ", ";
//  }
//
//  return ss.str().substr(0, ss.str().length() - 2);
//}


} // namespace

// Taint all columns with the insert columns they are derived from
void QueryImpl::RunForwardsTaintAnalysis(void) {
  forwards_col_taints.clear();
  forwards_col_taints.emplace_back(std::make_shared<std::unordered_set<COL *>>());

  std::vector<VIEW *> sorted_views;
  ForEachViewInReverseDepthOrder([&](VIEW *view) {
    sorted_views.push_back(view);
    for (auto c : view->columns) {
      auto col_taints = std::make_shared<std::unordered_set<COL *>>();
      forwards_col_taints.emplace_back(col_taints);
      c->forwards_col_taints = col_taints;
    }
  });

  for (auto insert : inserts) {
    for (auto col : insert->input_columns) {
      forwards_col_taints[col->id]->insert(col);

//#ifndef NDEBUG
//        col->forwards_col_taint_ids = TaintsAsString(col->id, impl->forwards_col_taints);
//#endif
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
              if (in_col.impl->view->in_to_out.find(in) != in_col.impl->view->in_to_out.end() &&
                  in_col.impl->view->in_to_out.find(in)->second == in_col.impl) {
                auto res = TaintWithCol(in_col.impl, in, forwards_col_taints);
                changed = changed || res;
              }
            }
            auto res = TaintWithCol(out_col->impl, in_col.impl, forwards_col_taints);
            changed = changed || res;
            res = TaintWithCol(in_col.impl, out_col->impl, forwards_col_taints);
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
            auto res = TaintWithCol(in_col.impl, out_col->impl, forwards_col_taints);
            changed = changed || res;
            break;
          }
          case InputColumnRole::kAggregatedColumn:
          case InputColumnRole::kIndexValue:
          case InputColumnRole::kPublished: break;
          case InputColumnRole::kMaterialized:
            if (out_col) {
              auto res = TaintWithCol(in_col.impl, out_col->impl, forwards_col_taints);
              changed = changed || res;
            }
            break;
        }

#ifndef NDEBUG
//          in_col.impl->forwards_col_taint_ids = TaintsAsString(in_col.impl->id, impl->forwards_col_taints);
//          if (out_col) {
//            out_col->impl->forwards_col_taint_ids = TaintsAsString(out_col->impl->id, impl->forwards_col_taints);
//          }
#endif
      });
    }
  }
}

// Taint all columns with the list of columns which their outputs effect
void QueryImpl::RunBackwardsTaintAnalysis(void) {
  backwards_col_taints.clear();
  backwards_col_taints.emplace_back(std::make_shared<std::unordered_set<COL *>>());

  std::vector<VIEW *> sorted_views;
  ForEachViewInReverseDepthOrder([&](VIEW *view) {
    sorted_views.push_back(view);
    for (auto c : view->columns) {
      auto col_taints = std::make_shared<std::unordered_set<COL *>>();
      backwards_col_taints.emplace_back(col_taints);
      c->backwards_col_taints = col_taints;
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
            auto res = TaintWithCol(out_col->impl, in_col.impl, backwards_col_taints, true);
            changed = changed || res;
            break;
          }
          case InputColumnRole::kJoinPivot: {

            //auto res = TaintWithCol(in_col.impl, out_col->impl, true);
            auto res = TaintWithCol(out_col->impl, in_col.impl, backwards_col_taints, true);
            changed = changed || res;
            break;
          }
          case InputColumnRole::kAggregatedColumn:
          case InputColumnRole::kIndexValue:
          case InputColumnRole::kPublished: break;
          case InputColumnRole::kMaterialized:
            if (out_col) {
              auto res = TaintWithCol(out_col->impl, in_col.impl, backwards_col_taints, true);
              changed = changed || res;
            }
            break;
        }
      });
    }
  }
}
}  // namespace hyde
