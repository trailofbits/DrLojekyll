// Copyright 2020, Trail of Bits. All rights reserved.

#include "Query.h"

#include <vector>
#include <unordered_set>

//#include <iostream>
#include <sstream>


namespace hyde {

// TODO(sonya): generalize RunForwardAnalysis/RunBackwardAnalysis/TaintWithCol
// for taint types besides COL *
template <typename TaintType>
class TaintTracker {

 public:
  inline explicit TaintTracker(std::shared_ptr<QueryImpl> impl_) : impl(impl_) {}

  bool TaintWithCol(COL *col, COL *taint_col, bool is_backward = false) {
    auto old_size = col_taints[col->id].size();
    auto index = taint_col->id;
    if (is_backward) {
      col_taints[col->id].insert(taint_col);
    }
    col_taints[col->id].insert(col_taints[index].begin(), col_taints[index].end());
    return old_size != col_taints[col->id].size();
  }

  const std::unordered_set<TaintType> &Taints(COL *col) {
    return col_taints[col->id];
  }

  const std::unordered_set<TaintType> &Taints(unsigned col_id) {
    return col_taints[col_id];
  }

  const std::string TaintsAsString(unsigned col_id) const {
    if (col_taints[col_id].empty()) {
      return "";
    }

    std::stringstream ss;
    for (auto col : col_taints[col_id]) {
      ss << col->id << ", ";
    }

    return ss.str().substr(0, ss.str().length() - 2);
  }

  // Taint all columns with the insert columns they are derived from
  void RunForwardAnalysis(void) {
    col_taints.clear();
    col_taints.emplace_back(std::unordered_set<TaintType>());
    std::vector<VIEW*> sorted_views;
    impl->ForEachViewInReverseDepthOrder([&](VIEW *view) {
      sorted_views.push_back(view);
      for (auto c : view->columns) {
        col_taints.emplace_back(std::unordered_set<TaintType>());
      }
    });

    for (auto insert : impl->inserts) {
      for (auto col : insert->input_columns) {
        col_taints[col->id].insert(col);

#ifndef NDEBUG
        col->taint_ids = TaintsAsString(col->id);
#endif

      }
    }

      for (auto changed = true; changed; ) {
        changed = false;
        for (auto view : sorted_views) {
          QueryView(view).ForEachUse([&](QueryColumn in_col, InputColumnRole role,
                                          std::optional<QueryColumn> out_col) {
            switch(role) {
              case InputColumnRole::kJoinPivot: {
                // TODO(sonya): Check this for robustness. It might recursion or another loop
                for (auto in : in_col.impl->view->input_columns) {
                  if (in_col.impl->view->in_to_out.find(in)->second == in_col.impl) {
                    auto res = TaintWithCol(in_col.impl, in);
                    changed = changed || res;
                  }
                }
                auto res = TaintWithCol(out_col->impl, in_col.impl);
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
                auto res = TaintWithCol(in_col.impl, out_col->impl);
                changed = changed || res;
                break;
              }
              case InputColumnRole::kAggregatedColumn:
              case InputColumnRole::kDeleted:
              case InputColumnRole::kIndexValue:
              case InputColumnRole::kPublished:
                break;
              case InputColumnRole::kMaterialized:
              if (out_col) {
                auto res = TaintWithCol(in_col.impl, out_col->impl);
                changed = changed || res;
              }
              break;
            }

#ifndef NDEBUG
            in_col.impl->taint_ids = TaintsAsString(in_col.impl->id);
            if (out_col) {
              out_col->impl->taint_ids = TaintsAsString(out_col->impl->id);
            }
#endif
          });
        }
      }
  }

  // Taint all columns with the list of columns which their outputs effect
  void RunBackwardAnalysis(void) {
    col_taints.clear();
    col_taints.emplace_back(std::unordered_set<TaintType>());
    std::vector<VIEW*> sorted_views;
    impl->ForEachViewInReverseDepthOrder([&](VIEW *view) {
      sorted_views.push_back(view);
      for (auto c : view->columns) {
        col_taints.emplace_back(std::unordered_set<TaintType>());
      }
    });

    for (auto insert : impl->inserts) {
      for (auto col : insert->input_columns) {
        col_taints[col->id].insert(col);

#ifndef NDEBUG
        col->taint_ids = TaintsAsString(col->id);
#endif

      }
    }

    for (auto changed = true; changed;) {
      changed = false;
      for (auto view : sorted_views) {
        QueryView(view).ForEachUse(
            [&](QueryColumn in_col, InputColumnRole role,
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
                  auto res = TaintWithCol(out_col->impl, in_col.impl, true);
                  changed = changed || res;
                  break;
                }
                case InputColumnRole::kJoinPivot: {
                  auto res = TaintWithCol(out_col->impl, in_col.impl, true);
                  changed = changed || res;
                  break;
                }
                case InputColumnRole::kAggregatedColumn:
                case InputColumnRole::kDeleted:
                case InputColumnRole::kIndexValue:
                case InputColumnRole::kPublished:
                  break;
                case InputColumnRole::kMaterialized:
                  if (out_col) {
                    auto res = TaintWithCol(out_col->impl, in_col.impl, true);
                    changed = changed || res;
                  }
                  break;
              }
#ifndef NDEBUG
            in_col.impl->taint_ids = TaintsAsString(in_col.impl->id);
            if (out_col) {
              out_col->impl->taint_ids = TaintsAsString(out_col->impl->id);
            }
#endif
            });
      }
    }
  }

 private:
  std::vector<std::unordered_set<TaintType>> col_taints;
  std::shared_ptr<QueryImpl> impl;


};

}  // namespace hyde

