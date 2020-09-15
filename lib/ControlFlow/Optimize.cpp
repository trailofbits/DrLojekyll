// Copyright 2020, Trail of Bits. All rights reserved.

#include "Program.h"

namespace hyde {

void ProgramImpl::Optimize(void) {
  for (auto changed = true; changed; ) {
    changed = false;

    auto removed_parallels = false;
    for (auto par : parallel_regions) {
      if (par->regions.Size() == 1u) {
        auto only_region = par->regions[0u];
        par->regions.Clear();
        par->ReplaceAllUsesWith(only_region);
        removed_parallels = true;
        changed = true;
      }
    }

    for (auto par : parallel_regions) {

      // This is a parallel region with only one child, so we can elevate the
      // child to replace the parent.
      if (par->regions.Size() == 1u) {
        auto only_region = par->regions[0u];
        par->regions.Clear();
        if (!only_region->IsNoOp()) {
          par->ReplaceAllUsesWith(only_region);
        }

        changed = true;

      // This parallel node's parent is also a parallel node.
      } else if (auto parent_par = par->parent->AsParallel(); parent_par) {
        changed = true;

        for (auto child_region : par->regions) {
          child_region->parent = parent_par;
          parent_par->regions.AddUse(child_region);
        }

        par->regions.Clear();

      // Erase any empty child regions.
      } else {
        par->regions.RemoveIf([&changed] (REGION *child_region) {
          if (child_region->IsNoOp()) {
            changed = true;
            return true;
          } else {
            return false;
          }
        });
      }
    }

    for (auto series : series_regions) {

      // This is a series region with only one child, so we can elevate the
      // child to replace the parent.
      if (series->regions.Size() == 1u) {
        auto only_region = series->regions[0u];
        series->regions.Clear();
        if (!only_region->IsNoOp()) {
          series->ReplaceAllUsesWith(only_region);
        }

        changed = true;

      // This series node's parent is also a series node.
      } else if (auto parent_series = series->parent->AsSeries(); parent_series) {
        changed = true;

        UseList<REGION> new_siblings(parent_series);
        for (auto sibling_region : parent_series->regions) {
          if (!sibling_region->IsNoOp()) {
            if (sibling_region == series) {
              for (auto child_region : series->regions) {
                new_siblings.AddUse(child_region);
                child_region->parent = parent_series;
              }
            } else {
              new_siblings.AddUse(sibling_region);
            }
          }
        }

        series->regions.Clear();
        parent_series->regions.Swap(new_siblings);

      // Erase any empty child regions.
      } else {
        series->regions.RemoveIf([&changed] (REGION *child_region) {
          if (child_region->IsNoOp()) {
            changed = true;
            return true;
          } else {
            return false;
          }
        });
      }
    }

    for (auto op : operation_regions) {
      if (auto let = op->AsLetBinding(); let) {

        // This LET binding's parent is also a LET binding; we can merge into
        // the parent.
        if (auto parent_op = let->parent->AsOperation(); parent_op) {
          if (auto parent_let = parent_op->AsLetBinding(); parent_let) {
            changed = true;
            for (auto var : let->variables) {
              parent_let->variables.AddUse(var);
            }

            let->variables.Clear();
          }
        }

        // It's a LET binding without any variables.
        if (let->variables.Empty()) {
          if (auto body = let->body.get(); body) {
            changed = true;
            UseRef<REGION>().Swap(let->body);
            let->ReplaceAllUsesWith(body);
          }
        }
      }
    }

    parallel_regions.RemoveUnused();
    series_regions.RemoveUnused();
    operation_regions.RemoveUnused();
  }
}

}  // namespace hyde
