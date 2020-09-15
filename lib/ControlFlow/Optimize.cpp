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

      // We try to aggressively eliminate LET bindings by down-propagating
      // variable assignments.
      if (auto let = op->AsLetBinding(); let) {

        // Down-propagate all bindings.
        for (auto i = 0u, max_i = let->defined_vars.Size(); i < max_i; ++i) {
          changed = true;
          const auto var_def = let->defined_vars[i];
          const auto var_use = let->used_vars[i];
          var_def->ReplaceAllUsesWith(var_use);
        }

        let->defined_vars.Clear();
        let->used_vars.Clear();

        if (auto body = let->body.get(); body) {
          changed = true;
          UseRef<REGION>().Swap(let->body);
          let->ReplaceAllUsesWith(body);
        }

      // If we have an exists check nested inside another one, then try to merge
      // upward.
      } else if (auto exists = op->AsExistenceCheck(); exists) {
        if (auto parent_op = exists->parent->AsOperation(); parent_op) {
          if (auto parent_exists = parent_op->AsExistenceCheck();
              parent_exists && exists->op == parent_exists->op) {

            for (auto cond : exists->cond_vars) {
              changed = true;
              parent_exists->cond_vars.AddUse(cond);
            }

            exists->cond_vars.Clear();

            if (auto body = exists->body.get(); body) {
              changed = true;
              UseRef<REGION>().Swap(exists->body);
              exists->ReplaceAllUsesWith(body);
            }
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
