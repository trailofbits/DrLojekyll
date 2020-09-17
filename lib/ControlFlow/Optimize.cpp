// Copyright 2020, Trail of Bits. All rights reserved.

#include "Program.h"

namespace hyde {

void ProgramImpl::Optimize(void) {
  for (auto changed = true; changed; ) {
    changed = false;

    for (auto par : parallel_regions) {
      if (!par->IsUsed() || !par->parent) {
        continue;
      }

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
      } else if (auto parent_par = par->parent->AsParallel();
                 parent_par && !par->regions.Empty()) {

        for (auto child_region : par->regions) {
          child_region->parent = parent_par;
          parent_par->regions.AddUse(child_region);
        }

        par->regions.Clear();
        changed = true;

      // Erase any empty child regions.
      } else {
        par->regions.RemoveIf([&changed] (REGION *child_region) {
          if (child_region->IsNoOp()) {
            child_region->parent = nullptr;
            changed = true;
            return true;
          } else {
            return false;
          }
        });
      }
    }

    // Clear out empty output regions of inductions.
    for (auto induction : induction_regions) {
      if (induction->output_region &&
          induction->output_region->IsNoOp()) {
        induction->output_region->parent = nullptr;
        UseRef<REGION>().Swap(induction->output_region);
        changed = true;
      }
    }

    for (auto series : series_regions) {
      if (!series->IsUsed() || !series->parent) {
        continue;
      }

      // This is a series region with only one child, so we can elevate the
      // child to replace the parent.
      if (series->regions.Size() == 1u) {
        const auto only_region = series->regions[0u];
        series->regions.Clear();

        if (!only_region->IsNoOp()) {
          series->ReplaceAllUsesWith(only_region);
        }

        changed = true;

      // This series node's parent is also a series node.
      } else if (auto parent_series = series->parent->AsSeries();
                 parent_series && !series->regions.Empty()) {

        UseList<REGION> new_siblings(parent_series);
        for (auto sibling_region : parent_series->regions) {
          if (!sibling_region->IsNoOp()) {
            if (sibling_region == series) {
              for (auto child_region : series->regions) {
                if (!child_region->IsNoOp()) {
                  new_siblings.AddUse(child_region);
                  child_region->parent = parent_series;
                } else {
                  child_region->parent = nullptr;
                }
              }
            } else {
              new_siblings.AddUse(sibling_region);
            }
          } else {
            sibling_region->parent = nullptr;
          }
        }

        series->regions.Clear();
        series->parent = nullptr;
        changed = true;

        parent_series->regions.Swap(new_siblings);

      // Erase any empty child regions.
      } else {
        series->regions.RemoveIf([&changed] (REGION *child_region) {
          if (child_region->IsNoOp()) {
            child_region->parent = nullptr;
            changed = true;
            return true;
          } else {
            return false;
          }
        });
      }
    }

    for (auto op : operation_regions) {
      if (!op->IsUsed() || !op->parent) {
        continue;
      }

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

        const auto body = let->body.get();
        UseRef<REGION>().Swap(let->body);

        if (body && !body->IsNoOp()) {
          changed = true;
          let->ReplaceAllUsesWith(body);
        }

        let->defined_vars.Clear();
        let->used_vars.Clear();

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

            const auto body = exists->body.get();
            UseRef<REGION>().Swap(exists->body);

            if (body && !body->IsNoOp()) {
              changed = true;
              exists->ReplaceAllUsesWith(body);
            }
          }
        }

      } else {
        if (op->body && op->body->IsNoOp()) {
          op->body->parent = nullptr;
          UseRef<REGION>().Swap(op->body);
          changed = true;
        }
      }
    }

    parallel_regions.RemoveUnused();
    series_regions.RemoveUnused();
    operation_regions.RemoveUnused();
  }
}

}  // namespace hyde
