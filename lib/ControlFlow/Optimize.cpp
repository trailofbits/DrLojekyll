// Copyright 2020, Trail of Bits. All rights reserved.

#include "Program.h"

namespace hyde {
namespace {

// TODO(pag): Implement an optimization that checks to see if two
//            `ProgramCheckStateRegion` operate on the same tuple in
//            parallel, and if so, merge their bodies.

static bool OptimizeImpl(PARALLEL *par) {
  if (!par->IsUsed() || !par->parent) {
    return false;

  // This is a parallel region with only one child, so we can elevate the
  // child to replace the parent.
  } else if (par->regions.Size() == 1u) {
    auto only_region = par->regions[0u];
    par->regions.Clear();

    if (!only_region->IsNoOp()) {
      par->ReplaceAllUsesWith(only_region);
    }

    return true;

  // This parallel node's parent is also a parallel node.
  } else if (auto parent_par = par->parent->AsParallel();
             parent_par && !par->regions.Empty()) {

    for (auto child_region : par->regions) {
      child_region->parent = parent_par;
      parent_par->regions.AddUse(child_region);
    }

    par->regions.Clear();
    return true;

    // Erase any empty child regions.
  }
  auto changed = false;
  par->regions.RemoveIf([&changed](REGION *child_region) {
    if (child_region->IsNoOp()) {
      child_region->parent = nullptr;
      changed = true;
      return true;
    } else {
      return false;
    }
  });

  if (changed) {
    return true;
  }

  // The PARALLEL node is "canonical" as far as we can tell, so check to see
  // if any of its child regions might be mergeable.

  std::unordered_map<unsigned, std::vector<REGION *>> grouped_regions;
  for (auto region : par->regions) {
    unsigned index = 0u;
    if (region->AsSeries()) {
      index = ~0u;
    } else if (region->AsInduction()) {
      index = ~0u - 1u;
    } else if (auto op = region->AsOperation(); op) {
      index = static_cast<unsigned>(op->op);

    // Don't bother trying to merge parallel regions until they've been
    // flattened completely. It is also impossible to put a procedure inside
    // of a parallel region.
    } else {
      return false;
    }

    grouped_regions[index].push_back(region);
  }

  // Go remove duplicate child regions.
  EqualitySet eq;
  for (const auto &[index, similar_regions] : grouped_regions) {
    (void) index;
    const auto num_similar_regions = similar_regions.size();
    for (auto i = 1u; i < num_similar_regions; ++i) {
      REGION *region1 = similar_regions[i - 1u];
      if (!region1->parent) {
        continue;  // Already removed;
      }

      for (auto j = i; j < num_similar_regions; ++j) {
        REGION *region2 = similar_regions[j];
        if (!region2->parent) {
          continue;  // Already removed.
        }

        if (region1->Equals(eq, region2)) {
          assert(region1 != region2);
          par->regions.RemoveIf([=](REGION *r) { return r == region2; });
          region2->parent = nullptr;
          changed = true;
        }
        eq.Clear();
      }
    }
  }

  return changed;
}

// Clear out empty output regions of inductions.
static bool OptimizeImpl(INDUCTION *induction) {
  if (induction->output_region && induction->output_region->IsNoOp()) {
    induction->output_region->parent = nullptr;
    UseRef<REGION>().Swap(induction->output_region);
    return true;
  }
  return false;
}

static bool OptimizeImpl(SERIES *series) {
  if (!series->IsUsed() || !series->parent) {
    return false;

  // This is a series region with only one child, so we can elevate the
  // child to replace the parent.
  } else if (series->regions.Size() == 1u) {
    const auto only_region = series->regions[0u];
    series->regions.Clear();

    if (!only_region->IsNoOp()) {
      series->ReplaceAllUsesWith(only_region);
    }

    return true;

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
    parent_series->regions.Swap(new_siblings);
    return true;

  // Erase any empty child regions.
  } else {
    auto changed = false;
    series->regions.RemoveIf([&changed](REGION *child_region) {
      if (child_region->IsNoOp()) {
        child_region->parent = nullptr;
        changed = true;
        return true;
      } else {
        return false;
      }
    });
    return changed;
  }
}

// Down-propagate all bindings.
static bool OptimizeImpl(LET *let) {
  bool changed = false;

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

  if (changed) {
    let->defined_vars.Clear();
    let->used_vars.Clear();
    return true;

  } else {
    return false;
  }
}

static bool OptimizeImpl(EXISTS *exists) {
  bool changed = false;
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
  return changed;
}

// Propagate comparisons upwards, trying to join towers of comparisons into
// single tuple group comparisons.
static bool OptimizeImpl(TUPLECMP *cmp) {
  bool changed = false;

  auto max_i = cmp->lhs_vars.Size();
  if (auto parent_op = cmp->parent->AsOperation(); max_i && parent_op) {
    if (auto parent_cmp = parent_op->AsTupleCompare();
        parent_cmp && cmp->cmp_op == parent_cmp->cmp_op) {

      for (auto i = 0u; i < max_i; ++i) {
        parent_cmp->lhs_vars.AddUse(cmp->lhs_vars[i]);
        parent_cmp->rhs_vars.AddUse(cmp->rhs_vars[i]);
      }

      cmp->lhs_vars.Clear();
      cmp->rhs_vars.Clear();
      max_i = 0u;
    }
  }

  // There are variables being compared.
  if (max_i) {
    bool all_same = true;
    for (auto i = 0u; i < max_i; ++i) {
      if (cmp->lhs_vars[i] != cmp->rhs_vars[i]) {
        all_same = false;
        break;
      }
    }

    if (all_same) {
      // The comparison is trivially true; eliminate it.
      if (ComparisonOperator::kEqual == cmp->cmp_op) {
        max_i = 0;
        cmp->lhs_vars.Clear();
        cmp->rhs_vars.Clear();

      // The comparison is unsatisfiable.
      } else if (ComparisonOperator::kNotEqual == cmp->cmp_op ||
                 ComparisonOperator::kLessThan == cmp->cmp_op ||
                 ComparisonOperator::kGreaterThan == cmp->cmp_op) {
        changed = true;
        cmp->lhs_vars.Clear();
        cmp->rhs_vars.Clear();
      }
    }
  }

  // This compare has no variables being compared, so replace it with its
  // body.
  if (!max_i) {
    const auto body = cmp->body.get();
    UseRef<REGION>().Swap(cmp->body);
    if (body) {
      cmp->ReplaceAllUsesWith(body);
      changed = true;
    }
  }

  return changed;
}

}  // namespace

void ProgramImpl::Optimize(void) {
  for (auto changed = true; changed;) {
    changed = false;

    for (auto par : parallel_regions) {
      changed = OptimizeImpl(par) | changed;
    }

    for (auto induction : induction_regions) {
      changed = OptimizeImpl(induction) | changed;
    }

    for (auto series : series_regions) {
      changed = OptimizeImpl(series) | changed;
    }

    for (auto op : operation_regions) {
      if (!op->IsUsed() || !op->parent) {
        continue;
      }

      // We try to aggressively eliminate LET bindings by down-propagating
      // variable assignments.
      if (auto let = op->AsLetBinding(); let) {
        changed = OptimizeImpl(let) | changed;

      // If we have an exists check nested inside another one, then try to merge
      // upward.
      } else if (auto exists = op->AsExistenceCheck(); exists) {
        changed = OptimizeImpl(exists) | changed;

      } else if (auto tuple_cmp = op->AsTupleCompare(); tuple_cmp) {
        changed = OptimizeImpl(tuple_cmp) | changed;

      // All other operations check to see if they are no-ops and if so
      // remove the bodies.
      } else if (op->body && op->body->IsNoOp()) {
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

}  // namespace hyde
