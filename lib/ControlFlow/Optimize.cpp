// Copyright 2020, Trail of Bits. All rights reserved.

#include <iostream>

#include "Program.h"

namespace hyde {
namespace {

// TODO(pag): Find all ending returns in the children of the par, and if there
//            are any, check that they all match, and if so, create a sequence
//            that moves the `return <X>` to after the parallel, and also
//            assert(false).
static bool OptimizeImpl(ProgramImpl *prog, PARALLEL *par) {
  if (!par->IsUsed() || !par->parent) {
    return false;

  // This is a parallel region with only one child, so we can elevate the
  // child to replace the parent.
  } else if (par->regions.Size() == 1u) {
    const auto only_region = par->regions[0u];
    par->regions.Clear();
    par->ReplaceAllUsesWith(only_region);
    return true;

  // This parallel node's parent is also a parallel node.
  } else if (auto parent_par = par->parent->AsParallel();
             parent_par && !par->regions.Empty()) {

    for (auto child_region : par->regions) {
      assert(child_region->parent == par);
      child_region->parent = parent_par;
      parent_par->AddRegion(child_region);
    }

    par->regions.Clear();
    return true;
  }

  // Erase any empty or no-op child regions.
  auto changed = false;
  auto has_ends_with_return = false;
  par->regions.RemoveIf(
      [&changed, &has_ends_with_return](REGION *child_region) {
        if (child_region->EndsWithReturn()) {
          has_ends_with_return = true;
        }

        if (child_region->IsNoOp()) {
          child_region->parent = nullptr;
          changed = true;
          return true;
        } else {
          return false;
        }
      });

  if (changed) {
    OptimizeImpl(prog, par);
    return true;
  }

  assert(!has_ends_with_return);

  //  // One or more of the children of the parallel regions ends with a return.
  //  // That's a bit problematic.
  //  if (has_ends_with_return) {
  //    auto seq = prog->series_regions.Create(par->parent);
  //    par->ReplaceAllUsesWith(seq);
  //    par->parent = seq;
  //    seq->AddRegion(par);
  //  }

  // The PARALLEL node is "canonical" as far as we can tell, so check to see
  // if any of its child regions might be mergeable.

  // Group together all children of this parallel region; we'll use this
  // grouping to identify merge candidates, as well as strip-mining candidates.
  std::unordered_map<uint64_t, std::vector<REGION *>> grouped_regions;
  for (auto region : par->regions) {
    assert(region->parent == par);
    grouped_regions[region->Hash(0)].push_back(region);
  }

  // Go remove duplicate child regions. Note: we remove the regions from the
  // `par` node below by looking for regions whose parents are `nullptr`.
  //
  // NOTE(pag): This is is basically an application of common subexpression
  //            elimination.
  EqualitySet eq;
  for (const auto &hash_to_regions : grouped_regions) {
    const auto &similar_regions = hash_to_regions.second;
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

        eq.Clear();
        if (region1->Equals(eq, region2, UINT32_MAX)) {
          assert(region1 != region2);
          region2->parent = nullptr;
          changed = true;
        }
      }
    }
  }

  // Go try to merge similar child regions. Note: we remove the regions from the
  // `par` node below by looking for regions whose parents are `nullptr`.
  //
  // This looks for child regions that are superficially the same, so that the
  // grand-children of two similar child regions can be merged under a single
  // child region.
  std::vector<REGION *> merge_candidates;
  for (const auto &hash_to_regions : grouped_regions) {
    const auto &similar_regions = hash_to_regions.second;
    const auto num_similar_regions = similar_regions.size();
    for (auto i = 1u; i < num_similar_regions; ++i) {
      REGION *region1 = similar_regions[i - 1u];
      if (!region1->parent) {
        continue;  // Already removed;
      }

      merge_candidates.clear();

      for (auto j = i; j < num_similar_regions; ++j) {
        REGION *region2 = similar_regions[j];
        if (!region2->parent) {
          continue;  // Already removed.
        }

        eq.Clear();
        if (region1->Equals(eq, region2, 0)) {
          assert(region1 != region2);
          merge_candidates.push_back(region2);
        }
      }

      // NOTE(pag): This clear the `parent` pointer of each region in
      //            `merge_candidates`.
      if (!merge_candidates.empty() &&
          region1->MergeEqual(prog, merge_candidates)) {
        changed = true;
      }
    }
  }

  if (changed) {
    // Remove any redundant or strip-minded regions in bulk.
    const auto old_num_children = par->regions.Size();
    par->regions.RemoveIf([=](REGION *r) { return !r->parent; });
    assert(old_num_children > par->regions.Size());
    OptimizeImpl(prog, par);
    (void) old_num_children;
    return true;
  }

  return false;
}

// Optimize induction regions.
// * Clear out empty output regions of inductions.
// * Optimize nested loop inductions.
//
// TODO(pag): Check if the fixpoint loop region ends in a return. If so, bail
//            out.
static bool OptimizeImpl(ProgramImpl *prog, INDUCTION *induction) {
  if (!induction->IsUsed() || !induction->parent) {
    return false;
  }

  auto changed = false;

  // Clear out empty output regions of inductions.
  if (induction->output_region && induction->output_region->IsNoOp()) {
    induction->output_region->parent = nullptr;
    induction->output_region.Clear();
    changed = true;
  }

  return changed;
//
//  auto parent_region = induction->parent;
//  if (!parent_region) {
//    return changed;
//  }
//
//  auto parent_induction = parent_region->AsInduction();
//  if (!parent_induction) {
//    return changed;
//  }
//
//  // Optimize nested inductions.
//
//  // Form like
//  // induction
//  //   init
//  //    induction
//  if (induction == parent_induction->init_region.get()) {
//
//    // Fixup vectors
//    for (auto def : induction->vectors) {
//      changed = true;
//      parent_induction->vectors.AddUse(def);
//    }
//    induction->vectors.Clear();
//
//    // Fixup output region
//    if (auto output_region = induction->output_region.get(); output_region) {
//      assert(!output_region->IsNoOp());  // Handled above.
//      induction->output_region.Clear();
//      output_region->parent = parent_induction;
//      if (auto parent_output_region = parent_induction->output_region.get();
//          parent_output_region) {
//        output_region->ExecuteBefore(prog, parent_output_region);
//      } else {
//        parent_induction->output_region.Emplace(parent_induction,
//                                                output_region);
//      }
//    }
//
//    // Fixup init region
//    auto init_region = induction->init_region.get();
//    induction->init_region.Clear();
//    init_region->parent = parent_induction;
//    parent_induction->init_region.Emplace(parent_induction, init_region);
//
//    // Fixup cyclic region
//    auto cyclic_region = induction->cyclic_region.get();
//    induction->cyclic_region.Clear();
//    cyclic_region->parent = parent_induction;
//    if (auto parent_cyclic_region = parent_induction->cyclic_region.get();
//        parent_cyclic_region) {
//      cyclic_region->ExecuteBefore(prog, parent_cyclic_region);
//    } else {
//      parent_induction->cyclic_region.Emplace(parent_induction, cyclic_region);
//    }
//
//    induction->parent = nullptr;
//
//    changed = true;
//
//  // Form like
//  // induction:
//  // init:
//  //   init-code-0
//  // fixpoint-loop:
//  //   induction:
//  //     init:
//  //       init-code-1
//  //     fixpoint-loop:
//  //       code-2
//  //   code-3
//  } else if (induction == parent_induction->cyclic_region.get()) {
//
//    // TODO(ekilmer)
//  }
//
//  return changed;
}

static bool OptimizeImpl(SERIES *series) {
  if (!series->IsUsed() || !series->parent) {
    return false;

  // This is a series region with only one child, so we can elevate the
  // child to replace the parent.
  } else if (series->regions.Size() == 1u) {
    const auto only_region = series->regions[0u];
    series->regions.Clear();
    series->ReplaceAllUsesWith(only_region);
    return true;

  // This series node's parent is also a series node.
  } else if (auto parent_series = series->parent->AsSeries();
             parent_series && !series->regions.Empty()) {
    UseList<REGION> new_siblings(parent_series);
    auto found = false;

    for (auto sibling_region : parent_series->regions) {
      assert(sibling_region->parent == parent_series);
      if (sibling_region == series) {
        for (auto child_region : series->regions) {
          assert(child_region->parent == series);
          new_siblings.AddUse(child_region);
          child_region->parent = parent_series;
          found = true;
        }
      } else {
        new_siblings.AddUse(sibling_region);
      }
    }

    assert(found);
    (void) found;

    series->regions.Clear();
    series->parent = nullptr;
    parent_series->regions.Swap(new_siblings);
    return true;

  // Erase any empty child regions.
  } else {

    auto has_unneeded = false;
    auto seen_return = false;
    auto seen_indirect_return = false;

    for (auto region : series->regions) {
      assert(region->parent == series);

      // There's a region following a `RETURN` in a series. This is unreachable.
      if (seen_return) {
        assert(region->AsOperation() && region->AsOperation()->AsReturn() &&
               "Unreachable code in SERIES region");
        has_unneeded = true;
        break;

      } else if (seen_indirect_return) {
        assert(false);
        has_unneeded = true;
        break;

      // This region is a No-op, it's not needed.
      } else if (region->IsNoOp()) {
        has_unneeded = true;
        break;

      } else if (region->EndsWithReturn()) {
        seen_indirect_return = true;
        if (auto op = region->AsOperation(); op && op->AsReturn()) {
          seen_return = true;
        }
      }
    }

    // Remove no-op regions, and unreachable regions.
    if (!has_unneeded) {
      return false;
    }

    UseList<REGION> new_regions(series);
    for (auto region : series->regions) {
      assert(region->parent == series);
      if (region->IsNoOp()) {
        region->parent = nullptr;

      } else if (region->EndsWithReturn()) {
        new_regions.AddUse(region);
        break;

      } else {
        new_regions.AddUse(region);
      }
    }

    series->regions.Swap(new_regions);
    return true;
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

  let->defined_vars.Clear();
  let->used_vars.Clear();

  if (const auto body = let->body.get(); body) {
    changed = true;
    let->body.Clear();
    let->ReplaceAllUsesWith(body);
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
        parent_cmp && cmp->cmp_op == ComparisonOperator::kEqual &&
        cmp->cmp_op == parent_cmp->cmp_op) {

      for (auto i = 0u; i < max_i; ++i) {
        parent_cmp->lhs_vars.AddUse(cmp->lhs_vars[i]);
        parent_cmp->rhs_vars.AddUse(cmp->rhs_vars[i]);
        changed = true;
      }

      cmp->lhs_vars.Clear();
      cmp->rhs_vars.Clear();
      max_i = 0u;
    }
  }

  // This compare has no variables being compared, so replace it with its
  // body.
  if (!max_i) {
    const auto body = cmp->body.get();
    cmp->body.Clear();
    if (body) {
      cmp->ReplaceAllUsesWith(body);
      changed = true;
    }

    return changed;
  }
  auto has_matching = false;
  for (auto i = 0u; i < max_i; ++i) {
    if (cmp->lhs_vars[i] == cmp->rhs_vars[i]) {
      has_matching = true;
      break;
    }
  }

  if (!has_matching) {
    return changed;
  }

  if (cmp->cmp_op == ComparisonOperator::kEqual) {
    UseList<VAR> new_lhs_vars(cmp);
    UseList<VAR> new_rhs_vars(cmp);
    for (auto i = 0u; i < max_i; ++i) {
      if (cmp->lhs_vars[i] != cmp->rhs_vars[i]) {
        new_lhs_vars.AddUse(cmp->lhs_vars[i]);
        new_rhs_vars.AddUse(cmp->rhs_vars[i]);
      }
    }

    // This comparison is trivially true, replace it with its body.
    if (new_lhs_vars.Empty()) {
      cmp->lhs_vars.Clear();
      cmp->rhs_vars.Clear();
      OptimizeImpl(cmp);
      return true;

    // This comparison had some redundant comparisons, swap in the less
    // redundant ones.
    } else {
      cmp->lhs_vars.Swap(new_lhs_vars);
      cmp->rhs_vars.Swap(new_rhs_vars);
    }

  // This tuple compare with never be satisfiable, and so everything inside
  // it is dead.
  } else {
    if (const auto body = cmp->body.get(); body) {
      body->parent = nullptr;
    }
    cmp->body.Clear();
    cmp->lhs_vars.Clear();
    cmp->rhs_vars.Clear();
  }

  return true;
}

// Try to eliminate unnecessary function calls. This is pretty common when
// generating bottom-up deleters.
static bool OptimizeImpl(ProgramImpl *impl, CALL *call) {
  auto changed = false;

  if (call->body) {
    assert(call->body->parent == call);

    if (call->body->IsNoOp()) {
      call->body->parent = nullptr;
      call->body.Clear();
      changed = true;
    }
  }

  if (call->false_body) {
    assert(call->false_body->parent == call);

    if (call->false_body->IsNoOp()) {
      call->false_body->parent = nullptr;
      call->false_body.Clear();
      changed = true;
    }
  }

  if (call->body && call->false_body) {
    assert(call->body.get() != call->false_body.get());
  }

  return changed;
}

// Try to eliminate unnecessary function calls. This is pretty common when
// generating bottom-up deleters.
static bool OptimizeImpl(ProgramImpl *impl, GENERATOR *gen) {

  auto changed = false;

  if (gen->body) {
    assert(gen->body->parent == gen);

    if (gen->body->IsNoOp()) {
      gen->body->parent = nullptr;
      gen->body.Clear();
      changed = true;
    }
  }
  if (gen->empty_body) {
    assert(gen->empty_body->parent == gen);

    if (gen->empty_body->IsNoOp()) {
      gen->empty_body->parent = nullptr;
      gen->empty_body.Clear();
      changed = true;
    }
  }

  if (gen->body && gen->empty_body) {
    assert(gen->body.get() != gen->empty_body.get());
  }

  return changed;
}


// Try to eliminate unnecessary children of a check state region.
static bool OptimizeImpl(ProgramImpl *impl, CHECKSTATE *check) {
  auto changed = false;

  if (check->body) {
    assert(check->body->parent == check);

    if (check->body->IsNoOp()) {
      check->body->parent = nullptr;
      check->body.Clear();
      changed = true;
    }
  }

  if (check->unknown_body) {
    assert(check->unknown_body->parent == check);

    if (check->unknown_body->IsNoOp()) {
      check->unknown_body->parent = nullptr;
      check->unknown_body.Clear();
      changed = true;
    }
  }

  if (check->absent_body) {
    assert(check->absent_body->parent == check);

    if (check->absent_body->IsNoOp()) {
      check->absent_body->parent = nullptr;
      check->absent_body.Clear();
      changed = true;
    }
  }

  // Dead code eliminate the check state.
  if (!check->body && !check->unknown_body && !check->absent_body) {
    auto let = impl->operation_regions.CreateDerived<LET>(check->parent);
    check->ReplaceAllUsesWith(let);
    check->parent = nullptr;
    changed = true;
  }

  return changed;
}

// Perform dead argument elimination.
static bool OptimizeImpl(PROC *proc) {
  return false;
}

}  // namespace

void ProgramImpl::Optimize(void) {
#ifndef NDEBUG
  for (auto par : parallel_regions) {
    assert(par->parent != nullptr);
    for (auto region : par->regions) {
      if (region->parent != par) {
        assert(false);
        region->parent = par;
      }
    }
  }

  for (auto series : series_regions) {
    assert(series->parent != nullptr);
    for (auto region : series->regions) {
      if (region->parent != series) {
        assert(false);
        region->parent = series;
      }
    }
  }

  for (auto op : operation_regions) {
    assert(op->parent != nullptr);
  }
#endif

  // A bunch of the optimizations check `region->IsNoOp()`, which looks down
  // to their children, or move children nodes into parent nodes. Thus, we want
  // to start deep and "bubble up" removal of no-ops and other things.
  auto depth_cmp =
      +[](REGION *a, REGION *b) { return a->CachedDepth() > b->CachedDepth(); };

  // Iteratively remove unused regions.
  auto remove_unused = [this] (void) {
    for (size_t changed = 1; changed;) {
      changed = 0;
      changed |= parallel_regions.RemoveUnused();
      changed |= series_regions.RemoveUnused();
      changed |= operation_regions.RemoveUnused();
      changed |= procedure_regions.RemoveIf([](PROC *proc) {
        if (proc->kind == ProcedureKind::kInitializer ||
            proc->kind == ProcedureKind::kMessageHandler) {
          return false;
        } else {
          return !proc->has_raw_use && !proc->IsUsed();
        }
      });
    }
  };

  for (auto changed = true; changed;) {
    changed = false;

    parallel_regions.Sort(depth_cmp);

    // NOTE(pag): Optimizing parallel regions may create more parallel regions
    //            via `MergeEquals`.
    for (auto i = 0ull; i < parallel_regions.Size(); ++i) {
      const auto par = parallel_regions[i];
      changed = OptimizeImpl(this, par) | changed;
    }

    induction_regions.Sort(depth_cmp);
    for (auto induction : induction_regions) {
      changed = OptimizeImpl(this, induction) | changed;
    }

    series_regions.Sort(depth_cmp);
    for (auto series : series_regions) {
      changed = OptimizeImpl(series) | changed;
    }

    operation_regions.Sort(depth_cmp);
    for (auto i = 0u; i < operation_regions.Size(); ++i) {
      const auto op = operation_regions[i];
      if (!op->IsUsed() || !op->parent) {
        continue;
      }

      // We try to aggressively eliminate LET bindings by down-propagating
      // variable assignments.
      if (auto let = op->AsLetBinding(); let) {
        changed = OptimizeImpl(let) | changed;

      } else if (auto tuple_cmp = op->AsTupleCompare(); tuple_cmp) {
        changed = OptimizeImpl(tuple_cmp) | changed;

      } else if (auto call = op->AsCall(); call) {
        changed = OptimizeImpl(this, call) | changed;

      } else if (auto gen = op->AsGenerate(); gen) {
        changed = OptimizeImpl(this, gen) | changed;

      } else if (auto check = op->AsCheckState(); check) {
        changed = OptimizeImpl(this, check) | changed;

      // All other operations check to see if they are no-ops and if so
      // remove the bodies.
      } else if (op->body) {
        assert(op->body->parent == op);
        if (op->body->IsNoOp()) {

          //          op->body->comment = "NOP? " + op->body->comment;

          op->body->parent = nullptr;
          op->body.Clear();
          changed = true;
        }
      }
    }

    for (auto proc : procedure_regions) {
      changed = OptimizeImpl(proc) | changed;
    }

    remove_unused();
  }

  // Go find possibly similar procedures.
  std::unordered_map<uint64_t, std::vector<PROC *>> similar_procs;
  for (auto proc : procedure_regions) {
    if (proc->kind == ProcedureKind::kInitializer ||
        proc->kind == ProcedureKind::kMessageHandler) {
      continue;

    } else if (proc->IsUsed() || proc->has_raw_use) {
      const auto hash = proc->Hash(UINT32_MAX);
      similar_procs[hash].emplace_back(proc);
    }
  }

  std::unordered_map<PROC *, PROC *> raw_use_change;

  // Go through an compare procedures for equality and replace any unused ones.
  EqualitySet eq;
  for (auto &hash_to_procs : similar_procs) {
    auto &procs = hash_to_procs.second;

    std::vector<bool> dead(procs.size());
    for (size_t i = 0u, max_i = procs.size(); i < max_i; ++i) {
      if (dead[i]) {
        continue;
      }
      PROC *&i_proc = procs[i];

      eq.Clear();
      raw_use_change.clear();
      for (auto j = i + 1u; j < max_i; ++j) {
        if (dead[j]) {
          continue;
        }

        PROC *&j_proc = procs[j];

        if (i_proc->Equals(eq, j_proc, UINT32_MAX)) {

          if (j_proc->has_raw_use) {
            raw_use_change.emplace(j_proc, i_proc);
            i_proc->has_raw_use = true;
            j_proc->has_raw_use = false;
          }

          j_proc->ReplaceAllUsesWith(i_proc);
          dead[j] = true;

        } else {
          eq.Clear();
        }
      }

      if (raw_use_change.empty()) {
        continue;
      }

      // Rewrite the raw uses, which should be isolated to being checkers
      // and forcing functions in queries.
      for (auto &query : queries) {
        if (query.tuple_checker) {
          auto &ref_to_proc = query.tuple_checker->impl;
          if (auto new_proc_it = raw_use_change.find(ref_to_proc);
              new_proc_it != raw_use_change.end()) {
            ref_to_proc = new_proc_it->second;
          }
        }

        if (query.forcing_function) {
          auto &ref_to_proc = query.forcing_function->impl;
          if (auto new_proc_it = raw_use_change.find(ref_to_proc);
              new_proc_it != raw_use_change.end()) {
            ref_to_proc = new_proc_it->second;
          }
        }
      }
    }
  }

  remove_unused();
}

}  // namespace hyde
