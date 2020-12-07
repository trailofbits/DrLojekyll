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

// Optimize induction regions to reduce unnecessary complexity.
// * Clear out empty output regions of inductions.
// * Optimize nested loop inductions.
static bool OptimizeImpl(INDUCTION *induction) {
  auto changed = false;

  // Clear out empty output regions of inductions.
  if (induction->output_region && induction->output_region->IsNoOp()) {
    induction->output_region->parent = nullptr;
    induction->output_region.Clear();
    changed = true;
  }

  auto parent_region = induction->parent;
  if (!parent_region) {
    return changed;
  }

  // Optimize nested inductions.
  auto parent_induction = induction->parent->AsInduction();
  if (parent_induction) {

    // Form like
    // induction
    //   init
    //    induction
    if (induction == parent_induction->init_region.get()) {
      for (auto def : induction->vectors) {
        changed = true;
        parent_induction->vectors.AddUse(def);
      }
      induction->vectors.Clear();

      if (induction->output_region && !induction->output_region->IsNoOp()) {
        auto out_par = parent_induction->output_region->AsSeries();
        assert(out_par);
        for (auto region : induction->output_region->AsSeries()->regions) {
          region->parent = out_par;
          out_par->regions.AddUse(region);
        }
        induction->output_region->parent = nullptr;
        induction->output_region.Clear();
      }

      auto init_region = induction->init_region.get();
      init_region->parent = parent_induction;
      parent_induction->init_region.Emplace(parent_induction, init_region);
      induction->init_region->parent = nullptr;
      induction->init_region.Clear();

      auto cycle_par = parent_induction->cyclic_region->AsSeries();
      assert(cycle_par);
      for (auto region : induction->cyclic_region->AsSeries()->regions) {
        region->parent = cycle_par;
        cycle_par->regions.AddUse(region);
      }
      induction->cyclic_region->parent = nullptr;
      induction->cyclic_region.Clear();

      induction->parent = nullptr;

      changed = true;

    // Form like
    // induction:
    // init:
    //   init-code-0
    // fixpoint-loop:
    //   induction:
    //     init:
    //       init-code-1
    //     fixpoint-loop:
    //       code-2
    //   code-3
    } else if (induction == parent_induction->cyclic_region.get()) {

      // TODO(ekilmer)
    }
  }

  return changed;
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
  let->body.Clear();

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

  // If there is a conditional body then don't optimize.
  if (exists->body) {
    return changed;
  }

  // Find a parent existence check, and if it does the same type of check,
  // then try to merge this existence check into the parent by moving its
  // condition variables up, and replacing all uses of it with its conditional
  // body.
  if (auto parent_op = exists->parent->AsOperation(); parent_op) {
    if (auto parent_exists = parent_op->AsExistenceCheck();
        parent_exists && exists->op == parent_exists->op) {

      for (auto cond : exists->cond_vars) {
        changed = true;
        parent_exists->cond_vars.AddUse(cond);
      }
      exists->cond_vars.Clear();

      const auto body = exists->body.get();
      exists->body.Clear();

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

  // This compare has no variables being compared, so replace it with its
  // body.
  if (!max_i) {
    const auto body = cmp->body.get();
    cmp->body.Clear();
    if (body) {
      cmp->ReplaceAllUsesWith(body);
      changed = true;
    }
  }

  return changed;
}

// Process a function as if it contains just simple function calls and a return.
// We permit series and parallel regions inside. This roughly corresponds to
// the trivial case of bottom-up procedures that "prove to remove."
static std::pair<bool, RETURN *>
FindReturnAfterSimpleCalls(const UseList<Node<ProgramRegion>> &regions) {

  RETURN *target_return = nullptr;

  for (auto target_region : regions) {
    if (auto target_par = target_region->AsParallel(); target_par) {
      const auto [good, ret] = FindReturnAfterSimpleCalls(target_par->regions);
      if (!good) {
        return {false, nullptr};
      }
      target_return = ret;

    } else if (auto target_series = target_region->AsSeries(); target_series) {
      const auto [good, ret] =
          FindReturnAfterSimpleCalls(target_series->regions);
      if (!good) {
        return {false, nullptr};
      }
      target_return = ret;

    } else if (auto target_op = target_region->AsOperation(); target_op) {

      if (auto target_call = target_op->AsCall(); target_call) {
        if (!target_call->arg_vecs.Empty()) {
          return {false, nullptr};
        } else if (target_call->op != ProgramOperation::kCallProcedure) {
          return {false, nullptr};
        } else {
          assert(!target_call->body);
        }

      } else if (auto found_return = target_op->AsReturn(); found_return) {
        assert(!target_return);
        target_return = found_return;

      // Found something that isn't a call or return.
      } else {
        return {false, nullptr};
      }

    // Found something that isn't an operation, and thus cannot be a
    // call or return.
    } else {
      return {false, nullptr};
    }
  }

  return {true, target_return};
}

static void InlineCalls(const UseList<Node<ProgramRegion>> &from_regions,
                        ProgramImpl *impl, REGION *into_parent,
                        UseList<Node<ProgramRegion>> &into_parent_regions,
                        std::unordered_map<VAR *, VAR *> &target_to_local) {

  for (auto target_region : from_regions) {
    if (auto target_par = target_region->AsParallel(); target_par) {
      const auto copied_par = impl->parallel_regions.Create(into_parent);
      into_parent_regions.AddUse(copied_par);
      InlineCalls(target_par->regions, impl, copied_par, copied_par->regions,
                  target_to_local);

    } else if (auto target_series = target_region->AsSeries(); target_series) {
      const auto copied_series = impl->series_regions.Create(into_parent);
      into_parent_regions.AddUse(copied_series);
      InlineCalls(target_series->regions, impl, copied_series,
                  copied_series->regions, target_to_local);

    } else if (auto target_op = target_region->AsOperation(); target_op) {

      if (auto target_call = target_op->AsCall(); target_call) {
        auto copied_call = impl->operation_regions.CreateDerived<CALL>(
            impl->next_id++, into_parent, target_call->called_proc.get(),
            target_call->op);
        into_parent_regions.AddUse(copied_call);

        for (auto target_var : target_call->arg_vars) {

          // Local variable.
          if (auto local_var = target_to_local[target_var]; local_var) {
            copied_call->arg_vars.AddUse(local_var);

          // Global variable.
          } else {
            copied_call->arg_vars.AddUse(target_var);
          }
        }

      } else if (target_op->AsReturn()) {
        return;

      // Found something that isn't a call or return.
      } else {
        assert(false);
      }

    // Found something that isn't an operation, and thus cannot be a
    // call or return.
    } else {
      assert(false);
    }
  }
}

// Try to eliminate unnecessary function calls. This is pretty common when
// generating bottom-up deleters.
static bool OptimizeImpl(ProgramImpl *impl, CALL *call) {

  const auto target_func = call->called_proc.get();
  if (!target_func) {
    return false;  // Dead.
  }

  const auto target_body = target_func->body.get();
  assert(target_body);
  assert(call->arg_vars.Size() == target_func->input_vars.Size());
  assert(call->arg_vecs.Size() == target_func->input_vecs.Size());

  const auto call_body = call->body.get();
  if (call->op != ProgramOperation::kCallProcedure) {
    if (!call_body) {
      assert(false);
      call->op = ProgramOperation::kCallProcedure;

    // E.g. an empty `LET`, `SERIES`, or `PARALLEL`.
    } else if (call_body->IsNoOp()) {
      call_body->parent = nullptr;
      call->body.Clear();
      call->op = ProgramOperation::kCallProcedure;
    }
  }

  if (auto target_op = target_body->AsOperation(); target_op) {

    // If the target function is trivial, i.e. just returns `true` or `false`
    // then we can probably eliminate it.
    if (auto target_ret = target_op->AsReturn(); target_ret) {
      auto can_remove = false;
      auto is_conditional = false;

      switch (call->op) {
        case ProgramOperation::kCallProcedure: can_remove = true; break;
        case ProgramOperation::kCallProcedureCheckFalse:
          can_remove =
              target_ret->op != ProgramOperation::kReturnFalseFromProcedure;
          is_conditional = true;
          break;
        case ProgramOperation::kCallProcedureCheckTrue:
          can_remove =
              target_ret->op != ProgramOperation::kReturnTrueFromProcedure;
          is_conditional = true;
          break;
        default: break;
      }

      // The call is useless, or the condition tested by the call is never true,
      // so remove it.
      if (can_remove) {
        if (call_body) {
          call_body->parent = nullptr;
        }

        auto empty = impl->parallel_regions.Create(call->parent);
        call->ReplaceAllUsesWith(empty);
        call->called_proc.Clear();
        call->body.Clear();
        call->arg_vars.Clear();
        call->arg_vecs.Clear();
        return true;

      // The condition tested by the call is trivially true, replace the call with
      // the body that previously executed conditionally.
      } else if (is_conditional) {
        call->called_proc.Clear();
        call->body.Clear();
        call->ReplaceAllUsesWith(call_body);
        call->arg_vars.Clear();
        call->arg_vecs.Clear();
        return true;
      }
    }
    return false;

  // Look to see if the target function is a call to one or more other functions
  // and if so, inline them.
  } else if (auto target_series = target_body->AsSeries(); target_series) {

    // Don't inline functions with vector arguments.
    if (!call->arg_vecs.Empty()) {
      return false;
    }

    assert(target_func->input_vecs.Empty());

    const auto [good, target_return] =
        FindReturnAfterSimpleCalls(target_series->regions);

    if (!good || !target_return) {
      return false;
    }

    // Create a variable renaming of variables in the target function to
    // variables in the current function.
    std::unordered_map<VAR *, VAR *> target_to_local;
    for (auto i = 0u, max_i = call->arg_vars.Size(); i < max_i; ++i) {
      target_to_local.emplace(target_func->input_vars[i], call->arg_vars[i]);
    }

    // Inline the function calls into `series`.
    const auto series = impl->series_regions.Create(call->parent);
    InlineCalls(target_series->regions, impl, series, series->regions,
                target_to_local);

    // Replace the call with the inlined body.
    call->ReplaceAllUsesWith(series);
    call->arg_vars.Clear();
    call->arg_vecs.Clear();
    call->called_proc.Clear();
    call->body.Clear();

    // Inspect the return statement from the function that we just inlined,
    // and try to see if we should keep or omit the conditional body of the
    // call.
    switch (call->op) {
      case ProgramOperation::kCallProcedure: break;
      case ProgramOperation::kCallProcedureCheckTrue:
        assert(call_body != nullptr);
        if (target_return->op == ProgramOperation::kReturnTrueFromProcedure) {
          call_body->parent = series;
          series->regions.AddUse(call_body);
        } else {
          call_body->parent = nullptr;
        }
        break;
      case ProgramOperation::kCallProcedureCheckFalse:
        if (target_return->op == ProgramOperation::kReturnTrueFromProcedure) {
          call_body->parent = nullptr;
        } else {
          call_body->parent = series;
          series->regions.AddUse(call_body);
        }
        break;
      default: assert(false); break;
    }

    return true;
  }

  return false;
}

// Perform dead argument elimination.
static bool OptimizeImpl(PROC *proc) {
  return false;
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

    for (auto i = 0u; i < operation_regions.Size(); ++i) {
      const auto op = operation_regions[i];
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

      } else if (auto call = op->AsCall(); call) {
        changed = OptimizeImpl(this, call) | changed;

      // All other operations check to see if they are no-ops and if so
      // remove the bodies.
      } else if (op->body && op->body->IsNoOp()) {
        op->body->parent = nullptr;
        op->body.Clear();
        changed = true;
      }
    }

    for (auto proc : procedure_regions) {
      changed = OptimizeImpl(proc) | changed;
    }
  }

  // Go find possibly similar procedures.
  std::unordered_map<uint64_t, std::vector<PROC *>> similar_procs;
  for (auto proc : procedure_regions) {
    if (proc->kind == ProcedureKind::kInitializer ||
        proc->kind == ProcedureKind::kMessageHandler || proc->is_alias) {
      continue;

    } else if (proc->IsUsed() || proc->has_raw_use) {
      const auto hash = proc->Hash();
      similar_procs[hash].emplace_back(proc);
    }
  }

  // Go through an compare procedures for equality and replace any unused ones.
  for (auto &[hash, procs] : similar_procs) {
    (void) hash;
    std::vector<bool> dead(procs.size());
    for (size_t i = 0u, max_i = procs.size(); i < max_i; ++i) {
      if (dead[i]) {
        continue;
      }
      PROC *&i_proc = procs[i];

      for (auto j = i + 1u; j < max_i; ++j) {
        if (dead[j]) {
          continue;
        }

        PROC *&j_proc = procs[j];

        EqualitySet eq;
        if (i_proc->Equals(eq, j_proc)) {

          // If both need to be used, then make one call the other.
          if (i_proc->has_raw_use && j_proc->has_raw_use) {
            j_proc->is_alias = true;
            j_proc->ReplaceAllUsesWith(i_proc);
            j_proc->body->parent = nullptr;
            j_proc->body.Clear();
            auto seq = series_regions.Create(j_proc);
            auto call_i = operation_regions.CreateDerived<CALL>(
                next_id++, seq, i_proc,
                ProgramOperation::kCallProcedureCheckTrue);

            for (auto arg_var : j_proc->input_vars) {
              call_i->arg_vars.AddUse(arg_var);
            }
            for (auto arg_vec : j_proc->input_vecs) {
              call_i->arg_vecs.AddUse(arg_vec);
            }

            auto ret_true = operation_regions.CreateDerived<RETURN>(
                call_i, ProgramOperation::kReturnTrueFromProcedure);
            auto ret_false = operation_regions.CreateDerived<RETURN>(
                seq, ProgramOperation::kReturnFalseFromProcedure);

            j_proc->body.Emplace(j_proc, seq);
            seq->regions.AddUse(call_i);
            seq->regions.AddUse(ret_false);
            call_i->body.Emplace(call_i, ret_true);

          // The first one needs to be preserved.
          } else if (i_proc->has_raw_use) {
            j_proc->ReplaceAllUsesWith(i_proc);

          // The second needs to be preserved.
          } else if (j_proc->has_raw_use) {
            i_proc->ReplaceAllUsesWith(j_proc);
            i_proc = j_proc;

          // Neither needs to be preserved.
          } else {
            j_proc->ReplaceAllUsesWith(i_proc);
          }
          dead[j] = true;
        }
      }
    }
  }

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
}

}  // namespace hyde
