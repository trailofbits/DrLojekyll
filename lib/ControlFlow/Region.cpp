// Copyright 2020, Trail of Bits. All rights reserved.

#include "Program.h"

namespace hyde {

ProgramRegionImpl::~ProgramRegionImpl(void) {}

ProgramRegionImpl::ProgramRegionImpl(
    ProgramProcedureImpl *containing_procedure_, bool)
    : Def<ProgramRegionImpl>(this),
      User(this),
      containing_procedure(containing_procedure_),
      parent(containing_procedure_) {
  if (parent && parent != parent->parent) {
    col_id_to_var = parent->col_id_to_var;
  }
}

ProgramRegionImpl::ProgramRegionImpl(ProgramRegionImpl *parent_)
    : Def<ProgramRegionImpl>(this),
      User(this),
      containing_procedure(parent_->containing_procedure),
      parent(parent_) {
  assert(containing_procedure == parent->parent->containing_procedure);
}

// Find the nearest containing mode switch.
ProgramModeSwitchRegionImpl *
ProgramRegionImpl::ContainingModeSwitch(void) noexcept {
  for (auto region = this->parent; region && region != region->parent;
       region = region->parent) {
    if (auto op = region->AsOperation()) {
      if (auto ret = op->AsModeSwitch()) {
        return ret;
      }
    }
  }
  return nullptr;
}

ProgramProcedureImpl *ProgramRegionImpl::AsProcedure(void) noexcept {
  return nullptr;
}

ProgramOperationRegionImpl *ProgramRegionImpl::AsOperation(void) noexcept {
  return nullptr;
}

ProgramSeriesRegionImpl *ProgramRegionImpl::AsSeries(void) noexcept {
  return nullptr;
}

ProgramParallelRegionImpl *ProgramRegionImpl::AsParallel(void) noexcept {
  return nullptr;
}

ProgramInductionRegionImpl *ProgramRegionImpl::AsInduction(void) noexcept {
  return nullptr;
}

// Returns the lexical level of this node.
unsigned ProgramRegionImpl::Depth(void) const noexcept {
  if (parent == containing_procedure || parent == this || !parent) {
    return 0u;
  } else {
    return parent->Depth() + 1u;
  }
}

// Returns the lexical level of this node.
unsigned ProgramRegionImpl::CachedDepth(void) noexcept {
  if (cached_depth) {
    return cached_depth;

  } else if (parent == containing_procedure || parent == this || !parent) {
    return 0u;

  } else {
    cached_depth = parent->CachedDepth() + 1u;
    return cached_depth;
  }
}

// Returns true if this region is a no-op.
bool ProgramRegionImpl::IsNoOp(void) const noexcept {
  return false;
}

// Returns `true` if `this` and `that` are structurally equivalent (after
// variable renaming) after searching down `depth` levels or until leaf,
// whichever is first, and where `depth` is 0, compare `this` to `that.
bool ProgramRegionImpl::Equals(EqualitySet &, ProgramRegionImpl *,
                                 uint32_t) const noexcept {
  return false;
}

const bool
ProgramRegionImpl::MergeEqual(ProgramImpl *prog,
                                std::vector<ProgramRegionImpl *> &merges) {
  return false;
}

// Return the farthest ancestor of this region, in terms of linkage. Often this
// just returns a `PROC *` if this region is linked in to its procedure.
ProgramRegionImpl *ProgramRegionImpl::Ancestor(void) noexcept {
  auto ret_region = this;
  for (auto region = this; region; region = region->parent) {
    ret_region = region;
    if (region == region->parent) {
      break;
    }
  }
  return ret_region;
}

// Return the nearest enclosing region that is itself enclosed by an
// induction.
ProgramRegionImpl *
ProgramRegionImpl::NearestRegionEnclosedByInduction(void) noexcept {
  auto ret_region = this;
  for (auto region = this; !region->AsProcedure() && !region->AsInduction();
       region = region->parent) {
    ret_region = region;
  }
  return ret_region;
}

// Find an ancestor node that's shared by both `this` and `that`.
ProgramRegionImpl *
ProgramRegionImpl::FindCommonAncestor(ProgramRegionImpl *that) noexcept {
  auto self = this;
  auto self_depth = self->Depth();
  auto that_depth = that->Depth();
  for (; self_depth || that_depth;) {
    if (self_depth > that_depth) {
      self = self->parent;
      self_depth -= 1u;

    } else if (self_depth < that_depth) {
      that = that->parent;
      that_depth -= 1u;

    } else if (self != that) {
      self = self->parent;
      that = that->parent;
      self_depth -= 1u;
      that_depth -= 1u;

    } else {
      return self;
    }
  }
  return containing_procedure;
}

// Make sure that `this` will execute before `that`.
void ProgramRegionImpl::ExecuteBefore(ProgramImpl *program,
                                        ProgramRegionImpl *that) noexcept {

  if (auto series = that->AsSeries(); series) {
    UseList<REGION> new_regions(series);
    new_regions.AddUse(this);
    for (auto later_region : series->regions) {
      assert(later_region->parent == series);
      new_regions.AddUse(later_region);
    }
    series->regions.Swap(new_regions);
    this->parent = series;

  } else if (auto proc = that->AsProcedure(); proc) {
    if (auto proc_body = proc->body.get(); proc_body) {
      this->ExecuteBefore(program, proc_body);

    } else {
      proc->body.Emplace(proc, this);
      this->parent = proc;
    }

  } else {
    auto series = program->series_regions.Create(that->parent);
    that->ReplaceAllUsesWith(series);

    that->parent = series;
    this->parent = series;

    series->AddRegion(this);
    series->AddRegion(that);
  }
}

// Make sure that `this` will execute after `that`.
void ProgramRegionImpl::ExecuteAfter(ProgramImpl *program,
                                       ProgramRegionImpl *that) noexcept {
  if (auto series = that->AsSeries(); series) {
    this->parent = series;
    series->AddRegion(this);

  } else if (auto proc = that->AsProcedure(); proc) {
    if (auto proc_body = proc->body.get(); proc_body) {
      this->ExecuteAfter(program, proc_body);

    } else {
      proc->body.Emplace(proc, this);
      this->parent = proc;
    }

  } else {
    auto series = program->series_regions.Create(that->parent);
    that->ReplaceAllUsesWith(series);
    that->parent = series;
    this->parent = series;
    series->AddRegion(that);
    series->AddRegion(this);
  }
}

// Make sure that `this` will execute alongside `that`.
void ProgramRegionImpl::ExecuteAlongside(ProgramImpl *program,
                                           ProgramRegionImpl *that) noexcept {
  if (auto par = that->AsParallel(); par) {
    this->parent = par;
    par->AddRegion(this);

  } else if (auto proc = that->AsProcedure()) {
    if (auto proc_body = proc->body.get(); proc_body) {
      this->ExecuteAlongside(program, proc_body);
    } else {
      proc->body.Emplace(proc, this);
      this->parent = proc;
    }

  } else {
    auto par = program->parallel_regions.Create(that->parent);
    that->ReplaceAllUsesWith(par);
    that->parent = par;
    this->parent = par;
    par->AddRegion(that);
    par->AddRegion(this);
  }
}

// Return a lexically available use of a variable.
DataVariableImpl *ProgramRegionImpl::VariableFor(
    ProgramImpl *impl, QueryColumn col) {
  auto &var = col_id_to_var[col.Id()];
  if (!var) {

    // NOTE(pag): This is a bit subtle. Sometimes we'll want to force our own
    //            variable value in for `col_id_to_var`, regardless of if
    //            `col.IsConstantOrConstantRef()` is true. Thus, any value
    //            already present is given priority. So, we do a lexical lookup
    //            first.
    if (this != parent && parent) {
      var = parent->VariableForRec(col);
    }

    // If the lexical lookup failed, and if the column is a constant, then
    // use our constant.
    if (!var && col.IsConstantOrConstantRef()) {
      var = impl->const_to_var[QueryConstant::From(col)];
    }
  }

  assert(var != nullptr);
  assert(var->Type() == col.Type());
  return var;
}

// Return a lexically available use of a variable.
VAR *ProgramRegionImpl::VariableForRec(QueryColumn col) {
  auto it = col_id_to_var.find(col.Id());
  if (it != col_id_to_var.end()) {
    assert(it->second != nullptr);
    return it->second;
  }

  if (!parent || this == parent) {
    return nullptr;
  }

  if (auto var = parent->VariableForRec(col); var) {
    col_id_to_var.emplace(col.Id(), var);
    return var;
  }

  return nullptr;
}

}  // namespace hyde
