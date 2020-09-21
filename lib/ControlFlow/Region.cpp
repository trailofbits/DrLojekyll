// Copyright 2020, Trail of Bits. All rights reserved.

#include "Program.h"

namespace hyde {

Node<ProgramRegion>::~Node(void) {}

Node<ProgramRegion>::Node(Node<ProgramProcedure> *containing_procedure_)
    : Def<Node<ProgramRegion>>(this),
      User(this),
      containing_procedure(containing_procedure_),
      parent(containing_procedure_) {}

Node<ProgramRegion>::Node(Node<ProgramRegion> *parent_)
    : Def<Node<ProgramRegion>>(this),
      User(this),
      containing_procedure(parent_->containing_procedure),
      parent(parent_) {}

Node<ProgramProcedure> *Node<ProgramRegion>::AsProcedure(void) noexcept {
  return nullptr;
}

Node<ProgramOperationRegion> *Node<ProgramRegion>::AsOperation(void) noexcept {
  return nullptr;
}

Node<ProgramSeriesRegion> *Node<ProgramRegion>::AsSeries(void) noexcept {
  return nullptr;
}

Node<ProgramParallelRegion> *Node<ProgramRegion>::AsParallel(void) noexcept {
  return nullptr;
}

Node<ProgramInductionRegion> *Node<ProgramRegion>::AsInduction(void) noexcept {
  return nullptr;
}

// Returns the lexical level of this node.
unsigned Node<ProgramRegion>::Depth(void) const noexcept {
  if (parent == containing_procedure || parent == this) {
    return 0u;
  } else {
    return parent->Depth() + 1u;
  }
}

// Returns true if this region is a no-op.
bool Node<ProgramRegion>::IsNoOp(void) const noexcept {
  return false;
}

// Returns `true` if `this` and `that` are structurally equivalent (after
// variable renaming).
bool Node<ProgramRegion>::Equals(EqualitySet &,
                                 Node<ProgramRegion> *) const noexcept {
  return false;
}

// Return the nearest enclosing region that is itself enclosed by an
// induction.
Node<ProgramRegion> *
Node<ProgramRegion>::NearestRegionEnclosedByInduction(void) noexcept {
  auto ret_region = this;
  for (auto region = this; !region->AsProcedure() && !region->AsInduction();
       region = region->parent) {
    ret_region = region;
  }
  return ret_region;
}

// Find an ancestor node that's shared by both `this` and `that`.
Node<ProgramRegion> *
Node<ProgramRegion>::FindCommonAncestor(Node<ProgramRegion> *that) noexcept {
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
void Node<ProgramRegion>::ExecuteBefore(ProgramImpl *program,
                                        Node<ProgramRegion> *that) noexcept {

  if (auto series = that->AsSeries(); series) {
    UseList<REGION> new_regions(series);
    new_regions.AddUse(this);
    for (auto later_region : series->regions) {
      new_regions.AddUse(later_region);
    }
    series->regions.Swap(new_regions);
    this->parent = series;

  } else if (auto proc = that->AsProcedure(); proc) {
    if (auto proc_body = proc->body.get(); proc_body) {
      this->ExecuteBefore(program, proc_body);

    } else {
      UseRef<REGION>(proc, this).Swap(proc->body);
      this->parent = proc;
    }

  } else {
    auto series = program->series_regions.Create(that->parent);
    that->ReplaceAllUsesWith(series);

    series->regions.AddUse(this);
    series->regions.AddUse(that);

    that->parent = series;
    this->parent = series;
  }
}

// Make sure that `this` will execute after `that`.
void Node<ProgramRegion>::ExecuteAfter(ProgramImpl *program,
                                       Node<ProgramRegion> *that) noexcept {
  if (auto series = that->AsSeries(); series) {
    series->regions.AddUse(this);
    this->parent = series;

  } else if (auto proc = that->AsProcedure(); proc) {
    if (auto proc_body = proc->body.get(); proc_body) {
      this->ExecuteAfter(program, proc_body);

    } else {
      UseRef<REGION>(proc, this).Swap(proc->body);
      this->parent = proc;
    }

  } else {
    auto series = program->series_regions.Create(that->parent);
    that->ReplaceAllUsesWith(series);
    series->regions.AddUse(that);
    series->regions.AddUse(this);
    that->parent = series;
    this->parent = series;
  }
}

// Make sure that `this` will execute alongside `that`.
void Node<ProgramRegion>::ExecuteAlongside(ProgramImpl *program,
                                           Node<ProgramRegion> *that) noexcept {
  if (auto par = that->AsParallel(); par) {
    par->regions.AddUse(this);
    this->parent = par;

  } else if (auto proc = that->AsProcedure()) {
    if (auto proc_body = proc->body.get(); proc_body) {
      this->ExecuteAlongside(program, proc_body);
    } else {
      UseRef<REGION>(proc, this).Swap(proc->body);
      this->parent = proc;
    }

  } else {
    auto par = program->parallel_regions.Create(that->parent);
    that->ReplaceAllUsesWith(par);
    par->regions.AddUse(that);
    par->regions.AddUse(this);
    that->parent = par;
    this->parent = par;
  }
}

// Return a lexically available use of a variable.
VAR *Node<ProgramRegion>::VariableFor(ProgramImpl *impl, QueryColumn col) {
  if (col.IsConstantOrConstantRef()) {
    return impl->const_to_var[QueryConstant::From(col)];
  } else {
    return VariableForRec(col);
  }
}

// Return a lexically available use of a variable.
VAR *Node<ProgramRegion>::VariableForRec(QueryColumn col) {
  auto &var = col_id_to_var[col.Id()];
  if (!var && this != parent) {
    var = parent->VariableForRec(col);
    assert(var != nullptr);
  }
  return var;
}

}  // namespace hyde