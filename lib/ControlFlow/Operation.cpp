// Copyright 2020, Trail of Bits. All rights reserved.

#include "Program.h"

namespace hyde {

Node<ProgramOperationRegion>::~Node(void) {}
Node<ProgramCallRegion>::~Node(void) {}
Node<ProgramReturnRegion>::~Node(void) {}
Node<ProgramExistenceAssertionRegion>::~Node(void) {}
Node<ProgramExistenceCheckRegion>::~Node(void) {}
Node<ProgramGenerateRegion>::~Node(void) {}
Node<ProgramLetBindingRegion>::~Node(void) {}
Node<ProgramPublishRegion>::~Node(void) {}
Node<ProgramTransitionStateRegion>::~Node(void) {}
Node<ProgramCheckStateRegion>::~Node(void) {}
Node<ProgramTableJoinRegion>::~Node(void) {}
Node<ProgramTableProductRegion>::~Node(void) {}
Node<ProgramTableScanRegion>::~Node(void) {}
Node<ProgramTupleCompareRegion>::~Node(void) {}
Node<ProgramVectorLoopRegion>::~Node(void) {}
Node<ProgramVectorAppendRegion>::~Node(void) {}
Node<ProgramVectorClearRegion>::~Node(void) {}
Node<ProgramVectorUniqueRegion>::~Node(void) {}

Node<ProgramOperationRegion>::Node(REGION *parent_, ProgramOperation op_)
    : Node<ProgramRegion>(parent_),
      op(op_) {}

Node<ProgramOperationRegion> *
Node<ProgramOperationRegion>::AsOperation(void) noexcept {
  return this;
}

Node<ProgramCallRegion>::Node(Node<ProgramRegion> *parent_,
                              Node<ProgramProcedure> *called_proc_,
                              ProgramOperation op_)
    : Node<ProgramOperationRegion>(parent_, op_),
      called_proc(this, called_proc_),
      arg_vars(this),
      arg_vecs(this) {}

Node<ProgramCallRegion> *Node<ProgramOperationRegion>::AsCall(void) noexcept {
  return nullptr;
}

Node<ProgramReturnRegion> *Node<ProgramOperationRegion>::AsReturn(void) noexcept {
  return nullptr;
}

Node<ProgramPublishRegion> *
Node<ProgramOperationRegion>::AsPublish(void) noexcept {
  return nullptr;
}

Node<ProgramVectorLoopRegion> *
Node<ProgramOperationRegion>::AsVectorLoop(void) noexcept {
  return nullptr;
}

Node<ProgramVectorAppendRegion> *
Node<ProgramOperationRegion>::AsVectorAppend(void) noexcept {
  return nullptr;
}

Node<ProgramVectorClearRegion> *
Node<ProgramOperationRegion>::AsVectorClear(void) noexcept {
  return nullptr;
}

Node<ProgramVectorUniqueRegion> *
Node<ProgramOperationRegion>::AsVectorUnique(void) noexcept {
  return nullptr;
}

Node<ProgramLetBindingRegion> *
Node<ProgramOperationRegion>::AsLetBinding(void) noexcept {
  return nullptr;
}

Node<ProgramTransitionStateRegion> *
Node<ProgramOperationRegion>::AsTransitionState(void) noexcept {
  return nullptr;
}

Node<ProgramCheckStateRegion> *
Node<ProgramOperationRegion>::AsCheckState(void) noexcept {
  return nullptr;
}

Node<ProgramTableJoinRegion> *
Node<ProgramOperationRegion>::AsTableJoin(void) noexcept {
  return nullptr;
}

Node<ProgramTableProductRegion> *
Node<ProgramOperationRegion>::AsTableProduct(void) noexcept {
  return nullptr;
}

Node<ProgramTableScanRegion> *
Node<ProgramOperationRegion>::AsTableScan(void) noexcept {
  return nullptr;
}

Node<ProgramExistenceCheckRegion> *
Node<ProgramOperationRegion>::AsExistenceCheck(void) noexcept {
  return nullptr;
}

Node<ProgramExistenceAssertionRegion> *
Node<ProgramOperationRegion>::AsExistenceAssertion(void) noexcept {
  return nullptr;
}

Node<ProgramGenerateRegion> *
Node<ProgramOperationRegion>::AsGenerate(void) noexcept {
  return nullptr;
}

Node<ProgramTupleCompareRegion> *
Node<ProgramOperationRegion>::AsTupleCompare(void) noexcept {
  return nullptr;
}

bool Node<ProgramVectorLoopRegion>::Equals(
    EqualitySet &eq, Node<ProgramRegion> *that_) const noexcept {
  const auto that_op = that_->AsOperation();
  if (!that_op) {
    return false;
  }

  const auto that = that_op->AsVectorLoop();
  if (!that || !eq.Contains(vector.get(), that->vector.get()) ||
      (!body.get()) != (!that->body.get())) {
    return false;
  }

  if (auto that_body = that->OP::body.get(); that_body) {

    for (auto i = 0u, max_i = defined_vars.Size(); i < max_i; ++i) {
      eq.Insert(defined_vars[i], that->defined_vars[i]);
    }

    return this->OP::body->Equals(eq, that_body);

  } else {
    return true;
  }
}

Node<ProgramVectorLoopRegion> *
Node<ProgramVectorLoopRegion>::AsVectorLoop(void) noexcept {
  return this;
}

bool Node<ProgramVectorLoopRegion>::IsNoOp(void) const noexcept {
  return !this->OP::body || this->OP::body->IsNoOp();
}

Node<ProgramLetBindingRegion> *
Node<ProgramLetBindingRegion>::AsLetBinding(void) noexcept {
  return this;
}

bool Node<ProgramLetBindingRegion>::IsNoOp(void) const noexcept {
  return !this->OP::body || this->OP::body->IsNoOp();
}

bool Node<ProgramLetBindingRegion>::Equals(
    EqualitySet &eq, Node<ProgramRegion> *that_) const noexcept {
  const auto that_op = that_->AsOperation();
  if (!that_op) {
    return false;
  }

  const auto that = that_op->AsLetBinding();
  const auto num_vars = defined_vars.Size();
  if (!that || num_vars != that->defined_vars.Size() ||
      (!body.get()) != (!that->body.get())) {
    return false;
  }

  for (auto i = 0u; i < num_vars; ++i) {
    if (!eq.Contains(used_vars[i], that->used_vars[i])) {
      return false;
    }
  }

  if (auto that_body = that->OP::body.get(); that_body) {
    for (auto i = 0u; i < num_vars; ++i) {
      eq.Insert(defined_vars[i], that->defined_vars[i]);
    }

    return this->OP::body->Equals(eq, that_body);

  } else {
    return true;
  }
}

bool Node<ProgramVectorAppendRegion>::Equals(
    EqualitySet &eq, Node<ProgramRegion> *that_) const noexcept {
  const auto that_op = that_->AsOperation();
  if (!that_op) {
    return false;
  }

  const auto that = that_op->AsVectorAppend();
  if (!that || !eq.Contains(vector.get(), that->vector.get())) {
    return false;
  }

  for (auto i = 0u, max_i = tuple_vars.Size(); i < max_i; ++i) {
    if (!eq.Contains(tuple_vars[i], that->tuple_vars[i])) {
      return false;
    }
  }

  return true;
}

Node<ProgramVectorAppendRegion> *
Node<ProgramVectorAppendRegion>::AsVectorAppend(void) noexcept {
  return this;
}

Node<ProgramTransitionStateRegion> *
Node<ProgramTransitionStateRegion>::AsTransitionState(void) noexcept {
  return this;
}

bool Node<ProgramTransitionStateRegion>::Equals(
    EqualitySet &eq, Node<ProgramRegion> *that_) const noexcept {
  const auto that_op = that_->AsOperation();
  if (!that_op) {
    return false;
  }

  const auto that = that_op->AsTransitionState();
  if (!that || table.get() != that->table.get()) {
    return false;
  }

  for (auto i = 0u, max_i = col_values.Size(); i < max_i; ++i) {
    if (!eq.Contains(col_values[i], that->col_values[i])) {
      return false;
    }
  }

  return true;
}

bool Node<ProgramExistenceCheckRegion>::IsNoOp(void) const noexcept {
  return !this->OP::body || this->OP::body->IsNoOp();
}

bool Node<ProgramExistenceCheckRegion>::Equals(
    EqualitySet &eq, Node<ProgramRegion> *that_) const noexcept {
  const auto that_op = that_->AsOperation();
  if (!that_op || this->OP::op != that_op->OP::op) {
    return false;
  }

  const auto num_conds = cond_vars.Size();
  const auto that = that_op->AsExistenceCheck();
  if (!that || num_conds != that->cond_vars.Size() ||
      (!this->OP::body.get()) != (!that->OP::body.get())) {
    return false;
  }

  // NOTE(pag): Condition variables are global, so we do equality checks.
  for (auto i = 0u; i < num_conds; ++i) {
    if (cond_vars[i] != that->cond_vars[i]) {
      return false;
    }
  }

  if (auto that_body = that->OP::body.get(); that_body) {
    return this->OP::body->Equals(eq, that_body);

  } else {
    return true;
  }
}

Node<ProgramExistenceCheckRegion> *
Node<ProgramExistenceCheckRegion>::AsExistenceCheck(void) noexcept {
  return this;
}

bool Node<ProgramExistenceAssertionRegion>::IsNoOp(void) const noexcept {
  return cond_vars.Empty();
}

bool Node<ProgramExistenceAssertionRegion>::Equals(
    EqualitySet &eq, Node<ProgramRegion> *that_) const noexcept {
  const auto that_op = that_->AsOperation();
  if (!that_op || this->OP::op != that_op->OP::op) {
    return false;
  }

  const auto num_conds = cond_vars.Size();
  const auto that = that_op->AsExistenceAssertion();
  if (!that || num_conds != that->cond_vars.Size()) {
    return false;
  }

  // NOTE(pag): Condition variables are global, so we do equality checks.
  for (auto i = 0u; i < num_conds; ++i) {
    if (cond_vars[i] != that->cond_vars[i]) {
      return false;
    }
  }

  return true;
}


Node<ProgramExistenceAssertionRegion> *
Node<ProgramExistenceAssertionRegion>::AsExistenceAssertion(void) noexcept {
  return this;
}

Node<ProgramTableJoinRegion> *
Node<ProgramTableJoinRegion>::AsTableJoin(void) noexcept {
  return this;
}

Node<ProgramTableProductRegion> *
Node<ProgramTableProductRegion>::AsTableProduct(void) noexcept {
  return this;
}

Node<ProgramTableScanRegion> *
Node<ProgramTableScanRegion>::AsTableScan(void) noexcept {
  return this;
}

bool Node<ProgramVectorClearRegion>::Equals(
    EqualitySet &eq, Node<ProgramRegion> *that_) const noexcept {
  const auto that_op = that_->AsOperation();
  if (!that_op) {
    return false;
  }

  const auto that = that_op->AsVectorClear();
  if (!that || !eq.Contains(vector.get(), that->vector.get())) {
    return false;
  } else {
    return true;
  }
}

Node<ProgramVectorClearRegion> *
Node<ProgramVectorClearRegion>::AsVectorClear(void) noexcept {
  return this;
}

bool Node<ProgramVectorUniqueRegion>::Equals(
    EqualitySet &eq, Node<ProgramRegion> *that_) const noexcept {
  const auto that_op = that_->AsOperation();
  if (!that_op) {
    return false;
  }

  const auto that = that_op->AsVectorUnique();
  if (!that || !eq.Contains(vector.get(), that->vector.get())) {
    return false;
  } else {
    return true;
  }
}

Node<ProgramVectorUniqueRegion> *
Node<ProgramVectorUniqueRegion>::AsVectorUnique(void) noexcept {
  return this;
}

bool Node<ProgramTableJoinRegion>::IsNoOp(void) const noexcept {
  return !this->OP::body || this->OP::body->IsNoOp();
}

bool Node<ProgramTableJoinRegion>::Equals(
    EqualitySet &eq, Node<ProgramRegion> *that_) const noexcept {
  const auto op = that_->AsOperation();
  if (!op) {
    return false;
  }
  const auto num_tables = tables.Size();
  const auto that = op->AsTableJoin();
  if (!that || num_tables != that->tables.Size() ||
      (!this->OP::body.get()) != (!that->OP::body.get())) {
    return false;
  }

  for (auto i = 0u; i < num_tables; ++i) {
    if (tables[i] != that->tables[i]) {
      return false;
    }
  }

  for (auto i = 0u; i < num_tables; ++i) {
    if (indices[i] != that->indices[i]) {
      return false;
    }
  }

  for (auto i = 0u; i < num_tables; ++i) {
    const auto &cols_1 = pivot_cols[i];
    const auto &cols_2 = that->pivot_cols[i];
    for (auto j = 0u, max_j = cols_1.Size(); j < max_j; ++j) {
      if (!eq.Contains(cols_1[j], cols_2[j])) {
        return false;
      }
    }
  }

  for (auto i = 0u; i < num_tables; ++i) {
    const auto &cols_1 = output_cols[i];
    const auto &cols_2 = that->output_cols[i];
    for (auto j = 0u, max_j = cols_1.Size(); j < max_j; ++j) {
      if (!eq.Contains(cols_1[j], cols_2[j])) {
        return false;
      }
    }
  }

  if (auto that_body = that->OP::body.get(); that_body) {
    const auto &pivot_vars_1 = pivot_vars;
    const auto &pivot_vars_2 = that->pivot_vars;
    for (auto j = 0u, max_j = pivot_vars_1.Size(); j < max_j; ++j) {
      eq.Insert(pivot_vars_1[j], pivot_vars_2[j]);
    }

    for (auto i = 0u; i < num_tables; ++i) {
      const auto &vars_1 = output_vars[i];
      const auto &vars_2 = that->output_vars[i];
      for (auto j = 0u, max_j = vars_1.Size(); j < max_j; ++j) {
        eq.Insert(vars_1[j], vars_2[j]);
      }
    }

    return this->OP::body->Equals(eq, that_body);

  } else {
    return true;
  }
}

bool Node<ProgramTableProductRegion>::IsNoOp(void) const noexcept {
  return !this->OP::body || this->OP::body->IsNoOp();
}

bool Node<ProgramTableProductRegion>::Equals(
    EqualitySet &eq, Node<ProgramRegion> *that_) const noexcept {
  const auto op = that_->AsOperation();
  if (!op) {
    return false;
  }
  const auto num_tables = tables.Size();
  const auto that = op->AsTableProduct();
  if (!that || num_tables != that->tables.Size() ||
      (!this->OP::body.get()) != (!that->OP::body.get())) {
    return false;
  }

  for (auto i = 0u; i < num_tables; ++i) {
    if (tables[i] != that->tables[i]) {
      return false;
    }
  }

  for (auto i = 0u; i < num_tables; ++i) {
    if (!eq.Contains(input_vecs[i], that->input_vecs[i])) {
      return false;
    }
  }

  if (auto that_body = that->OP::body.get(); that_body) {

    for (auto i = 0u; i < num_tables; ++i) {
      auto &vars_1 = output_vars[i];
      auto &vars_2 = that->output_vars[i];
      for (auto j = 0u, max_j = vars_1.Size(); j < max_j; ++j) {
        eq.Insert(vars_1[j], vars_2[j]);
      }
    }

    return this->OP::body->Equals(eq, that_body);

  } else {
    return true;
  }
}

bool Node<ProgramTableScanRegion>::IsNoOp(void) const noexcept {
  return !output_vector->IsRead();
}

bool Node<ProgramTableScanRegion>::Equals(
    EqualitySet &eq, Node<ProgramRegion> *that_) const noexcept {
  const auto op = that_->AsOperation();
  if (!op) {
    return false;
  }

  const auto that = op->AsTableScan();
  if (!that || table.get() != that->table.get() ||
      index.get() != that->index.get() ||
      (!this->OP::body.get()) != (!that->OP::body.get())) {
    return false;
  }

  const auto num_vars = this->in_vars.Size();
  if (that->in_vars.Size() != num_vars) {
    assert(false);
    return false;
  }

  for (auto i = 0u; i < num_vars; ++i) {
    if (!eq.Contains(this->in_vars[i], that->in_vars[i])) {
      return false;
    }
  }

  return eq.Contains(this->output_vector.get(), that->output_vector.get());
}

Node<ProgramTupleCompareRegion> *
Node<ProgramTupleCompareRegion>::AsTupleCompare(void) noexcept {
  return this;
}

bool Node<ProgramTupleCompareRegion>::IsNoOp(void) const noexcept {
  return !this->OP::body || this->OP::body->IsNoOp();
}

bool Node<ProgramTupleCompareRegion>::Equals(
    EqualitySet &eq, Node<ProgramRegion> *that_) const noexcept {
  const auto op = that_->AsOperation();
  if (!op) {
    return false;
  }
  const auto num_vars = lhs_vars.Size();
  const auto that = op->AsTupleCompare();
  if (!that || cmp_op != that->cmp_op || num_vars != that->lhs_vars.Size() ||
      (!this->OP::body.get()) != (!that->OP::body.get())) {
    return false;
  }

  for (auto i = 0u; i < num_vars; ++i) {
    if (!eq.Contains(lhs_vars[i], that->lhs_vars[i]) ||
        !eq.Contains(rhs_vars[i], that->rhs_vars[i])) {
      return false;
    }
  }

  if (auto that_body = that->OP::body.get(); that_body) {
    return this->OP::body->Equals(eq, that_body);

  } else {
    return true;
  }
}

Node<ProgramGenerateRegion> *
Node<ProgramGenerateRegion>::AsGenerate(void) noexcept {
  return this;
}

bool Node<ProgramGenerateRegion>::IsNoOp(void) const noexcept {
  if (functor.IsPure()) {
    return !this->OP::body || this->OP::body->IsNoOp();

  } else {
    return false;
  }
}

// Returns `true` if `this` and `that` are structurally equivalent (after
// variable renaming).
bool Node<ProgramGenerateRegion>::Equals(
    EqualitySet &eq, Node<ProgramRegion> *that_) const noexcept {
  const auto op = that_->AsOperation();
  if (!op) {
    return false;
  }

  const auto that = op->AsGenerate();
  if (!that || this->OP::op != that->OP::op || functor != that->functor ||
      (!this->OP::body.get()) != (!that->OP::body.get())) {
    return false;
  }

  for (auto i = 0u, max_i = used_vars.Size(); i < max_i; ++i) {
    if (!eq.Contains(used_vars[i], that->used_vars[i])) {
      return false;
    }
  }

  if (auto that_body = that->OP::body.get(); that_body) {
    for (auto i = 0u, max_i = defined_vars.Size(); i < max_i; ++i) {
      eq.Insert(defined_vars[i], that->defined_vars[i]);
    }

    return this->OP::body->Equals(eq, that_body);

  } else {
    return true;
  }
}

Node<ProgramCallRegion> *Node<ProgramCallRegion>::AsCall(void) noexcept {
  return this;
}

bool Node<ProgramCallRegion>::IsNoOp(void) const noexcept {
  return !called_proc || called_proc->IsNoOp();
}

// Returns `true` if `this` and `that` are structurally equivalent (after
// variable renaming).
bool Node<ProgramCallRegion>::Equals(
    EqualitySet &eq, Node<ProgramRegion> *that_) const noexcept {
  const auto op = that_->AsOperation();
  if (!op) {
    return false;
  }

  const auto that = op->AsCall();
  if (!that) {
    return false;
  }

  if (!body != !that->body) {
    return false;
  }

  for (auto i = 0u, max_i = arg_vars.Size(); i < max_i; ++i) {
    if (!eq.Contains(arg_vars[i], that->arg_vars[i])) {
      return false;
    }
  }

  for (auto i = 0u, max_i = arg_vecs.Size(); i < max_i; ++i) {
    if (!eq.Contains(arg_vecs[i], that->arg_vecs[i])) {
      return false;
    }
  }

  // This function tests the true/false return value of the called procedure.
  if (body && !body->Equals(eq, that->body.get())) {
    return false;
  }

  const auto this_called_proc = called_proc.get();
  const auto that_called_proc = that->called_proc.get();

  if (this_called_proc == that_called_proc) {
    return true;

  } else if (eq.Contains(this_called_proc, that_called_proc)) {
    return true;

  // Different functions are being called, check to see if their function bodies
  // are the same.
  } else {

    if (!this_called_proc->body && !that_called_proc->body) {
      return true;

    } else if (!this_called_proc->body != !that_called_proc->body) {
      return false;

    } else {
      eq.Insert(this_called_proc, that_called_proc);

      for (auto i = 0u, max_i = arg_vars.Size(); i < max_i; ++i) {
        eq.Insert(this_called_proc->input_vars[i],
                  that_called_proc->input_vars[i]);
      }

      for (auto i = 0u, max_i = arg_vecs.Size(); i < max_i; ++i) {
        eq.Insert(this_called_proc->input_vecs[i],
                  that_called_proc->input_vecs[i]);
      }

      return this_called_proc->body->Equals(eq, that_called_proc->body.get());
    }
  }
}

Node<ProgramPublishRegion> *
Node<ProgramPublishRegion>::AsPublish(void) noexcept {
  return this;
}

// Returns `true` if `this` and `that` are structurally equivalent (after
// variable renaming).
bool Node<ProgramPublishRegion>::Equals(
    EqualitySet &eq, Node<ProgramRegion> *that_) const noexcept {
  const auto op = that_->AsOperation();
  if (!op) {
    return false;
  }

  const auto that = op->AsPublish();
  if (!that || message != that->message) {
    return false;
  }

  for (auto i = 0u, max_i = arg_vars.Size(); i < max_i; ++i) {
    if (!eq.Contains(arg_vars[i], that->arg_vars[i])) {
      return false;
    }
  }

  return true;
}


Node<ProgramReturnRegion> *Node<ProgramReturnRegion>::AsReturn(void) noexcept {
  return this;
}

bool Node<ProgramReturnRegion>::IsNoOp(void) const noexcept {
  return false;
}

// Returns `true` if `this` and `that` are structurally equivalent (after
// variable renaming).
bool Node<ProgramReturnRegion>::Equals(
    EqualitySet &eq, Node<ProgramRegion> *that_) const noexcept {
  const auto op = that_->AsOperation();
  if (!op) {
    return false;
  }

  const auto that = op->AsReturn();
  if (!that) {
    return false;
  }

  return this->OP::op == that->OP::op;
}

Node<ProgramCheckStateRegion> *
Node<ProgramCheckStateRegion>::AsCheckState(void) noexcept {
  return this;
}

bool Node<ProgramCheckStateRegion>::IsNoOp(void) const noexcept {
  if (body && !body->IsNoOp()) {
    return false;
  }

  if (absent_body && !absent_body->IsNoOp()) {
    return false;
  }

  if (unknown_body && !unknown_body->IsNoOp()) {
    return false;
  }

  return true;
}

// Returns `true` if `this` and `that` are structurally equivalent (after
// variable renaming).
bool Node<ProgramCheckStateRegion>::Equals(EqualitySet &eq,
            Node<ProgramRegion> *that_) const noexcept {
  const auto op = that_->AsOperation();
  if (!op) {
    return false;
  }

  const auto that = op->AsCheckState();
  if (!that) {
    return false;
  }

  if (this->table != that->table) {
    return false;
  }

  if (!this->body != !that->body ||
      !this->absent_body != !that->absent_body ||
      !this->unknown_body != !that->unknown_body) {
    return false;
  }

  const auto num_cols = this->col_values.Size();
  for (auto i = 0u; i < num_cols; ++i) {
    if (!eq.Contains(this->col_values[i], that->col_values[i])) {
      return false;
    }
  }

  if ((body && !body->Equals(eq, that->body.get())) ||
      (absent_body && !absent_body->Equals(eq, that->absent_body.get())) ||
      (unknown_body && !unknown_body->Equals(eq, that->unknown_body.get()))) {
    return false;
  }

  return true;
}

}  // namespace hyde
