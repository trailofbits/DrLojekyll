// Copyright 2020, Trail of Bits. All rights reserved.

#include "Program.h"

namespace hyde {

Node<ProgramOperationRegion>::~Node(void) {}
Node<ProgramLetBindingRegion>::~Node(void) {}
Node<ProgramVectorLoopRegion>::~Node(void) {}
Node<ProgramVectorAppendRegion>::~Node(void) {}
Node<ProgramVectorClearRegion>::~Node(void) {}
Node<ProgramVectorUniqueRegion>::~Node(void) {}
Node<ProgramTableInsertRegion>::~Node(void) {}
Node<ProgramTableJoinRegion>::~Node(void) {}
Node<ProgramTableProductRegion>::~Node(void) {}
Node<ProgramExistenceCheckRegion>::~Node(void) {}
Node<ProgramTupleCompareRegion>::~Node(void) {}

Node<ProgramOperationRegion>::Node(REGION *parent_, ProgramOperation op_)
    : Node<ProgramRegion>(parent_),
      op(op_) {}

Node<ProgramOperationRegion> *
Node<ProgramOperationRegion>::AsOperation(void) noexcept {
  return this;
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

Node<ProgramTableInsertRegion> *
Node<ProgramOperationRegion>::AsTableInsert(void) noexcept {
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

Node<ProgramExistenceCheckRegion> *
Node<ProgramOperationRegion>::AsExistenceCheck(void) noexcept {
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

  for (auto i = 0u, max_i = defined_vars.Size(); i < max_i; ++i) {
    eq.Insert(defined_vars[i], that->defined_vars[i]);
  }

  if (auto that_body = that->OP::body.get(); that_body) {
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

  for (auto i = 0u; i < num_vars; ++i) {
    eq.Insert(defined_vars[i], that->defined_vars[i]);
  }

  if (auto that_body = that->OP::body.get(); that_body) {
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

Node<ProgramTableInsertRegion> *
Node<ProgramTableInsertRegion>::AsTableInsert(void) noexcept {
  return this;
}

bool Node<ProgramTableInsertRegion>::Equals(
    EqualitySet &eq, Node<ProgramRegion> *that_) const noexcept {
  const auto that_op = that_->AsOperation();
  if (!that_op) {
    return false;
  }

  const auto that = that_op->AsTableInsert();
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

Node<ProgramTableJoinRegion> *
Node<ProgramTableJoinRegion>::AsTableJoin(void) noexcept {
  return this;
}

Node<ProgramTableProductRegion> *
Node<ProgramTableProductRegion>::AsTableProduct(void) noexcept {
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

  if (auto that_body = that->OP::body.get(); that_body) {
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
    if (!eq.Contains(input_vectors[i], that->input_vectors[i])) {
      return false;
    }
  }

  for (auto i = 0u; i < num_tables; ++i) {
    auto &vars_1 = output_vars[i];
    auto &vars_2 = that->output_vars[i];
    for (auto j = 0u, max_j = vars_1.Size(); j < max_j; ++j) {
      eq.Insert(vars_1[j], vars_2[j]);
    }
  }

  if (auto that_body = that->OP::body.get(); that_body) {
    return this->OP::body->Equals(eq, that_body);

  } else {
    return true;
  }
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

}  // namespace hyde
