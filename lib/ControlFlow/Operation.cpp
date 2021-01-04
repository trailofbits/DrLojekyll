// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/Util/BitManipulation.h>

#include <iostream>

#include "Program.h"

// #define FAILED_EQ(...)

#define FAILED_EQ(that) \
  std::cerr << __LINE__ << ": " << this->containing_procedure->id \
            << " != " << that->containing_procedure->id << std::endl

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
Node<ProgramVectorSwapRegion>::~Node(void) {}
Node<ProgramVectorUniqueRegion>::~Node(void) {}

Node<ProgramOperationRegion>::Node(REGION *parent_, ProgramOperation op_)
    : Node<ProgramRegion>(parent_),
      op(op_) {}

Node<ProgramOperationRegion> *
Node<ProgramOperationRegion>::AsOperation(void) noexcept {
  return this;
}

// Returns `true` if all paths through `this` ends with a `return` region.
bool Node<ProgramOperationRegion>::EndsWithReturn(void) const noexcept {
  return false;
}

Node<ProgramCallRegion>::Node(unsigned id_, Node<ProgramRegion> *parent_,
                              Node<ProgramProcedure> *called_proc_,
                              ProgramOperation op_)
    : Node<ProgramOperationRegion>(parent_, op_),
      called_proc(this, called_proc_),
      arg_vars(this),
      arg_vecs(this),
      id(id_) {}

Node<ProgramCallRegion> *Node<ProgramOperationRegion>::AsCall(void) noexcept {
  return nullptr;
}

Node<ProgramReturnRegion> *
Node<ProgramOperationRegion>::AsReturn(void) noexcept {
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

Node<ProgramVectorSwapRegion> *
Node<ProgramOperationRegion>::AsVectorSwap(void) noexcept {
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

bool Node<ProgramVectorLoopRegion>::Equals(EqualitySet &eq,
                                           Node<ProgramRegion> *that_,
                                           uint32_t depth) const noexcept {
  const auto that_op = that_->AsOperation();
  if (!that_op) {
    FAILED_EQ(that_);
    return false;
  }

  const auto that = that_op->AsVectorLoop();
  if (!that || !eq.Contains(vector.get(), that->vector.get()) ||
      defined_vars.Size() != that->defined_vars.Size()) {
    FAILED_EQ(that_);
    return false;
  }

  for (auto i = 0u, max_i = defined_vars.Size(); i < max_i; ++i) {
    if (!eq.Contains(defined_vars[i], that->defined_vars[i])) {
      FAILED_EQ(that_);
      return false;
    } else {
      eq.Insert(defined_vars[i], that->defined_vars[i]);
    }
  }

  if (depth == 0) {
    return true;
  }

  if ((!body.get()) != (!that->body.get())) {
    FAILED_EQ(that_);
    return false;
  }

  if (auto that_body = that->OP::body.get(); that_body) {
    return this->OP::body->Equals(eq, that_body, depth - 1u);
  } else {
    return true;
  }
}

const bool Node<ProgramVectorLoopRegion>::MergeEqual(
    ProgramImpl *prog, std::vector<Node<ProgramRegion> *> &merges) {
  NOTE("Unimplemented merging of ProgramVectorLoopRegion");
  return false;
}

Node<ProgramVectorLoopRegion> *
Node<ProgramVectorLoopRegion>::AsVectorLoop(void) noexcept {
  return this;
}

uint64_t Node<ProgramVectorLoopRegion>::Hash(uint32_t depth) const {
  uint64_t hash = static_cast<unsigned>(this->OP::op) * 53;
  hash ^= RotateRight64(hash, 13) * (static_cast<unsigned>(vector->kind) + 17u);
  for (auto type : vector->col_types) {
    hash ^= RotateRight64(hash, 13) * (static_cast<unsigned>(type) + 11u);
  }
  if (depth == 0) {
    return hash;
  }

  if (this->OP::body) {
    hash ^= RotateRight64(hash, 13) * this->OP::body->Hash(depth - 1u);
  }
  return hash;
}

bool Node<ProgramVectorLoopRegion>::IsNoOp(void) const noexcept {
  return !this->OP::body || this->OP::body->IsNoOp();
}

Node<ProgramLetBindingRegion> *
Node<ProgramLetBindingRegion>::AsLetBinding(void) noexcept {
  return this;
}

uint64_t Node<ProgramLetBindingRegion>::Hash(uint32_t depth) const {
  uint64_t hash = static_cast<unsigned>(this->OP::op) * 53;
  for (auto var : used_vars) {
    hash ^= RotateRight64(hash, 13) *
            ((static_cast<unsigned>(var->role) + 7u) *
             (static_cast<unsigned>(DataVariable(var).Type().Kind()) + 11u));
  }
  if (depth == 0) {
    return hash;
  }

  if (this->OP::body) {
    hash ^= RotateRight64(hash, 13) * this->OP::body->Hash(depth - 1u);
  }
  return hash;
}

bool Node<ProgramLetBindingRegion>::IsNoOp(void) const noexcept {
  return !this->OP::body || this->OP::body->IsNoOp();
}

bool Node<ProgramLetBindingRegion>::Equals(EqualitySet &eq,
                                           Node<ProgramRegion> *that_,
                                           uint32_t depth) const noexcept {
  const auto that_op = that_->AsOperation();
  if (!that_op) {
    FAILED_EQ(that_);
    return false;
  }

  const auto that = that_op->AsLetBinding();
  const auto num_vars = defined_vars.Size();
  if (!that || num_vars != that->defined_vars.Size() ||
      (!body.get()) != (!that->body.get())) {
    FAILED_EQ(that_);
    return false;
  }

  for (auto i = 0u; i < num_vars && depth != 0; ++i) {
    if (!eq.Contains(used_vars[i], that->used_vars[i])) {
      FAILED_EQ(that_);
      return false;
    }
  }


  if (auto that_body = that->OP::body.get(); that_body) {
    for (auto i = 0u; i < num_vars; ++i) {
      eq.Insert(defined_vars[i], that->defined_vars[i]);
    }

    if (depth == 0) {
      return true;
    }

    return this->OP::body->Equals(eq, that_body, depth - 1u);

  } else {
    return true;
  }
}

const bool Node<ProgramLetBindingRegion>::MergeEqual(
    ProgramImpl *prog, std::vector<Node<ProgramRegion> *> &merges) {
  if (defined_vars.Size() > 0 || used_vars.Size() > 0) {
    NOTE("Unimplemented merging of ProgramLetBinding");
  }
  return false;
}

uint64_t Node<ProgramVectorAppendRegion>::Hash(uint32_t depth) const {
  uint64_t hash = static_cast<unsigned>(this->OP::op) * 53;
  hash ^= RotateRight64(hash, 13) * (static_cast<unsigned>(vector->kind) + 17u);
  for (auto type : vector->col_types) {
    hash ^= RotateRight64(hash, 13) * (static_cast<unsigned>(type) + 11u);
  }
  for (auto var : tuple_vars) {
    hash ^= RotateRight64(hash, 13) *
            ((static_cast<unsigned>(var->role) + 7u) *
             (static_cast<unsigned>(DataVariable(var).Type().Kind()) + 11u));
  }
  (void) depth;
  return hash;
}

bool Node<ProgramVectorAppendRegion>::Equals(EqualitySet &eq,
                                             Node<ProgramRegion> *that_,
                                             uint32_t depth) const noexcept {
  const auto that_op = that_->AsOperation();
  if (!that_op) {
    FAILED_EQ(that_);
    return false;
  }

  const auto that = that_op->AsVectorAppend();
  if (!that) {
    FAILED_EQ(that_);
    return false;
  }

  if (!eq.Contains(vector.get(), that->vector.get())) {
    return false;
  }

  for (auto i = 0u, max_i = tuple_vars.Size(); i < max_i; ++i) {
    if (!eq.Contains(tuple_vars[i], that->tuple_vars[i])) {
      FAILED_EQ(that_);
      return false;
    }
  }

  if (depth == 0) {
    return true;
  }

  return true;
}

const bool Node<ProgramVectorAppendRegion>::MergeEqual(
    ProgramImpl *prog, std::vector<Node<ProgramRegion> *> &merges) {
  NOTE("Unimplemented merging of ProgramVectorAppendRegion");
  return false;
}

Node<ProgramVectorAppendRegion> *
Node<ProgramVectorAppendRegion>::AsVectorAppend(void) noexcept {
  return this;
}

Node<ProgramTransitionStateRegion> *
Node<ProgramTransitionStateRegion>::AsTransitionState(void) noexcept {
  return this;
}

uint64_t Node<ProgramTransitionStateRegion>::Hash(uint32_t depth) const {
  uint64_t hash = static_cast<unsigned>(this->OP::op) * 53;
  hash ^= RotateRight64(hash, 13) * static_cast<unsigned>(from_state) * 13;
  hash ^= RotateRight64(hash, 13) * static_cast<unsigned>(to_state) * 17;
  hash ^= RotateRight64(hash, 13) * table->id * 17;
  for (auto var : col_values) {
    hash ^= RotateRight64(hash, 13) *
            ((static_cast<unsigned>(var->role) + 7u) *
             (static_cast<unsigned>(DataVariable(var).Type().Kind()) + 11u));
  }
  if (depth == 0) {
    return hash;
  }

  if (this->OP::body) {
    hash ^= RotateRight64(hash, 13) * this->OP::body->Hash(depth - 1u);
  }
  return hash;
}

bool Node<ProgramTransitionStateRegion>::IsNoOp(void) const noexcept {
  return false;
}

bool Node<ProgramTransitionStateRegion>::Equals(EqualitySet &eq,
                                                Node<ProgramRegion> *that_,
                                                uint32_t depth) const noexcept {
  const auto that_op = that_->AsOperation();
  if (!that_op) {
    FAILED_EQ(that_);
    return false;
  }

  const auto that = that_op->AsTransitionState();
  if (!that || table.get() != that->table.get()) {
    FAILED_EQ(that_);
    return false;
  }

  for (auto i = 0u, max_i = col_values.Size(); i < max_i; ++i) {
    if (!eq.Contains(col_values[i], that->col_values[i])) {
      FAILED_EQ(that_);
      return false;
    }
  }

  if (!body != !(that->body)) {
    return false;
  }

  if (depth == 0) {
    return true;
  }

  if (body) {
    return body->Equals(eq, that->body.get(), depth - 1u);
  }

  return true;
}

const bool Node<ProgramTransitionStateRegion>::MergeEqual(
    ProgramImpl *prog, std::vector<Node<ProgramRegion> *> &merges) {

  // New parallel region for merged bodies into 'this'
  auto new_par = prog->parallel_regions.Create(this);
  auto transition_body = body.get();
  if (transition_body) {
    new_par->regions.AddUse(transition_body);
    transition_body->parent = new_par;
  }
  body.Clear();
  body.Emplace(this, new_par);
  for (auto region : merges) {
    auto merge = region->AsOperation()->AsTransitionState();
    assert(merge);  // These should all be the same type
    const auto merge_body = merge->body.get();
    if (merge_body) {
      new_par->regions.AddUse(merge_body);
      merge_body->parent = new_par;
    }
    merge->body.Clear();
    merge->parent = nullptr;
  }
  return true;
}

uint64_t Node<ProgramExistenceCheckRegion>::Hash(uint32_t depth) const {
  uint64_t hash = static_cast<unsigned>(this->OP::op) * 53;
  for (auto var : cond_vars) {
    hash ^= RotateRight64(hash, 13) *
            ((static_cast<unsigned>(var->role) + 7u) *
             (static_cast<unsigned>(DataVariable(var).Type().Kind()) + 11u));
  }
  if (depth == 0) {
    return hash;
  }

  if (this->OP::body) {
    hash ^= RotateRight64(hash, 13) * this->OP::body->Hash(depth - 1u);
  }
  return hash;
}

bool Node<ProgramExistenceCheckRegion>::IsNoOp(void) const noexcept {
  return !this->OP::body || this->OP::body->IsNoOp();
}

bool Node<ProgramExistenceCheckRegion>::Equals(EqualitySet &eq,
                                               Node<ProgramRegion> *that_,
                                               uint32_t depth) const noexcept {
  const auto that_op = that_->AsOperation();
  if (!that_op || this->OP::op != that_op->OP::op) {
    FAILED_EQ(that_);
    return false;
  }

  const auto num_conds = cond_vars.Size();
  const auto that = that_op->AsExistenceCheck();
  if (!that || num_conds != that->cond_vars.Size() ||
      (!this->OP::body.get()) != (!that->OP::body.get())) {
    FAILED_EQ(that_);
    return false;
  }

  // NOTE(pag): Condition variables are global, so we do equality checks.
  for (auto i = 0u; i < num_conds; ++i) {
    if (cond_vars[i] != that->cond_vars[i]) {
      FAILED_EQ(that_);
      return false;
    }
  }

  if (depth == 0) {
    return true;
  }

  if (auto that_body = that->OP::body.get(); that_body) {
    return this->OP::body->Equals(eq, that_body, depth - 1u);

  } else {
    return true;
  }
}

const bool Node<ProgramExistenceCheckRegion>::MergeEqual(
    ProgramImpl *prog, std::vector<Node<ProgramRegion> *> &merges) {
  NOTE("Unimplemented merging of ProgramExistenceCheckRegion");
  return false;
}

Node<ProgramExistenceCheckRegion> *
Node<ProgramExistenceCheckRegion>::AsExistenceCheck(void) noexcept {
  return this;
}

uint64_t Node<ProgramExistenceAssertionRegion>::Hash(uint32_t depth) const {
  uint64_t hash = static_cast<unsigned>(this->OP::op) * 53;
  for (auto var : cond_vars) {
    hash ^= RotateRight64(hash, 13) *
            ((static_cast<unsigned>(var->role) + 7u) *
             (static_cast<unsigned>(DataVariable(var).Type().Kind()) + 11u));
  }
  if (depth == 0) {
    return hash;
  }

  if (this->OP::body) {
    hash ^= RotateRight64(hash, 13) * this->OP::body->Hash(depth - 1u);
  }
  return hash;
}

bool Node<ProgramExistenceAssertionRegion>::IsNoOp(void) const noexcept {
  return cond_vars.Empty();
}

bool Node<ProgramExistenceAssertionRegion>::Equals(
    EqualitySet &eq, Node<ProgramRegion> *that_,
    uint32_t depth) const noexcept {
  const auto that_op = that_->AsOperation();
  if (!that_op || this->OP::op != that_op->OP::op) {
    FAILED_EQ(that_);
    return false;
  }

  const auto num_conds = cond_vars.Size();
  const auto that = that_op->AsExistenceAssertion();
  if (!that || num_conds != that->cond_vars.Size()) {
    FAILED_EQ(that_);
    return false;
  }

  // NOTE(pag): Condition variables are global, so we do equality checks.
  for (auto i = 0u; i < num_conds; ++i) {
    if (cond_vars[i] != that->cond_vars[i]) {
      FAILED_EQ(that_);
      return false;
    }
  }

  return true;
}

const bool Node<ProgramExistenceAssertionRegion>::MergeEqual(
    ProgramImpl *prog, std::vector<Node<ProgramRegion> *> &merges) {
  NOTE("Unimplemented merging of ProgramExistenceAssertionRegion");
  return false;
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

uint64_t Node<ProgramVectorClearRegion>::Hash(uint32_t depth) const {
  uint64_t hash = static_cast<unsigned>(this->OP::op) * 53;
  hash ^= (static_cast<unsigned>(vector->kind) + 1u) * 17;
  for (auto type : vector->col_types) {
    hash ^= RotateRight64(hash, 13) * (static_cast<unsigned>(type) + 11u);
  }
  (void) depth;
  return hash;
}

bool Node<ProgramVectorClearRegion>::Equals(EqualitySet &eq,
                                            Node<ProgramRegion> *that_,
                                            uint32_t depth) const noexcept {
  const auto that_op = that_->AsOperation();
  if (!that_op) {
    FAILED_EQ(that_);
    return false;
  }

  const auto that = that_op->AsVectorClear();
  if (!that || !eq.Contains(vector.get(), that->vector.get())) {
    FAILED_EQ(that_);
    return false;
  } else {
    return true;
  }
}

const bool Node<ProgramVectorClearRegion>::MergeEqual(
    ProgramImpl *prog, std::vector<Node<ProgramRegion> *> &merges) {
  NOTE("Unimplemented merging of ProgramVectorClearRegion");
  return false;
}

Node<ProgramVectorClearRegion> *
Node<ProgramVectorClearRegion>::AsVectorClear(void) noexcept {
  return this;
}

Node<ProgramVectorSwapRegion> *
Node<ProgramVectorSwapRegion>::AsVectorSwap(void) noexcept {
  return this;
}


uint64_t Node<ProgramVectorSwapRegion>::Hash(uint32_t depth) const {
  uint64_t hash = static_cast<unsigned>(this->OP::op) * 53;
  hash ^= (static_cast<unsigned>(lhs->kind) + 1u) * 17;
  hash ^= (static_cast<unsigned>(rhs->kind) + 1u) * 17;
  for (auto type : lhs->col_types) {
    hash ^= RotateRight64(hash, 13) * (static_cast<unsigned>(type) + 11u);
  }
  (void) depth;
  return hash;
}

bool Node<ProgramVectorSwapRegion>::Equals(EqualitySet &eq,
                                           Node<ProgramRegion> *that_,
                                           uint32_t depth) const noexcept {
  const auto that_op = that_->AsOperation();
  if (!that_op) {
    FAILED_EQ(that_);
    return false;
  }

  const auto that = that_op->AsVectorSwap();
  if (!that) {
    FAILED_EQ(that_);
    return false;

  } else if (eq.Contains(lhs.get(), that->lhs.get()) &&
             eq.Contains(rhs.get(), that->rhs.get())) {
    return true;

  } else if (eq.Contains(lhs.get(), that->rhs.get()) &&
             eq.Contains(rhs.get(), that->lhs.get())) {
    return true;

  } else {
    return false;
  }
}

const bool Node<ProgramVectorSwapRegion>::MergeEqual(
    ProgramImpl *prog, std::vector<Node<ProgramRegion> *> &merges) {
  NOTE("Unimplemented merging of ProgramVectorSwapRegion");
  return false;
}

uint64_t Node<ProgramVectorUniqueRegion>::Hash(uint32_t depth) const {
  uint64_t hash = static_cast<unsigned>(this->OP::op) * 53;
  hash ^= (static_cast<unsigned>(vector->kind) + 1u) * 17;
  for (auto type : vector->col_types) {
    hash ^= RotateRight64(hash, 13) * (static_cast<unsigned>(type) + 11u);
  }
  (void) depth;
  return hash;
}

bool Node<ProgramVectorUniqueRegion>::Equals(EqualitySet &eq,
                                             Node<ProgramRegion> *that_,
                                             uint32_t depth) const noexcept {
  const auto that_op = that_->AsOperation();
  if (!that_op) {
    FAILED_EQ(that_);
    return false;
  }

  const auto that = that_op->AsVectorUnique();
  if (!that || !eq.Contains(vector.get(), that->vector.get())) {
    FAILED_EQ(that_);
    return false;
  } else {
    return true;
  }
}

const bool Node<ProgramVectorUniqueRegion>::MergeEqual(
    ProgramImpl *prog, std::vector<Node<ProgramRegion> *> &merges) {
  NOTE("Unimplemented merging of ProgramVectorUniqueRegion");
  return false;
}

Node<ProgramVectorUniqueRegion> *
Node<ProgramVectorUniqueRegion>::AsVectorUnique(void) noexcept {
  return this;
}

uint64_t Node<ProgramTableJoinRegion>::Hash(uint32_t depth) const {
  uint64_t hash = static_cast<unsigned>(this->OP::op) * 53;
  for (auto table : this->tables) {
    hash ^= RotateRight64(hash, 17) * (table->id + 17u);
  }
  for (auto index : this->indices) {
    hash ^= RotateRight64(hash, 13) * (index->id + 13u);
  }
  if (depth == 0) {
    return hash;
  }

  if (this->OP::body) {
    hash ^= RotateRight64(hash, 11) * this->OP::body->Hash(depth - 1u);
  }
  return hash;
}

bool Node<ProgramTableJoinRegion>::IsNoOp(void) const noexcept {
  return !this->OP::body || this->OP::body->IsNoOp();
}

bool Node<ProgramTableJoinRegion>::Equals(EqualitySet &eq,
                                          Node<ProgramRegion> *that_,
                                          uint32_t depth) const noexcept {
  const auto op = that_->AsOperation();
  if (!op) {
    FAILED_EQ(that_);
    return false;
  }
  const auto num_tables = tables.Size();
  const auto that = op->AsTableJoin();
  if (!that || num_tables != that->tables.Size() ||
      (!this->OP::body.get()) != (!that->OP::body.get())) {
    FAILED_EQ(that_);
    return false;
  }

  for (auto i = 0u; i < num_tables; ++i) {
    if (tables[i] != that->tables[i]) {
      FAILED_EQ(that_);
      return false;
    }
  }

  for (auto i = 0u; i < num_tables; ++i) {
    if (indices[i] != that->indices[i]) {
      FAILED_EQ(that_);
      return false;
    }
  }

  for (auto i = 0u; i < num_tables; ++i) {
    const auto &cols_1 = pivot_cols[i];
    const auto &cols_2 = that->pivot_cols[i];
    for (auto j = 0u, max_j = cols_1.Size(); j < max_j; ++j) {
      if (!eq.Contains(cols_1[j], cols_2[j])) {
        FAILED_EQ(that_);
        return false;
      }
    }
  }

  for (auto i = 0u; i < num_tables; ++i) {
    const auto &cols_1 = output_cols[i];
    const auto &cols_2 = that->output_cols[i];
    for (auto j = 0u, max_j = cols_1.Size(); j < max_j; ++j) {
      if (!eq.Contains(cols_1[j], cols_2[j])) {
        FAILED_EQ(that_);
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

    if (depth == 0) {
      return true;
    }

    return this->OP::body->Equals(eq, that_body, depth - 1u);

  } else {
    return true;
  }
}

const bool Node<ProgramTableJoinRegion>::MergeEqual(
    ProgramImpl *prog, std::vector<Node<ProgramRegion> *> &merges) {
  NOTE("Unimplemented merging of ProgramTableJoinRegion");
  return false;
}

bool Node<ProgramTableProductRegion>::IsNoOp(void) const noexcept {
  return !this->OP::body || this->OP::body->IsNoOp();
}

uint64_t Node<ProgramTableProductRegion>::Hash(uint32_t depth) const {
  uint64_t hash = static_cast<unsigned>(this->OP::op) * 53;
  for (auto table : this->tables) {
    hash ^= RotateRight64(hash, 17) * (table->id + 17u);
  }
  if (depth == 0) {
    return hash;
  }

  if (this->OP::body) {
    hash ^= RotateRight64(hash, 11) * this->OP::body->Hash(depth - 1u);
  }
  return hash;
}

bool Node<ProgramTableProductRegion>::Equals(EqualitySet &eq,
                                             Node<ProgramRegion> *that_,
                                             uint32_t depth) const noexcept {
  const auto op = that_->AsOperation();
  if (!op) {
    FAILED_EQ(that_);
    return false;
  }
  const auto num_tables = tables.Size();
  const auto that = op->AsTableProduct();
  if (!that || num_tables != that->tables.Size() ||
      (!this->OP::body.get()) != (!that->OP::body.get())) {
    FAILED_EQ(that_);
    return false;
  }

  for (auto i = 0u; i < num_tables; ++i) {
    if (tables[i] != that->tables[i]) {
      FAILED_EQ(that_);
      return false;
    }
  }

  for (auto i = 0u; i < num_tables; ++i) {
    if (!eq.Contains(input_vecs[i], that->input_vecs[i])) {
      FAILED_EQ(that_);
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

    if (depth == 0) {
      return true;
    }

    return this->OP::body->Equals(eq, that_body, depth - 1u);

  } else {
    return true;
  }
}

const bool Node<ProgramTableProductRegion>::MergeEqual(
    ProgramImpl *prog, std::vector<Node<ProgramRegion> *> &merges) {
  NOTE("Unimplemented merging of ProgramTableProductRegion");
  return false;
}

uint64_t Node<ProgramTableScanRegion>::Hash(uint32_t depth) const {
  uint64_t hash = static_cast<unsigned>(this->OP::op) * 53;
  hash ^= RotateRight64(hash, 17) * (table->id + 17u);
  hash ^= RotateRight64(hash, 15) * (index->id + 13u);
  if (depth == 0) {
    return hash;
  }

  if (this->OP::body) {
    hash ^= RotateRight64(hash, 11) * this->OP::body->Hash(depth - 1u);
  }
  return hash;
}

bool Node<ProgramTableScanRegion>::IsNoOp(void) const noexcept {
  if (output_vector->NumUses() == 1u) {
    return true;
  } else {
    return !output_vector->IsRead();
  }
}

bool Node<ProgramTableScanRegion>::Equals(EqualitySet &eq,
                                          Node<ProgramRegion> *that_,
                                          uint32_t depth) const noexcept {
  const auto op = that_->AsOperation();
  if (!op) {
    FAILED_EQ(that_);
    return false;
  }

  const auto that = op->AsTableScan();
  if (!that || table.get() != that->table.get() ||
      index.get() != that->index.get() ||
      (!this->OP::body.get()) != (!that->OP::body.get())) {
    FAILED_EQ(that_);
    return false;
  }

  const auto num_vars = this->in_vars.Size();
  if (that->in_vars.Size() != num_vars) {
    assert(false);
    FAILED_EQ(that_);
    return false;
  }

  for (auto i = 0u; i < num_vars; ++i) {
    if (!eq.Contains(this->in_vars[i], that->in_vars[i])) {
      FAILED_EQ(that_);
      return false;
    }
  }

  return eq.Contains(this->output_vector.get(), that->output_vector.get());
}

const bool Node<ProgramTableScanRegion>::MergeEqual(
    ProgramImpl *prog, std::vector<Node<ProgramRegion> *> &merges) {
  NOTE("Unimplemented merging of ProgramTableScanRegion");
  return false;
}

uint64_t Node<ProgramTupleCompareRegion>::Hash(uint32_t depth) const {
  uint64_t hash = static_cast<unsigned>(this->OP::op) * 53;
  hash ^= RotateRight64(hash, 17) * (static_cast<unsigned>(this->cmp_op) + 17u);
  for (auto var : this->lhs_vars) {
    hash ^= RotateRight64(hash, 13) *
            ((static_cast<unsigned>(var->role) + 7u) *
             (static_cast<unsigned>(DataVariable(var).Type().Kind()) + 11u));
  }
  for (auto var : this->rhs_vars) {
    hash ^= RotateRight64(hash, 15) *
            ((static_cast<unsigned>(var->role) + 11u) *
             (static_cast<unsigned>(DataVariable(var).Type().Kind()) + 13u));
  }
  if (depth == 0) {
    return hash;
  }

  if (this->OP::body) {
    hash ^= RotateRight64(hash, 11) * this->OP::body->Hash(depth - 1u);
  }
  return hash;
}

Node<ProgramTupleCompareRegion> *
Node<ProgramTupleCompareRegion>::AsTupleCompare(void) noexcept {
  return this;
}

bool Node<ProgramTupleCompareRegion>::IsNoOp(void) const noexcept {
  return !this->OP::body || this->OP::body->IsNoOp();
}

bool Node<ProgramTupleCompareRegion>::Equals(EqualitySet &eq,
                                             Node<ProgramRegion> *that_,
                                             uint32_t depth) const noexcept {
  const auto op = that_->AsOperation();
  if (!op) {
    FAILED_EQ(that_);
    return false;
  }
  const auto num_vars = lhs_vars.Size();
  const auto that = op->AsTupleCompare();
  if (!that || cmp_op != that->cmp_op || num_vars != that->lhs_vars.Size()) {
    FAILED_EQ(that_);
    return false;
  }

  if ((!this->OP::body.get()) != (!that->OP::body.get())) {
    return false;
  }

  for (auto i = 0u; i < num_vars; ++i) {
    if (!eq.Contains(lhs_vars[i], that->lhs_vars[i]) ||
        !eq.Contains(rhs_vars[i], that->rhs_vars[i])) {
      FAILED_EQ(that_);
      return false;
    }
  }

  if (depth == 0) {
    return true;
  }

  if (auto that_body = that->OP::body.get(); that_body) {
    return this->OP::body->Equals(eq, that_body, depth - 1u);

  } else {
    return true;
  }
}

const bool Node<ProgramTupleCompareRegion>::MergeEqual(
    ProgramImpl *prog, std::vector<Node<ProgramRegion> *> &merges) {
  NOTE("Unimplemented merging of ProgramTupleCompareRegion");
  return false;
}

Node<ProgramGenerateRegion> *
Node<ProgramGenerateRegion>::AsGenerate(void) noexcept {
  return this;
}

uint64_t Node<ProgramGenerateRegion>::Hash(uint32_t depth) const {
  uint64_t hash = static_cast<unsigned>(this->OP::op) * 53;
  hash ^= RotateRight64(hash, 17) *
          (static_cast<unsigned>(this->functor.Id()) + 17u);

  for (auto var : this->used_vars) {
    hash ^= RotateRight64(hash, 13) *
            ((static_cast<unsigned>(var->role) + 7u) *
             (static_cast<unsigned>(DataVariable(var).Type().Kind()) + 11u));
  }
  if (depth == 0) {
    return hash;
  }

  if (this->OP::body) {
    hash ^= RotateRight64(hash, 11) * this->OP::body->Hash(depth - 1u);
  }
  return hash;
}

bool Node<ProgramGenerateRegion>::IsNoOp(void) const noexcept {
  if (functor.IsPure()) {
    return !this->OP::body || this->OP::body->IsNoOp();

  } else {
    return false;
  }
}

// Returns `true` if `this` and `that` are structurally equivalent (after
// variable renaming) after searching down `depth` levels or until leaf,
// whichever is first, and where `depth` is 0, compare `this` to `that.
bool Node<ProgramGenerateRegion>::Equals(EqualitySet &eq,
                                         Node<ProgramRegion> *that_,
                                         uint32_t depth) const noexcept {
  const auto op = that_->AsOperation();
  if (!op) {
    FAILED_EQ(that_);
    return false;
  }

  const auto that = op->AsGenerate();
  if (!that || this->OP::op != that->OP::op || functor != that->functor ||
      (!this->OP::body.get()) != (!that->OP::body.get())) {
    FAILED_EQ(that_);
    return false;
  }

  for (auto i = 0u, max_i = used_vars.Size(); i < max_i; ++i) {
    if (!eq.Contains(used_vars[i], that->used_vars[i])) {
      FAILED_EQ(that_);
      return false;
    }
  }

  if (depth == 0) {
    return true;
  }

  if (auto that_body = that->OP::body.get(); that_body) {
    for (auto i = 0u, max_i = defined_vars.Size(); i < max_i; ++i) {
      eq.Insert(defined_vars[i], that->defined_vars[i]);
    }

    return this->OP::body->Equals(eq, that_body, depth - 1u);

  } else {
    return true;
  }
}

const bool Node<ProgramGenerateRegion>::MergeEqual(
    ProgramImpl *prog, std::vector<Node<ProgramRegion> *> &merges) {
  NOTE("Unimplemented merging of ProgramGenerateRegion");
  return false;
}

Node<ProgramCallRegion> *Node<ProgramCallRegion>::AsCall(void) noexcept {
  return this;
}

uint64_t Node<ProgramCallRegion>::Hash(uint32_t depth) const {
  uint64_t hash = static_cast<unsigned>(this->OP::op) * 53;

  for (auto var : this->arg_vars) {
    hash ^= RotateRight64(hash, 13) *
            ((static_cast<unsigned>(var->role) + 7u) *
             (static_cast<unsigned>(DataVariable(var).Type().Kind()) + 11u));
  }

  for (auto vec : this->arg_vecs) {
    hash ^= RotateRight64(hash, 13) * (static_cast<unsigned>(vec->kind) + 7u);
    for (auto type : vec->col_types) {
      hash ^= RotateRight64(hash, 7) * (static_cast<unsigned>(type) + 3u);
    }
  }
  if (depth == 0) {
    return hash;
  }

  if (this->OP::body) {
    hash ^= RotateRight64(hash, 11) * this->OP::body->Hash(depth - 1u);
  }
  return hash;
}

bool Node<ProgramCallRegion>::IsNoOp(void) const noexcept {
  return false;

  // NOTE(pag): Not really worth checking as even trivial procedures are
  //            treated as non-no-ops.

  //  if (!called_proc) {
  //    return true;
  //
  //  } else if (this->OP::body) {
  //    if (this->OP::body->IsNoOp()) {
  //      return called_proc->IsNoOp();
  //    } else {
  //      return false;
  //    }
  //
  //  } else {
  //    return called_proc->IsNoOp();
  //  }
}

// Returns `true` if `this` and `that` are structurally equivalent (after
// variable renaming) after searching down `depth` levels or until leaf,
// whichever is first, and where `depth` is 0, compare `this` to `that.
bool Node<ProgramCallRegion>::Equals(EqualitySet &eq,
                                     Node<ProgramRegion> *that_,
                                     uint32_t depth) const noexcept {
  const auto op = that_->AsOperation();
  if (!op) {
    FAILED_EQ(that_);
    return false;
  }

  const auto that = op->AsCall();
  if (!that) {
    FAILED_EQ(that_);
    return false;
  }

  const auto num_arg_vars = arg_vars.Size();
  const auto num_arg_vecs = arg_vecs.Size();
  if (num_arg_vars != that->arg_vars.Size() ||
      num_arg_vecs != that->arg_vecs.Size()) {
    FAILED_EQ(that_);
    return false;
  }

  const auto this_called_proc = called_proc.get();
  const auto that_called_proc = that->called_proc.get();

  for (auto i = 0u, max_i = num_arg_vars; i < max_i; ++i) {
    if (!eq.Contains(arg_vars[i], that->arg_vars[i])) {
      FAILED_EQ(that_);
      return false;
    }
  }

  for (auto i = 0u, max_i = num_arg_vecs; i < max_i; ++i) {
    if (!eq.Contains(arg_vecs[i], that->arg_vecs[i])) {
      FAILED_EQ(that_);
      return false;
    }
  }

  if (!body != !that->body) {
    FAILED_EQ(that_);
    return false;
  }

  // This function tests the true/false return value of the called procedure.
  if (depth != 0 && body && !body->Equals(eq, that->body.get(), depth - 1)) {
    FAILED_EQ(that_);
    return false;
  }

  if (this_called_proc == that_called_proc ||
      eq.Contains(this_called_proc, that_called_proc)) {
    return true;
  } else {
    if (depth == 0) {
      return false;
    }
    // Different functions are being called, check to see if their function bodies
    // are the same.
    return this_called_proc->Equals(eq, that_called_proc, depth - 1);
  }
}

const bool Node<ProgramCallRegion>::MergeEqual(
    ProgramImpl *prog, std::vector<Node<ProgramRegion> *> &merges) {
  NOTE("Unimplemented merging of ProgramCallRegion");
  return false;
}

Node<ProgramPublishRegion> *
Node<ProgramPublishRegion>::AsPublish(void) noexcept {
  return this;
}

uint64_t Node<ProgramPublishRegion>::Hash(uint32_t depth) const {
  uint64_t hash = static_cast<unsigned>(this->OP::op) * 53;
  hash ^= RotateRight64(hash, 17) * this->message.Id();

  for (auto var : this->arg_vars) {
    hash ^= RotateRight64(hash, 13) *
            ((static_cast<unsigned>(var->role) + 7u) *
             (static_cast<unsigned>(DataVariable(var).Type().Kind()) + 11u));
  }
  (void) depth;
  return hash;
}

// Returns `true` if `this` and `that` are structurally equivalent (after
// variable renaming) after searching down `depth` levels or until leaf,
// whichever is first, and where `depth` is 0, compare `this` to `that.
bool Node<ProgramPublishRegion>::Equals(EqualitySet &eq,
                                        Node<ProgramRegion> *that_,
                                        uint32_t depth) const noexcept {
  const auto op = that_->AsOperation();
  if (!op) {
    FAILED_EQ(that_);
    return false;
  }

  const auto that = op->AsPublish();
  if (!that || message != that->message) {
    FAILED_EQ(that_);
    return false;
  }

  for (auto i = 0u, max_i = arg_vars.Size(); i < max_i; ++i) {
    if (!eq.Contains(arg_vars[i], that->arg_vars[i])) {
      FAILED_EQ(that_);
      return false;
    }
  }

  return true;
}

const bool Node<ProgramPublishRegion>::MergeEqual(
    ProgramImpl *prog, std::vector<Node<ProgramRegion> *> &merges) {
  NOTE("Unimplemented merging of ProgramPublishRegion");
  return false;
}

Node<ProgramReturnRegion> *Node<ProgramReturnRegion>::AsReturn(void) noexcept {
  return this;
}

// Returns `true` if all paths through `this` ends with a `return` region.
bool Node<ProgramReturnRegion>::EndsWithReturn(void) const noexcept {
  return true;
}

uint64_t Node<ProgramReturnRegion>::Hash(uint32_t depth) const {
  (void) depth;
  return static_cast<unsigned>(this->OP::op) * 53;
}

bool Node<ProgramReturnRegion>::IsNoOp(void) const noexcept {
  if (parent->AsProcedure()) {
    return true;
  } else {
    return false;
  }
}

// Returns `true` if `this` and `that` are structurally equivalent (after
// variable renaming) after searching down `depth` levels or until leaf,
// whichever is first, and where `depth` is 0, compare `this` to `that.
bool Node<ProgramReturnRegion>::Equals(EqualitySet &eq,
                                       Node<ProgramRegion> *that_,
                                       uint32_t depth) const noexcept {
  const auto op = that_->AsOperation();
  if (!op) {
    FAILED_EQ(that_);
    return false;
  }

  const auto that = op->AsReturn();
  if (!that) {
    FAILED_EQ(that_);
    return false;
  }

  return this->OP::op == that->OP::op;
}

const bool Node<ProgramReturnRegion>::MergeEqual(
    ProgramImpl *prog, std::vector<Node<ProgramRegion> *> &merges) {
  NOTE("Unimplemented merging of ProgramReturnRegion");
  return false;
}

Node<ProgramCheckStateRegion> *
Node<ProgramCheckStateRegion>::AsCheckState(void) noexcept {
  return this;
}

// Returns `true` if all paths through `this` ends with a `return` region.
bool Node<ProgramCheckStateRegion>::EndsWithReturn(void) const noexcept {
  auto has_any = 0;
  if (body) {
    if (!body->EndsWithReturn()) {
      return false;
    } else {
      ++has_any;
    }
  }
  if (absent_body) {
    if (!absent_body->EndsWithReturn()) {
      return false;
    } else {
      ++has_any;
    }
  }
  if (unknown_body) {
    if (!unknown_body->EndsWithReturn()) {
      return false;
    } else {
      ++has_any;
    }
  }

  return 3 == has_any;
}

uint64_t Node<ProgramCheckStateRegion>::Hash(uint32_t depth) const {
  uint64_t hash = static_cast<unsigned>(this->OP::op) * 53;
  hash ^= RotateRight64(hash, 17) * (this->table->id * 13u);

  for (auto var : this->col_values) {
    hash ^= RotateRight64(hash, 13) *
            ((static_cast<unsigned>(var->role) + 7u) *
             (static_cast<unsigned>(DataVariable(var).Type().Kind()) + 11u));
  }
  if (depth == 0) {
    return hash;
  }

  if (this->OP::body) {
    hash ^= RotateRight64(hash, 11) * this->OP::body->Hash(depth - 1u);
  }
  if (this->absent_body) {
    hash ^= RotateRight64(hash, 13) * this->absent_body->Hash(depth - 1u);
  }
  if (this->unknown_body) {
    hash ^= RotateRight64(hash, 15) * this->unknown_body->Hash(depth - 1u);
  }
  return hash;
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
// variable renaming) after searching down `depth` levels or until leaf,
// whichever is first, and where `depth` is 0, compare `this` to `that.
bool Node<ProgramCheckStateRegion>::Equals(EqualitySet &eq,
                                           Node<ProgramRegion> *that_,
                                           uint32_t depth) const noexcept {
  const auto op = that_->AsOperation();
  if (!op) {
    FAILED_EQ(that_);
    return false;
  }

  const auto that = op->AsCheckState();
  if (!that) {
    FAILED_EQ(that_);
    return false;
  }

  if (this->table->id != that->table->id) {
    FAILED_EQ(that_);
    return false;
  }

  if (!this->body != !that->body || !this->absent_body != !that->absent_body ||
      !this->unknown_body != !that->unknown_body) {
    FAILED_EQ(that_);
    return false;
  }

  const auto num_cols = this->col_values.Size();
  for (auto i = 0u; i < num_cols; ++i) {
    if (!eq.Contains(this->col_values[i], that->col_values[i])) {
      FAILED_EQ(that_);
      return false;
    }
  }

  if (depth == 0) {
    return true;
  }
  auto next_depth = depth - 1;

  if ((body && !body->Equals(eq, that->body.get(), next_depth)) ||
      (absent_body &&
       !absent_body->Equals(eq, that->absent_body.get(), next_depth)) ||
      (unknown_body &&
       !unknown_body->Equals(eq, that->unknown_body.get(), next_depth))) {
    FAILED_EQ(that_);
    return false;
  }

  return true;
}

const bool Node<ProgramCheckStateRegion>::MergeEqual(
    ProgramImpl *prog, std::vector<Node<ProgramRegion> *> &merges) {
  NOTE("Unimplemented merging of ProgramCheckStateRegion");
  return false;
}

}  // namespace hyde
