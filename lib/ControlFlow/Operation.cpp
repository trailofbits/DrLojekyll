// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/Util/BitManipulation.h>

// #include <iostream>

#include "Program.h"

#define FAILED_EQ(...)

// #define FAILED_EQ(that)
//   std::cerr << __LINE__ << ": " << this->containing_procedure->id
//             << " != " << that->containing_procedure->id << std::endl

namespace hyde {

Node<ProgramOperationRegion>::~Node(void) {}
Node<ProgramCallRegion>::~Node(void) {}
Node<ProgramReturnRegion>::~Node(void) {}
Node<ProgramTestAndSetRegion>::~Node(void) {}
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

Node<ProgramTestAndSetRegion> *
Node<ProgramOperationRegion>::AsTestAndSet(void) noexcept {
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
  const auto def_vars_size = defined_vars.Size();
  if (!that || !eq.Contains(vector.get(), that->vector.get()) ||
      def_vars_size != that->defined_vars.Size()) {
    FAILED_EQ(that_);
    return false;
  }

  if (depth) {
    if ((!body.get()) != (!that->body.get())) {
      FAILED_EQ(that_);
      return false;
    }

    if (auto that_body = that->OP::body.get(); that_body) {
      for (auto i = 0u, max_i = def_vars_size; i < max_i; ++i) {
        eq.Insert(defined_vars[i], that->defined_vars[i]);
      }

      return this->OP::body->Equals(eq, that_body, depth - 1u);
    }
  }

  return true;
}

const bool Node<ProgramVectorLoopRegion>::MergeEqual(
    ProgramImpl *impl, std::vector<Node<ProgramRegion> *> &merges) {

  auto par = impl->parallel_regions.Create(this);

  if (auto body_ptr = body.get()) {
    body.Clear();
    body_ptr->parent = par;
    par->AddRegion(body_ptr);
  }

  body.Emplace(this, par);

  const auto num_defined_vars = defined_vars.Size();

  for (auto merge : merges) {
    auto merged_loop = merge->AsOperation()->AsVectorLoop();
    assert(merged_loop != nullptr);
    assert(merged_loop != this);
    assert(merged_loop->defined_vars.Size() == num_defined_vars);
    assert(merged_loop->vector.get() == vector.get());

    for (auto i = 0u; i < num_defined_vars; ++i) {
      merged_loop->defined_vars[i]->ReplaceAllUsesWith(defined_vars[i]);
    }

    if (auto merged_body = merged_loop->body.get(); merged_body) {
      merged_body->parent = par;
      par->AddRegion(merged_body);
      merged_loop->body.Clear();
    }

    merged_loop->defined_vars.Clear();
    merged_loop->vector.Clear();
    merged_loop->parent = nullptr;
  }

  return true;
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
  const auto num_defined_vars = defined_vars.Size();
  const auto num_used_vars = used_vars.Size();
  if (!that || num_defined_vars != that->defined_vars.Size() ||
      num_used_vars != that->used_vars.Size()) {
    FAILED_EQ(that_);
    return false;
  }

  for (auto i = 0u; i < num_used_vars; ++i) {
    if (!eq.Contains(used_vars[i], that->used_vars[i])) {
      FAILED_EQ(that_);
      return false;
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
    for (auto i = 0u; i < num_defined_vars; ++i) {
      eq.Insert(defined_vars[i], that->defined_vars[i]);
    }

    return this->OP::body->Equals(eq, that_body, depth - 1u);

  } else {
    return true;
  }
}

const bool Node<ProgramLetBindingRegion>::MergeEqual(
    ProgramImpl *prog, std::vector<Node<ProgramRegion> *> &merges) {

  const auto num_defined_vars = defined_vars.Size();
  const auto num_used_vars = used_vars.Size();
  assert(num_defined_vars == num_used_vars);

  auto par = prog->parallel_regions.Create(this);
  if (auto curr_body = body.get(); curr_body) {
    curr_body->parent = par;
    par->AddRegion(curr_body);
    body.Clear();
  }

  body.Emplace(this, par);

  for (auto merge : merges) {
    auto merged_let = merge->AsOperation()->AsLetBinding();
    assert(merged_let != nullptr);
    assert(merged_let != this);
    assert(merged_let->defined_vars.Size() == num_defined_vars);

    for (auto i = 0u; i < num_used_vars; ++i) {
      assert(used_vars[i] == merged_let->used_vars[i]);
      merged_let->defined_vars[i]->ReplaceAllUsesWith(defined_vars[i]);
    }

    if (auto merged_body = merged_let->body.get(); merged_body) {
      merged_body->parent = par;
      par->AddRegion(merged_body);
      merged_let->body.Clear();
    }

    merged_let->defined_vars.Clear();
    merged_let->parent = nullptr;
  }

  (void) num_defined_vars;

  return true;
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
                                             uint32_t) const noexcept {
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

  return true;
}

const bool Node<ProgramVectorAppendRegion>::MergeEqual(
    ProgramImpl *prog, std::vector<Node<ProgramRegion> *> &merges) {

  assert(false && "Likely a bug has ocurred somewhere");

  // NOTE(pag): This should probably always return false because this should be
  // covered by the CSE over REGIONs.
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
  if (!that || table.get() != that->table.get() ||
      from_state != that->from_state || to_state != that->to_state) {
    FAILED_EQ(that_);
    return false;
  }

  for (auto i = 0u, max_i = col_values.Size(); i < max_i; ++i) {
    if (!eq.Contains(col_values[i], that->col_values[i])) {
      FAILED_EQ(that_);
      return false;
    }
  }

  if (depth == 0) {
    return true;
  }

  if (!body != !(that->body)) {
    return false;
  }

  if (body) {
    return body->Equals(eq, that->body.get(), depth - 1u);
  }

  return true;
}

const bool Node<ProgramTransitionStateRegion>::MergeEqual(
    ProgramImpl *prog, std::vector<Node<ProgramRegion> *> &merges) {

  // The implication is that two regions wanted to do the same transition. If
  // we are doing code gen, the that likely means one region is serialized
  // before the other, and so there is a kind of race condition, where only
  // one of them is likely to execute and the other will never execute.
  comment = "!!! STRIP MINING " + std::to_string(reinterpret_cast<uintptr_t>(this));
  for (auto region : merges) {
    region->comment = "??? STRIP MINING WITH " + std::to_string(reinterpret_cast<uintptr_t>(this));
  }

  assert(false && "Probable error when trying to strip mine program state transitions");
  return false;

  // New parallel region for merged bodies into 'this'
  auto new_par = prog->parallel_regions.Create(this);
  auto transition_body = body.get();
  if (transition_body) {
    transition_body->parent = new_par;
    new_par->AddRegion(transition_body);
    body.Clear();
  }
  body.Emplace(this, new_par);
  for (auto region : merges) {
    auto merge = region->AsOperation()->AsTransitionState();
    assert(merge);  // These should all be the same type
    assert(merge != this);
    const auto merge_body = merge->body.get();
    if (merge_body) {
      merge_body->parent = new_par;
      new_par->AddRegion(merge_body);
      merge->body.Clear();
    }
    merge->parent = nullptr;
  }
  return true;
}

uint64_t Node<ProgramTestAndSetRegion>::Hash(uint32_t depth) const {
  uint64_t hash = static_cast<unsigned>(this->OP::op) * 53;
  hash ^= RotateRight64(hash, 13) *
      ((static_cast<unsigned>(accumulator->role) + 7u) *
       (static_cast<unsigned>(DataVariable(accumulator.get()).Type().Kind()) + 11u));

  hash ^= RotateRight64(hash, 12) *
      ((static_cast<unsigned>(displacement->role) + 7u) *
       (static_cast<unsigned>(DataVariable(displacement.get()).Type().Kind()) + 12u));

  hash ^= RotateRight64(hash, 11) *
      ((static_cast<unsigned>(comparator->role) + 7u) *
       (static_cast<unsigned>(DataVariable(comparator.get()).Type().Kind()) + 13u));

  if (depth == 0) {
    return hash;
  }

  if (this->OP::body) {
    hash ^= RotateRight64(hash, 13) * this->OP::body->Hash(depth - 1u);
  }
  return hash;
}

bool Node<ProgramTestAndSetRegion>::IsNoOp(void) const noexcept {
  return false;
}

bool Node<ProgramTestAndSetRegion>::Equals(
    EqualitySet &eq, Node<ProgramRegion> *that_,
    uint32_t depth) const noexcept {
  const auto that_op = that_->AsOperation();
  if (!that_op || this->OP::op != that_op->OP::op) {
    FAILED_EQ(that_);
    return false;
  }

  const auto that = that_op->AsTestAndSet();
  if (!that) {
    FAILED_EQ(that_);
    return false;
  }

  if (!eq.Contains(accumulator.get(), that->accumulator.get()) ||
      !eq.Contains(displacement.get(), that->displacement.get()) ||
      !eq.Contains(comparator.get(), that->comparator.get())) {
    FAILED_EQ(that_);
    return false;
  }

  if (!depth) {
    return true;
  }

  if (!body != !that->body) {
    return false;
  }

  if (body) {
    return body->Equals(eq, that->body.get(), depth - 1u);
  }

  return true;
}

const bool Node<ProgramTestAndSetRegion>::MergeEqual(
    ProgramImpl *prog, std::vector<Node<ProgramRegion> *> &merges) {
  NOTE(
      "TODO(ekilmer): Unimplemented merging of TestAndSetRegion");
  assert(false);
  return false;
}

Node<ProgramTestAndSetRegion> *
Node<ProgramTestAndSetRegion>::AsTestAndSet(void) noexcept {
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

uint64_t Node<ProgramVectorClearRegion>::Hash(uint32_t) const {
  uint64_t hash = static_cast<unsigned>(this->OP::op) * 53;
  hash ^= (static_cast<unsigned>(vector->kind) + 1u) * 17;
  for (auto type : vector->col_types) {
    hash ^= RotateRight64(hash, 13) * (static_cast<unsigned>(type) + 11u);
  }
  return hash;
}

bool Node<ProgramVectorClearRegion>::Equals(EqualitySet &eq,
                                            Node<ProgramRegion> *that_,
                                            uint32_t) const noexcept {
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
  NOTE("TODO(ekilmer): Unimplemented merging of ProgramVectorClearRegion");
  assert(false);
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
                                           uint32_t) const noexcept {
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

  // NOTE(pag): if this condition is ever satisfied, then a bug has probably
  // been discovered.
  assert(false && "FIX: Bug has likely been discovered.");
  return false;
}

uint64_t Node<ProgramVectorUniqueRegion>::Hash(uint32_t) const {
  uint64_t hash = static_cast<unsigned>(this->OP::op) * 53;
  hash ^= (static_cast<unsigned>(vector->kind) + 1u) * 17;
  for (auto type : vector->col_types) {
    hash ^= RotateRight64(hash, 13) * (static_cast<unsigned>(type) + 11u);
  }
  return hash;
}

bool Node<ProgramVectorUniqueRegion>::Equals(EqualitySet &eq,
                                             Node<ProgramRegion> *that_,
                                             uint32_t) const noexcept {
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
  NOTE("TODO(ekilmer): Unimplemented merging of ProgramVectorUniqueRegion");
  assert(false);
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
  if (!that || num_tables != that->tables.Size()) {
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

  if (depth) {
    if ((!this->OP::body.get()) != (!that->OP::body.get())) {
      return false;
    }

    if (auto that_body = that->body.get(); that_body) {
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

      if (!body->Equals(eq, that_body, depth - 1u)) {
        return false;
      }
    }
  }
  return true;
}

const bool Node<ProgramTableJoinRegion>::MergeEqual(
    ProgramImpl *prog, std::vector<Node<ProgramRegion> *> &merges) {
  NOTE("TODO(ekilmer): Unimplemented merging of ProgramTableJoinRegion");
  assert(false);
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
  if (!that || num_tables != that->tables.Size()) {
    FAILED_EQ(that_);
    return false;
  }

  for (auto i = 0u; i < num_tables; ++i) {
    if (tables[i] != that->tables[i]) {
      FAILED_EQ(that_);
      return false;
    }
  }

  // Each table has a corresponding index.
  for (auto i = 0u; i < num_tables; ++i) {
    if (!eq.Contains(input_vecs[i], that->input_vecs[i])) {
      FAILED_EQ(that_);
      return false;
    }
  }

  if (depth) {
    if ((!this->OP::body.get()) != (!that->OP::body.get())) {
      return false;
    }

    if (auto that_body = that->OP::body.get(); that_body) {

      for (auto i = 0u; i < num_tables; ++i) {
        auto &vars_1 = output_vars[i];
        auto &vars_2 = that->output_vars[i];
        for (auto j = 0u, max_j = vars_1.Size(); j < max_j; ++j) {
          eq.Insert(vars_1[j], vars_2[j]);
        }
      }

      if (!this->OP::body->Equals(eq, that_body, depth - 1u)) {
        return false;
      }
    }
  }

  return true;
}

const bool Node<ProgramTableProductRegion>::MergeEqual(
    ProgramImpl *prog, std::vector<Node<ProgramRegion> *> &merges) {
  NOTE("TODO(ekilmer): Unimplemented merging of ProgramTableProductRegion");
  assert(false);
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
      index.get() != that->index.get()) {
    FAILED_EQ(that_);
    return false;
  }

  const auto num_vars = this->in_vars.Size();
  if (that->in_vars.Size() != num_vars) {

    // If the table and indices match, then that implies that the the number
    // of input variables must be the same, otherwise something is very wrong.
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

  // Table scans don't have bodies.
  assert(!this->body);
  assert(!that->body);

  eq.Insert(this->output_vector.get(), that->output_vector.get());

  return true;
}

const bool Node<ProgramTableScanRegion>::MergeEqual(
    ProgramImpl *prog, std::vector<Node<ProgramRegion> *> &merges) {
  NOTE("TODO(ekilmer): Unimplemented merging of ProgramTableScanRegion");
  assert(false);
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
  const auto num_lhs_vars = lhs_vars.Size();
  const auto num_rhs_vars = rhs_vars.Size();
  const auto that = op->AsTupleCompare();
  if (!that || cmp_op != that->cmp_op ||
      num_lhs_vars != that->lhs_vars.Size() ||
      num_rhs_vars != that->rhs_vars.Size()) {
    FAILED_EQ(that_);
    return false;
  }

  for (auto i = 0u; i < num_lhs_vars; ++i) {
    if (!eq.Contains(lhs_vars[i], that->lhs_vars[i])) {
      FAILED_EQ(that_);
      return false;
    }
  }

  for (auto i = 0u; i < num_rhs_vars; ++i) {
    if (!eq.Contains(rhs_vars[i], that->rhs_vars[i])) {
      FAILED_EQ(that_);
      return false;
    }
  }

  if (depth == 0) {
    return true;
  }

  if ((!this->OP::body.get()) != (!that->OP::body.get())) {
    return false;
  }

  if (auto that_body = that->OP::body.get(); that_body) {
    return this->OP::body->Equals(eq, that_body, depth - 1u);

  } else {
    return true;
  }
}

const bool Node<ProgramTupleCompareRegion>::MergeEqual(
    ProgramImpl *prog, std::vector<Node<ProgramRegion> *> &merges) {

  const auto num_vars = lhs_vars.Size();

  // New parallel region for merged bodies into 'this'
  auto new_par = prog->parallel_regions.Create(this);
  auto this_body = body.get();
  if (this_body) {
    new_par->regions.AddUse(this_body);
    this_body->parent = new_par;
  }
  body.Clear();
  body.Emplace(this, new_par);
  for (auto region : merges) {
    auto merge = region->AsOperation()->AsTupleCompare();
    assert(merge);  // These should all be the same type
    assert(merge->lhs_vars.Size() == num_vars);

    const auto merge_body = merge->body.get();
    if (merge_body) {
      new_par->regions.AddUse(merge_body);
      merge_body->parent = new_par;
    }
    merge->body.Clear();
    merge->parent = nullptr;
  }

  (void) num_vars;

  return true;
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

  if (this->empty_body) {
    hash ^= RotateRight64(hash, 53) * ~this->empty_body->Hash(depth - 1u);
  }

  return hash;
}

bool Node<ProgramGenerateRegion>::IsNoOp(void) const noexcept {
  if (functor.IsPure()) {
    if (body && empty_body) {
      return body->IsNoOp() && empty_body->IsNoOp();
    } else if (body) {
      return body->IsNoOp();
    } else if (empty_body) {
      return empty_body->IsNoOp();
    } else {
      return true;
    }
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
  auto used_vars_size = used_vars.Size();
  auto defined_vars_size = defined_vars.Size();
  if (!that || this->OP::op != that->OP::op || functor != that->functor ||
      used_vars_size != that->used_vars.Size() ||
      defined_vars_size != that->defined_vars.Size()) {
    FAILED_EQ(that_);
    return false;
  }

  for (auto i = 0u, max_i = used_vars_size; i < max_i; ++i) {
    if (!eq.Contains(used_vars[i], that->used_vars[i])) {
      FAILED_EQ(that_);
      return false;
    }
  }

  if (depth) {

    if ((!this->body.get()) != (!that->body.get())) {
      FAILED_EQ(that_);
      return false;
    }

    if ((!this->empty_body) != (!that->empty_body.get())) {
      FAILED_EQ(that_);
      return false;
    }

    // Check the `empty_body` before variable assignments; those assignments
    // don't exist in the empty body.
    if (auto that_empty_body = that->empty_body.get(); that_empty_body) {
      if (!empty_body->Equals(eq, that_empty_body, depth - 1u)) {
        return false;
      }
    }

    if (auto that_body = that->OP::body.get(); that_body) {
      for (auto i = 0u, max_i = defined_vars_size; i < max_i; ++i) {
        eq.Insert(defined_vars[i], that->defined_vars[i]);
      }

      if (!body->Equals(eq, that_body, depth - 1u)) {
        return false;
      }
    }
  }

  return true;
}

const bool Node<ProgramGenerateRegion>::MergeEqual(
    ProgramImpl *prog, std::vector<Node<ProgramRegion> *> &merges) {

  // New parallel region for merged `body` into 'this'
  const auto new_par = prog->parallel_regions.Create(this);
  if (auto body_ptr = body.get(); body_ptr) {
    body.Clear();
    body_ptr->parent = new_par;
    new_par->AddRegion(body_ptr);
  }

  // New parallel region for merged `empty_body` into 'this'
  const auto new_empty_par = prog->parallel_regions.Create(this);
  if (auto empty_body_ptr = empty_body.get(); empty_body_ptr) {
    empty_body.Clear();
    empty_body_ptr->parent = new_empty_par;
    new_empty_par->AddRegion(empty_body_ptr);
  }

  body.Emplace(this, new_par);
  empty_body.Emplace(this, new_empty_par);

  for (auto region : merges) {
    const auto merge = region->AsOperation()->AsGenerate();
    assert(merge);  // These should all be the same type
    assert(merge != this);

    if (auto merge_body_ptr = merge->body.get(); merge_body_ptr) {
      merge->body.Clear();
      merge_body_ptr->parent = new_par;
      new_par->AddRegion(merge_body_ptr);
    }

    if (auto merge_empty_body_ptr = merge->empty_body.get();
        merge_empty_body_ptr) {
      merge->empty_body.Clear();
      merge_empty_body_ptr->parent = new_empty_par;
      new_empty_par->AddRegion(merge_empty_body_ptr);
    }

    merge->parent = nullptr;

    // Replace all defined variables in the merge with this's defined variables
    for (auto i = 0u, max_i = defined_vars.Size(); i < max_i; ++i) {
      merge->defined_vars[i]->ReplaceAllUsesWith(defined_vars[i]);
    }
  }
  return true;
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

  if (this->false_body) {
    hash ^= RotateRight64(hash, 53) * ~this->false_body->Hash(depth - 1u);
  }

  return hash;
}

bool Node<ProgramCallRegion>::IsNoOp(void) const noexcept {
  return false;

  // NOTE(pag): Not really worth checking as even trivial procedures are
  //            treated as non-no-ops.
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
  if (!that || this->OP::op != that->OP::op) {
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

  if (depth) {

    if (!body != !that->body) {
      FAILED_EQ(that_);
      return false;
    }

    if (!false_body != !that->false_body) {
      FAILED_EQ(that_);
      return false;
    }

    // This function tests the true return value of the called procedure.
    if (body && !body->Equals(eq, that->body.get(), depth - 1)) {
      FAILED_EQ(that_);
      return false;
    }

    // This function tests the false return value of the called procedure.
    if (false_body && !false_body->Equals(eq, that->false_body.get(), depth - 1)) {
      FAILED_EQ(that_);
      return false;
    }
  }

  if (eq.Contains(this_called_proc, that_called_proc)) {
    return true;

  // The procedures don't appear to be the same, and we're not going deep,
  // so don't compare them.
  } else if (!depth) {
    return false;

  // Different functions are being called, check to see if their function bodies
  // are the same.
  } else {
    return this_called_proc->Equals(eq, that_called_proc, depth - 1);
  }
}

const bool Node<ProgramCallRegion>::MergeEqual(
    ProgramImpl *prog, std::vector<Node<ProgramRegion> *> &merges) {

  // New parallel region for merged `body` into 'this'
  auto new_par = prog->parallel_regions.Create(this);
  if (auto body_ptr = body.get(); body_ptr) {
    body_ptr->parent = new_par;
    new_par->AddRegion(body_ptr);
    body.Clear();
  }

  // New parallel region for merged `false_body` into 'this'
  auto new_false_par = prog->parallel_regions.Create(this);
  if (auto false_body_ptr = false_body.get(); false_body_ptr) {
    false_body_ptr->parent = new_false_par;
    new_false_par->AddRegion(false_body_ptr);
    false_body.Clear();
  }

  body.Emplace(this, new_par);
  false_body.Emplace(this, new_false_par);

  for (auto region : merges) {
    auto merge = region->AsOperation()->AsCall();
    assert(merge);  // These should all be the same type
    assert(merge != this);

    if (auto merge_body_ptr = merge->body.get(); merge_body_ptr) {
      merge_body_ptr->parent = new_par;
      new_par->AddRegion(merge_body_ptr);
      merge->body.Clear();
    }

    if (auto merge_false_body_ptr = merge->false_body.get();
        merge_false_body_ptr) {
      merge_false_body_ptr->parent = new_false_par;
      new_false_par->AddRegion(merge_false_body_ptr);
      merge->false_body.Clear();
    }

    merge->parent = nullptr;
  }
  return true;
}

Node<ProgramPublishRegion> *
Node<ProgramPublishRegion>::AsPublish(void) noexcept {
  return this;
}

uint64_t Node<ProgramPublishRegion>::Hash(uint32_t) const {
  uint64_t hash = static_cast<unsigned>(this->OP::op) * 53;
  hash ^= RotateRight64(hash, 17) * this->message.Id();

  for (auto var : this->arg_vars) {
    hash ^= RotateRight64(hash, 13) *
            ((static_cast<unsigned>(var->role) + 7u) *
             (static_cast<unsigned>(DataVariable(var).Type().Kind()) + 11u));
  }
  return hash;
}

// Returns `true` if `this` and `that` are structurally equivalent (after
// variable renaming) after searching down `depth` levels or until leaf,
// whichever is first, and where `depth` is 0, compare `this` to `that.
bool Node<ProgramPublishRegion>::Equals(EqualitySet &eq,
                                        Node<ProgramRegion> *that_,
                                        uint32_t) const noexcept {
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
  NOTE("TODO(ekilmer): Unimplemented merging of ProgramPublishRegion");
  assert(false);
  return false;
}

Node<ProgramReturnRegion> *Node<ProgramReturnRegion>::AsReturn(void) noexcept {
  return this;
}

// Returns `true` if all paths through `this` ends with a `return` region.
bool Node<ProgramReturnRegion>::EndsWithReturn(void) const noexcept {
  return true;
}

uint64_t Node<ProgramReturnRegion>::Hash(uint32_t) const {
  return static_cast<unsigned>(this->OP::op) * 53;
}

bool Node<ProgramReturnRegion>::IsNoOp(void) const noexcept {

  // NOTE(pag): This is a bit subtle, but basically, we'd like to be able to
  //            test if a procedure is a NOP, and to do so, we're really asking:
  //            does the procedure contain just a RETURN?
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
                                       uint32_t) const noexcept {
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
  NOTE("TODO(ekilmer): Unimplemented merging of ProgramReturnRegion");
  assert(false);
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

  if (!this->body != !that->body || !this->absent_body != !that->absent_body ||
      !this->unknown_body != !that->unknown_body) {
    FAILED_EQ(that_);
    return false;
  }

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
  NOTE("TODO(ekilmer): Unimplemented merging of ProgramCheckStateRegion");
  assert(false);
  return false;
}

}  // namespace hyde
