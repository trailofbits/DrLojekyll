// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/Util/BitManipulation.h>

// #include <iostream>

#include "Program.h"

#define FAILED_EQ(...)

// #define FAILED_EQ(that)
//   std::cerr << __LINE__ << ": " << this->containing_procedure->id
//             << " != " << that->containing_procedure->id << std::endl

namespace hyde {

ProgramOperationRegionImpl::~ProgramOperationRegionImpl(void) {}
ProgramCallRegionImpl::~ProgramCallRegionImpl(void) {}
ProgramReturnRegionImpl::~ProgramReturnRegionImpl(void) {}
ProgramTestAndSetRegionImpl::~ProgramTestAndSetRegionImpl(void) {}
ProgramGenerateRegionImpl::~ProgramGenerateRegionImpl(void) {}
ProgramModeSwitchRegionImpl::~ProgramModeSwitchRegionImpl(void) {}
ProgramLetBindingRegionImpl::~ProgramLetBindingRegionImpl(void) {}
ProgramWorkerIdRegionImpl::~ProgramWorkerIdRegionImpl(void) {}
ProgramPublishRegionImpl::~ProgramPublishRegionImpl(void) {}
ProgramChangeTupleRegionImpl::~ProgramChangeTupleRegionImpl(void) {}
ProgramChangeRecordRegionImpl::~ProgramChangeRecordRegionImpl(void) {}
ProgramCheckTupleRegionImpl::~ProgramCheckTupleRegionImpl(void) {}
ProgramCheckRecordRegionImpl::~ProgramCheckRecordRegionImpl(void) {}
ProgramTableJoinRegionImpl::~ProgramTableJoinRegionImpl(void) {}
ProgramTableProductRegionImpl::~ProgramTableProductRegionImpl(void) {}
ProgramTableScanRegionImpl::~ProgramTableScanRegionImpl(void) {}
ProgramTupleCompareRegionImpl::~ProgramTupleCompareRegionImpl(void) {}
ProgramVectorLoopRegionImpl::~ProgramVectorLoopRegionImpl(void) {}
ProgramVectorAppendRegionImpl::~ProgramVectorAppendRegionImpl(void) {}
ProgramVectorClearRegionImpl::~ProgramVectorClearRegionImpl(void) {}
ProgramVectorSwapRegionImpl::~ProgramVectorSwapRegionImpl(void) {}
ProgramVectorUniqueRegionImpl::~ProgramVectorUniqueRegionImpl(void) {}

ProgramOperationRegionImpl::ProgramOperationRegionImpl(REGION *parent_, ProgramOperation op_)
    : REGION(parent_),
      op(op_),
      body(this) {
  assert(parent_->Ancestor()->AsProcedure());
}

ProgramOperationRegionImpl *
ProgramOperationRegionImpl::AsOperation(void) noexcept {
  return this;
}

// Returns `true` if all paths through `this` ends with a `return` region.
bool ProgramOperationRegionImpl::EndsWithReturn(void) const noexcept {
  return false;
}

ProgramCallRegionImpl::ProgramCallRegionImpl(unsigned id_, REGION *parent_,
                                             ProgramProcedureImpl *called_proc_,
                                             ProgramOperation op_)
    : ProgramOperationRegionImpl(parent_, op_),
      called_proc(this, called_proc_),
      arg_vars(this),
      arg_vecs(this),
      false_body(this),
      id(id_) {}

ProgramCallRegionImpl *ProgramOperationRegionImpl::AsCall(void) noexcept {
  return nullptr;
}

ProgramReturnRegionImpl *
ProgramOperationRegionImpl::AsReturn(void) noexcept {
  return nullptr;
}

ProgramPublishRegionImpl *
ProgramOperationRegionImpl::AsPublish(void) noexcept {
  return nullptr;
}

ProgramVectorLoopRegionImpl *
ProgramOperationRegionImpl::AsVectorLoop(void) noexcept {
  return nullptr;
}

ProgramVectorAppendRegionImpl *
ProgramOperationRegionImpl::AsVectorAppend(void) noexcept {
  return nullptr;
}

ProgramVectorClearRegionImpl *
ProgramOperationRegionImpl::AsVectorClear(void) noexcept {
  return nullptr;
}

ProgramVectorSwapRegionImpl *
ProgramOperationRegionImpl::AsVectorSwap(void) noexcept {
  return nullptr;
}

ProgramVectorUniqueRegionImpl *
ProgramOperationRegionImpl::AsVectorUnique(void) noexcept {
  return nullptr;
}

ProgramWorkerIdRegionImpl *
ProgramOperationRegionImpl::AsWorkerId(void) noexcept {
  return nullptr;
}

ProgramModeSwitchRegionImpl *
ProgramOperationRegionImpl::AsModeSwitch(void) noexcept {
  return nullptr;
}

ProgramLetBindingRegionImpl *
ProgramOperationRegionImpl::AsLetBinding(void) noexcept {
  return nullptr;
}

ProgramChangeTupleRegionImpl *
ProgramOperationRegionImpl::AsChangeTuple(void) noexcept {
  return nullptr;
}

ProgramChangeRecordRegionImpl *
ProgramOperationRegionImpl::AsChangeRecord(void) noexcept {
  return nullptr;
}

ProgramCheckTupleRegionImpl *
ProgramOperationRegionImpl::AsCheckTuple(void) noexcept {
  return nullptr;
}

ProgramCheckRecordRegionImpl *
ProgramOperationRegionImpl::AsCheckRecord(void) noexcept {
  return nullptr;
}

ProgramTableJoinRegionImpl *
ProgramOperationRegionImpl::AsTableJoin(void) noexcept {
  return nullptr;
}

ProgramTableProductRegionImpl *
ProgramOperationRegionImpl::AsTableProduct(void) noexcept {
  return nullptr;
}

ProgramTableScanRegionImpl *
ProgramOperationRegionImpl::AsTableScan(void) noexcept {
  return nullptr;
}

ProgramTestAndSetRegionImpl *
ProgramOperationRegionImpl::AsTestAndSet(void) noexcept {
  return nullptr;
}

ProgramGenerateRegionImpl *
ProgramOperationRegionImpl::AsGenerate(void) noexcept {
  return nullptr;
}

ProgramTupleCompareRegionImpl *
ProgramOperationRegionImpl::AsTupleCompare(void) noexcept {
  return nullptr;
}

// -----------------------------------------------------------------------------

bool ProgramVectorLoopRegionImpl::Equals(EqualitySet &eq,
                                           REGION *that_,
                                           uint32_t depth) const noexcept {
  const auto that_op = that_->AsOperation();
  if (!that_op) {
    FAILED_EQ(that_);
    return false;
  }

  const auto that = that_op->AsVectorLoop();
  const auto def_vars_size = defined_vars.Size();
  if (!that || !eq.Contains(vector.get(), that->vector.get()) ||
      induction_table.get() != that->induction_table.get() ||
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

const bool ProgramVectorLoopRegionImpl::MergeEqual(
    ProgramImpl *impl, std::vector<REGION *> &merges) {

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

ProgramVectorLoopRegionImpl *
ProgramVectorLoopRegionImpl::AsVectorLoop(void) noexcept {
  return this;
}

uint64_t ProgramVectorLoopRegionImpl::Hash(uint32_t depth) const {
  uint64_t hash = static_cast<unsigned>(this->OP::op) * 53;
  hash ^= RotateRight64(hash, 13) * (static_cast<unsigned>(vector->kind) + 17u);
  if (auto table = induction_table.get()) {
    hash ^= RotateRight64(hash, 13) * (table->id + 13u);
  }
  for (auto type : vector->col_types) {
    hash ^= RotateRight64(hash, 13) * (static_cast<unsigned>(type.Kind()) + 11u);
  }
  if (depth == 0) {
    return hash;
  }

  if (this->OP::body) {
    hash ^= RotateRight64(hash, 13) * this->OP::body->Hash(depth - 1u);
  }
  return hash;
}

bool ProgramVectorLoopRegionImpl::IsNoOp(void) const noexcept {
  return !this->OP::body || this->OP::body->IsNoOp();
}

// -----------------------------------------------------------------------------

ProgramWorkerIdRegionImpl *
ProgramWorkerIdRegionImpl::AsWorkerId(void) noexcept {
  return this;
}

uint64_t ProgramWorkerIdRegionImpl::Hash(uint32_t depth) const {
  uint64_t hash = static_cast<unsigned>(this->OP::op) * 53;
  for (auto var : hashed_vars) {
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

bool ProgramWorkerIdRegionImpl::Equals(EqualitySet &eq,
                                         REGION *that_,
                                         uint32_t depth) const noexcept {
  const auto that_op = that_->AsOperation();
  if (!that_op) {
    FAILED_EQ(that_);
    return false;
  }

  const auto that = that_op->AsWorkerId();
  const auto num_hashed_vars = hashed_vars.Size();
  if (!that || num_hashed_vars != that->hashed_vars.Size()) {
    FAILED_EQ(that_);
    return false;
  }

  for (auto i = 0u; i < num_hashed_vars; ++i) {
    if (!eq.Contains(hashed_vars[i], that->hashed_vars[i])) {
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
    eq.Insert(worker_id.get(), that->worker_id.get());

    return this->OP::body->Equals(eq, that_body, depth - 1u);

  } else {
    return true;
  }
}

const bool ProgramWorkerIdRegionImpl::MergeEqual(
    ProgramImpl *prog, std::vector<REGION *> &merges) {

  const auto num_hashed_vars = hashed_vars.Size();
  (void) num_hashed_vars;

  auto par = prog->parallel_regions.Create(this);
  if (auto curr_body = body.get(); curr_body) {
    curr_body->parent = par;
    par->AddRegion(curr_body);
    body.Clear();
  }

  body.Emplace(this, par);

  for (auto merge : merges) {
    auto merged_worker_id = merge->AsOperation()->AsWorkerId();
    assert(merged_worker_id != nullptr);
    assert(merged_worker_id != this);
    assert(merged_worker_id->hashed_vars.Size() == num_hashed_vars);

    merged_worker_id->worker_id->ReplaceAllUsesWith(worker_id.get());

    if (auto merged_body = merged_worker_id->body.get(); merged_body) {
      merged_body->parent = par;
      par->AddRegion(merged_body);
      merged_worker_id->body.Clear();
    }

    merged_worker_id->hashed_vars.Clear();
    merged_worker_id->parent = nullptr;
  }

  return true;
}

bool ProgramWorkerIdRegionImpl::IsNoOp(void) const noexcept {
  return !this->OP::body || this->OP::body->IsNoOp();
}

// -----------------------------------------------------------------------------

ProgramModeSwitchRegionImpl *
ProgramModeSwitchRegionImpl::AsModeSwitch(void) noexcept {
  return this;
}

uint64_t ProgramModeSwitchRegionImpl::Hash(uint32_t depth) const {
  uint64_t hash = static_cast<unsigned>(this->OP::op) * 53;
  hash ^= RotateRight64(hash, 13) *
          static_cast<unsigned>(this->new_mode) * 17;

  if (depth == 0) {
    return hash;
  }

  if (this->OP::body) {
    hash ^= RotateRight64(hash, 13) * this->OP::body->Hash(depth - 1u);
  }
  return hash;
}

bool ProgramModeSwitchRegionImpl::IsNoOp(void) const noexcept {
  return !this->OP::body || this->OP::body->IsNoOp();
}

bool ProgramModeSwitchRegionImpl::Equals(EqualitySet &eq,
                                           REGION *that_,
                                           uint32_t depth) const noexcept {
  const auto that_op = that_->AsOperation();
  if (!that_op) {
    FAILED_EQ(that_);
    return false;
  }

  const auto that = that_op->AsModeSwitch();
  if (!that || that->new_mode != this->new_mode) {
    FAILED_EQ(that_);
    return false;
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

const bool ProgramModeSwitchRegionImpl::MergeEqual(
    ProgramImpl *prog, std::vector<REGION *> &merges) {

  auto par = prog->parallel_regions.Create(this);
  if (auto curr_body = body.get(); curr_body) {
    curr_body->parent = par;
    par->AddRegion(curr_body);
    body.Clear();
  }

  body.Emplace(this, par);

  for (auto merge : merges) {
    auto merged_ms = merge->AsOperation()->AsModeSwitch();
    assert(merged_ms != nullptr);
    assert(merged_ms != this);
    assert(merged_ms->new_mode == this->new_mode);

    if (auto merged_body = merged_ms->body.get(); merged_body) {
      merged_body->parent = par;
      par->AddRegion(merged_body);
      merged_ms->body.Clear();
    }

    merged_ms->parent = nullptr;
  }

  return true;
}

// -----------------------------------------------------------------------------

ProgramLetBindingRegionImpl *
ProgramLetBindingRegionImpl::AsLetBinding(void) noexcept {
  return this;
}

uint64_t ProgramLetBindingRegionImpl::Hash(uint32_t depth) const {
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

bool ProgramLetBindingRegionImpl::IsNoOp(void) const noexcept {
  return !this->OP::body || this->OP::body->IsNoOp();
}

bool ProgramLetBindingRegionImpl::Equals(EqualitySet &eq,
                                           REGION *that_,
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

const bool ProgramLetBindingRegionImpl::MergeEqual(
    ProgramImpl *prog, std::vector<REGION *> &merges) {

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

// -----------------------------------------------------------------------------

uint64_t ProgramVectorAppendRegionImpl::Hash(uint32_t depth) const {
  uint64_t hash = static_cast<unsigned>(this->OP::op) * 53;
  hash ^= RotateRight64(hash, 13) * (static_cast<unsigned>(vector->kind) + 17u);
  for (auto type : vector->col_types) {
    hash ^= RotateRight64(hash, 13) * (static_cast<unsigned>(type.Kind()) + 11u);
  }
  for (auto var : tuple_vars) {
    hash ^= RotateRight64(hash, 13) *
            ((static_cast<unsigned>(var->role) + 7u) *
             (static_cast<unsigned>(DataVariable(var).Type().Kind()) + 11u));
  }
  if (auto wid = worker_id.get(); wid) {
    hash ^= RotateRight64(hash, 17) *
            ((static_cast<unsigned>(wid->role) + 7u) *
             (static_cast<unsigned>(DataVariable(wid).Type().Kind()) + 13u));
  }
  (void) depth;
  return hash;
}

bool ProgramVectorAppendRegionImpl::Equals(EqualitySet &eq,
                                             REGION *that_,
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

  if (!eq.Contains(worker_id.get(), that->worker_id.get())) {
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

const bool ProgramVectorAppendRegionImpl::MergeEqual(
    ProgramImpl *prog, std::vector<REGION *> &merges) {

  assert(false && "Likely a bug has occurred somewhere");

  // NOTE(pag): This should probably always return false because this should be
  // covered by the CSE over REGIONs.
  return false;
}

ProgramVectorAppendRegionImpl *
ProgramVectorAppendRegionImpl::AsVectorAppend(void) noexcept {
  return this;
}

// -----------------------------------------------------------------------------

ProgramChangeTupleRegionImpl *
ProgramChangeTupleRegionImpl::AsChangeTuple(void) noexcept {
  return this;
}

uint64_t ProgramChangeTupleRegionImpl::Hash(uint32_t depth) const {
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

  if (this->failed_body) {
    hash ^= RotateRight64(hash, 53) * ~this->failed_body->Hash(depth - 1u);
  }

  return hash;
}

bool ProgramChangeTupleRegionImpl::IsNoOp(void) const noexcept {
  assert(!col_values.Empty());
  return false;
}

bool ProgramChangeTupleRegionImpl::Equals(EqualitySet &eq,
                                                REGION *that_,
                                                uint32_t depth) const noexcept {
  const auto that_op = that_->AsOperation();
  if (!that_op) {
    FAILED_EQ(that_);
    return false;
  }

  const auto that = that_op->AsChangeTuple();
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

  if (!body != !(that->body) || !failed_body != !(that->failed_body)) {
    return false;
  }

  if (body && !body->Equals(eq, that->body.get(), depth - 1u)) {
    return false;
  }

  if (failed_body &&
      !failed_body->Equals(eq, that->failed_body.get(), depth - 1u)) {
    return false;
  }

  return true;
}

// Returns `true` if all paths through `this` ends with a `return` region.
bool ProgramChangeTupleRegionImpl::EndsWithReturn(void) const noexcept {
  if (body && failed_body) {
    return body->EndsWithReturn() && failed_body->EndsWithReturn();
  } else {
    return false;
  }
}

const bool ProgramChangeTupleRegionImpl::MergeEqual(
    ProgramImpl *prog, std::vector<REGION *> &merges) {

  // The implication is that two regions wanted to do the same transition. If
  // we are doing code gen, the that likely means one region is serialized
  // before the other, and so there is a kind of race condition, where only
  // one of them is likely to execute and the other will never execute.
  assert(false);
  comment =
      "!!! STRIP MINING " + std::to_string(reinterpret_cast<uintptr_t>(this));
  for (auto region : merges) {
    region->comment = "??? STRIP MINING WITH " +
                      std::to_string(reinterpret_cast<uintptr_t>(this));
  }

  assert(false &&
         "Probable error when trying to strip mine program state transitions");

  // New parallel region for merged bodies into 'this'
  auto new_par = prog->parallel_regions.Create(this);
  auto new_failed_par = prog->parallel_regions.Create(this);

  if (auto transition_body = body.get(); transition_body) {
    transition_body->parent = new_par;
    new_par->AddRegion(transition_body);
    body.Clear();
  }

  if (auto transition_failed_body = failed_body.get(); transition_failed_body) {
    transition_failed_body->parent = new_failed_par;
    new_failed_par->AddRegion(transition_failed_body);
    failed_body.Clear();
  }

  body.Emplace(this, new_par);
  failed_body.Emplace(this, new_failed_par);

  for (auto region : merges) {
    auto merge = region->AsOperation()->AsChangeTuple();
    assert(merge);  // These should all be the same type
    assert(merge != this);

    if (const auto merge_body = merge->body.get(); merge_body) {
      merge_body->parent = new_par;
      new_par->AddRegion(merge_body);
      merge->body.Clear();
    }

    if (const auto merge_failed_body = merge->failed_body.get();
        merge_failed_body) {
      merge_failed_body->parent = new_failed_par;
      new_failed_par->AddRegion(merge_failed_body);
      merge->failed_body.Clear();
    }

    merge->parent = nullptr;
  }
  return true;
}

// -----------------------------------------------------------------------------

ProgramChangeRecordRegionImpl *
ProgramChangeRecordRegionImpl::AsChangeRecord(void) noexcept {
  return this;
}

uint64_t ProgramChangeRecordRegionImpl::Hash(uint32_t depth) const {
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

  if (this->failed_body) {
    hash ^= RotateRight64(hash, 53) * ~this->failed_body->Hash(depth - 1u);
  }

  return hash;
}

bool ProgramChangeRecordRegionImpl::IsNoOp(void) const noexcept {
  assert(!col_values.Empty());
  assert(!record_vars.Empty());
  return false;
}

bool ProgramChangeRecordRegionImpl::Equals(EqualitySet &eq,
                                             REGION *that_,
                                             uint32_t depth) const noexcept {
  const auto that_op = that_->AsOperation();
  if (!that_op) {
    FAILED_EQ(that_);
    return false;
  }

  const auto that = that_op->AsChangeRecord();
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

  if (!body != !(that->body) || !failed_body != !(that->failed_body)) {
    return false;
  }

  // Mark the read record variables as equivalent.
  for (auto i= 0u, max_i = record_vars.Size(); i < max_i; ++i) {
    eq.Insert(record_vars[i], that->record_vars[i]);
  }

  if (body && !body->Equals(eq, that->body.get(), depth - 1u)) {
    return false;
  }

  if (failed_body &&
      !failed_body->Equals(eq, that->failed_body.get(), depth - 1u)) {
    return false;
  }

  return true;
}

// Returns `true` if all paths through `this` ends with a `return` region.
bool ProgramChangeRecordRegionImpl::EndsWithReturn(void) const noexcept {
  if (body && failed_body) {
    return body->EndsWithReturn() && failed_body->EndsWithReturn();
  } else {
    return false;
  }
}

const bool ProgramChangeRecordRegionImpl::MergeEqual(
    ProgramImpl *prog, std::vector<REGION *> &merges) {

  // The implication is that two regions wanted to do the same transition. If
  // we are doing code gen, the that likely means one region is serialized
  // before the other, and so there is a kind of race condition, where only
  // one of them is likely to execute and the other will never execute.
  assert(false);
  comment =
      "!!! STRIP MINING " + std::to_string(reinterpret_cast<uintptr_t>(this));
  for (auto region : merges) {
    region->comment = "??? STRIP MINING WITH " +
                      std::to_string(reinterpret_cast<uintptr_t>(this));
  }

  assert(false &&
         "Probable error when trying to strip mine program state emplaces");

  // New parallel region for merged bodies into 'this'
  auto new_par = prog->parallel_regions.Create(this);
  auto new_failed_par = prog->parallel_regions.Create(this);

  if (auto transition_body = body.get(); transition_body) {
    transition_body->parent = new_par;
    new_par->AddRegion(transition_body);
    body.Clear();
  }

  if (auto transition_failed_body = failed_body.get(); transition_failed_body) {
    transition_failed_body->parent = new_failed_par;
    new_failed_par->AddRegion(transition_failed_body);
    failed_body.Clear();
  }

  body.Emplace(this, new_par);
  failed_body.Emplace(this, new_failed_par);

  for (auto region : merges) {
    auto merge = region->AsOperation()->AsChangeRecord();
    assert(merge);  // These should all be the same type
    assert(merge != this);

    if (const auto merge_body = merge->body.get(); merge_body) {
      merge_body->parent = new_par;
      new_par->AddRegion(merge_body);
      merge->body.Clear();
    }

    if (const auto merge_failed_body = merge->failed_body.get();
        merge_failed_body) {
      merge_failed_body->parent = new_failed_par;
      new_failed_par->AddRegion(merge_failed_body);
      merge->failed_body.Clear();
    }

    merge->parent = nullptr;
  }
  return true;
}

// -----------------------------------------------------------------------------

uint64_t ProgramTestAndSetRegionImpl::Hash(uint32_t depth) const {
  uint64_t hash = static_cast<unsigned>(this->OP::op) * 53;
  hash ^=
      RotateRight64(hash, 13) *
      ((static_cast<unsigned>(accumulator->role) + 7u) *
       (static_cast<unsigned>(DataVariable(accumulator.get()).Type().Kind()) +
        11u));

  hash ^=
      RotateRight64(hash, 12) *
      ((static_cast<unsigned>(displacement->role) + 7u) *
       (static_cast<unsigned>(DataVariable(displacement.get()).Type().Kind()) +
        12u));

  hash ^=
      RotateRight64(hash, 11) *
      ((static_cast<unsigned>(comparator->role) + 7u) *
       (static_cast<unsigned>(DataVariable(comparator.get()).Type().Kind()) +
        13u));

  if (depth == 0) {
    return hash;
  }

  if (this->OP::body) {
    hash ^= RotateRight64(hash, 13) * this->OP::body->Hash(depth - 1u);
  }
  return hash;
}

bool ProgramTestAndSetRegionImpl::IsNoOp(void) const noexcept {
  return false;
}

bool ProgramTestAndSetRegionImpl::Equals(EqualitySet &eq,
                                           REGION *that_,
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

const bool ProgramTestAndSetRegionImpl::MergeEqual(
    ProgramImpl *prog, std::vector<REGION *> &merges) {
  NOTE("TODO(ekilmer): Unimplemented merging of TestAndSetRegion");
  assert(false);
  return false;
}

// -----------------------------------------------------------------------------

ProgramTestAndSetRegionImpl *
ProgramTestAndSetRegionImpl::AsTestAndSet(void) noexcept {
  return this;
}

ProgramTableJoinRegionImpl *
ProgramTableJoinRegionImpl::AsTableJoin(void) noexcept {
  return this;
}

ProgramTableProductRegionImpl *
ProgramTableProductRegionImpl::AsTableProduct(void) noexcept {
  return this;
}

ProgramTableScanRegionImpl *
ProgramTableScanRegionImpl::AsTableScan(void) noexcept {
  return this;
}

// -----------------------------------------------------------------------------

uint64_t ProgramVectorClearRegionImpl::Hash(uint32_t) const {
  uint64_t hash = static_cast<unsigned>(this->OP::op) * 53;
  hash ^= (static_cast<unsigned>(vector->kind) + 1u) * 17;
  for (auto type : vector->col_types) {
    hash ^= RotateRight64(hash, 13) * (static_cast<unsigned>(type.Kind()) + 11u);
  }
  return hash;
}

bool ProgramVectorClearRegionImpl::Equals(EqualitySet &eq,
                                            REGION *that_,
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

const bool ProgramVectorClearRegionImpl::MergeEqual(
    ProgramImpl *prog, std::vector<REGION *> &merges) {
  NOTE("TODO(ekilmer): Unimplemented merging of ProgramVectorClearRegion");
  assert(false);
  return false;
}

ProgramVectorClearRegionImpl *
ProgramVectorClearRegionImpl::AsVectorClear(void) noexcept {
  return this;
}

// -----------------------------------------------------------------------------

ProgramVectorSwapRegionImpl *
ProgramVectorSwapRegionImpl::AsVectorSwap(void) noexcept {
  return this;
}

uint64_t ProgramVectorSwapRegionImpl::Hash(uint32_t depth) const {
  uint64_t hash = static_cast<unsigned>(this->OP::op) * 53;
  hash ^= (static_cast<unsigned>(lhs->kind) + 1u) * 17;
  hash ^= (static_cast<unsigned>(rhs->kind) + 1u) * 17;
  for (auto type : lhs->col_types) {
    hash ^= RotateRight64(hash, 13) * (static_cast<unsigned>(type.Kind()) + 11u);
  }
  (void) depth;
  return hash;
}

bool ProgramVectorSwapRegionImpl::Equals(EqualitySet &eq,
                                           REGION *that_,
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

const bool ProgramVectorSwapRegionImpl::MergeEqual(
    ProgramImpl *prog, std::vector<REGION *> &merges) {

  // NOTE(pag): if this condition is ever satisfied, then a bug has probably
  // been discovered.
  assert(false && "FIX: Bug has likely been discovered.");
  return false;
}

// -----------------------------------------------------------------------------

uint64_t ProgramVectorUniqueRegionImpl::Hash(uint32_t) const {
  uint64_t hash = static_cast<unsigned>(this->OP::op) * 53;
  hash ^= (static_cast<unsigned>(vector->kind) + 1u) * 17;
  for (auto type : vector->col_types) {
    hash ^= RotateRight64(hash, 13) * (static_cast<unsigned>(type.Kind()) + 11u);
  }
  return hash;
}

bool ProgramVectorUniqueRegionImpl::Equals(EqualitySet &eq,
                                             REGION *that_,
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

const bool ProgramVectorUniqueRegionImpl::MergeEqual(
    ProgramImpl *prog, std::vector<REGION *> &merges) {
  NOTE("TODO(ekilmer): Unimplemented merging of ProgramVectorUniqueRegion");
  assert(false);
  return false;
}

ProgramVectorUniqueRegionImpl *
ProgramVectorUniqueRegionImpl::AsVectorUnique(void) noexcept {
  return this;
}

// -----------------------------------------------------------------------------

uint64_t ProgramTableJoinRegionImpl::Hash(uint32_t depth) const {
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

bool ProgramTableJoinRegionImpl::IsNoOp(void) const noexcept {
  return !this->OP::body || this->OP::body->IsNoOp();
}

bool ProgramTableJoinRegionImpl::Equals(EqualitySet &eq,
                                          REGION *that_,
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
    if (index_of_index[i] != that->index_of_index[i]) {
      FAILED_EQ(that_);
      return false;
    }
  }

  // NOTE(pag): There might be fewer indices than tables.
  for (auto i = 0u, max_i = indices.Size(); i < max_i; ++i) {
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

const bool ProgramTableJoinRegionImpl::MergeEqual(
    ProgramImpl *prog, std::vector<REGION *> &merges) {
  NOTE("TODO(ekilmer): Unimplemented merging of ProgramTableJoinRegion");
  assert(false);
  return false;
}

// -----------------------------------------------------------------------------

bool ProgramTableProductRegionImpl::IsNoOp(void) const noexcept {
  return !this->OP::body || this->OP::body->IsNoOp();
}

uint64_t ProgramTableProductRegionImpl::Hash(uint32_t depth) const {
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

bool ProgramTableProductRegionImpl::Equals(EqualitySet &eq,
                                             REGION *that_,
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

const bool ProgramTableProductRegionImpl::MergeEqual(
    ProgramImpl *prog, std::vector<REGION *> &merges) {
  NOTE("TODO(ekilmer): Unimplemented merging of ProgramTableProductRegion");
  assert(false);
  return false;
}

// -----------------------------------------------------------------------------

uint64_t ProgramTableScanRegionImpl::Hash(uint32_t depth) const {
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

bool ProgramTableScanRegionImpl::IsNoOp(void) const noexcept {
  return !this->OP::body || this->OP::body->IsNoOp();
}

bool ProgramTableScanRegionImpl::Equals(EqualitySet &eq,
                                          REGION *that_,
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

  if (depth) {
    for (auto i = 0u, max_i = this->out_vars.Size(); i < max_i; ++i) {
      eq.Insert(this->out_vars[i], that->out_vars[i]);
    }

    if (!this->OP::body != !that->OP::body) {
      return false;
    }

    if (this->OP::body) {
      return this->OP::body->Equals(eq, that->OP::body.get(), depth - 1u);
    }
  }

  return true;
}

const bool ProgramTableScanRegionImpl::MergeEqual(
    ProgramImpl *impl, std::vector<REGION *> &merges) {

  auto par = impl->parallel_regions.Create(this);

  if (auto body_ptr = this->OP::body.get()) {
    this->OP::body.Clear();
    body_ptr->parent = par;
    par->AddRegion(body_ptr);
  }

  this->OP::body.Emplace(this, par);

  const auto num_defined_vars = out_vars.Size();

  for (auto merge : merges) {
    TABLESCAN * const merged_scan = merge->AsOperation()->AsTableScan();
    assert(merged_scan != nullptr);
    assert(merged_scan != this);
    assert(merged_scan->out_vars.Size() == num_defined_vars);

    for (auto i = 0u; i < num_defined_vars; ++i) {
      merged_scan->out_vars[i]->ReplaceAllUsesWith(out_vars[i]);
    }

    if (auto merged_body = merged_scan->body.get(); merged_body) {
      merged_body->parent = par;
      par->AddRegion(merged_body);
      merged_scan->body.Clear();
    }

    merged_scan->in_vars.Clear();
    merged_scan->out_vars.Clear();
    merged_scan->table.Clear();
    merged_scan->index.Clear();
    merged_scan->parent = nullptr;
  }

  return true;

}

// -----------------------------------------------------------------------------

uint64_t ProgramTupleCompareRegionImpl::Hash(uint32_t depth) const {
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
  if (this->false_body) {
    hash ^= RotateRight64(hash, 53) * ~this->false_body->Hash(depth - 1u);
  }
  return hash;
}

ProgramTupleCompareRegionImpl *
ProgramTupleCompareRegionImpl::AsTupleCompare(void) noexcept {
  return this;
}

// Returns `true` if all paths through `this` ends with a `return` region.
bool ProgramTupleCompareRegionImpl::EndsWithReturn(void) const noexcept {
  if (body && false_body) {
    return body->EndsWithReturn() && false_body->EndsWithReturn();
  } else {
    return false;
  }
}

bool ProgramTupleCompareRegionImpl::IsNoOp(void) const noexcept {
  if (body && !body->IsNoOp()) {
    return false;
  } else if (false_body && !false_body->IsNoOp()) {
    return false;
  } else {
    return true;
  }
}

bool ProgramTupleCompareRegionImpl::Equals(EqualitySet &eq,
                                             REGION *that_,
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

  if ((!this->false_body.get()) != (!that->false_body.get())) {
    return false;
  }

  if (auto that_body = that->OP::body.get();
      that_body && !body->Equals(eq, that_body, depth - 1u)) {
    return false;
  }

  if (auto that_false_body = that->false_body.get();
      that_false_body && !false_body->Equals(eq, that_false_body, depth - 1u)) {
    return false;
  }

  return true;
}

const bool ProgramTupleCompareRegionImpl::MergeEqual(
    ProgramImpl *prog, std::vector<REGION *> &merges) {

  const auto num_vars = lhs_vars.Size();

  // New parallel region for merged bodies into 'this'
  auto new_par = prog->parallel_regions.Create(this);
  auto new_false_par = prog->parallel_regions.Create(this);

  if (auto this_body = body.get(); this_body) {
    assert(this_body->parent == this);
    assert(!this_body->EndsWithReturn());
    this_body->parent = new_par;
    new_par->AddRegion(this_body);
    body.Clear();
  }

  if (auto this_false_body = false_body.get(); this_false_body) {
    assert(this_false_body->parent == this);
    assert(!this_false_body->EndsWithReturn());
    this_false_body->parent = new_false_par;
    new_false_par->AddRegion(this_false_body);
    false_body.Clear();
  }

  body.Emplace(this, new_par);
  false_body.Emplace(this, new_false_par);

  for (auto region : merges) {
    auto merge = region->AsOperation()->AsTupleCompare();
    assert(merge);  // These should all be the same type
    assert(merge != this);
    assert(merge->lhs_vars.Size() == num_vars);

    if (const auto merge_body = merge->body.get(); merge_body) {
      assert(merge_body->parent == merge);
      assert(!merge_body->EndsWithReturn());
      merge_body->parent = new_par;
      new_par->AddRegion(merge_body);
      merge->body.Clear();
    }

    if (const auto merge_false_body = merge->false_body.get();
        merge_false_body) {
      assert(merge_false_body->parent == merge);
      assert(!merge_false_body->EndsWithReturn());
      merge_false_body->parent = new_false_par;
      new_false_par->AddRegion(merge_false_body);
      merge->false_body.Clear();
    }

    merge->lhs_vars.Clear();
    merge->rhs_vars.Clear();
    merge->parent = nullptr;
  }

  (void) num_vars;

  return true;
}

// -----------------------------------------------------------------------------

ProgramGenerateRegionImpl *
ProgramGenerateRegionImpl::AsGenerate(void) noexcept {
  return this;
}

// Returns `true` if all paths through `this` ends with a `return` region.
bool ProgramGenerateRegionImpl::EndsWithReturn(void) const noexcept {
  if (body && empty_body) {
    return body->EndsWithReturn() && empty_body->EndsWithReturn();
  } else {
    return false;
  }
}

uint64_t ProgramGenerateRegionImpl::Hash(uint32_t depth) const {
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

bool ProgramGenerateRegionImpl::IsNoOp(void) const noexcept {
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
bool ProgramGenerateRegionImpl::Equals(EqualitySet &eq,
                                         REGION *that_,
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

const bool ProgramGenerateRegionImpl::MergeEqual(
    ProgramImpl *prog, std::vector<REGION *> &merges) {

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

// -----------------------------------------------------------------------------

ProgramCallRegionImpl *ProgramCallRegionImpl::AsCall(void) noexcept {
  return this;
}

// Returns `true` if all paths through `this` ends with a `return` region.
bool ProgramCallRegionImpl::EndsWithReturn(void) const noexcept {
  if (body && false_body) {
    return body->EndsWithReturn() && false_body->EndsWithReturn();
  } else {
    return false;
  }
}

uint64_t ProgramCallRegionImpl::Hash(uint32_t depth) const {
  uint64_t hash = static_cast<unsigned>(this->OP::op) * 53;

  for (auto var : this->arg_vars) {
    hash ^= RotateRight64(hash, 13) *
            ((static_cast<unsigned>(var->role) + 7u) *
             (static_cast<unsigned>(DataVariable(var).Type().Kind()) + 11u));
  }

  for (auto vec : this->arg_vecs) {
    hash ^= RotateRight64(hash, 13) * (static_cast<unsigned>(vec->kind) + 7u);
    for (auto type : vec->col_types) {
      hash ^= RotateRight64(hash, 7) * (static_cast<unsigned>(type.Kind()) + 3u);
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

bool ProgramCallRegionImpl::IsNoOp(void) const noexcept {
  return false;

  // NOTE(pag): Not really worth checking as even trivial procedures are
  //            treated as non-no-ops.
}

// Returns `true` if `this` and `that` are structurally equivalent (after
// variable renaming) after searching down `depth` levels or until leaf,
// whichever is first, and where `depth` is 0, compare `this` to `that.
bool ProgramCallRegionImpl::Equals(EqualitySet &eq,
                                     REGION *that_,
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
    if (false_body &&
        !false_body->Equals(eq, that->false_body.get(), depth - 1)) {
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

const bool ProgramCallRegionImpl::MergeEqual(
    ProgramImpl *prog, std::vector<REGION *> &merges) {

  // New parallel region for merged `body` into 'this'
  auto new_par = prog->parallel_regions.Create(this);
  if (auto body_ptr = body.get(); body_ptr) {
    assert(body_ptr->parent == this);
    body_ptr->parent = new_par;
    new_par->AddRegion(body_ptr);
    body.Clear();
  }

  // New parallel region for merged `false_body` into 'this'
  auto new_false_par = prog->parallel_regions.Create(this);
  if (auto false_body_ptr = false_body.get(); false_body_ptr) {
    assert(false_body_ptr->parent == this);
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
      assert(merge_body_ptr->parent == merge);
      merge_body_ptr->parent = new_par;
      new_par->AddRegion(merge_body_ptr);
      merge->body.Clear();
    }

    if (auto merge_false_body_ptr = merge->false_body.get();
        merge_false_body_ptr) {
      assert(merge_false_body_ptr->parent == merge);
      merge_false_body_ptr->parent = new_false_par;
      new_false_par->AddRegion(merge_false_body_ptr);
      merge->false_body.Clear();
    }

    merge->parent = nullptr;
  }
  return true;
}

// -----------------------------------------------------------------------------

ProgramPublishRegionImpl *
ProgramPublishRegionImpl::AsPublish(void) noexcept {
  return this;
}

uint64_t ProgramPublishRegionImpl::Hash(uint32_t) const {
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
bool ProgramPublishRegionImpl::Equals(EqualitySet &eq,
                                        REGION *that_,
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

const bool ProgramPublishRegionImpl::MergeEqual(
    ProgramImpl *prog, std::vector<REGION *> &merges) {
  NOTE("TODO(ekilmer): Unimplemented merging of ProgramPublishRegion");
  assert(false);
  return false;
}

ProgramReturnRegionImpl *ProgramReturnRegionImpl::AsReturn(void) noexcept {
  return this;
}

// -----------------------------------------------------------------------------

// Returns `true` if all paths through `this` ends with a `return` region.
bool ProgramReturnRegionImpl::EndsWithReturn(void) const noexcept {
  return true;
}

uint64_t ProgramReturnRegionImpl::Hash(uint32_t) const {
  return static_cast<unsigned>(this->OP::op) * 53;
}

bool ProgramReturnRegionImpl::IsNoOp(void) const noexcept {

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
bool ProgramReturnRegionImpl::Equals(EqualitySet &eq,
                                       REGION *that_,
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

const bool ProgramReturnRegionImpl::MergeEqual(
    ProgramImpl *prog, std::vector<REGION *> &merges) {
  NOTE("TODO(ekilmer): Unimplemented merging of ProgramReturnRegion");
  assert(false);
  return false;
}

// -----------------------------------------------------------------------------

ProgramCheckTupleRegionImpl *
ProgramCheckTupleRegionImpl::AsCheckTuple(void) noexcept {
  return this;
}

// Returns `true` if all paths through `this` ends with a `return` region.
bool ProgramCheckTupleRegionImpl::EndsWithReturn(void) const noexcept {
  if (body && absent_body && unknown_body) {
    return body->EndsWithReturn() && absent_body->EndsWithReturn() &&
           unknown_body->EndsWithReturn();
  } else {
    return false;
  }
}

uint64_t ProgramCheckTupleRegionImpl::Hash(uint32_t depth) const {
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

bool ProgramCheckTupleRegionImpl::IsNoOp(void) const noexcept {
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
bool ProgramCheckTupleRegionImpl::Equals(EqualitySet &eq,
                                           REGION *that_,
                                           uint32_t depth) const noexcept {
  const auto op = that_->AsOperation();
  if (!op) {
    FAILED_EQ(that_);
    return false;
  }

  const auto that = op->AsCheckTuple();
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

const bool ProgramCheckTupleRegionImpl::MergeEqual(
    ProgramImpl *prog, std::vector<REGION *> &merges) {

  // New parallel region for merged `body` into 'this'
  auto new_par = prog->parallel_regions.Create(this);
  if (auto body_ptr = body.get(); body_ptr) {
    body_ptr->parent = new_par;
    new_par->AddRegion(body_ptr);
    body.Clear();
  }

  // New parallel region for merged `absent_body` into 'this'
  auto new_absent_body = prog->parallel_regions.Create(this);
  if (auto absent_body_ptr = absent_body.get(); absent_body_ptr) {
    absent_body_ptr->parent = new_absent_body;
    new_absent_body->AddRegion(absent_body_ptr);
    absent_body.Clear();
  }

  // New parallel region for merged `unknown_body` into 'this'
  auto new_unknown_body = prog->parallel_regions.Create(this);
  if (auto unknown_body_ptr = unknown_body.get(); unknown_body_ptr) {
    unknown_body_ptr->parent = new_unknown_body;
    new_unknown_body->AddRegion(unknown_body_ptr);
    unknown_body.Clear();
  }

  body.Emplace(this, new_par);
  absent_body.Emplace(this, new_absent_body);
  unknown_body.Emplace(this, new_unknown_body);

  for (auto region : merges) {
    auto merge = region->AsOperation()->AsCheckTuple();
    assert(merge);  // These should all be the same type
    assert(merge != this);

    if (auto merge_body_ptr = merge->body.get(); merge_body_ptr) {
      merge_body_ptr->parent = new_par;
      new_par->AddRegion(merge_body_ptr);
      merge->body.Clear();
    }

    if (auto merge_unknown_body_ptr = merge->unknown_body.get();
        merge_unknown_body_ptr) {
      merge_unknown_body_ptr->parent = new_unknown_body;
      new_unknown_body->AddRegion(merge_unknown_body_ptr);
      merge->unknown_body.Clear();
    }

    if (auto merge_absent_body_ptr = merge->absent_body.get();
        merge_absent_body_ptr) {
      merge_absent_body_ptr->parent = new_absent_body;
      new_absent_body->AddRegion(merge_absent_body_ptr);
      merge->absent_body.Clear();
    }

    merge->parent = nullptr;
  }

  return true;
}

// -----------------------------------------------------------------------------

ProgramCheckRecordRegionImpl *
ProgramCheckRecordRegionImpl::AsCheckRecord(void) noexcept {
  return this;
}

// Returns `true` if all paths through `this` ends with a `return` region.
bool ProgramCheckRecordRegionImpl::EndsWithReturn(void) const noexcept {
  if (body && absent_body && unknown_body) {
    return body->EndsWithReturn() && absent_body->EndsWithReturn() &&
           unknown_body->EndsWithReturn();
  } else {
    return false;
  }
}

uint64_t ProgramCheckRecordRegionImpl::Hash(uint32_t depth) const {
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

bool ProgramCheckRecordRegionImpl::IsNoOp(void) const noexcept {
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
bool ProgramCheckRecordRegionImpl::Equals(EqualitySet &eq, REGION *that_,
                                          uint32_t depth) const noexcept {
  const auto op = that_->AsOperation();
  if (!op) {
    FAILED_EQ(that_);
    return false;
  }

  const auto that = op->AsCheckRecord();
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

  // Make the record variables seem equivalent before descending.
  for (auto i = 0u; i < num_cols; ++i) {
    eq.Insert(record_vars[i], that->record_vars[i]);
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

const bool ProgramCheckRecordRegionImpl::MergeEqual(
    ProgramImpl *prog, std::vector<REGION *> &merges) {

  // New parallel region for merged `body` into 'this'
  auto new_par = prog->parallel_regions.Create(this);
  if (auto body_ptr = body.get(); body_ptr) {
    body_ptr->parent = new_par;
    new_par->AddRegion(body_ptr);
    body.Clear();
  }

  // New parallel region for merged `absent_body` into 'this'
  auto new_absent_body = prog->parallel_regions.Create(this);
  if (auto absent_body_ptr = absent_body.get(); absent_body_ptr) {
    absent_body_ptr->parent = new_absent_body;
    new_absent_body->AddRegion(absent_body_ptr);
    absent_body.Clear();
  }

  // New parallel region for merged `unknown_body` into 'this'
  auto new_unknown_body = prog->parallel_regions.Create(this);
  if (auto unknown_body_ptr = unknown_body.get(); unknown_body_ptr) {
    unknown_body_ptr->parent = new_unknown_body;
    new_unknown_body->AddRegion(unknown_body_ptr);
    unknown_body.Clear();
  }

  body.Emplace(this, new_par);
  absent_body.Emplace(this, new_absent_body);
  unknown_body.Emplace(this, new_unknown_body);

  for (auto region : merges) {
    auto merge = region->AsOperation()->AsCheckRecord();
    assert(merge);  // These should all be the same type
    assert(merge != this);

    if (auto merge_body_ptr = merge->body.get(); merge_body_ptr) {
      merge_body_ptr->parent = new_par;
      new_par->AddRegion(merge_body_ptr);
      merge->body.Clear();
    }

    if (auto merge_unknown_body_ptr = merge->unknown_body.get();
        merge_unknown_body_ptr) {
      merge_unknown_body_ptr->parent = new_unknown_body;
      new_unknown_body->AddRegion(merge_unknown_body_ptr);
      merge->unknown_body.Clear();
    }

    if (auto merge_absent_body_ptr = merge->absent_body.get();
        merge_absent_body_ptr) {
      merge_absent_body_ptr->parent = new_absent_body;
      new_absent_body->AddRegion(merge_absent_body_ptr);
      merge->absent_body.Clear();
    }

    merge->parent = nullptr;
  }

  return true;
}

}  // namespace hyde
