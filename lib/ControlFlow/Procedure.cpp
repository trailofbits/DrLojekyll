// Copyright 2020, Trail of Bits. All rights reserved.

#include "Program.h"

namespace hyde {

Node<ProgramProcedure>::~Node(void) {}

uint64_t Node<ProgramProcedure>::Hash(uint32_t depth) const {
  uint64_t hash = 1u;
  if (depth == 0) {
    return hash;
  }

  if (body) {
    return body->Hash(depth - 1u);
  }
  return hash;
}

// Returns `true` if this region is a no-op.
bool Node<ProgramProcedure>::IsNoOp(void) const noexcept {
  if (checking_if_nop) {
    return true;
  }

  checking_if_nop = true;
  const auto ret = body ? body->IsNoOp() : true;
  checking_if_nop = false;
  return ret;
}

bool Node<ProgramProcedure>::Equals(EqualitySet &eq, Node<ProgramRegion> *that_,
                                    uint32_t depth) const noexcept {
  const auto that = that_->AsProcedure();
  if (!that) {
    return false;
  }

  if (!body != !that->body) {
    return false;
  }

  if (eq.Contains(this, that)) {
    return true;
  }

  const auto num_arg_vars = input_vars.Size();
  const auto num_arg_vecs = input_vecs.Size();
  const auto num_defined_vecs = vectors.Size();
  if (num_arg_vars != that->input_vars.Size() ||
      num_arg_vecs != that->input_vecs.Size() ||
      num_defined_vecs != that->vectors.Size()) {
    return false;
  }

  for (auto i = 0u; i < num_arg_vars; ++i) {
    const auto this_var = input_vars[i];
    const auto that_var = that->input_vars[i];
    if (this_var->role != that_var->role ||
        DataVariable(this_var).Type() != DataVariable(that_var).Type()) {
      return false;
    }
  }

  auto cmp_vec = [](VECTOR *this_vec, VECTOR *that_vec) {
    if (this_vec->kind != that_vec->kind) {
      return false;
    }
    const auto num_vec_entries = this_vec->col_types.size();
    if (num_vec_entries != that_vec->col_types.size()) {
      return false;
    }
    for (auto j = 0u; j < num_vec_entries; ++j) {
      if (this_vec->col_types[j] != that_vec->col_types[j]) {
        return false;
      }
    }
    return true;
  };

  for (auto i = 0u; i < num_arg_vecs; ++i) {
    const auto this_vec = input_vecs[i];
    const auto that_vec = that->input_vecs[i];
    if (!cmp_vec(this_vec, that_vec)) {
      return false;
    }
  }

  for (auto i = 0u, max_i = num_defined_vecs; i < max_i; ++i) {
    const auto this_vec = vectors[i];
    const auto that_vec = that->vectors[i];
    if (!cmp_vec(this_vec, that_vec)) {
      return false;
    }
  }

  for (auto i = 0u; i < num_arg_vars; ++i) {
    eq.Insert(input_vars[i], that->input_vars[i]);
  }

  for (auto i = 0u; i < num_arg_vecs; ++i) {
    eq.Insert(input_vecs[i], that->input_vecs[i]);
  }

  for (auto i = 0u; i < num_defined_vecs; ++i) {
    eq.Insert(vectors[i], that->vectors[i]);
  }

  eq.Insert(this, that_);

  if (depth == 0) {
    return true;
  }

  // This function tests the true/false return value of the called procedure.
  if (body && !body->Equals(eq, that->body.get(), depth - 1u)) {
    return false;

  } else {
    return true;
  }
}

const bool
Node<ProgramProcedure>::MergeEqual(ProgramImpl *prog,
                                   std::vector<Node<ProgramRegion> *> &merges) {
  NOTE("TODO(ekilmer): Unimplemented merging of ProgramProcedure");
  assert(false);
  return false;
}

Node<ProgramProcedure> *Node<ProgramProcedure>::AsProcedure(void) noexcept {
  return this;
}

// Returns `true` if all paths through `this` ends with a `return` region.
bool Node<ProgramProcedure>::EndsWithReturn(void) const noexcept {
  if (body) {
    return body->EndsWithReturn();
  } else {
    return false;
  }
}

// Get or create a table in a procedure.
VECTOR *Node<ProgramProcedure>::VectorFor(ProgramImpl *impl, VectorKind kind,
                                          DefinedNodeRange<QueryColumn> cols) {
  const auto next_id = impl->next_id++;
  if (VectorKind::kParameter == kind) {
    return input_vecs.Create(next_id, kind, cols);
  } else {
    return vectors.Create(next_id, kind, cols);
  }
}

}  // namespace hyde
