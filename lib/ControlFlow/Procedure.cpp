// Copyright 2020, Trail of Bits. All rights reserved.

#include "Program.h"

namespace hyde {

Node<ProgramProcedure>::~Node(void) {}

Node<ProgramProcedure> *
Node<ProgramProcedure>::AsProcedure(void) noexcept {
  return this;
}

// Get or create a table in a procedure.
VECTOR *Node<ProgramProcedure>::VectorFor(
    ProgramImpl *impl,
    VectorKind kind,
    DefinedNodeRange<QueryColumn> cols) {
  const auto next_id = impl->next_id++;
  if (VectorKind::kInput == kind) {
    return input_vectors.Create(next_id, kind, cols);
  } else {
    return vectors.Create(next_id, kind, cols);
  }
}

}  // namespace hyde
