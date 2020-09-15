// Copyright 2020, Trail of Bits. All rights reserved.

#include "Program.h"

namespace hyde {

Node<ProgramProcedure>::~Node(void) {}

Node<ProgramVectorProcedure>::~Node(void) {}
Node<ProgramTupleProcedure>::~Node(void) {}

Node<ProgramProcedure>::Node(QueryView view, ProgramImpl *program)
    : Node<ProgramRegion>(this),
      tables(this) {}

Node<ProgramProcedure> *
Node<ProgramProcedure>::AsProcedure(void) noexcept {
  return this;
}

Node<ProgramVectorProcedure> *Node<ProgramProcedure>::AsVector(void) {
  return nullptr;
}

Node<ProgramTupleProcedure> *Node<ProgramProcedure>::AsTuple(void) {
  return nullptr;
}

// Get or create a table in a procedure.
VECTOR *Node<ProgramProcedure>::VectorFor(
    VectorKind kind,
    DefinedNodeRange<QueryColumn> cols) {
  return vectors.Create(vectors.Size(), kind, cols);
}

Node<ProgramVectorProcedure> *Node<ProgramVectorProcedure>::AsVector(void) {
  return this;
}

Node<ProgramTupleProcedure> *Node<ProgramTupleProcedure>::AsTuple(void) {
  return this;
}

}  // namespace hyde
