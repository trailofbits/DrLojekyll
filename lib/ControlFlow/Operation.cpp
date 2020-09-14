// Copyright 2020, Trail of Bits. All rights reserved.

#include "Program.h"

namespace hyde {

Node<ProgramOperationRegion>::~Node(void) {}
Node<ProgramLetBindingRegion>::~Node(void) {}
Node<ProgramVectorLoopRegion>::~Node(void) {}
Node<ProgramVectorAppendRegion>::~Node(void) {}
Node<ProgramViewInsertRegion>::~Node(void) {}
Node<ProgramViewJoinRegion>::~Node(void) {}

Node<ProgramOperationRegion>::Node(REGION *parent_, ProgramOperation op_)
    : Node<ProgramRegion>(parent_),
      op(op_),
      variables(this),
      tables(this),
      views(this),
      indices(this) {}

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

Node<ProgramLetBindingRegion> *
Node<ProgramOperationRegion>::AsLetBinding(void) noexcept {
  return nullptr;
}

Node<ProgramViewInsertRegion> *
Node<ProgramOperationRegion>::AsViewInsert(void) noexcept {
  return nullptr;
}

Node<ProgramViewJoinRegion> *
Node<ProgramOperationRegion>::AsViewJoin(void) noexcept {
  return nullptr;
}

Node<ProgramVectorLoopRegion> *
Node<ProgramVectorLoopRegion>::AsVectorLoop(void) noexcept {
  return this;
}

Node<ProgramLetBindingRegion> *
Node<ProgramLetBindingRegion>::AsLetBinding(void) noexcept {
  return this;
}

Node<ProgramVectorAppendRegion> *
Node<ProgramVectorAppendRegion>::AsVectorAppend(void) noexcept {
  return this;
}

Node<ProgramViewInsertRegion> *
Node<ProgramViewInsertRegion>::AsViewInsert(void) noexcept {
  return this;
}

Node<ProgramViewJoinRegion> *
Node<ProgramViewJoinRegion>::AsViewJoin(void) noexcept {
  return this;
}


}  // namespace hyde
