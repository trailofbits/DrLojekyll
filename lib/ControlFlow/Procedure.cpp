// Copyright 2020, Trail of Bits. All rights reserved.

#include "Program.h"

namespace hyde {

Node<ProgramProcedure>::~Node(void) {}

Node<ProgramVectorProcedure>::~Node(void) {}
Node<ProgramTupleProcedure>::~Node(void) {}

Node<ProgramProcedure>::Node(QueryView view, ProgramImpl *program)
    : Node<ProgramRegion>(this),
      locals(this),
      tables(this) {
  auto loop = program->operation_regions.Create(
      this, ProgramOperation::kLoopOverInputVector);

  for (auto col : view.Columns()) {
    loop->variables.AddUse(VariableFor(col));
  }

  UseRef<REGION>(this, loop).Swap(body);
}

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
TABLE *Node<ProgramProcedure>::VectorFor(
    DefinedNodeRange<QueryColumn> cols, TableKind kind) {
  const auto table = tables.Create(kind);
  auto &columns = table->columns;
  for (auto col : cols) {
    columns.push_back(col);
  }

  std::sort(columns.begin(), columns.end());
  auto it = std::unique(columns.begin(), columns.end());
  columns.erase(it, columns.end());

  return table;
}

// Gets or creates a local variable in the procedure.
VAR *Node<ProgramProcedure>::VariableFor(QueryColumn col) {
  auto &var = col_id_to_var[col.Id()];
  if (!var) {
    var = locals.Create(col.Id(), VariableRole::kLocal);
  }

  var->query_columns.push_back(col);
  var->parsed_vars.push_back(col.Variable());

  return var;
}

Node<ProgramVectorProcedure> *Node<ProgramVectorProcedure>::AsVector(void) {
  return this;
}

Node<ProgramTupleProcedure> *Node<ProgramTupleProcedure>::AsTuple(void) {
  return this;
}

}  // namespace hyde
