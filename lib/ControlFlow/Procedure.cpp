// Copyright 2020, Trail of Bits. All rights reserved.

#include "Program.h"

namespace hyde {

Node<ProgramProcedureRegion>::~Node(void) {}

Node<ProgramProcedureRegion>::Node(QueryView view, ProgramImpl *program)
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

Node<ProgramProcedureRegion> *
Node<ProgramProcedureRegion>::AsProcedure(void) noexcept {
  return this;
}


// Get or create a table in a procedure.
TABLE *Node<ProgramProcedureRegion>::VectorFor(
    DefinedNodeRange<QueryColumn> cols) {
  const auto table = tables.Create(TableKind::kVector);
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
VAR *Node<ProgramProcedureRegion>::VariableFor(QueryColumn col) {
  auto &var = col_id_to_var[col.Id()];
  if (!var) {
    var = locals.Create(col.Id(), VariableRole::kLocal);
  }

  var->query_columns.push_back(col);
  var->parsed_vars.push_back(col.Variable());

  return var;
}

}  // namespace hyde
