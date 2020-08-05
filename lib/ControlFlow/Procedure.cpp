// Copyright 2020, Trail of Bits. All rights reserved.

#include "Program.h"

namespace hyde {

Node<ProgramProcedureRegion>::~Node(void) {}

Node<ProgramProcedureRegion>::Node(QueryView view, ProgramImpl *program)
    : Node<ProgramRegion>(this),
      locals(this),
      tables(this) {
  auto loop = program->operation_regions.Create(
      this, ProgramOperation::kLoopOverImplicitInputVector);

  for (auto col : view.Columns()) {
    loop->variables.AddUse(GetOrCreateLocal(col));
  }

  UseRef<REGION>(this, loop).Swap(body);
}

Node<ProgramProcedureRegion> *Node<ProgramProcedureRegion>::AsProcedure(void) noexcept {
  return this;
}

// Gets or creates a local variable in the procedure.
VAR *Node<ProgramProcedureRegion>::GetOrCreateLocal(QueryColumn col) {
  VAR *&var = col_id_to_var.find(col.Id());
  if (!var) {
    var = locals.Create(col.Id(), VariableRole::kLocal);
  }

  var->query_columns.push_back(col);
  var->parsed_vars.push_back(col.Variable());

  return var;
}

}  // namespace hyde
