// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/ControlFlow/Format.h>
#include <drlojekyll/DataFlow/Format.h>
#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Lex/Format.h>
#include <drlojekyll/Parse/Format.h>

#include "Program.h"

namespace hyde {
namespace {

static void DefineTable(OutputStream &os, DataTable table) {
  os << os.Indent() << "create " << table;
  os.PushIndent();
  for (auto col : table.Columns()) {
    os << '\n';
    os << os.Indent() << col.Type() << '\t' << col;
    auto sep = "\t; ";
    for (auto name : col.PossibleNames()) {
      os << sep << name;
      sep = ", ";
    }
  }

  auto index_sep = "\n\n";
  for (auto index : table.Indices()) {
    os << index_sep << os.Indent() << index;
    auto sep = " on ";
    for (auto col : index.KeyColumns()) {
      os << sep << col;
      sep = ", ";
    }
    index_sep = "\n";
  }

  os.PopIndent();
}

}  // namespace

OutputStream &operator<<(OutputStream &os, DataColumn col) {
  os << "%col:" << col.Id();
  return os;
}

OutputStream &operator<<(OutputStream &os, DataIndex index) {
  os << "%index:" << index.Id();
  auto sep = "[";
  auto i = 0u;
  for (auto col : index.KeyColumns()) {
    for (; i < col.Index(); ++i) {
      os << sep << '_';
      sep = ",";
    }
    os << sep << col.Type();
    i = col.Index() + 1u;
    sep = ",";
  }
  auto table = DataTable::Backing(index);
  for (auto max_i = table.Columns().size(); i < max_i; ++i) {
    os << sep << '_';
    sep = ",";
  }
  os << ']';
  return os;
}

OutputStream &operator<<(OutputStream &os, DataTable table) {
  os << "%table:" << table.Id();
  auto sep = "[";
  for (auto col : table.Columns()) {
    os << sep << col.Type();
    sep = ",";
  }
  os << ']';
  return os;
}

OutputStream &operator<<(OutputStream &os, DataVector vec) {

  switch (vec.Kind()) {
    case VectorKind::kInput: os << "$input"; break;
    case VectorKind::kInduction: os << "$induction"; break;
    case VectorKind::kJoinPivots: os << "$pivots"; break;
    case VectorKind::kProductInput: os << "$product"; break;
    case VectorKind::kTableScan: os << "$scan"; break;
  }

  os << ':' << vec.Id();
  auto sep = "<";
  for (auto type_kind : vec.ColumnTypes()) {
    os << sep << type_kind;
    sep = ",";
  }
  os << '>';
  return os;
}

OutputStream &operator<<(OutputStream &os, DataVariable var) {
  os << '@';
  if (auto name = var.Name(); name.Lexeme() == Lexeme::kIdentifierAtom ||
                              name.Lexeme() == Lexeme::kIdentifierVariable) {
    os << name << ':';
  }
  os << var.Id();
  return os;
}

OutputStream &operator<<(OutputStream &os, ProgramPublishRegion region) {
  auto message = region.Message();
  os << os.Indent() << "publish " << message.Name() << '/' << message.Arity();
  auto sep = "(";
  auto end = "";
  for (auto arg : region.VariableArguments()) {
    os << sep << arg;
    sep = ", ";
    end = ")";
  }
  os << end;
  return os;
}

OutputStream &operator<<(OutputStream &os, ProgramCallRegion region) {
  os << os.Indent();

  const auto conditional = region.ExecuteBodyIfReturnIsTrue() ||
                           region.ExecuteBodyIfReturnIsFalse();
  if (conditional) {
    os << "if ";
  }

  os << "call " << region.CalledProcedure();
  auto sep = "(";
  auto end = "";
  for (auto arg : region.VectorArguments()) {
    os << sep << arg;
    sep = ", ";
    end = ")";
  }
  for (auto arg : region.VariableArguments()) {
    os << sep << arg;
    sep = ", ";
    end = ")";
  }
  os << end;

  if (region.ExecuteBodyIfReturnIsTrue()) {
    os << " returns true";
  } else if (region.ExecuteBodyIfReturnIsFalse()) {
    os << " returns false";
  }

  if (auto body = region.Body(); body) {
    assert(conditional);

    os << '\n';
    os.PushIndent();
    os << *body;
    os.PopIndent();

  } else {
    assert(!conditional);
  }

  return os;
}

OutputStream &operator<<(OutputStream &os, ProgramReturnRegion region) {
  auto ret = "return-false";
  if (region.ReturnsTrue()) {
    ret = "return-true";
  }
  os << os.Indent() << ret;
  return os;
}

OutputStream &operator<<(OutputStream &os, ProgramTupleCompareRegion region) {
  if (auto maybe_body = region.Body(); maybe_body) {
    os << os.Indent();
    auto sep = "if-compare {";
    for (auto var : region.LHS()) {
      os << sep << var;
      sep = ", ";
    }

    os << "} " << region.Operator();

    sep = " {";
    for (auto var : region.RHS()) {
      os << sep << var;
      sep = ", ";
    }
    os << "}\n";

    os.PushIndent();
    os << (*maybe_body);
    os.PopIndent();
  } else {
    os << os.Indent() << "empty";
  }
  return os;
}

OutputStream &operator<<(OutputStream &os,
                         ProgramExistenceAssertionRegion region) {
  os << os.Indent();
  auto sep = "increment ";
  if (region.IsDecrement()) {
    sep = "decrement ";
  }

  for (auto var : region.ReferenceCounts()) {
    os << sep << var;
    sep = ", ";
  }

  return os;
}

OutputStream &operator<<(OutputStream &os, ProgramExistenceCheckRegion region) {
  if (auto maybe_body = region.Body(); maybe_body) {
    os << os.Indent() << "test ";
    if (region.CheckForNotZero()) {
      os << "not-zero";
    } else {
      os << "zero";
    }

    auto sep = " ";
    for (auto var : region.ReferenceCounts()) {
      os << sep << var;
      sep = ", ";
    }

    os << "\n";
    os.PushIndent();
    os << (*maybe_body);
    os.PopIndent();
  } else {
    os << os.Indent() << "empty";
  }
  return os;
}

OutputStream &operator<<(OutputStream &os, ProgramGenerateRegion region) {
  auto functor = region.Functor();
  os << os.Indent();
  if (region.IsFilter()) {
    os << "if ";
  } else {
    const char *sep = nullptr;
    switch (functor.Range()) {
      case FunctorRange::kZeroOrOne: sep = "assign-if {"; break;
      case FunctorRange::kOneToOne: sep = "assign {"; break;
      default: sep = "assign-each {"; break;
    }
    for (auto var : region.OutputVariables()) {
      os << sep << var;
      sep = ", ";
    }
    os << "} from ";
  }

  os << functor.Name();

  auto sep = "(";
  auto i = 0u;
  auto input_vars = region.InputVariables();

  for (auto param : functor.Parameters()) {
    if (param.Binding() == ParameterBinding::kFree) {
      os << sep << '_';
    } else {
      os << sep << input_vars[i++];
    }
    sep = ", ";
  }

  os << ")\n";


  if (auto maybe_body = region.Body(); maybe_body) {
    os.PushIndent();
    os << (*maybe_body);
    os.PopIndent();
  } else {
    os << os.Indent() << "empty";
  }
  return os;
}

OutputStream &operator<<(OutputStream &os, ProgramLetBindingRegion region) {
  if (auto maybe_body = region.Body(); maybe_body) {
    if (region.DefinedVariables().empty()) {
      os << (*maybe_body);
      return os;
    } else {
      os << os.Indent();
      auto sep = "let {";
      for (auto var : region.DefinedVariables()) {
        os << sep << var;
        sep = ", ";
      }
      sep = "} = {";
      for (auto var : region.UsedVariables()) {
        os << sep << var;
        sep = ", ";
      }
      os << "}\n";
      os.PushIndent();
      os << (*maybe_body);
      os.PopIndent();
    }
  } else {
    os << os.Indent() << "empty";
  }
  return os;
}

OutputStream &operator<<(OutputStream &os, ProgramVectorLoopRegion region) {
  if (auto maybe_body = region.Body(); maybe_body) {
    os << os.Indent() << "vector-loop {";
    auto sep = "";
    for (auto var : region.TupleVariables()) {
      os << sep << var;
      sep = ", ";
    }
    os << "} over " << region.Vector() << '\n';
    os.PushIndent();
    os << (*maybe_body);
    os.PopIndent();
  } else {
    os << os.Indent() << "empty";
  }
  return os;
}

OutputStream &operator<<(OutputStream &os, ProgramVectorAppendRegion region) {
  os << os.Indent() << "vector-append {";
  auto sep = "";
  for (auto var : region.TupleVariables()) {
    os << sep << var;
    sep = ", ";
  }
  os << "} into " << region.Vector();
  return os;
}

OutputStream &operator<<(OutputStream &os, ProgramVectorClearRegion region) {
  os << os.Indent() << "vector-clear " << region.Vector();
  return os;
}

OutputStream &operator<<(OutputStream &os, ProgramVectorUniqueRegion region) {
  os << os.Indent() << "vector-unique " << region.Vector();
  return os;
}

OutputStream &operator<<(OutputStream &os, ProgramTransitionStateRegion region) {

  os << os.Indent();
  if (region.Body()) {
    os << "if-";
  }

  os << "transition-state {";

  auto sep = "";
  for (auto var : region.TupleVariables()) {
    os << sep << var;
    sep = ", ";
  }
  os << "} in " << region.Table() << " from ";

  switch (region.FromState()) {
    case TupleState::kPresent:
      os << "present to ";
      break;
    case TupleState::kAbsent:
      os << "absent to ";
      break;
    case TupleState::kUnknown:
      os << "unknown to ";
      break;
    case TupleState::kAbsentOrUnknown:
      os << "absent|unknown to ";
      break;
  }

  switch (region.ToState()) {
    case TupleState::kPresent:
      os << "present";
      break;
    case TupleState::kAbsent:
      os << "absent";
      break;
    case TupleState::kUnknown:
    case TupleState::kAbsentOrUnknown:
      os << "unknown";
      break;
  }

  if (auto maybe_body = region.Body(); maybe_body) {
    os << '\n';
    os.PushIndent();
    os << (*maybe_body);
    os.PopIndent();
  }
  return os;
}

OutputStream &operator<<(OutputStream &os, ProgramCheckStateRegion region) {
  os << os.Indent() << "check-state {";
  auto sep = "";
  for (auto var : region.TupleVariables()) {
    os << sep << var;
    sep = ", ";
  }
  os << "} in " << region.Table();

  os.PushIndent();
  if (auto maybe_body = region.IfPresent(); maybe_body) {
    os << '\n';
    os << os.Indent() << "if-present\n";
    os.PushIndent();
    os << (*maybe_body);
    os.PopIndent();
  }
  if (auto maybe_body = region.IfAbsent(); maybe_body) {
    os << '\n';
    os << os.Indent() << "if-absent\n";
    os.PushIndent();
    os << (*maybe_body);
    os.PopIndent();
  }
  if (auto maybe_body = region.IfUnknown(); maybe_body) {
    os << '\n';
    os << os.Indent() << "if-unknown\n";
    os.PushIndent();
    os << (*maybe_body);
    os.PopIndent();
  }
  os.PopIndent();
  return os;
}

OutputStream &operator<<(OutputStream &os, ProgramTableJoinRegion region) {
  if (auto maybe_body = region.Body(); maybe_body) {
    os << os.Indent() << "join-tables\n";
    os.PushIndent();
    os << os.Indent();
    auto sep = "vector-loop {";
    const auto pivot_vars = region.OutputPivotVariables();
    for (auto var : pivot_vars) {
      os << sep << var;
      sep = ", ";
    }
    os << "} over " << region.PivotVector() << '\n';

    const auto tables = region.Tables();
    for (auto i = 0u; i < tables.size(); ++i) {
      auto table = tables[i];
      auto index = region.Index(i);
      os << os.Indent();

      sep = "select {";
      auto end = "select {} from ";
      auto cols = region.SelectedColumns(i);
      auto j = 0u;
      for (auto var : region.OutputVariables(i)) {
        os << sep << cols[j++] << " as " << var;
        sep = ", ";
        end = "} from ";
      }

      os << end << table << " using " << index;

      sep = " where ";
      j = 0u;
      for (auto col : region.IndexedColumns(i)) {
        os << sep << col << " = " << pivot_vars[j++];
        sep = " and ";
      }

      os << '\n';
    }
    os.PushIndent();
    os << (*maybe_body);
    os.PopIndent();
    os.PopIndent();
  } else {
    os << os.Indent() << "empty";
  }
  return os;
}

OutputStream &operator<<(OutputStream &os, ProgramTableProductRegion region) {
  if (auto maybe_body = region.Body(); maybe_body) {
    os << os.Indent() << "cross-product\n";
    os.PushIndent();
    const auto tables = region.Tables();
    for (auto i = 0u; i < tables.size(); ++i) {
      auto table = tables[i];
      auto vector = region.Vector(i);
      auto outputs = region.OutputVariables(i);
      os << os.Indent() << "from " << table << " and " << vector;
      auto sep = " select {";
      for (auto var : outputs) {
        os << sep << var;
        sep = ", ";
      }
      os << "}\n";
    }
    os.PushIndent();
    os << (*maybe_body);
    os.PopIndent();
    os.PopIndent();
  } else {
    os << os.Indent() << "empty";
  }
  return os;
}


OutputStream &operator<<(OutputStream &os, ProgramTableScanRegion region) {
  os << os.Indent() << "scan";
  if (region.Index()) {
    os << "-index";
  } else {
    os << "-table";
  }

  auto sep = "";
  os << " select {";
  for (auto col : region.SelectedColumns()) {
    os << sep << col;
    sep = ", ";
  }

  os << "} from " << region.Table();

  if (auto index = region.Index(); index) {
    os << " where {";
    sep = "";
    for (auto var : region.InputVariables()) {
      os << sep << var;
      sep = ", ";
    }
    os << "} in " << *index;
  }

  os << " into " << region.FilledVector();
  return os;
}

OutputStream &operator<<(OutputStream &os, ProgramInductionRegion region) {
  os << os.Indent() << "induction\n";
  os.PushIndent();
  os << os.Indent() << "init\n";
  os.PushIndent();
  os << region.Initializer() << '\n';
  os.PopIndent();
  os << os.Indent() << "fixpoint-loop testing ";

  auto sep = "";
  for (auto vec : region.Vectors()) {
    os << sep << vec;
    sep = ", ";
  }
  os << '\n';

  os.PushIndent();
  os << region.FixpointLoop();
  os.PopIndent();
  if (auto output = region.Output(); output) {
    os << '\n' << os.Indent() << "output\n";
    os.PushIndent();
    os << (*output);
    os.PopIndent();
  }
  os.PopIndent();
  return os;
}

OutputStream &operator<<(OutputStream &os, ProgramSeriesRegion region) {
  auto sep = "";
  for (auto sub_region : region.Regions()) {
    os << sep << sub_region;
    sep = "\n";
  }
  return os;
}

OutputStream &operator<<(OutputStream &os, ProgramParallelRegion region) {
  if (auto regions = region.Regions(); !regions.empty()) {
    auto sep = "";
    os << os.Indent() << "par\n";
    os.PushIndent();
    for (auto sub_region : regions) {
      os << sep << sub_region;
      sep = "\n";
    }
    os.PopIndent();
  } else {
    os << os.Indent() << "empty";
  }
  return os;
}

namespace {

class FormatDispatcher final : public ProgramVisitor {
 public:
  virtual ~FormatDispatcher(void) = default;

  explicit FormatDispatcher(OutputStream &os_)
      : os(os_) {}

#define MAKE_VISITOR(cls) \
    void Visit(cls region) override { \
      os << region; \
    } \

  MAKE_VISITOR(ProgramCallRegion)
  MAKE_VISITOR(ProgramReturnRegion)
  MAKE_VISITOR(ProgramExistenceAssertionRegion)
  MAKE_VISITOR(ProgramExistenceCheckRegion)
  MAKE_VISITOR(ProgramGenerateRegion)
  MAKE_VISITOR(ProgramInductionRegion)
  MAKE_VISITOR(ProgramLetBindingRegion)
  MAKE_VISITOR(ProgramParallelRegion)
  MAKE_VISITOR(ProgramProcedure)
  MAKE_VISITOR(ProgramPublishRegion)
  MAKE_VISITOR(ProgramSeriesRegion)
  MAKE_VISITOR(ProgramVectorAppendRegion)
  MAKE_VISITOR(ProgramVectorClearRegion)
  MAKE_VISITOR(ProgramVectorLoopRegion)
  MAKE_VISITOR(ProgramVectorUniqueRegion)
  MAKE_VISITOR(ProgramTransitionStateRegion)
  MAKE_VISITOR(ProgramCheckStateRegion)
  MAKE_VISITOR(ProgramTableJoinRegion)
  MAKE_VISITOR(ProgramTableProductRegion)
  MAKE_VISITOR(ProgramTableScanRegion)
  MAKE_VISITOR(ProgramTupleCompareRegion)

 private:
  OutputStream &os;
};

}  // namespace

OutputStream &operator<<(OutputStream &os, ProgramRegion region) {
  FormatDispatcher dispatcher(os);
  region.Accept(dispatcher);
  return os;
}

OutputStream &operator<<(OutputStream &os, ProgramProcedure proc) {
  switch (proc.Kind()) {
    case ProcedureKind::kInitializer:
      os << "^init:";
      break;
    case ProcedureKind::kMessageHandler:
      os << "^receive:";
      if (auto message = proc.Message(); message) {
        os << message->Name() << '/' << message->Arity() << ':';
      }
      break;
    case ProcedureKind::kTupleFinder:
      os << "^find:";
      break;
    case ProcedureKind::kTupleRemover:
      os << "^remove:";
      break;
  }
  os << proc.Id();

  return os;
}

OutputStream &operator<<(OutputStream &os, Program program) {
  auto sep = "";
  for (auto var : program.Constants()) {
    os << sep << os.Indent() << "const " << var.Type() << ' ' << var << " = "
       << *(var.Value());
    sep = "\n\n";
  }
  for (auto var : program.GlobalVariables()) {
    os << sep << os.Indent() << "global " << var.Type() << ' ' << var;
    sep = "\n\n";
  }
  for (auto table : program.Tables()) {
    os << sep;
    DefineTable(os, table);
    sep = "\n\n";
  }

  for (auto proc : program.Procedures()) {
    os << sep << os.Indent();
    if (proc.Kind() == ProcedureKind::kInitializer) {
      os << "init ";
    }

    os << "proc " << proc;

    os << '(';
    auto param_sep = "";
    for (auto vec : proc.VectorParameters()) {
      os << param_sep << vec;
      param_sep = ", ";
    }
    for (auto var : proc.VariableParameters()) {
      os << param_sep << var;
      param_sep = ", ";
    }
    os << ")\n";
    os.PushIndent();

    for (auto vec : proc.DefinedVectors()) {
      os << os.Indent() << "vector-define " << vec << '\n';
    }

    os << proc.Body();
    os.PopIndent();

    sep = "\n\n";
  }
  os << sep;
  return os;
}

}  // namespace hyde
