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
    for (auto col : index.Columns()) {
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
  for (auto col : index.Columns()) {
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
  os << os.Indent() << "call " << region.CalledProcedure();
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

OutputStream &operator<<(OutputStream &os, ProgramTableInsertRegion region) {

  os << os.Indent();
  if (region.Body()) {
    os << "if-";
  }

  os << "insert-into-table {";
  auto sep = "";
  for (auto var : region.TupleVariables()) {
    os << sep << var;
    sep = ", ";
  }
  os << "} into " << region.Table();

  if (auto maybe_body = region.Body(); maybe_body) {
    os << '\n';
    os.PushIndent();
    os << (*maybe_body);
    os.PopIndent();
  }
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

OutputStream &operator<<(OutputStream &os, ProgramRegion region) {
  if (region.IsSeries()) {
    os << ProgramSeriesRegion::From(region);
  } else if (region.IsParallel()) {
    os << ProgramParallelRegion::From(region);
  } else if (region.IsLetBinding()) {
    os << ProgramLetBindingRegion::From(region);
  } else if (region.IsInduction()) {
    os << ProgramInductionRegion::From(region);
  } else if (region.IsVectorLoop()) {
    os << ProgramVectorLoopRegion::From(region);
  } else if (region.IsVectorAppend()) {
    os << ProgramVectorAppendRegion::From(region);
  } else if (region.IsVectorClear()) {
    os << ProgramVectorClearRegion::From(region);
  } else if (region.IsVectorUnique()) {
    os << ProgramVectorUniqueRegion::From(region);
  } else if (region.IsTableInsert()) {
    os << ProgramTableInsertRegion::From(region);
  } else if (region.IsTableJoin()) {
    os << ProgramTableJoinRegion::From(region);
  } else if (region.IsTableProduct()) {
    os << ProgramTableProductRegion::From(region);
  } else if (region.IsExistenceCheck()) {
    os << ProgramExistenceCheckRegion::From(region);
  } else if (region.IsExistenceAssertion()) {
    os << ProgramExistenceAssertionRegion::From(region);
  } else if (region.IsTupleCompare()) {
    os << ProgramTupleCompareRegion::From(region);
  } else if (region.IsGenerate()) {
    os << ProgramGenerateRegion::From(region);
  } else if (region.IsCall()) {
    os << ProgramCallRegion::From(region);
  } else if (region.IsPublish()) {
    os << ProgramPublishRegion::From(region);
  } else {
    assert(false);
    os << "<<unknown region>>";
  }
  return os;
}

OutputStream &operator<<(OutputStream &os, ProgramProcedure proc) {

  if (proc.Kind() == ProcedureKind::kInitializer) {
    os << "init ";
  }

  os << "proc ";
  if (auto message = proc.Message(); message) {
    os << message->Name() << '/' << message->Arity() << ':' << proc.Id();
  } else {
    os << '$' << proc.Id();
  }

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
    os << sep << os.Indent() << proc;

    os << '(';
    auto param_sep = "";
    for (auto vec : proc.VectorParameters()) {
      os << param_sep << vec;
      sep = ", ";
    }
    for (auto var : proc.VariableParameters()) {
      os << param_sep << var;
      sep = ", ";
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
  return os;
}

}  // namespace hyde
