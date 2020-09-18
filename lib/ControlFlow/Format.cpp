// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/ControlFlow/Format.h>

#include "Program.h"

#include <drlojekyll/DataFlow/Format.h>
#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Lex/Format.h>
#include <drlojekyll/Parse/Format.h>

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
    case VectorKind::kInput:
      os << "$input";
      break;
    case VectorKind::kInduction:
      os << "$induction";
      break;
    case VectorKind::kJoinPivots:
      os << "$pivots";
      break;
    case VectorKind::kProductInput:
      os << "$product";
      break;
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
  if (auto name = var.Name();
      name.Lexeme() == Lexeme::kIdentifierAtom ||
      name.Lexeme() == Lexeme::kIdentifierVariable) {
    os << name << ':';
  }
  os << var.Id();
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
    os << os.Indent() << "empty-if";
  }
  return os;
}

OutputStream &operator<<(OutputStream &os, ProgramExistenceCheckRegion region) {
  if (auto maybe_body = region.Body(); maybe_body) {
    os << os.Indent() << "if-check ";
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

    os << " {\n";
    os.PushIndent();
    os << (*maybe_body);
    os.PopIndent();
  } else {
    os << os.Indent() << "empty-if";
  }
  return os;
}

OutputStream &operator<<(OutputStream &os, ProgramLetBindingRegion region) {
  if (auto maybe_body = region.Body(); maybe_body) {
    if (region.DefinedVars().empty()) {
      os << (*maybe_body);
      return os;
    } else {
      os << os.Indent();
      auto sep = "let {";
      for (auto var : region.DefinedVars()) {
        os << sep << var;
        sep = ", ";
      }
      sep = "} = {";
      for (auto var : region.UsedVars()) {
        os << sep << var;
        sep = ", ";
      }
      os << "}\n";
      os.PushIndent();
      os << (*maybe_body);
      os.PopIndent();
    }
  } else {
    os << os.Indent() << "empty-let";
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
    os << os.Indent() << "empty-vector-loop";
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
    const auto tables = region.Tables();
    for (auto i = 0u; i < tables.size(); ++i) {
      auto table = tables[i];
      auto index = region.Index(i);
      auto pivots = region.PivotVariables(i);
      auto outputs = region.OutputVariables(i);
      os << os.Indent() << "from " << table << " using " << index;
      auto sep = " with {";
      for (auto var : pivots) {
        os << sep << var;
        sep = ", ";
      }
      sep = "} select {";
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
    os << os.Indent() << "empty-join-tables";
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
    os << os.Indent() << "empty-cross-product";
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
  auto sep = "";
  os << os.Indent() << "par\n";
  os.PushIndent();
  for (auto sub_region : region.Regions()) {
    os << sep << sub_region;
    sep = "\n";
  }
  os.PopIndent();
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
  } else if (region.IsTupleCompare()) {
    os << ProgramTupleCompareRegion::From(region);
  } else {
    assert(false);
    os << "<<unknown region>>";
  }
  return os;
}

OutputStream &operator<<(OutputStream &os, ProgramProcedure proc) {

  os << "proc ";
  if (auto message = proc.Message(); message) {
    os << message->Name() << '/' << message->Arity() << ':' << proc.Id();
  } else {
    os << '$' << proc.Id();
  }
  os << '(';
  auto sep = "";
  for (auto vec : proc.InputVectors()) {
    os << sep << vec;
    sep = ", ";
  }
  os << ")\n";
  os.PushIndent();

  for (auto vec : proc.DefinedVectors()) {
    os << os.Indent() << "vector-define " << vec << '\n';
  }

  os << proc.Body() << '\n';
  os.PopIndent();
  return os;
}

OutputStream &operator<<(OutputStream &os, Program program) {
  auto sep = "";
  for (auto var : program.Constants()) {
    os << sep << os.Indent() << "const " << var.Type() << ' '<< var << " = "
       << *(var.Value());
    sep = "\n\n";
  }
  for (auto table : program.Tables()) {
    os << sep;
    DefineTable(os, table);
    sep = "\n\n";
  }
  for (auto proc : program.Procedures()) {
    os << sep << os.Indent() << proc;
    sep = "\n\n";
  }
  return os;
}

}  // namespace hyde
