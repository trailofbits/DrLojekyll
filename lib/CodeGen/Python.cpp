// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/CodeGen/CodeGen.h>
#include <drlojekyll/ControlFlow/Format.h>
#include <drlojekyll/ControlFlow/Program.h>
#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Lex/Format.h>
#include <drlojekyll/Parse/Format.h>

#include <algorithm>
#include <sstream>
#include <unordered_set>
#include <vector>

namespace hyde {
namespace {

static const char *TypeName(TypeKind kind) {
  switch (kind) {
    case TypeKind::kSigned8:
    case TypeKind::kSigned16:
    case TypeKind::kSigned32:
    case TypeKind::kSigned64:
    case TypeKind::kUnsigned8:
    case TypeKind::kUnsigned16:
    case TypeKind::kUnsigned32:
    case TypeKind::kUnsigned64: return "int";
    case TypeKind::kFloat:
    case TypeKind::kDouble: return "float";
    case TypeKind::kBytes: return "bytes";
    case TypeKind::kASCII:
    case TypeKind::kUTF8:
    case TypeKind::kUUID: return "str";
    default: assert(false); return "None";
  }
}

// Declare a set to hold the table.
static void DeclareTable(OutputStream &os, DataTable table) {
  os << "table_" << table.Id() << ": DefaultDict[Tuple[";
  auto sep = "";
  for (auto col : table.Columns()) {
    os << sep << TypeName(col.Type());
    sep = ", ";
  }
  os << "], int] = defaultdict(int)\n";

  for (auto index : table.Indices()) {
    os << "index_" << index.Id() << ": DefaultDict[Tuple[";
    sep = "";
    for (auto col : index.Columns()) {
      os << TypeName(col.Type()) << sep;
      sep = ", ";
    }
    os << "], Set[Tuple[";
    sep = "";
    for (auto col : index.MappedColumns()) {
      os << TypeName(col.Type()) << sep;
      sep = ", ";
    }
    os << "]]] = defaultdict(set)\n";
  }
  os << "\n";
}

class PythonCodeGenVisitor final : public ProgramVisitor {
 public:
  explicit PythonCodeGenVisitor(OutputStream &os_) : os(os_) {}

  void Visit(ProgramCallRegion val) override {
    os << "ProgramCallRegion\n";
  }

  void Visit(ProgramReturnRegion val) override {
    os << os.Indent() << "return " << (val.ReturnsFalse() ? "False" : "True")
       << "\n";
  }

  void Visit(ProgramExistenceAssertionRegion val) override {
    os << "ProgramExistenceAssertionRegion\n";
  }

  void Visit(ProgramExistenceCheckRegion val) override {
    os << "ProgramExistenceCheckRegion\n";
  }

  void Visit(ProgramGenerateRegion val) override {
    os << "ProgramGenerateRegion\n";
  }

  void Visit(ProgramInductionRegion val) override {

    // Base case
    val.Initializer().Accept(*this);

    // Induction
    os << os.Indent() << "while ";
    auto sep = "";
    for (auto vec : val.Vectors()) {
      os << sep << "index_vec_" << vec.Id() << " < len(vec_" << vec.Id() << ")";
      sep = " or ";
    }
    os << ":\n";

    os.PushIndent();
    val.FixpointLoop().Accept(*this);
    os.PopIndent();

    // Output
    if (auto output = val.Output(); output) {
      output->Accept(*this);
    }
  }

  void Visit(ProgramLetBindingRegion val) override {
    os << "ProgramLetBindingRegion\n";
  }

  void Visit(ProgramParallelRegion val) override {

    // Same as SeriesRegion
    for (auto region : val.Regions()) {
      region.Accept(*this);
    }
  }

  void Visit(ProgramProcedure val) override {
    os << "ProgramProcedure\n";
  }

  void Visit(ProgramPublishRegion val) override {
    os << "ProgramPublishRegion\n";
  }

  void Visit(ProgramSeriesRegion val) override {
    for (auto region : val.Regions()) {
      region.Accept(*this);
    }
  }

  void Visit(ProgramVectorAppendRegion val) override {
    os << os.Indent() << "vec_" << val.Vector().Id() << ".append((";
    for (auto var : val.TupleVariables()) {
      os << "var_" << var.Id() << ", ";
    }
    os << "))\n";
  }

  void Visit(ProgramVectorClearRegion val) override {
    os << os.Indent() << "del vec_" << val.Vector().Id() << "[:]\n";
    os << os.Indent() << "index_vec_" << val.Vector().Id() << " = 0\n";
  }

  void Visit(ProgramVectorLoopRegion val) override {
    auto vec = val.Vector();
    os << os.Indent() << "while index_vec_" << vec.Id() << " < len(vec_"
       << vec.Id() << "):\n";
    os.PushIndent();
    os << os.Indent();
    for (auto var : val.TupleVariables()) {
      os << "var_" << var.Id() << ", ";
    }
    os << "= vec_" << vec.Id() << "[index_vec_" << vec.Id() << "]\n";

    os << os.Indent() << "index_vec_" << vec.Id() << " += 1\n";

    if (auto body = val.Body(); body) {
      body->Accept(*this);
    }
    os.PopIndent();
  }

  void Visit(ProgramVectorUniqueRegion val) override {
    os << os.Indent() << "vec_" << val.Vector().Id() << " = list(set(vec_"
       << val.Vector().Id() << "))\n";
    os << os.Indent() << "index_vec_" << val.Vector().Id() << " = 0\n";
  }

  void Visit(ProgramTransitionStateRegion val) override {
    std::stringstream tuple;
    tuple << "tuple";
    for (auto tuple_var : val.TupleVariables()) {
      tuple << "_" << tuple_var.Id();
    }

    auto tuple_var = tuple.str();
    os << os.Indent() << tuple_var << " = (";
    for (auto var : val.TupleVariables()) {
      os << "var_" << var.Id() << ", ";
    }
    os << ")\n";

    os << os.Indent() << "state = table_" << val.Table().Id() << "["
       << tuple_var << "]\n";

    os << os.Indent() << "if ";
    switch (val.FromState()) {
      case TupleState::kAbsent: os << "state == 0:\n"; break;
      case TupleState::kPresent: os << "state == 1:\n"; break;
      case TupleState::kUnknown: os << "state == 2:\n"; break;
      case TupleState::kAbsentOrUnknown:
        os << "state == 0 or state == 2:\n";
        break;
    }
    os.PushIndent();
    os << os.Indent() << "table_" << val.Table().Id() << "[" << tuple_var
       << "] = ";
    switch (val.ToState()) {
      case TupleState::kAbsent: os << "0\n"; break;
      case TupleState::kPresent: os << "1\n"; break;
      case TupleState::kUnknown: os << "2\n"; break;
      case TupleState::kAbsentOrUnknown:
        os << "2\n";
        assert(false);
        break;
    }

    for (auto index : val.Table().Indices()) {
      os << os.Indent() << "index_" << index.Id() << "[(";
      for (auto indexed_col : index.Columns()) {
        os << tuple_var << "[" << indexed_col.Index() << "], ";
      }
      os << ")].add((";
      for (auto mapped_col : index.MappedColumns()) {
        os << tuple_var << "[" << mapped_col.Index() << "], ";
      }
      os << "))\n";
    }

    if (auto body = val.Body(); body) {
      body->Accept(*this);
    }
    os.PopIndent();
  }

  void Visit(ProgramCheckStateRegion val) override {
    os << "ProgramCheckStateRegion\n";
  }

  void Visit(ProgramTableJoinRegion val) override {

    // Nested loop join
    auto vec = val.PivotVector();
    os << os.Indent() << "while index_vec_" << vec.Id() << " < len(vec_"
       << vec.Id() << "):\n";
    os.PushIndent();
    os << os.Indent();
    std::vector<std::string> var_names;
    for (auto var : val.OutputPivotVariables()) {
      std::stringstream var_name;
      var_name << "var_" << var.Id();
      var_names.emplace_back(var_name.str());
      os << var_names.back() << ", ";
    }
    os << "= vec_" << vec.Id() << "[index_vec_" << vec.Id() << "]\n";

    os << os.Indent() << "index_vec_" << vec.Id() << " += 1\n";

    auto tables = val.Tables();
    for (auto i = 0u; i < tables.size(); ++i) {
      auto index = val.Index(i);
      os << os.Indent() << "for tuple_" << val.UniqueId() << "_" << i
         << " in set(index_" << index.Id() << "[(";
      for (auto var : index.Columns()) {
        auto j = 0u;
        for (auto idx_col : val.IndexedColumns(i)) {
          if (idx_col == var) {
            os << var_names[j] << ", ";
          }
          ++j;
        }
      }
      os << ")]):\n";
      os.PushIndent();
      auto out_vars = val.OutputVariables(i);
      if (!out_vars.empty()) {
        os << os.Indent();
        for (auto var : out_vars) {
          os << "var_" << var.Id() << ", ";
        }
        os << "= tuple_" << val.UniqueId() << "_" << i << "\n";
      }
    }

    if (auto body = val.Body(); body) {
      body->Accept(*this);
    } else {
      os << os.Indent() << "pass\n";
    }

    for (auto table : tables) {
      (void) table;
      os.PopIndent();
    }
    os.PopIndent();
  }

  void Visit(ProgramTableProductRegion val) override {
    os << "ProgramTableProductRegion\n";
  }

  void Visit(ProgramTableScanRegion val) override {
    os << "ProgramTableScanRegion\n";
  }

  void Visit(ProgramTupleCompareRegion val) override {
    os << "ProgramTupleCompareRegion\n";
  }

 private:
  OutputStream &os;
};

static void DefineProcedure(OutputStream &os, ProgramProcedure proc) {
  os << "def proc_" << proc.Id() << "(";

  auto param_sep = "";

  // All vector params before all variable params
  for (auto param : proc.VectorParameters()) {
    os << param_sep << "vec_" << param.Id() << ": List[Tuple[";

    auto type_sep = "";
    for (auto type : param.ColumnTypes()) {
      os << type_sep << TypeName(type);
      type_sep = ", ";
    }

    os << "]]";
    param_sep = ", ";
  }

  for (auto param : proc.VariableParameters()) {
    os << param_sep << "var_" << param.Id() << ": " << TypeName(param.Type());
    param_sep = ", ";
  }

  os << ") -> bool:\n";

  os.PushIndent();
  for (auto param : proc.VectorParameters()) {
    os << os.Indent() << "index_vec_" << param.Id() << ": int = 0\n";
  }

  os << os.Indent() << "state: int = 0\n";

  for (auto vec : proc.DefinedVectors()) {
    os << os.Indent() << "vec_" << vec.Id() << ": List[Tuple[";

    auto type_sep = "";
    for (auto type : vec.ColumnTypes()) {
      os << type_sep << TypeName(type);
      type_sep = ", ";
    }

    os << "]] = list()\n";
    os << os.Indent() << "index_vec_" << vec.Id() << ": int = 0\n";
  }
  // Body
  PythonCodeGenVisitor visitor(os);
  proc.Body().Accept(visitor);

  os.PopIndent();
  os << "\n";
}


}  // namespace

// Emits Python code for the given program to `os`.
void GeneratePythonCode(Program &program, OutputStream &os) {
  os << "# Auto-generated file \n\n";

  os << "from collections import defaultdict\n";
  os << "from typing import *\n\n";

  for (auto table : program.Tables()) {
    DeclareTable(os, table);
  }

  // TODO(ekilmer): Global variables
  //   Single type, bool=false, int=0

  for (auto proc : program.Procedures()) {
    DefineProcedure(os, proc);
  }

  /*
  os << "if __name__ == \"__main__\":\n"
     << "  proc_1([(0,1), (0,2), (2,0), (1,2), (2,3)])\n"
     << "  for edge, state in table_7.items():\n"
     << "    print(edge)\n";
  */
}

}  // namespace hyde
