// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/CodeGen/CodeGen.h>

#include <unordered_set>

#include <drlojekyll/ControlFlow/Program.h>
#include <drlojekyll/Display/Format.h>
#include <drlojekyll/ControlFlow/Format.h>
#include <drlojekyll/Parse/Format.h>
#include <drlojekyll/Lex/Format.h>

namespace hyde {
namespace {

static const char *TypeName(TypeKind kind) {
  switch (kind) {
    case TypeKind::kSigned8: return "int8_t";
    case TypeKind::kSigned16: return "int16_t";
    case TypeKind::kSigned32: return "int32_t";
    case TypeKind::kSigned64: return "int64_t";
    case TypeKind::kUnsigned8: return "uint8_t";
    case TypeKind::kUnsigned16: return "uint16_t";
    case TypeKind::kUnsigned32: return "uint32_t";
    case TypeKind::kUnsigned64: return "uint64_t";
    case TypeKind::kFloat: return "float";
    case TypeKind::kDouble: return "double";
    case TypeKind::kBytes: return "::hyde::rt::Bytes";
    case TypeKind::kASCII: return "::hyde::rt::ASCII";
    case TypeKind::kUTF8: return "::hyde::rt::UTF8";
    case TypeKind::kUUID: return "::hyde::rt::UUID";
    default: assert(false); return "void";
  }
}

// Print out the full location of a token.
static void OutputToken(OutputStream &os, Token tok) {
  const auto pos = tok.Position();
  os << "{\"";
  os.DisplayNameOr(pos, "");
  os << "\", ";
  os.LineNumberOr(pos, "0");
  os << ", ";
  os.ColumnNumberOr(pos, "0");
  os << ", \"" << tok << "\"}";
}

// Declare a structure containing the information about a column.
static void DeclareColumn(OutputStream &os, DataTable table, DataColumn col) {
  os << "struct col_" << table.Id() << '_' << col.Index() << " {\n";
  os.PushIndent();

  const auto i = col.Index();
  os << os.Indent() << "using Type = " << TypeName(col.Type()) << ";\n";
  os << os.Indent() << "static constexpr std::size_t kIndex = " << i << "u;\n";
  if (i) {
    os << os.Indent() << "static constexpr std::size_t kOffset = col_"
       << table.Id() << '_' << (i - 1u) << "::kOffset + col_"
       << table.Id() << '_' << (i - 1u) << "::kSize;\n";
  } else {
    os << os.Indent() << "static constexpr std::size_t kOffset = 0u;\n";
  }
  os << os.Indent() << "static constexpr std::size_t kSize = sizeof(Type);\n";
  const auto names = col.PossibleNames();

  os << os.Indent() << "static const std::array<Token, "
     << names.size() << "> kNames{\n";
  os.PushIndent();
  for (auto name : names) {
    os << os.Indent();
    OutputToken(os, name);
    os << ",\n";
  }
  os.PopIndent();
  os << os.Indent() << "};\n";
  os.PopIndent();
  os << "};\n";
}

// Visit all uses of a vector. We care about uses that extract out tuples
// from the vector and bind their elements to variables.
class VectorUseVisitor final : public ProgramVisitor {
 public:
  explicit VectorUseVisitor(DataVector vec_)
      : vec(vec_),
        names(vec.ColumnTypes().size()) {}

  void Visit(ProgramVectorAppendRegion append) {
    unsigned i = 0u;
    for (auto var : append.TupleVariables()) {
      names[i++].insert(var.Name());
    }
  }

  void Visit(ProgramVectorLoopRegion loop) {
    unsigned i = 0u;
    for (auto var : loop.TupleVariables()) {
      names[i++].insert(var.Name());
    }
  }

  void Visit(ProgramTableJoinRegion join) {
    unsigned i = 0u;
    for (auto var : join.OutputPivotVariables()) {
      names[i++].insert(var.Name());
    }
  }

  void Visit(ProgramTableScanRegion scan) {
    unsigned i = 0u;
    for (auto col : scan.SelectedColumns()) {
      for (auto name : col.PossibleNames()) {
        names[i].insert(name);
      }
      ++i;
    }
  }


  const DataVector vec;

  // List of all variables associated with the `N`th vector element.
  std::vector<std::unordered_set<Token>> names;
};

// Declare structures for each of the columns used in a vector.
static void DeclareVectorColumns(OutputStream &os, DataVector vec) {
  VectorUseVisitor use_visitor(vec);
  vec.VisitUsers(use_visitor);

  unsigned i = 0u;
  for (auto type : vec.ColumnTypes()) {
    const auto &names = use_visitor.names[i];

    os << "struct col_" << vec.Id() << '_' << i << " {\n";
    os.PushIndent();

    os << os.Indent() << "using Type = " << TypeName(type) << ";\n";
    os << os.Indent() << "static constexpr std::size_t kIndex = " << i << "u;\n";
    if (i) {
      os << os.Indent() << "static constexpr std::size_t kOffset = col_"
         << vec.Id() << '_' << (i - 1u) << "::kOffset + col_"
         << vec.Id() << '_' << (i - 1u) << "::kSize;\n";
    } else {
      os << os.Indent() << "static constexpr std::size_t kOffset = 0u;\n";
    }
    os << os.Indent() << "static constexpr std::size_t kSize = sizeof(Type);\n";

    os << os.Indent() << "static const std::array<Token, "
       << names.size() << "> kNames{\n";
    os.PushIndent();

    for (auto name : names) {
      os << os.Indent();
      OutputToken(os, name);
      os << ",\n";
    }

    os.PopIndent();
    os << os.Indent() << "};\n";
    os.PopIndent();
    os << "};\n";

    ++i;
  }
}

// Declare a structure containing the information about a table.
static void DeclareTable(OutputStream &os, DataTable table) {
  os << "struct table_" << table.Id() << " {\n";
  os.PushIndent();
  os.PopIndent();
  os << "};\n";
}

}  // namespace

class CPPCodeGenVisitor final : public ProgramVisitor {
 public:
  explicit CPPCodeGenVisitor(OutputStream &os_) : os(os_) {}

  void Visit(DataColumn val) override {
    os << "DataColumn\n";
  }

  void Visit(DataIndex val) override {
    os << "DataIndex\n";
  }

  void Visit(DataTable val) override {
    os << "DataTable\n";
  }

  void Visit(DataVariable val) override {
    os << "DataVariable\n";
  }

  void Visit(DataVector val) override {
    os << "DataVector\n";
  }

  void Visit(ProgramCallRegion val) override {
    os << "ProgramCallRegion\n";
  }

  void Visit(ProgramReturnRegion val) override {
    os << "ProgramReturnRegion\n";
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
    os << "ProgramInductionRegion\n";
  }

  void Visit(ProgramLetBindingRegion val) override {
    os << "ProgramLetBindingRegion\n";
  }

  void Visit(ProgramParallelRegion val) override {
    os << "ProgramParallelRegion\n";
  }

  void Visit(ProgramProcedure val) override {
    os << "ProgramProcedure\n";
  }

  void Visit(ProgramPublishRegion val) override {
    os << "ProgramPublishRegion\n";
  }

  void Visit(ProgramSeriesRegion val) override {
    os << "ProgramSeriesRegion\n";
  }

  void Visit(ProgramVectorAppendRegion val) override {
    os << "ProgramVectorAppendRegion\n";
  }

  void Visit(ProgramVectorClearRegion val) override {
    os << "ProgramVectorClearRegion\n";
  }

  void Visit(ProgramVectorLoopRegion val) override {
    os << "ProgramVectorLoopRegion\n";
  }

  void Visit(ProgramVectorUniqueRegion val) override {
    os << "ProgramVectorUniqueRegion\n";
  }

  void Visit(ProgramTransitionStateRegion val) override {
    os << "ProgramTransitionStateRegion\n";
  }

  void Visit(ProgramCheckStateRegion val) override {
    os << "ProgramCheckStateRegion\n";
  }

  void Visit(ProgramTableJoinRegion val) override {
    os << "ProgramTableJoinRegion\n";
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

  void Visit(Program program) {
    for (auto table : program.Tables()) {
      for (auto col : table.Columns()) {
        DeclareColumn(os, table, col);
      }
      DeclareTable(os, table);
    }

    for (auto proc : program.Procedures()) {
      for (auto vec : proc.VectorParameters()) {
        DeclareVectorColumns(os, vec);
      }
      for (auto vec : proc.DefinedVectors()) {
        DeclareVectorColumns(os, vec);
      }
    }
  }

 private:
  OutputStream &os;
};

void GenerateCode(Program &program, OutputStream &os) {
  os << "using Token = std::tuple<std::string_view, unsigned, unsigned, std::string_view>;\n";
  CPPCodeGenVisitor visitor(os);
  visitor.Visit(program);
  os.Flush();
}

}  // namespace hyde
