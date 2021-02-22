// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/CodeGen/CodeGen.h>
#include <drlojekyll/ControlFlow/Format.h>
#include <drlojekyll/ControlFlow/Program.h>
#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Lex/Format.h>
#include <drlojekyll/Parse/Format.h>

#include <algorithm>
#include <unordered_set>
#include <vector>

namespace hyde {
namespace {

static const char *TypeName(TypeKind kind) {
  switch (kind) {
    case TypeKind::kBoolean: return "bool";
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

  // Calculate the number of indices in which this column participates.
  auto num_indices = 0;
  for (auto index : table.Indices()) {
    for (auto index_col : index.KeyColumns()) {
      if (index_col == col) {
        ++num_indices;
      }
    }
  }

  const auto names = col.PossibleNames();
  const auto i = col.Index();

  os << os.Indent() << "using Type = " << TypeName(col.Type()) << ";\n"
     << os.Indent() << "static constexpr bool kIsPersistent = true;\n"
     << os.Indent()
     << "static constexpr unsigned kNumIndexUses = " << num_indices << "u;\n"
     << os.Indent() << "static constexpr unsigned kId = " << col.Id() << "u;\n"
     << os.Indent() << "static constexpr unsigned kTableId = " << table.Id()
     << "u;\n"
     << os.Indent() << "static constexpr unsigned kIndex = " << i << "u;\n";
  if (i) {
    os << os.Indent() << "static constexpr unsigned kOffset = col_"
       << table.Id() << '_' << (i - 1u) << "::kOffset + col_" << table.Id()
       << '_' << (i - 1u) << "::kSize;\n";
  } else {
    os << os.Indent() << "static constexpr unsigned kOffset = 0u;\n";
  }
  os << os.Indent() << "static constexpr unsigned kSize = "
     << "static_cast<unsigned>(sizeof(Type));\n";

  os << os.Indent() << "static const Token kNames[] = {\n";
  os.PushIndent();

  unsigned num_names = 0u;
  for (auto name : names) {
    if (name.IsValid() && name.Position().IsValid()) {
      os << os.Indent();
      OutputToken(os, name);
      os << ",\n";
      ++num_names;
    }
  }
  os.PopIndent();
  os << os.Indent() << "};\n"
     << os.Indent() << "static constexpr unsigned kNumNames = " << num_names
     << "u;\n";
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

    os << os.Indent() << "using Type = " << TypeName(type) << ";\n"
       << os.Indent() << "static constexpr bool kIsPersistent = false;\n"
       << os.Indent() << "static constexpr unsigned kIndex = " << i << "u;\n";
    if (i) {
      os << os.Indent() << "static constexpr unsigned kOffset = col_"
         << vec.Id() << '_' << (i - 1u) << "::kOffset + col_" << vec.Id() << '_'
         << (i - 1u) << "::kSize;\n";
    } else {
      os << os.Indent() << "static constexpr unsigned kOffset = 0u;\n";
    }
    os << os.Indent() << "static constexpr unsigned kSize = "
       << "static_cast<unsigned>(sizeof(Type));\n";

    os << os.Indent() << "static const Token kNames[] = {\n";
    os.PushIndent();

    unsigned num_names = 0u;
    for (auto name : names) {
      if (name.IsValid() && name.Position().IsValid()) {
        os << os.Indent();
        OutputToken(os, name);
        os << ",\n";
        ++num_names;
      }
    }
    os.PopIndent();
    os << os.Indent() << "};\n"
       << os.Indent() << "static constexpr unsigned kNumNames = " << num_names
       << "u;\n";
    os.PopIndent();
    os << "};\n";

    ++i;
  }
}

// Find the largest set in `work_list` that is a subset of `*cols`. `work_list`
// is sorted in smallest to largest sets.
static void FindCover(std::vector<DataIndex> &work_list,
                      std::vector<DataIndex> &next_work_list,
                      std::vector<DataIndex> &covered) {

  next_work_list.clear();

  for (auto it = work_list.rbegin(), end = work_list.rend(); it != end; ++it) {

    DataIndex curr_index = covered.back();
    DataIndex maybe_subset_index = *it;

    // Figure out if `maybe_subset` is a subset of `cols`.
    auto i = 0u;
    for (auto col : maybe_subset_index.KeyColumns()) {

      const auto col_index = col.Index();
      const auto match_col_index = curr_index.KeyColumns()[i].Index();

      if (col_index == match_col_index) {
        ++i;

      } else if (col_index < match_col_index) {
        continue;

      } else {
        next_work_list.push_back(maybe_subset_index);
        goto skipped;
      }
    }

    // Found a subset, continue with it.
    covered.push_back(maybe_subset_index);

  skipped:
    continue;
  }

  std::reverse(next_work_list.begin(), next_work_list.end());
  next_work_list.swap(work_list);
}

// Declare the indices.
static unsigned DeclareIndices(OutputStream &os, DataTable table,
                               unsigned &next_index_id) {
  const auto indices = table.Indices();

  std::vector<DataIndex> work_list(indices.begin(), indices.end());
  std::vector<DataIndex> next_work_list;
  std::vector<std::vector<DataIndex>> grouped_indices;

  // We don't want to represent indices that map all columns separately from
  // the table itself so we'll strip those out.
  for (auto index : indices) {
    if (index.KeyColumns().size() < table.Columns().size()) {
      work_list.push_back(index);

    // TODO(pag): Decide on how to declare these.
    } else {
      assert(false);
    }
  }

  // Put biggest indices last.
  std::sort(work_list.begin(), work_list.end(), [](DataIndex a, DataIndex b) {
    return a.KeyColumns().size() < b.KeyColumns().size();
  });

  // Pop off the biggest index, then merge into it the next smallest index
  // that covers a subset of the columns, then use that next one for grouping.
  while (!work_list.empty()) {
    auto &covered = grouped_indices.emplace_back();
    covered.emplace_back(std::move(work_list.back()));
    work_list.pop_back();
    FindCover(work_list, next_work_list, covered);
  }

  unsigned num_indices = 0u;

  // Output the grouped indices.
  std::unordered_set<DataColumn> seen_cols;
  for (const auto &indices : grouped_indices) {
    seen_cols.clear();
    os << "struct index_" << table.Id() << '_' << num_indices << " {\n";
    os.PushIndent();
    os << os.Indent() << "static constexpr unsigned kId = " << (next_index_id++)
       << "u;\n"
       << os.Indent() << "static constexpr unsigned kSlot = " << num_indices
       << "u;\n"
       << os.Indent() << "using Spec = KeyValues<Columns<";
    auto sep = "";
    for (auto index : indices) {
      for (auto col : index.KeyColumns()) {
        if (seen_cols.count(col)) {
          continue;
        }
        seen_cols.insert(col);
        os << sep << "col_" << table.Id() << '_' << col.Id();
        sep = ", ";
      }
    }

    os << ">, Columns<";
    sep = "";

    // Add in the rest of the columns.
    for (auto col : table.Columns()) {
      if (!seen_cols.count(col)) {
        os << sep << "col_" << table.Id() << '_' << col.Id();
        sep = ", ";
      }
    }

    os << ">>;\n" << os.Indent() << "static Index *gStorage = nullptr;\n";

    os.PopIndent();
    os << "};\n";
    for (auto index : indices) {
      os << "using index_" << index.Id() << " = index_" << table.Id() << '_'
         << num_indices << ";\n";
    }

    ++num_indices;
  }

  //  // Make sure there's always at least one index so that we can cover the table.
  //  if (!num_indices) {
  //    num_indices = 1u;
  //    os << "struct index_" << table.Id() << "_0 {\n";
  //    os.PushIndent();
  //    os << os.Indent() << "using Spec = Columns<";
  //    auto sep = "";
  //    for (auto col : table.Columns()) {
  //      os << sep << "col_" << table.Id() << '_' << col.Id();
  //      sep = ", ";
  //    }
  //    os << ">;\n"
  //       << os.Indent() << "static Index *gStorage = nullptr;\n";
  //
  //    os.PopIndent();
  //    os << "};\n";
  //  }

  return num_indices;
}

// Declare a structure containing the information about a table.
static void DeclareTable(OutputStream &os, DataTable table,
                         unsigned &next_index_id) {

  // Figure out if this table supports deletions.
  auto is_differential = "false";
  auto has_insert = false;
  table.ForEachUser([&is_differential, &has_insert](ProgramRegion region) {
    if (region.IsTransitionState()) {
      auto trans = ProgramTransitionStateRegion::From(region);
      if (TupleState::kPresent != trans.ToState()) {
        is_differential = "true";
      } else {
        has_insert = true;
      }
    }
  });

  assert(has_insert);

  const auto num_indices = DeclareIndices(os, table, next_index_id);

  const auto cols = table.Columns();
  os << "struct table_" << table.Id() << " {\n";
  os.PushIndent();
  os << os.Indent() << "static constexpr unsigned kId = " << table.Id()
     << "u;\n"
     << os.Indent()
     << "static constexpr bool kIsDifferential = " << is_differential << ";\n"
     << os.Indent() << "static constexpr unsigned kNumColumns = " << cols.size()
     << "u;\n"
     << os.Indent() << "static constexpr unsigned kTupleSize = ";
  auto sep = "";
  for (auto col : cols) {
    os << sep << "col_" << table.Id() << '_' << col.Index() << "::kSize";
    sep = " + ";
  }
  os << ";\n" << os.Indent() << "using ColumnSpec = Columns<";

  sep = "";
  for (auto col : cols) {
    os << sep << "col_" << table.Id() << '_' << col.Index();
    sep = ", ";
  }
  os << ">;\n" << os.Indent() << "using IndexSpec = Indices<";

  sep = "";
  for (auto i = 0u; i < num_indices; ++i) {
    os << sep << "index_" << table.Id() << '_' << i;
    sep = ", ";
  }
  os << ">;\n"
     << os.Indent() << "static constexpr unsigned kNumIndices = " << num_indices
     << "u;\n"
     << os.Indent() << "static Table *gStorage = nullptr;\n";

  os.PopIndent();
  os << "};\n\n";
}

class CPPCodeGenVisitor final : public ProgramVisitor {
 public:
  explicit CPPCodeGenVisitor(OutputStream &os_) : os(os_) {}

  void Visit(ProgramCallRegion val) override {
    os << "ProgramCallRegion\n";
  }

  void Visit(ProgramReturnRegion val) override {
    os << "ProgramReturnRegion\n";
  }

  void Visit(ProgramExistenceAssertionRegion val) override {
    os << "ProgramExistenceAssertionRegion\n";
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

 private:
  OutputStream &os;
};

void DefineMainFunction(OutputStream &os, Program program,
                        unsigned num_indices) {

  os << "extern \"C\" int main(int argc, char *argv[]) {\n";
  os.PushIndent();
  os << os.Indent() << "drlojekyll::Init(argc, argv, "
     << program.Tables().size() << ", " << num_indices << ", proc_0);\n";

  for (auto table : program.Tables()) {
    os << os.Indent() << "drlojekyll::CreateTable<table_" << table.Id()
       << ">();\n";
  }

  for (auto proc : program.Procedures()) {
    if (auto maybe_messsage = proc.Message(); maybe_messsage) {
      auto message = *maybe_messsage;
      os << os.Indent() << "drlojekyll::RegisterHandler(\"" << message.Name()
         << "\", proc_" << proc.Id() << ");\n";
    }
  }

  os << "  return drlojekyll::Run();\n";
  os.PopIndent();
  os << "}\n\n";
}

}  // namespace

// Emits C++ code for the given program to `os`.
void GenerateCxxDatabaseCode(const Program &program, OutputStream &os) {
  os << "/* Auto-generated file */\n\n"
     << "#include <drlojekyll/Runtime.h>\n\n"
     << "namespace {\n";

  unsigned next_index_id = 0u;

  for (auto table : program.Tables()) {
    for (auto col : table.Columns()) {
      DeclareColumn(os, table, col);
    }
    DeclareTable(os, table, next_index_id);
  }

  if (false) {
    for (auto proc : program.Procedures()) {
      for (auto vec : proc.VectorParameters()) {
        DeclareVectorColumns(os, vec);
      }
      for (auto vec : proc.DefinedVectors()) {
        DeclareVectorColumns(os, vec);
      }
    }
  }

  os << "}  // namespace\n\n";

  DefineMainFunction(os, program, next_index_id);

  //  CPPCodeGenVisitor visitor(os);
  //  visitor.Visit(program);
  //  os.Flush();
}

}  // namespace hyde
