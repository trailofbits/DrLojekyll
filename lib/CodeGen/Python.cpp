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

// NOTE(ekilmer): Classes are named all the same for now
constexpr auto gClassName = "Database";

// Make a comment in code for debugging purposes
static OutputStream &Comment(OutputStream &os, const char *message) {
#ifndef NDEBUG
  return os << os.Indent() << "# " << message << "\n";
#else
  return os;
#endif
}

static OutputStream &Procedure(OutputStream &os, const ProgramProcedure proc) {
  switch (proc.Kind()) {
    case ProcedureKind::kInitializer: return os << "init_" << proc.Id();
    case ProcedureKind::kMessageHandler:
      return os << proc.Message().value().Name() << "_"
                << proc.Message().value().Arity();
    case ProcedureKind::kTupleFinder:
    case ProcedureKind::kTupleRemover:
    default: return os << "proc_" << proc.Id();
  }
}

static OutputStream &Functor(OutputStream &os, const ParsedFunctor func) {
  return os << func.Name() << "_" << ParsedDeclaration(func).BindingPattern();
}

static OutputStream &Table(OutputStream &os, const DataTable table) {
  return os << "self.table_" << table.Id();
}

static OutputStream &TableIndex(OutputStream &os, const DataIndex index) {
  return os << "self.index_" << index.Id();
}

template <typename Stream>
static Stream &Var(Stream &os, const DataVariable var) {
  if (var.IsGlobal()) {
    os << "self.";
  }
  os << "var_" << var.Id();
  return os;
}

static OutputStream &Vector(OutputStream &os, const DataVector vec) {
  return os << "vec_" << vec.Id();
}

static OutputStream &VectorIndex(OutputStream &os, const DataVector vec) {
  return os << "vec_index" << vec.Id();
}

// Python representation of TypeKind
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
    default: assert(false); return "Any";
  }
}

static const char *OperatorString(ComparisonOperator op) {
  switch (op) {
    case ComparisonOperator::kEqual: return "==";
    case ComparisonOperator::kNotEqual: return "!=";
    case ComparisonOperator::kLessThan: return "<";
    case ComparisonOperator::kGreaterThan: return ">";

    // TODO(ekilmer): What's a good default operator?
    default: assert(false); return "None";
  }
}

static std::string TypeValueOrDefault(TypeKind kind,
                                      std::optional<ParsedLiteral> val) {
  auto prefix = "";
  auto suffix = "";

  // Default value
  switch (kind) {
    case TypeKind::kSigned8:
    case TypeKind::kSigned16:
    case TypeKind::kSigned32:
    case TypeKind::kSigned64:
    case TypeKind::kUnsigned8:
    case TypeKind::kUnsigned16:
    case TypeKind::kUnsigned32:
    case TypeKind::kUnsigned64:
      prefix = "int(";
      suffix = ")";
      break;
    case TypeKind::kFloat:
    case TypeKind::kDouble:
      prefix = "float(";
      suffix = ")";
      break;
    case TypeKind::kBytes:
      prefix = "b\"";
      suffix = "\"";
      break;
    case TypeKind::kASCII:
    case TypeKind::kUTF8:
    case TypeKind::kUUID:
      prefix = "\"";
      suffix = "\"";
      break;
    default: assert(false); prefix = "None  #";
  }

  std::stringstream value;
  value << prefix;
  if (val) {
    value << val->Spelling();
  }
  value << suffix;
  return value.str();
}

// Declare a set to hold the table.
static void DefineTable(OutputStream &os, DataTable table) {
  os << os.Indent() << Table(os, table) << ": DefaultDict[Tuple[";
  auto sep = "";
  for (auto col : table.Columns()) {
    os << sep << TypeName(col.Type());
    sep = ", ";
  }
  os << "], int] = defaultdict(int)\n";

  // We represent indices as mappings to vectors so that we can concurrently
  // write to them while iterating over them (via an index and length check).
  for (auto index : table.Indices()) {
    os << os.Indent() << TableIndex(os, index) << ": DefaultDict[Tuple[";
    sep = "";
    for (auto col : index.KeyColumns()) {
      os << sep << TypeName(col.Type());
      sep = ", ";
    }
    os << "], List[Tuple[";
    if (!index.ValueColumns().empty()) {
      sep = "";
      for (auto col : index.ValueColumns()) {
        os << sep << TypeName(col.Type());
        sep = ", ";
      }

    // Valid syntax for empty Tuple
    } else {
      os << "()";
    }
    os << "]]] = defaultdict(list)\n";
  }
  os << "\n";
}

static void DefineGlobal(OutputStream &os, DataVariable global) {
  auto type = global.Type();
  os << os.Indent() << Var(os, global) << ": " << TypeName(type) << " = "
     << TypeValueOrDefault(type, global.Value()) << "\n\n";
}

// Similar to DefineGlobal except has type-hint to enforce const-ness
static void DefineConstant(OutputStream &os, DataVariable global) {
  auto type = global.Type();
  os << os.Indent() << Var(os, global) << ": Final[" << TypeName(type)
     << "] = " << TypeValueOrDefault(type, global.Value()) << "\n\n";
}

class PythonCodeGenVisitor final : public ProgramVisitor {
 public:
  explicit PythonCodeGenVisitor(OutputStream &os_) : os(os_) {}

  void Visit(ProgramCallRegion region) override {
    os << Comment(os, "Program Call Region");
    os << os.Indent() << "# TODO(ekilmer): ProgramCallRegion\n";
  }

  void Visit(ProgramReturnRegion region) override {
    os << Comment(os, "Program Return Region");

    os << os.Indent() << "return " << (region.ReturnsFalse() ? "False" : "True")
       << "\n";
  }

  void Visit(ProgramExistenceAssertionRegion region) override {
    os << Comment(os, "Program ExistenceAssertion Region");
    os << os.Indent() << "# TODO(ekilmer): ProgramExistenceAssertionRegion\n";
  }

  void Visit(ProgramExistenceCheckRegion region) override {
    os << Comment(os, "Program ExistenceCheck Region");
    os << os.Indent() << "# TODO(ekilmer): ProgramExistenceCheckRegion\n";
  }

  void Visit(ProgramGenerateRegion region) override {
    os << Comment(os, "Program Generate Region");

    os << os.Indent();
    for (auto out_var : region.OutputVariables()) {
      os << Var(os, out_var) << ", ";
    }
    os << "= " << Functor(os, region.Functor()) << "(";
    auto sep = "";
    for (auto in_var : region.InputVariables()) {
      os << sep << Var(os, in_var);
      sep = ", ";
    }
    os << ")\n";

    if (auto body = region.Body(); body) {
      body->Accept(*this);
    }
  }

  void Visit(ProgramInductionRegion region) override {
    os << Comment(os, "Program Induction Init Region");

    // Base case
    region.Initializer().Accept(*this);

    // Fixpoint
    os << Comment(os, "Induction Fixpoint Loop Region");
    os << os.Indent() << "while ";
    auto sep = "";
    for (auto vec : region.Vectors()) {
      os << sep << VectorIndex(os, vec) << " < len(" << Vector(os, vec) << ")";
      sep = " or ";
    }
    os << ":\n";

    os.PushIndent();
    region.FixpointLoop().Accept(*this);
    os.PopIndent();

    for (auto vec : region.Vectors()) {
      os << os.Indent() << VectorIndex(os, vec) << " = 0\n";
    }

    // Output
    if (auto output = region.Output(); output) {
      os << Comment(os, "Induction Output Region");
      output->Accept(*this);
    }
  }

  void Visit(ProgramLetBindingRegion region) override {
    os << Comment(os, "Program LetBinding Region");
    os << os.Indent() << "# TODO(ekilmer): ProgramLetBindingRegion\n";
  }

  void Visit(ProgramParallelRegion region) override {
    os << Comment(os, "Program Parallel Region");

    // Same as SeriesRegion
    for (auto sub_region : region.Regions()) {
      sub_region.Accept(*this);
    }
  }

  void Visit(ProgramProcedure region) override {
    os << Comment(os, "Program Procedure Region");
    os << os.Indent() << "# TODO(ekilmer): ProgramProcedure\n";
  }

  void Visit(ProgramPublishRegion region) override {
    os << Comment(os, "Program Publish Region");
    os << os.Indent() << "# TODO(ekilmer): ProgramPublishRegion\n";
  }

  void Visit(ProgramSeriesRegion region) override {
    os << Comment(os, "Program Series Region");

    for (auto sub_region : region.Regions()) {
      sub_region.Accept(*this);
    }
  }

  void Visit(ProgramVectorAppendRegion region) override {
    os << Comment(os, "Program VectorAppend Region");

    os << os.Indent() << Vector(os, region.Vector()) << ".append((";
    for (auto var : region.TupleVariables()) {
      os << Var(os, var) << ", ";
    }
    os << "))\n";
  }

  void Visit(ProgramVectorClearRegion region) override {
    os << Comment(os, "Program VectorClear Region");

    os << os.Indent() << "del " << Vector(os, region.Vector()) << "[:]\n";
    os << os.Indent() << VectorIndex(os, region.Vector()) << " = 0\n";
  }

  void Visit(ProgramVectorLoopRegion region) override {
    os << Comment(os, "Program VectorLoop Region");

    auto vec = region.Vector();
    os << os.Indent() << "while " << VectorIndex(os, vec) << " < len("
       << Vector(os, vec) << "):\n";
    os.PushIndent();
    os << os.Indent();
    for (auto var : region.TupleVariables()) {
      os << Var(os, var) << ", ";
    }
    os << "= " << Vector(os, vec) << "[" << VectorIndex(os, vec) << "]\n";

    os << os.Indent() << VectorIndex(os, vec) << " += 1\n";

    if (auto body = region.Body(); body) {
      body->Accept(*this);
    }
    os.PopIndent();
  }

  void Visit(ProgramVectorUniqueRegion region) override {
    os << Comment(os, "Program VectorUnique Region");

    os << os.Indent() << Vector(os, region.Vector()) << " = list(set("
       << Vector(os, region.Vector()) << "))\n";
    os << os.Indent() << VectorIndex(os, region.Vector()) << " = 0\n";
  }

  void Visit(ProgramTransitionStateRegion region) override {
    os << Comment(os, "Program TransitionState Region");

    std::stringstream tuple;
    tuple << "tuple";
    for (auto tuple_var : region.TupleVariables()) {
      tuple << "_" << tuple_var.Id();
    }

    auto tuple_var = tuple.str();
    os << os.Indent() << tuple_var << " = (";
    for (auto var : region.TupleVariables()) {
      os << Var(os, var) << ", ";
    }
    os << ")\n";

    os << os.Indent() << "state = " << Table(os, region.Table()) << "["
       << tuple_var << "]\n";

    os << os.Indent() << "if ";
    switch (region.FromState()) {
      case TupleState::kAbsent: os << "state == 0:\n"; break;
      case TupleState::kPresent: os << "state == 1:\n"; break;
      case TupleState::kUnknown: os << "state == 2:\n"; break;
      case TupleState::kAbsentOrUnknown:
        os << "state == 0 or state == 2:\n";
        break;
    }
    os.PushIndent();
    os << os.Indent() << Table(os, region.Table()) << "[" << tuple_var
       << "] = ";
    switch (region.ToState()) {
      case TupleState::kAbsent: os << "0\n"; break;
      case TupleState::kPresent: os << "1\n"; break;
      case TupleState::kUnknown: os << "2\n"; break;
      case TupleState::kAbsentOrUnknown:
        os << "2\n";
        assert(false);
        break;
    }

    for (auto index : region.Table().Indices()) {
      os << os.Indent() << TableIndex(os, index) << "[(";
      for (auto indexed_col : index.KeyColumns()) {
        os << tuple_var << "[" << indexed_col.Index() << "], ";
      }
      os << ")].append((";
      for (auto mapped_col : index.ValueColumns()) {
        os << tuple_var << "[" << mapped_col.Index() << "], ";
      }
      os << "))\n";
    }

    if (auto body = region.Body(); body) {
      body->Accept(*this);
    }
    os.PopIndent();
  }

  void Visit(ProgramCheckStateRegion region) override {
    os << Comment(os, "Program CheckState Region");
    os << os.Indent() << "# TODO(ekilmer): ProgramCheckStateRegion\n";
  }

  void Visit(ProgramTableJoinRegion region) override {
    os << Comment(os, "Program TableJoin Region");

    // Nested loop join
    auto vec = region.PivotVector();
    os << os.Indent() << "while " << VectorIndex(os, vec) << " < len("
       << Vector(os, vec) << "):\n";
    os.PushIndent();
    os << os.Indent();

    std::vector<std::string> var_names;
    for (auto var : region.OutputPivotVariables()) {
      std::stringstream var_name;
      (void) Var(var_name, var);
      var_names.emplace_back(var_name.str());
      os << var_names.back() << ", ";
    }
    os << "= " << Vector(os, vec) << "[" << VectorIndex(os, vec) << "]\n";

    os << os.Indent() << VectorIndex(os, vec) << " += 1\n";

    auto tables = region.Tables();
    for (auto i = 0u; i < tables.size(); ++i) {
      auto index = region.Index(i);

      // We don't want to have to make a temporary copy of the current state
      // of the index, so instead what we do is we capture a reference to the
      // list of tuples in the index, and we also create an index variable
      // that tracks which tuple we can next look at. This allows us to observe
      // writes into the index as they happen.
      os << os.Indent() << "tuple_" << region.Id() << "_" << i
         << "_index: int = 0\n"
         << os.Indent() << "tuple_" << region.Id() << "_" << i
         << "_vec: List[Tuple[";

      auto sep = "";
      if (!index.ValueColumns().empty()) {
        for (auto col : index.ValueColumns()) {
          os << sep << TypeName(col.Type());
          sep = ", ";
        }
      } else {
        os << "()";
      }

      os << "]] = " << TableIndex(os, index) << "[(";

      // This is a bit ugly, but basically: we want to index into the
      // Python representation of this index, e.g. via `index_10[(a, b)]`,
      // where `a` and `b` are pivot variables. However, the pivot vector
      // might have the tuple entries in the order `(b, a)`. To easy matching
      // between pivot variables and indexed columns, `region.IndexedColumns`
      // exposes columns in the same order as the pivot variables, which as we
      // see, might not match the order of the columns in the index. Thus we
      // need to re-order our usage of variables so that they match the
      // order expected by `index_10[...]`.
      for (auto index_col : index.KeyColumns()) {
        auto j = 0u;
        for (auto used_col : region.IndexedColumns(i)) {
          if (used_col == index_col) {
            os << var_names[j] << ", ";
          }
          ++j;
        }
      }

      os << ")]\n";

      os << os.Indent() << "while tuple_" << region.Id() << "_" << i
         << "_index < len(tuple_" << region.Id() << "_" << i << "_vec):\n";

      // We increase indentation here, and the corresponding `PopIndent()`
      // only comes *after* visiting the `region.Body()`.
      os.PushIndent();

      os << os.Indent() << "tuple_" << region.Id() << "_" << i << " = "
         << "tuple_" << region.Id() << "_" << i << "_vec[tuple_" << region.Id()
         << "_" << i << "_index]\n";

      os << os.Indent() << "tuple_" << region.Id() << "_" << i
         << "_index += 1\n";

      auto out_vars = region.OutputVariables(i);
      if (!out_vars.empty()) {
        auto select_cols = region.SelectedColumns(i);
        assert(out_vars.size() == select_cols.size());

        auto indexed_cols = region.IndexedColumns(i);
        auto indexed_col_idx = 0u;
        auto out_var_idx = 0u;
        auto tuple_col_idx_offset = 0u;
        for (auto var : out_vars) {
          auto select_col_idx = select_cols[out_var_idx].Index();

          // Need to loop and count indexed columns before this selected
          // column and use as offset
          while (indexed_col_idx < indexed_cols.size() &&
                 select_col_idx > indexed_cols[indexed_col_idx].Index()) {
            ++tuple_col_idx_offset;
            ++indexed_col_idx;
          }

          os << os.Indent() << Var(os, var) << " = tuple_" << region.Id() << "_"
             << i << "[" << select_col_idx - tuple_col_idx_offset << "]\n";

          ++out_var_idx;
        }
      }
    }

    if (auto body = region.Body(); body) {
      body->Accept(*this);
    } else {
      os << os.Indent() << "pass\n";
    }

    // Outdent for each nested for loop over an index.
    for (auto table : tables) {
      (void) table;
      os.PopIndent();
    }

    // Output of the loop over the pivot vector.
    os.PopIndent();
  }

  void Visit(ProgramTableProductRegion region) override {
    os << Comment(os, "Program TableProduct Region");
    os << os.Indent() << "# TODO(ekilmer): ProgramTableProductRegion\n";
  }

  void Visit(ProgramTableScanRegion region) override {
    os << Comment(os, "Program TableScan Region");
    os << os.Indent() << "# TODO(ekilmer): ProgramTableScanRegion\n";
  }

  void Visit(ProgramTupleCompareRegion region) override {
    os << Comment(os, "Program TupleCompare Region");

    os << os.Indent() << "if (";
    for (auto lhs : region.LHS()) {
      os << Var(os, lhs) << ", ";
    }

    os << ") " << OperatorString(region.Operator()) << " (";

    for (auto rhs : region.RHS()) {
      os << Var(os, rhs) << ", ";
    }
    os << "):\n";

    os.PushIndent();

    // If there isn't a body, then an optimization to eliminate this hasn't applied,
    // or it hasn't been coded
    assert(region.Body());
    region.Body()->Accept(*this);
    os.PopIndent();
  }

 private:
  OutputStream &os;
};

static void DefineFunctor(OutputStream &os, ParsedFunctor func) {
  os << os.Indent() << "def " << Functor(os, func) << "(";

  std::stringstream return_tuple;
  auto sep_ret = "";
  auto sep_bound = "";
  for (auto param : func.Parameters()) {
    if (param.Binding() == ParameterBinding::kBound) {
      os << sep_bound << param.Name() << ": " << TypeName(param.Type().Kind());
      sep_bound = ", ";
    } else {
      return_tuple << sep_ret << TypeName(param.Type().Kind());
      sep_ret = ", ";
    }
  }

  os << ") -> ";

  if (func.IsFilter()) {
    os << "bool";
  } else {
    os << "Tuple[";
    if (auto ret_types = return_tuple.str(); !ret_types.empty()) {
      os << ret_types;
    } else {
      os << "()";
    }
    os << "]";
  }

  os << ":\n";

  os.PushIndent();
  os << os.Indent() << "pass\n\n";
  os.PopIndent();
}

static void DefineProcedure(OutputStream &os, ProgramProcedure proc) {
  os << os.Indent() << "def " << Procedure(os, proc) << "(self";

  // First, declare all vector parameters.
  for (auto vec : proc.VectorParameters()) {
    os << ", " << Vector(os, vec) << ": List[Tuple[";

    auto type_sep = "";
    for (auto type : vec.ColumnTypes()) {
      os << type_sep << TypeName(type);
      type_sep = ", ";
    }

    os << "]]";
  }

  // Then, declare all variable parameters.
  for (auto param : proc.VariableParameters()) {
    os << ", " << Var(os, param) << ": " << TypeName(param.Type());
  }

  // Every procedure has a boolean return type. A lot of the time the return
  // type is not used, but for top-down checkers (which try to prove whether or
  // not a tuple in an unknown state is either present or absent) it is used.
  os << ") -> bool:\n";

  os.PushIndent();
  os << os.Indent() << "state: int = 0\n";

  // Every vector, including parameter vectors, has a variable tracking the
  // current index into that vector.
  //
  // TODO(pag, ekilmer): Consider passing these as arguments... hrmm. This may
  //                     be relevant if we factor out common sub-regions into
  //                     procedures. Then there would need to be implied return
  //                     values of all of the updated index positions that would
  //                     turn the return value of the procedures from a `bool`
  //                     to a `tuple`. :-/
  for (auto vec : proc.VectorParameters()) {
    os << os.Indent() << VectorIndex(os, vec) << ": int = 0\n";
  }

  // Define the vectors that will be created and used within this procedure.
  // These vectors exist to support inductions, joins (pivot vectors), etc.
  for (auto vec : proc.DefinedVectors()) {
    os << os.Indent() << Vector(os, vec) << ": List[Tuple[";

    auto type_sep = "";
    for (auto type : vec.ColumnTypes()) {
      os << type_sep << TypeName(type);
      type_sep = ", ";
    }

    os << "]] = list()\n";

    // Tracking variable for the vector.
    os << os.Indent() << VectorIndex(os, vec) << ": int = 0\n";
  }

  // Visit the body of the procedure. Procedure bodies are never empty; the
  // most trivial procedure body contains a `return False`.
  PythonCodeGenVisitor visitor(os);
  proc.Body().Accept(visitor);

  os.PopIndent();
  os << "\n";
}

}  // namespace

// Emits Python code for the given program to `os`.
void GeneratePythonCode(Program &program, OutputStream &os) {
  os << "# Auto-generated file\n\n";

  os << "from collections import defaultdict\n";
  os << "from typing import *\n\n";

  // TODO(ekilmer): Functors are predefined with `pass` for now
  for (auto func : program.ParsedModule().Functors()) {
    DefineFunctor(os, func);
  }

  for (auto code : program.ParsedModule().Inlines()) {
    if (code.Language() == Language::kPython) {
      os << code.CodeToInline() << '\n';
    }
  }

  // A program gets its own class
  os << "class " << gClassName << ":\n\n";
  os.PushIndent();

  os << os.Indent() << "def __init__(self):\n";
  os.PushIndent();

  for (auto table : program.Tables()) {
    DefineTable(os, table);
  }

  for (auto global : program.GlobalVariables()) {
    DefineGlobal(os, global);
  }

  for (auto constant : program.Constants()) {
    DefineConstant(os, constant);
  }

  // Invoke the init procedure. Always first
  auto init_procedure = program.Procedures()[0];
  assert(init_procedure.Kind() == ProcedureKind::kInitializer);
  os << os.Indent() << "self." << Procedure(os, init_procedure) << "()\n\n";

  os.PopIndent();

  for (auto proc : program.Procedures()) {
    DefineProcedure(os, proc);
  }

  os.PopIndent();

  // os << "if __name__ == \"__main__\":\n"
  //    << "  db = Database()\n"
  //    << "  db.proc_1([(0,1), (0,2), (2,0), (1,2), (2,3)])\n"
  //    << "  for edge, state in db.table_7.items():\n"
  //    << "    print(edge)\n";
}

}  // namespace hyde
