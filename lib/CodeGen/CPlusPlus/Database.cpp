// Copyright 2021, Trail of Bits. All rights reserved.

#include <drlojekyll/CodeGen/CodeGen.h>
#include <drlojekyll/ControlFlow/Format.h>
#include <drlojekyll/ControlFlow/Program.h>
#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Lex/Format.h>
#include <drlojekyll/Parse/Format.h>
#include <drlojekyll/Parse/ModuleIterator.h>

#include <algorithm>
#include <sstream>
#include <unordered_set>
#include <vector>

#include "CPlusPlus/Util.h"

namespace hyde {
namespace cxx {
namespace {

static OutputStream &Table(OutputStream &os, const DataTable table) {
  return os << "table_" << table.Id();
}

static OutputStream &TableIndex(OutputStream &os, const DataIndex index) {
  return os << "index_" << index.Id();
}

// Declare a structure containing the information about a table.
static void DefineTable(OutputStream &os, ParsedModule module,
                        DataTable table) {
  os << os.Indent() << "::hyde::rt::Table<StorageEngine, " << table.Id()
     << ", ";
  auto sep = "";
  const auto cols = table.Columns();
  if (cols.size() == 1u) {
    os << TypeName(module, cols[0].Type());

  } else {
    for (auto col : cols) {
      os << sep << TypeName(module, col.Type());
      sep = ", ";
    }
  }
  os << "> " << Table(os, table) << ";\n";

  // We represent indices as mappings to vectors so that we can concurrently
  // write to them while iterating over them (via an index and length check).
  for (auto index : table.Indices()) {
    const auto key_cols = index.KeyColumns();
    const auto val_cols = index.ValueColumns();
    (void) val_cols;

    os << os.Indent() << "::hyde::rt::Index<StorageEngine, " << table.Id()
       << ", " << index.Id() << ", ::hyde::rt::Key<";
    sep = "";
    for (auto col : index.KeyColumns()) {
      os << sep << "::hyde::rt::Column<" << col.Index() << ", "
         << TypeName(module, col.Type()) << ">";
      sep = ", ";
    }
    os << ">, ::hyde::rt::Value<";
    sep = "";
    for (auto col : index.ValueColumns()) {
      os << sep << "::hyde::rt::Column<" << col.Index() << ", "
         << TypeName(module, col.Type()) << ">";
      sep = ", ";
    }
    os << ">>";

    // The index can be implemented with the keys in the Table.
    // In this case, the index lookup will be an `if ... in ...`.
    if (key_cols.size() == cols.size()) {
      assert(val_cols.empty());

      // Implement this index as an accessor that just uses the Table
      os << "& " << TableIndex(os, index) << "() { return &" << Table(os, table)
         << "; }\n";
    } else {
      os << " " << TableIndex(os, index) << ";\n";
    }
  }
  os << "\n";
}

static void DefineGlobal(OutputStream &os, ParsedModule module,
                         DataVariable global) {
  auto type = global.Type();
  os << os.Indent();
  if (global.IsConstant()) {
    os << "static constexpr ";
  }
  os << TypeName(module, type) << " ";
  os << Var(os, global) << ";\n";
}

// Similar to DefineGlobal except has constexpr to enforce const-ness
static void DefineConstant(OutputStream &os, ParsedModule module,
                           DataVariable global) {
  switch (global.DefiningRole()) {
    case VariableRole::kConstantZero:
    case VariableRole::kConstantOne:
    case VariableRole::kConstantFalse:
    case VariableRole::kConstantTrue: return;
    default: break;
  }
  auto type = global.Type();
  os << os.Indent() << "static constexpr " << TypeName(module, type) << " "
     << Var(os, global) << " = " << TypeName(module, type)
     << TypeValueOrDefault(module, type, global) << ";\n";
}

// We want to enable referential transparency in the code, so that if an Nth
// value is produced by some mechanism that is equal to some prior value, then
// we replace its usage with the prior value.
static void DefineTypeRefResolver(OutputStream &os) {  // , ParsedModule module,

  // ParsedForeignType type) {
  // if (type.IsBuiltIn()) {
  //   return;
  // }

  // Has merge_into method
  os << os.Indent() << "template<typename T>\n"
     << os.Indent()
     << "typename ::hyde::rt::enable_if<::hyde::rt::has_merge_into<T,\n"
     << os.Indent() << "         std::string(T::*)()>::value, T>::type\n"
     << os.Indent() << "_resolve(T obj) {\n";
  os.PushIndent();
  os << os.Indent() << "auto ref_list = _refs[std::hash<T>(obj)];\n"
     << os.Indent() << "for (auto maybe_obj : ref_list) {\n";
  os.PushIndent();
  os << os.Indent() << "if (&obj == &maybe_obj) {\n";
  os.PushIndent();
  os << os.Indent() << "return obj;\n";
  os.PopIndent();
  os << os.Indent() << "} else if (obj == maybe_obj) {\n";
  os.PushIndent();
  os << os.Indent() << "T prior_obj  = static_cast<T>(maybe_obj);\n"
     << os.Indent() << "obj.merge_into(prior_obj);\n"
     << os.Indent() << "return prior_obj;\n";
  os.PopIndent();
  os << os.Indent() << "}\n";
  os.PopIndent();
  os << os.Indent() << "}\n"
     << os.Indent() << "ref_list.push_back(obj);\n"
     << os.Indent() << "return obj;\n";
  os.PopIndent();
  os << os.Indent() << "}\n\n";

  // Does not have merge_into
  os << os.Indent() << "template<typename T>\n"
     << os.Indent()
     << "typename ::hyde::rt::enable_if<!::hyde::rt::has_merge_into<T,\n"
     << os.Indent() << "         std::string(T::*)()>::value, T>::type\n"
     << os.Indent() << "_resolve(T obj) {\n";
  os.PushIndent();
  os << os.Indent() << "return obj;\n";
  os.PopIndent();
  os << os.Indent() << "}\n\n";

  /* Old way following what Python has
  os << os.Indent() << "static constexpr bool _HAS_MERGE_METHOD_" << type.Name()
     << " = ::hyde::rt::hasattr<" << TypeName(type) << ">(\"merge_into\");\n"
     << os.Indent() << "static constexpr void (* _MERGE_METHOD_" << type.Name()
     << ")(" << TypeName(type) << ", " << TypeName(type) << ") = ::hyde::rt::getattr<"
     << TypeName(type) << ">(\"merge_into\", [] ("
     << TypeName(type) << " a, " << TypeName(type)
     << " b){return nullptr;});\n\n"
     << os.Indent() << TypeName(type) << " _resolve_" << type.Name() << "("
     << TypeName(type) << " obj) {\n";

  os.PushIndent();
  os << os.Indent() << "if constexpr (" << gClassName << "::_HAS_MERGE_METHOD_"
     << type.Name() << ") {\n";
  os.PushIndent();

  os << os.Indent() << "auto ref_list = _refs[std::hash<" << TypeName(type) << ">(obj)];\n"
     << os.Indent() << "for (auto maybe_obj : ref_list) {\n";
  os.PushIndent();

  // The proposed object is identical (referentially) to the old one.
  os << os.Indent() << "if (&obj == &maybe_obj) {\n";
  os.PushIndent();
  os << os.Indent() << "return obj;\n";
  os.PopIndent();

  // The proposed object is structurally equivalent to the old one.
  os << os.Indent() << "} else if (obj == maybe_obj) {\n";
  os.PushIndent();
  os << os.Indent() << TypeName(type) << " prior_obj "
     << " = static_cast<" << TypeName(type) << ">(maybe_obj);\n";

  // Merge the new value `obj` into the prior value, `prior_obj`.
  os << os.Indent() << gClassName << "::_MERGE_METHOD_" << type.Name()
     << "(obj, prior_obj);\n";
  os << os.Indent() << "return prior_obj;\n";

  os.PopIndent();
  os << os.Indent() << "}\n";
  os.PopIndent();
  os << os.Indent() << "}\n";

  // We didn't find a prior version of the object; add our object in.
  os << os.Indent() << "ref_list.push_back(obj);\n";
  os.PopIndent();
  os << os.Indent() << "}\n";

  os << os.Indent() << "return obj;\n";

  os.PopIndent();
  os << os.Indent() << "}\n\n";
  */
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

static void DeclareFunctor(OutputStream &os, ParsedModule module,
                           ParsedFunctor func) {
  std::stringstream return_tuple;
  std::vector<ParsedParameter> args;
  auto sep_ret = "";
  auto num_ret_types = 0u;
  for (auto param : func.Parameters()) {
    if (param.Binding() == ParameterBinding::kBound) {
      args.push_back(param);
    } else {
      ++num_ret_types;
      return_tuple << sep_ret << TypeName(module, param.Type().Kind());
      sep_ret = ", ";
    }
  }

  os << os.Indent();

  if (func.IsFilter()) {
    assert(func.Range() == FunctorRange::kZeroOrOne);
    os << "bool";

  } else {
    auto tuple_prefix = "";
    auto tuple_suffix = "";
    if (1u < num_ret_types) {
      tuple_prefix = "std::tuple<";
      tuple_suffix = ">";
    } else {
      assert(0u < num_ret_types);
    }

    switch (func.Range()) {
      case FunctorRange::kOneOrMore:
      case FunctorRange::kZeroOrMore:
        os << "std::vector<" << tuple_prefix << return_tuple.str()
           << tuple_suffix << ">";
        break;
      case FunctorRange::kOneToOne:
        os << tuple_prefix << return_tuple.str() << tuple_suffix;
        break;
      case FunctorRange::kZeroOrOne:
        os << "std::optional<" << tuple_prefix << return_tuple.str()
           << tuple_suffix << ">";
    }
  }

  os << " " << func.Name() << '_' << ParsedDeclaration(func).BindingPattern()
     << "(";

  auto arg_sep = "";
  for (ParsedParameter arg : args) {
    os << arg_sep << TypeName(module, arg.Type().Kind()) << " " << arg.Name();
    arg_sep = ", ";
  }

  os << ") {\n";

  os.PushIndent();
  os << os.Indent() << "return " << func.Name() << '_'
     << ParsedDeclaration(func).BindingPattern() << "(";
  sep_ret = "";
  for (auto param : func.Parameters()) {
    if (param.Binding() == ParameterBinding::kBound) {
      os << sep_ret << param.Name();
      sep_ret = ", ";
    }
  }
  os << ");\n";
  os.PopIndent();
  os << os.Indent() << "}\n\n";
}

static void DeclareFunctors(OutputStream &os, Program program,
                            ParsedModule root_module) {
  os << os.Indent() << "class " << gClassName << "Functors {\n";
  os.PushIndent();
  os << os.Indent() << "public:\n";
  os.PushIndent();

  std::unordered_set<std::string> seen;

  // auto has_functors = false;
  for (auto module : ParsedModuleIterator(root_module)) {
    for (auto first_func : module.Functors()) {
      for (auto func : first_func.Redeclarations()) {
        std::stringstream ss;
        ss << func.Id() << ':' << ParsedDeclaration(func).BindingPattern();
        if (auto [it, inserted] = seen.emplace(ss.str()); inserted) {
          DeclareFunctor(os, module, func);

          // has_functors = true;
          (void) it;
        }
      }
    }
  }
  os.PopIndent();

  os.PopIndent();
  os << os.Indent() << "};\n\n";
}

static void DeclareMessageLogger(OutputStream &os, ParsedModule module,
                                 ParsedMessage message, const char *impl,
                                 bool interface = false) {
  os << os.Indent();
  if (interface) {
    os << "virtual ";
  }
  os << "void " << message.Name() << "_" << message.Arity() << "(";

  auto sep = "";
  for (auto param : message.Parameters()) {
    os << sep << TypeName(module, param.Type()) << " " << param.Name();
    sep = ", ";
  }

  os << ", bool added) ";
  if (interface) {
    os << "= 0;\n\n";
  } else {
    os << "override {\n";
    os.PushIndent();
    os << os.Indent() << impl << "\n";
    os.PopIndent();
    os << os.Indent() << "}\n\n";
  }
}

static void DeclareMessageLog(OutputStream &os, Program program,
                              ParsedModule root_module) {
  os << os.Indent() << "class " << gClassName << "LogInterface {\n";
  os.PushIndent();
  os << os.Indent() << "public:\n";
  os.PushIndent();

  const auto messages = Messages(root_module);

  if (!messages.empty()) {
    for (auto message : messages) {
      DeclareMessageLogger(os, root_module, message, "", true);
    }
  }
  os.PopIndent();
  os.PopIndent();
  os << os.Indent() << "};\n\n";

  os << '\n';
  os << os.Indent() << "class " << gClassName << "Log : public " << gClassName
     << "LogInterface {\n";
  os.PushIndent();
  os << os.Indent() << "public:\n";
  os.PushIndent();

  if (!messages.empty()) {
    for (auto message : messages) {
      DeclareMessageLogger(os, root_module, message, "{}");
    }
  }
  os.PopIndent();
  os.PopIndent();
  os << os.Indent() << "};\n\n";
}

static void DefineProcedure(OutputStream &os, ParsedModule module,
                            ProgramProcedure proc) {
  os << os.Indent() << "bool " << Procedure(os, proc)
     << "(){/*TODO(ekilmer)*/}\n\n";
}

static void DefineQueryEntryPoint(OutputStream &os, ParsedModule module,
                                  const ProgramQuery &spec) {
  const ParsedDeclaration decl(spec.query);
  os << os.Indent() << "auto " << decl.Name() << '_' << decl.BindingPattern()
     << "(){/*TODO(ekilmer)*/}\n\n";
}

}  // namespace

// Emits C++ code for the given program to `os`.
void GenerateDatabaseCode(const Program &program, OutputStream &os) {
  os << "/* Auto-generated file */\n\n"
     << "#include <drlojekyll/Runtime.h>\n\n"
     << "#include <tuple>\n"
     << "#include <unordered_map>\n"
     << "#include <vector>\n"
     << "\n"
     << "namespace {\n\n";

  const auto module = program.ParsedModule();

  // Output prologue code.
  for (auto sub_module : ParsedModuleIterator(module)) {
    for (auto code : sub_module.Inlines()) {
      switch (code.Language()) {
        case Language::kUnknown:
        case Language::kCxx:
          if (code.IsPrologue()) {
            os << code.CodeToInline() << "\n\n";
          }
          break;
        default: break;
      }
    }
  }

  DeclareFunctors(os, program, module);
  DeclareMessageLog(os, program, module);

  // A program gets its own class
  os << "template <typename StorageEngine>\n";
  os << "class " << gClassName << " {\n";
  os.PushIndent();  // class

  os << os.Indent() << "public:\n";
  os.PushIndent();  // public:

  os << os.Indent() << gClassName << "LogInterface &log;\n";
  os << os.Indent() << gClassName << "Functors &functors;\n";

  os << os.Indent()
     << "std::unordered_map<std::size_t, std::vector<::hyde::rt::Any*>> _refs;\n";

  os << "\n";

  os << os.Indent() << gClassName << "(StorageEngine &storage, " << gClassName
     << "LogInterface &l, " << gClassName << "Functors &f)\n";
  os.PushIndent();  // constructor
  os << os.Indent() << ": log(l),\n" << os.Indent() << "  functors(f)";

  for (auto table : program.Tables()) {
    os << ",\n" << os.Indent() << "  " << Table(os, table) << "(storage)";
    for (auto index : table.Indices()) {

      // If value columns are empty, then we've already mapped it to the table
      // itself as an accessor function
      if (!index.ValueColumns().empty()) {
        os << ",\n"
           << os.Indent() << "  " << TableIndex(os, index) << "(storage)";
      }
    }
  }

  for (auto global : program.GlobalVariables()) {
    if (!global.IsConstant()) {
      os << ",\n"
         << os.Indent() << "  " << Var(os, global) << "("
         << TypeValueOrDefault(module, global.Type(), global) << ")";
    }
  }
  os << " {\n";

  // Invoke the init procedure. Always first
  auto init_procedure = program.Procedures()[0];
  assert(init_procedure.Kind() == ProcedureKind::kInitializer);
  os << os.Indent() << Procedure(os, init_procedure) << "();\n";

  os.PopIndent();  // constructor
  os << os.Indent() << "}\n\n";
  os.PopIndent();  // public:

  os << os.Indent() << "private:\n";
  os.PushIndent();  // private:

  for (auto table : program.Tables()) {
    DefineTable(os, module, table);
  }

  for (auto global : program.GlobalVariables()) {
    DefineGlobal(os, module, global);
  }
  os << "\n";

  for (auto constant : program.Constants()) {
    DefineConstant(os, module, constant);
  }
  os << "\n";

  // Old pythonic way
  // for (auto type : module.ForeignTypes()) {
  DefineTypeRefResolver(os);  // , module, type);

  // }

  for (auto proc : program.Procedures()) {
    DefineProcedure(os, module, proc);
  }

  for (const auto &query_spec : program.Queries()) {
    DefineQueryEntryPoint(os, module, query_spec);
  }

  os.PopIndent();  // private:

  os.PopIndent();  // class:
  os << os.Indent() << "};\n\n";

  // Output epilogue code.
  for (auto sub_module : ParsedModuleIterator(module)) {
    for (auto code : sub_module.Inlines()) {
      switch (code.Language()) {
        case Language::kUnknown:
        case Language::kCxx:
          if (code.IsEpilogue()) {
            os << code.CodeToInline() << "\n\n";
          }
          break;
        default: break;
      }
    }
  }

  os << "}  // namespace\n";
}

}  // namespace cxx
}  // namespace hyde
