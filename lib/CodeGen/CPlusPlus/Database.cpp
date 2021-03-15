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

static OutputStream &Vector(OutputStream &os, const DataVector vec) {
  return os << "vec_" << vec.Id();
}

static OutputStream &VectorIndex(OutputStream &os, const DataVector vec) {
  return os << "vec_index" << vec.Id();
}

// Declare a structure containing the information about a table.
static void DefineTable(OutputStream &os, ParsedModule module,
                        DataTable table) {
  os << os.Indent() << "::hyde::rt::Table<StorageEngine, table_desc_"
     << table.Id() << ", ";
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

    os << os.Indent() << "::hyde::rt::Index<StorageEngine, table_desc_"
       << table.Id() << ", " << index.Id() << ", ::hyde::rt::Key<";
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
static void DefineTypeRefResolver(OutputStream &os) {

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
}

class CPPCodeGenVisitor final : public ProgramVisitor {
 public:
  explicit CPPCodeGenVisitor(OutputStream &os_, ParsedModule module_)
      : os(os_),
        module(module_) {}

  void Visit(ProgramCallRegion region) override {
    os << Comment(os, region, "ProgramCallRegion");

    auto param_index = 0u;
    const auto called_proc = region.CalledProcedure();
    const auto vec_params = called_proc.VectorParameters();
    const auto var_params = called_proc.VariableParameters();

    // Create the by-reference vector parameters, if any.
    for (auto vec : region.VectorArguments()) {
      const auto param = vec_params[param_index];
      if (param.Kind() == VectorKind::kInputOutputParameter) {
        os << os.Indent() << "auto &param_" << region.Id() << '_' << param_index
           << " = " << Vector(os, vec) << ";\n";
      }
      ++param_index;
    }

    const auto num_vec_params = param_index;

    // Create the by-reference variable parameters, if any.
    for (auto var : region.VariableArguments()) {
      const auto param = var_params[param_index - num_vec_params];
      if (param.DefiningRole() == VariableRole::kInputOutputParameter) {
        os << os.Indent() << "auto &param_" << region.Id() << '_' << param_index
           << " = " << Var(os, var) << ";\n";
      }
      ++param_index;
    }

    os << os.Indent() << "ret = " << Procedure(os, called_proc) << "(";

    auto sep = "";
    param_index = 0u;

    // Pass in the vector parameters, or the references to the vectors.
    for (auto vec : region.VectorArguments()) {
      const auto param = vec_params[param_index];
      if (param.Kind() == VectorKind::kInputOutputParameter) {
        os << sep << "param_" << region.Id() << '_' << param_index;
      } else {
        os << sep << Vector(os, vec);
      }

      sep = ", ";
      ++param_index;
    }

    // Pass in the variable parameters, or the references to the variables.
    for (auto var : region.VariableArguments()) {
      const auto param = var_params[param_index - num_vec_params];
      if (param.DefiningRole() == VariableRole::kInputOutputParameter) {
        os << sep << "param_" << region.Id() << '_' << param_index;
      } else {
        os << sep << Var(os, var);
      }
      sep = ", ";
      ++param_index;
    }

    os << ");\n";

    param_index = 0u;

    // Pull out the updated version of the referenced vectors.
    for (auto vec : region.VectorArguments()) {
      const auto param = vec_params[param_index];
      if (param.Kind() == VectorKind::kInputOutputParameter) {
        os << os.Indent() << Vector(os, vec) << " = param_" << region.Id()
           << '_' << param_index << ";\n";
      }
      ++param_index;
    }

    // Pull out the updated version of the referenced variables.
    for (auto var : region.VariableArguments()) {
      const auto param = var_params[param_index - num_vec_params];
      if (param.DefiningRole() == VariableRole::kInputOutputParameter) {
        os << os.Indent() << Var(os, var) << " = param_" << region.Id() << '_'
           << param_index << ";\n";
      }
      ++param_index;
    }

    // Check the return value.
    bool is_cond = true;
    if (region.ExecuteBodyIfReturnIsTrue()) {
      os << os.Indent() << "if (ret) {\n";
    } else if (region.ExecuteBodyIfReturnIsFalse()) {
      os << os.Indent() << "if (!ret) {\n";
    } else {
      is_cond = false;
    }

    if (is_cond) {
      os.PushIndent();
      if (auto body = region.Body(); body) {
        body->Accept(*this);
      } else {
        os << os.Indent() << "{}\n";
      }
      os.PopIndent();
      os << os.Indent() << "}\n";
    } else {
      os << '\n';
    }
  }

  void Visit(ProgramReturnRegion region) override {
    os << Comment(os, region, "ProgramReturnRegion");

    auto proc = ProgramProcedure::Containing(region);
    auto param_index = 0u;

    // Update any vectors in the caller by reference.
    for (auto vec : proc.VectorParameters()) {
      if (vec.Kind() == VectorKind::kInputOutputParameter) {
        os << os.Indent() << "param_" << param_index
           << "[0] = " << Vector(os, vec) << ";\n";
      }
      ++param_index;
    }

    // Update any vectors in the caller by reference.
    for (auto var : proc.VariableParameters()) {
      if (var.DefiningRole() == VariableRole::kInputOutputParameter) {
        os << os.Indent() << "param_" << param_index << "[0] = " << Var(os, var)
           << ";\n";
      }
      ++param_index;
    }

    os << os.Indent() << "return " << (region.ReturnsFalse() ? "false" : "true")
       << ";\n";
  }

  void Visit(ProgramExistenceAssertionRegion region) override {
    os << Comment(os, region, "ProgramExistenceAssertionRegion");
    const auto vars = region.ReferenceCounts();
    for (auto var : vars) {
      if (region.IsIncrement()) {
        os << os.Indent() << Var(os, var) << " += 1;\n";
      } else {
        os << os.Indent() << Var(os, var) << " -= 1;\n";
      }
    }

    if (auto body = region.Body(); body) {
      assert(vars.size() == 1u);
      if (region.IsIncrement()) {
        os << os.Indent() << "if (" << Var(os, vars[0]) << " == 1) {\n";
      } else {
        os << os.Indent() << "if (" << Var(os, vars[0]) << " == 0) {\n";
      }
      os.PushIndent();
      body->Accept(*this);
      os.PopIndent();
      os << os.Indent() << "}\n";
    }
  }

  void Visit(ProgramGenerateRegion region) override {
    os << Comment(os, region, "ProgramGenerateRegion");
  }

  void Visit(ProgramInductionRegion region) override {
    os << Comment(os, region, "ProgramInductionRegion");

    // Base case
    region.Initializer().Accept(*this);

    // Fixpoint
    os << Comment(os, region, "Induction Fixpoint Loop Region");
    os << os.Indent() << "while (";
    auto sep = "";
    for (auto vec : region.Vectors()) {
      os << sep << Vector(os, vec) << ".size()";
      sep = " || ";
    }
    os << ") {\n";

    os.PushIndent();
    region.FixpointLoop().Accept(*this);
    os.PopIndent();
    os << os.Indent() << "}\n";

    // Output
    if (auto output = region.Output(); output) {
      for (auto vec : region.Vectors()) {
        os << os.Indent() << VectorIndex(os, vec) << " = 0;\n";
      }
      os << Comment(os, region, "Induction Output Region");
      output->Accept(*this);
    }
  }

  void Visit(ProgramLetBindingRegion region) override {
    os << Comment(os, region, "ProgramLetBindingRegion");
  }

  void Visit(ProgramParallelRegion region) override {
    os << Comment(os, region, "ProgramParallelRegion");

    auto any = false;
    for (auto sub_region : region.Regions()) {

      // Create new scope since there could be multiply defined variable names in the regions
      os << os.Indent() << "{\n";
      os.PushIndent();
      sub_region.Accept(*this);
      os.PopIndent();
      os << os.Indent() << "}\n";
      any = true;
    }

    if (!any) {
      os << os.Indent() << "{}\n";
    }
  }

  // Should never be reached; defined below.
  void Visit(ProgramProcedure region) override {
    assert(false);
  }

  void Visit(ProgramPublishRegion region) override {
    os << Comment(os, region, "ProgramPublishRegion");
  }

  void Visit(ProgramSeriesRegion region) override {
    os << Comment(os, region, "ProgramSeriesRegion");

    auto any = false;
    for (auto sub_region : region.Regions()) {
      sub_region.Accept(*this);
      any = true;
    }

    if (!any) {
      os << os.Indent() << "{}\n";
    }
  }

  void Visit(ProgramVectorAppendRegion region) override {
    os << Comment(os, region, "ProgramVectorAppendRegion");

    const auto tuple_vars = region.TupleVariables();

    // Make sure to resolve to the correct reference of the foreign object.
    switch (region.Usage()) {
      case VectorUsage::kInductionVector:
      case VectorUsage::kJoinPivots: ResolveReferences(tuple_vars); break;
      default: break;
    }

    os << os.Indent() << Vector(os, region.Vector()) << ".push_back(";
    if (tuple_vars.size() == 1u) {
      os << Var(os, tuple_vars[0]);

    } else {
      os << "std::make_tuple(";
      auto sep = "";
      for (auto var : tuple_vars) {
        os << sep << Var(os, var);
        sep = ", ";
      }
      os << ')';
    }
    os << ");\n";
  }

  void Visit(ProgramVectorClearRegion region) override {
    os << Comment(os, region, "ProgramVectorClearRegion");

    os << os.Indent() << Vector(os, region.Vector()) << ".clear();\n";
    os << os.Indent() << VectorIndex(os, region.Vector()) << " = 0;\n";
  }

  void Visit(ProgramVectorSwapRegion region) override {
    os << Comment(os, region, "Program VectorSwap Region");

    os << os.Indent() << Vector(os, region.LHS()) << ".swap("
       << Vector(os, region.RHS()) << ");\n";
  }

  void Visit(ProgramVectorLoopRegion region) override {
    os << Comment(os, region, "ProgramVectorLoopRegion");
    auto vec = region.Vector();
    if (region.Usage() != VectorUsage::kInductionVector) {
      os << os.Indent() << VectorIndex(os, vec) << " = 0;\n";
    }
    os << os.Indent() << "while (" << VectorIndex(os, vec) << " < "
       << Vector(os, vec) << ".size()) {\n";
    os.PushIndent();

    os << os.Indent() << "auto ";
    const auto tuple_vars = region.TupleVariables();
    if (tuple_vars.size() == 1u) {
      os << Var(os, tuple_vars[0]);

    } else {
      auto sep = "[";
      for (auto var : tuple_vars) {
        os << sep << Var(os, var);
        sep = ", ";
      }
      os << "]";
    }
    os << " = " << Vector(os, vec) << "[" << VectorIndex(os, vec) << "];\n";

    os << os.Indent() << VectorIndex(os, vec) << " += 1;\n";

    if (auto body = region.Body(); body) {
      body->Accept(*this);
    }
    os.PopIndent();
    os << os.Indent() << "}\n";
  }

  void Visit(ProgramVectorUniqueRegion region) override {
    os << Comment(os, region, "ProgramVectorUniqueRegion");

    os << os.Indent() << "std::unique(" << Vector(os, region.Vector())
       << ".begin(), " << Vector(os, region.Vector()) << ".end());\n";
    os << os.Indent() << VectorIndex(os, region.Vector()) << " = 0;\n";
  }

  void ResolveReference(DataVariable var) {
    if (auto foreign_type = module.ForeignType(var.Type()); foreign_type) {
      if (var.DefiningRegion() &&
          !foreign_type->IsReferentiallyTransparent(Language::kPython)) {
        os << os.Indent() << Var(os, var) << " = _resolve<"
           << foreign_type->Name() << ">(" << Var(os, var) << ");\n";
      } else {
        switch (var.DefiningRole()) {
          case VariableRole::kConditionRefCount:
          case VariableRole::kConstant:
          case VariableRole::kConstantZero:
          case VariableRole::kConstantOne:
          case VariableRole::kConstantFalse:
          case VariableRole::kConstantTrue: break;
          default: assert(false);
        }
      }
    }
  }

  void ResolveReferences(UsedNodeRange<DataVariable> vars) {
    for (auto var : vars) {
      ResolveReference(var);
    }
  }

  void Visit(ProgramTransitionStateRegion region) override {
    os << Comment(os, region, "ProgramTransitionStateRegion");
    const auto tuple_vars = region.TupleVariables();

    // Make sure to resolve to the correct reference of the foreign object.
    ResolveReferences(tuple_vars);

    std::stringstream tuple;
    tuple << "tuple";
    for (auto tuple_var : tuple_vars) {
      tuple << "_" << tuple_var.Id();
    }

    auto tuple_prefix = "std::make_tuple(";
    auto tuple_suffix = ")";
    if (tuple_vars.size() == 1u) {
      tuple_prefix = "";
      tuple_suffix = "";
    }

    auto sep = "";
    auto tuple_var = tuple.str();
    os << os.Indent() << "auto " << tuple_var << " = " << tuple_prefix;
    for (auto var : tuple_vars) {
      os << sep << Var(os, var);
      sep = ", ";
    }
    os << tuple_suffix << ";\n"
       << os.Indent() << "prev_state = " << Table(os, region.Table())
       << ".GetState(" << tuple_var << ");\n"
       << os.Indent() << "state = prev_state & " << kStateMask << ";\n"
       << os.Indent() << "present_bit = prev_state & " << kPresentBit << ";\n";

    os << os.Indent() << "if (";
    switch (region.FromState()) {
      case TupleState::kAbsent:
        os << "state == " << kStateAbsent << ") {\n";
        break;
      case TupleState::kPresent:
        os << "state == " << kStatePresent << ") {\n";
        break;
      case TupleState::kUnknown:
        os << "state == " << kStateUnknown << ") {\n";
        break;
      case TupleState::kAbsentOrUnknown:
        os << "state == " << kStateAbsent << " || state == " << kStateUnknown
           << ") {\n";
        break;
    }
    os.PushIndent();
    os << os.Indent() << Table(os, region.Table()) << ".SetState(" << tuple_var
       << ", ";

    switch (region.ToState()) {
      case TupleState::kAbsent:
        os << kStateAbsent << " | " << kPresentBit << ");\n";
        break;
      case TupleState::kPresent:
        os << kStatePresent << " | " << kPresentBit << ");\n";
        break;
      case TupleState::kUnknown:
        os << kStateUnknown << " | " << kPresentBit << ");\n";
        break;
      case TupleState::kAbsentOrUnknown:
        os << kStateUnknown << " | " << kPresentBit << ");\n";
        assert(false);  // Shouldn't be created.
        break;
    }

    // If we're transitioning to present, then add it to our indices.
    //
    // NOTE(pag): The codegen for negations depends upon transitioning from
    //            absent to unknown as a way of preventing race conditions.
    const auto indices = region.Table().Indices();
    if (region.ToState() == TupleState::kPresent ||
        region.FromState() == TupleState::kAbsent) {
      os << os.Indent() << "if (!present_bit) {\n";
      os.PushIndent();

      auto has_indices = false;
      for (auto index : indices) {
        const auto key_cols = index.KeyColumns();

        // The index is the set of keys in the table's `defaultdict`. Thus, we
        // don't need to add anything because adding to the table will have done
        // it.
        if (tuple_vars.size() == key_cols.size()) {
          continue;
        }

        has_indices = true;

        // Index will update based on runtime implementation using the column
        // index numbers as defined in its instantiation
        os << os.Indent() << TableIndex(os, index) << ".Update(" << tuple_var
           << ");\n";
      }

      if (!has_indices) {
        os << os.Indent() << "{}\n";
      }

      os.PopIndent();
      os << os.Indent() << "}\n";
    }

    if (auto body = region.Body(); body) {
      body->Accept(*this);
    }
    os.PopIndent();
    os << os.Indent() << "}\n";
  }

  void Visit(ProgramCheckStateRegion region) override {
    os << Comment(os, region, "ProgramCheckStateRegion");
    const auto table = region.Table();
    const auto vars = region.TupleVariables();
    os << os.Indent() << "state = " << Table(os, table) << "(";
    if (vars.size() == 1u) {
      os << Var(os, vars[0]);
    } else {
      auto sep = "";
      for (auto var : vars) {
        os << sep << Var(os, var);
        sep = ", ";
      }
    }
    os << ") & " << kStateMask << ";\n";

    auto sep = "if (";

    if (auto absent_body = region.IfAbsent(); absent_body) {
      os << os.Indent() << sep << "state == " << kStateAbsent << ") {\n";
      os.PushIndent();
      absent_body->Accept(*this);
      os.PopIndent();
      os << os.Indent() << "}\n";
      sep = "else if (";
    }

    if (auto present_body = region.IfPresent(); present_body) {
      os << os.Indent() << sep << "state == " << kStatePresent << ") {\n";
      os.PushIndent();
      present_body->Accept(*this);
      os.PopIndent();
      os << os.Indent() << "}\n";
      sep = "else if (";
    }

    if (auto unknown_body = region.IfUnknown(); unknown_body) {
      os << os.Indent() << sep << "state == " << kStateUnknown << ") {\n";
      os.PushIndent();
      unknown_body->Accept(*this);
      os.PopIndent();
      os << os.Indent() << "}\n";
    }
  }

  void Visit(ProgramTableJoinRegion region) override {
    os << Comment(os, region, "ProgramTableJoinRegion");
  }

  void Visit(ProgramTableProductRegion region) override {
    os << Comment(os, region, "ProgramTableProductRegion");

    os << os.Indent();

    auto i = 0u;
    auto sep = "std::vector<std::tuple<";
    for (auto table : region.Tables()) {
      (void) table;
      for (auto var : region.OutputVariables(i++)) {
        os << sep << TypeName(module, var.Type());
        sep = ", ";
      }
    }

    os << ">> "
       << "vec_" << region.Id() << " = {};\n";

    i = 0u;

    // Products work by having tables and vectors for each proposer. We want
    // to take the product of each proposer's vector against all other tables.
    // The outer loop deals with the vectors.
    for (auto outer_table : region.Tables()) {
      const auto outer_vars = region.OutputVariables(i);
      const auto outer_vec = region.Vector(i++);
      (void) outer_table;

      os << os.Indent() << "for (auto ";
      auto outer_vars_size = outer_vars.size();
      if (outer_vars_size > 1) {
        os << "[";
      }
      sep = "";
      for (auto var : outer_vars) {
        os << sep << Var(os, var);
        sep = ", ";
      }
      if (outer_vars_size > 1) {
        os << "]";
      }

      os << " : " << Vector(os, outer_vec) << ") {\n";
      auto indents = 1u;
      os.PushIndent();

      // The inner loop deals with the tables.
      auto j = 0u;
      for (auto inner_table : region.Tables()) {
        const auto inner_vars = region.OutputVariables(j++);

        // NOTE(pag): Both `i` and `j` are already `+1`d.
        if (i == j) {
          continue;
        }

        os << os.Indent() << "for (auto ";
        auto inner_vars_size = inner_vars.size();
        if (inner_vars_size > 1) {
          os << "[";
        }
        sep = "";
        for (auto var : inner_vars) {
          os << sep << Var(os, var);
          sep = ", ";
        }
        if (inner_vars_size > 1) {
          os << "]";
        }
        os << " : " << Table(os, inner_table) << ") {\n";
        os.PushIndent();
        ++indents;
      }

      // Collect all product things into a vector.
      os << os.Indent() << "vec_" << region.Id();
      sep = ".push_back(std::tuple(";
      auto k = 0u;
      for (auto table : region.Tables()) {
        (void) table;
        for (auto var : region.OutputVariables(k++)) {
          os << sep << Var(os, var);
          sep = ", ";
        }
      }
      os << "));\n";

      // De-dent everything.
      for (auto outer_vec : region.Tables()) {
        os.PopIndent();
        os << os.Indent() << "}\n";
        assert(0u < indents);
        indents--;
        (void) outer_vec;
      }
    }

    os << os.Indent();
    sep = "for (auto [";
    auto k = 0u;
    for (auto table : region.Tables()) {
      (void) table;
      for (auto var : region.OutputVariables(k++)) {
        os << sep << Var(os, var);
        sep = ", ";
      }
    }

    os << "] : vec_" << region.Id() << ") {\n";
    os.PushIndent();
    if (auto body = region.Body(); body) {
      body->Accept(*this);
    } else {
      os << os.Indent() << "{}\n";
    }
    os.PopIndent();
    os << os.Indent() << "}\n";
  }

  void Visit(ProgramTableScanRegion region) override {
    os << Comment(os, region, "ProgramTableScanRegion");
  }

  void Visit(ProgramTupleCompareRegion region) override {
    os << Comment(os, region, "ProgramTupleCompareRegion");

    const auto lhs_vars = region.LHS();
    const auto rhs_vars = region.RHS();

    if (lhs_vars.size() == 1u) {
      os << os.Indent() << "if (" << Var(os, lhs_vars[0]) << ' '
         << OperatorString(region.Operator()) << ' ' << Var(os, rhs_vars[0])
         << ") {\n";

    } else {
      os << os.Indent() << "if (";

      auto cond = "";
      for (size_t i = 0; i < region.LHS().size(); i++) {
        os << cond << '(' << Var(os, lhs_vars[i]) << ' '
           << OperatorString(region.Operator()) << ' ' << Var(os, rhs_vars[i])
           << ')';
        cond = " && ";
      }
      os << ") {\n";
    }

    os.PushIndent();
    if (auto body = region.Body(); body) {
      body->Accept(*this);
    } else {
      os << os.Indent() << "{}";
    }
    os.PopIndent();
    os << os.Indent() << "}\n";
  }

 private:
  OutputStream &os;
  const ParsedModule module;
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

  // Every procedure has a boolean return type. A lot of the time the return
  // type is not used, but for top-down checkers (which try to prove whether or
  // not a tuple in an unknown state is either present or absent) it is used.
  os << os.Indent() << "bool " << Procedure(os, proc) << "(";

  const auto vec_params = proc.VectorParameters();
  const auto var_params = proc.VariableParameters();
  auto param_index = 0u;

  // First, declare all vector parameters.
  auto sep = "";
  for (auto vec : vec_params) {
    const auto is_byref = vec.Kind() == VectorKind::kInputOutputParameter;
    os << sep;
    if (is_byref) {
      os << "std::vector<";
    }
    os << "std::vector<";
    const auto &col_types = vec.ColumnTypes();
    if (1u < col_types.size()) {
      os << "std::tuple<";
    }
    auto type_sep = "";
    for (auto type : col_types) {
      os << type_sep << TypeName(module, type);
      type_sep = ", ";
    }
    if (1u < col_types.size()) {
      os << '>';
    }
    if (is_byref) {
      os << '>';
    }
    os << "> ";
    if (is_byref) {
      os << "param_" << param_index;
    } else {
      os << Vector(os, vec);
    }
    ++param_index;
    sep = ", ";
  }

  // Then, declare all variable parameters.
  for (auto param : var_params) {
    if (param.DefiningRole() == VariableRole::kInputOutputParameter) {
      os << sep << "std::vector<" << TypeName(module, param.Type()) << ">"
         << "param_" << param_index;
    } else {
      os << sep << TypeName(module, param.Type()) << ' ' << Var(os, param);
    }
    ++param_index;
    sep = ", ";
  }

  os << ") {\n";
  os.PushIndent();
  os << os.Indent() << "int state = " << kStateUnknown << ";\n"
     << os.Indent() << "int prev_state = " << kStateUnknown << ";\n"
     << os.Indent() << "int present_bit = 0;\n"
     << os.Indent() << "bool ret = false;\n"
     << os.Indent() << "bool found = false;\n";

  param_index = 0u;

  // Pull out the referenced vectors.
  for (auto vec : vec_params) {
    if (vec.Kind() == VectorKind::kInputOutputParameter) {
      os << os.Indent() << "auto " << Vector(os, vec) << " = param_"
         << param_index << "[0];\n";
    }
    ++param_index;
  }

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
    os << os.Indent() << "int " << VectorIndex(os, vec) << " = 0;\n";
  }

  // Define the vectors that will be created and used within this procedure.
  // These vectors exist to support inductions, joins (pivot vectors), etc.
  for (auto vec : proc.DefinedVectors()) {
    os << os.Indent() << "std::vector<";

    const auto &col_types = vec.ColumnTypes();
    if (1u < col_types.size()) {
      os << "std::tuple<";
    }

    auto type_sep = "";
    for (auto type : col_types) {
      os << type_sep << TypeName(module, type);
      type_sep = ", ";
    }

    if (1u < col_types.size()) {
      os << ">";
    }

    os << "> " << Vector(os, vec) << " = {};\n";

    // Tracking variable for the vector.
    os << os.Indent() << "int " << VectorIndex(os, vec) << " = 0;\n";
  }

  // Visit the body of the procedure. Procedure bodies are never empty; the
  // most trivial procedure body contains a `return False`.
  CPPCodeGenVisitor visitor(os, module);
  proc.Body().Accept(visitor);

  os.PopIndent();
  os << os.Indent() << "}\n\n";
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
     << "\n";

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
  os << "#include <tuple>\n"
     << "#include <unordered_map>\n"
     << "#include <vector>\n\n";

  // Table descriptors
  for (auto table : program.Tables()) {
    os << "struct table_desc_" << table.Id() << ";\n";
  }
  os << "\n";

  os << "namespace {\n\n";

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

  DefineTypeRefResolver(os);

  for (auto proc : program.Procedures()) {
    DefineProcedure(os, module, proc);
  }

  for (const auto &query_spec : program.Queries()) {
    DefineQueryEntryPoint(os, module, query_spec);
  }

  os.PopIndent();  // private:

  os.PopIndent();  // class:
  os << os.Indent() << "};\n\n";

  os << "}  // namespace\n";

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
}

}  // namespace cxx
}  // namespace hyde
