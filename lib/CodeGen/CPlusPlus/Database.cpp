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

static OutputStream &Functor(OutputStream &os, const ParsedFunctor func) {
  return os << "functors." << func.Name() << '_'
            << ParsedDeclaration(func).BindingPattern();
}

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

// Declare Table Descriptors that contain additional metadata about
static void DeclareDescriptors(OutputStream &os, Program program,
                               ParsedModule module) {

  // Table and column descriptors
  for (auto table : program.Tables()) {
    os << "struct table_desc_" << table.Id() << " {};\n";
    for (auto col : table.Columns()) {
      os << "struct column_desc_" << table.Id() << "_" << col.Index() << " {\n";
      os.PushIndent();

      // NOTE(ekilmer): Will want to fill this programmatically someday
      os << os.Indent() << "static constexpr bool kIsNamed = false;\n";
      os << os.Indent() << "using type = " << TypeName(module, col.Type())
         << ";\n";
      os.PopIndent();
      os << "};\n";
    }
    os << "\n";
  }
  os << "\n";
}

// Print out the type of an index. Specify whether this is being used for an index or within a table type
static OutputStream &IndexTypeDecl(OutputStream &os, const DataTable table,
                                   const DataIndex index, bool index_decl) {
  const auto cols = table.Columns();
  const auto key_cols = index.KeyColumns();
  const auto val_cols = index.ValueColumns();

  // The index can be implemented with the keys in the Table.
  // In this case, the index lookup will be like an `if ... in ...`.
  if (key_cols.size() == cols.size()) {
    assert(val_cols.empty());
    if (!index_decl) {
      return os;
    }

    // Implement this index as a reference to the Table
    return os << "decltype(" << Table(os, table) << ") & "
              << TableIndex(os, index) << " = " << Table(os, table) << ";\n";
  }

  os << "::hyde::rt::Index<StorageT, table_desc_" << table.Id() << ", "
     << index.Id();

  // In C++ codegen, the Index knows which columns are keys/values, but they
  // need to be ordered as they were in the table
  auto key_col_iter = key_cols.begin();
  auto val_col_iter = val_cols.begin();

  // Assumes keys and values are sorted by index within their respective lists
  while (key_col_iter != key_cols.end() || val_col_iter != val_cols.end()) {
    os << ", ";
    if (val_col_iter == val_cols.end() ||
        (key_col_iter != key_cols.end() &&
         (*key_col_iter).Index() < (*val_col_iter).Index())) {
      os << "::hyde::rt::Key<column_desc_" << table.Id() << "_"
         << (*key_col_iter).Index() << ">";
      key_col_iter++;
    } else {
      os << "::hyde::rt::Value<column_desc_" << table.Id() << "_"
         << (*val_col_iter).Index() << ">";
      val_col_iter++;
    }
  }
  os << ">";

  // Declare the actual index
  if (index_decl) {
    os << " " << TableIndex(os, index) << ";\n";
  }
  return os;
}

// Declare a structure containing the information about a table.
static void DeclareTable(OutputStream &os, ParsedModule module,
                         DataTable table) {
  os << os.Indent() << "::hyde::rt::Table<StorageT, table_desc_" << table.Id()
     << ", hyde::rt::TypeList<";

  const auto cols = table.Columns();

  // List index types first
  auto sep = "";
  for (auto index : table.Indices()) {
    const auto key_cols = index.KeyColumns();

    // The index can be implemented with the keys in the Table.
    // In this case, the index lookup will be like an `if ... in ...`.
    if (key_cols.size() != cols.size()) {
      os << sep << IndexTypeDecl(os, table, index, false);
      sep = ", ";
    }
  }

  // Then column types
  sep = ">, hyde::rt::TypeList<";
  if (cols.size() == 1u) {
    os << sep << TypeName(module, cols[0].Type());

  } else {
    for (auto col : cols) {
      os << sep << TypeName(module, col.Type());
      sep = ", ";
    }
  }
  os << ">> " << Table(os, table) << ";\n";

  // We represent indices as mappings to vectors so that we can concurrently
  // write to them while iterating over them (via an index and length check).
  for (auto index : table.Indices()) {
    os << os.Indent() << IndexTypeDecl(os, table, index, true);
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
     << os.Indent() << "         void(T::*)(T &)>::value, T>::type\n"
     << os.Indent() << "_resolve(T &obj) {\n";
  os.PushIndent();
  os << os.Indent() << "auto ref_list = _refs[std::hash<T>(obj)];\n"
     << os.Indent() << "for (auto maybe_obj : ref_list) {\n";
  os.PushIndent();
  os << os.Indent() << "// TODO(ekilmer): This isn't going to work...\n";
  os << os.Indent() << "if (&obj == &maybe_obj) {\n";
  os.PushIndent();
  os << os.Indent() << "return obj;\n";
  os.PopIndent();
  os << os.Indent() << "} else if (obj == maybe_obj) {\n";
  os.PushIndent();
  os << os.Indent() << "T prior_obj = static_cast<T>(maybe_obj);\n"
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
     << os.Indent() << os.Indent() << "auto _resolve(T &&obj) {\n";
  os.PushIndent();
  os << os.Indent() << "return obj;\n";
  os.PopIndent();
  os << os.Indent() << "}\n\n";
  os << os.Indent() << "template<typename T>\n"
     << os.Indent() << os.Indent() << "auto _resolve(T &obj) {\n";
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

    const auto id = region.Id();

    const auto called_proc = region.CalledProcedure();

    os << os.Indent() << "auto ret_" << id << " = "
       << Procedure(os, called_proc) << "(";

    auto sep = "";

    // Pass in the vector parameters, or the references to the vectors.
    for (auto vec : region.VectorArguments()) {
      os << sep << Vector(os, vec);
      sep = ", ";
    }

    // Pass in the variable parameters, or the references to the variables.
    for (auto var : region.VariableArguments()) {
      os << sep << Var(os, var) << ReifyVar(os, var);
      sep = ", ";
    }

    os << ");\n";

    if (auto true_body = region.BodyIfTrue(); true_body) {
      os << os.Indent() << "if (ret_" << id << ") {\n";
      os.PushIndent();
      true_body->Accept(*this);
      os.PopIndent();
      os << os.Indent() << "}\n";
    }

    if (auto false_body = region.BodyIfFalse(); false_body) {
      os << os.Indent() << "if (!ret_" << id << ") {\n";
      os.PushIndent();
      false_body->Accept(*this);
      os.PopIndent();
      os << os.Indent() << "}\n";
    }
  }

  void Visit(ProgramReturnRegion region) override {
    os << Comment(os, region, "ProgramReturnRegion");

    os << os.Indent() << "return " << (region.ReturnsFalse() ? "false" : "true")
       << ";\n";
  }

  void Visit(ProgramTestAndSetRegion region) override {
    os << Comment(os, region, "ProgramTestAndSetRegion");
    const auto acc = region.Accumulator();
    const auto disp = region.Displacement();
    const auto cmp = region.Comparator();
    if (region.IsAdd()) {
      os << os.Indent() << Var(os, acc) << " += " << Var(os, disp) << ";\n";
    } else {
      os << os.Indent() << Var(os, acc) << " -= " << Var(os, disp) << ";\n";
    }

    if (auto body = region.Body(); body) {
      if (region.IsAdd()) {
        os << os.Indent() << "if (" << Var(os, acc) << " == " << Var(os, cmp)
           << ") {\n";
      } else {
        os << os.Indent() << "if (" << Var(os, acc) << " == " << Var(os, cmp)
           << ") {\n";
      }
      os.PushIndent();
      body->Accept(*this);
      os.PopIndent();
      os << os.Indent() << "}\n";
    }
  }

  void Visit(ProgramGenerateRegion region) override {
    os << Comment(os, region, "ProgramGenerateRegion");

    const auto functor = region.Functor();
    const auto id = region.Id();

    os << os.Indent() << "int num_results_" << id << " = 0;\n";

    auto output_vars = region.OutputVariables();

    auto call_functor = [&](void) {
      Functor(os, functor) << "(";
      auto sep = "";
      for (auto in_var : region.InputVariables()) {
        os << sep << Var(os, in_var);
        sep = ", ";
      }
      os << ")";
    };

    auto do_body = [&](void) {
      os << os.Indent() << "num_results_" << id << " += 1;\n";
      if (auto body = region.BodyIfResults(); body) {
        body->Accept(*this);

      // Break out of the body early if there is nothing to do, and if we've
      // already counted at least one instance of results (in the case of the
      // functor possibly producing more than one result tuples), then that
      // is sufficient information to be able to enter into the "empty" body.
      } else if (const auto range = functor.Range();
                 FunctorRange::kOneOrMore == range ||
                 FunctorRange::kZeroOrMore == range) {
        os << os.Indent() << "break;\n";
      }
    };

    switch (const auto range = functor.Range()) {

      // These behave like iterators.
      case FunctorRange::kOneOrMore:
      case FunctorRange::kZeroOrMore: {
        if (output_vars.size() == 1u) {
          os << os.Indent() << "auto tmp_" << id << " = ";
          call_functor();
          os << ";\n";
          auto optional = range == FunctorRange::kZeroOrMore;
          if (optional) {
            os << os.Indent() << "if (tmp_" << id << ") {\n";
            os.PushIndent();
          }
          os << os.Indent() << "for (auto " << Var(os, output_vars[0]) << " : ";
          if (optional) {

            // Dereference optional
            os << "*";
          }
          os << "tmp_" << id << ") {\n";
          os.PushIndent();
          do_body();
          os.PopIndent();
          os << os.Indent() << "}\n";
          if (optional) {
            os.PopIndent();
            os << os.Indent() << "}\n";
          }

        } else {
          assert(!output_vars.empty());

          os << os.Indent() << "for (auto tmp_" << id << " : ";
          call_functor();
          os << ") {\n";
          os.PushIndent();
          auto out_var_index = 0u;
          for (auto out_var : output_vars) {
            os << os.Indent() << Var(os, out_var) << " = tmp_" << id << '['
               << (out_var_index++) << "];\n";
          }
          do_body();
          os.PopIndent();
          os << os.Indent() << "}\n";
        }

        break;
      }

      // These behave like returns of tuples/values/optionals.
      case FunctorRange::kOneToOne:
      case FunctorRange::kZeroOrOne:

        // Only takes bound inputs, acts as a filter functor.
        if (output_vars.empty()) {
          assert(functor.IsFilter());

          os << os.Indent() << "if (";
          call_functor();
          os << ") {\n";
          os.PushIndent();
          do_body();
          os.PopIndent();
          os << os.Indent() << "}\n";

        // Produces a single value. This returns an `Optional` value.
        } else if (output_vars.size() == 1u) {
          assert(!functor.IsFilter());

          const auto out_var = output_vars[0];
          os << os.Indent() << "auto tmp_" << id << " = ";
          call_functor();
          os << ";\n";
          auto optional = range == FunctorRange::kZeroOrOne;
          if (optional) {
            os << os.Indent() << "if (tmp_" << id << ") {\n";
            os.PushIndent();
          }
          os << os.Indent() << "auto " << Var(os, out_var) << " = ";
          if (optional) {

            // Dereference optional
            os << "*";
          }
          os << "tmp_" << id << ";\n";
          do_body();
          if (optional) {
            os.PopIndent();
            os << os.Indent() << "}\n";
          }

        // Produces a tuple of values.
        } else {
          assert(!functor.IsFilter());

          os << os.Indent() << "auto tmp_" << id << " = ";
          call_functor();
          os << ";\n" << os.Indent() << "if (tmp_" << id << ") {\n";
          os.PushIndent();
          auto out_var_index = 0u;
          for (auto out_var : output_vars) {
            os << os.Indent() << Var(os, out_var) << " = tmp_" << id
               << ".value()[" << (out_var_index++) << "];\n";
          }
          do_body();
          os.PopIndent();
          os << os.Indent() << "}\n";
        }
        break;
    }

    if (auto empty_body = region.BodyIfEmpty(); empty_body) {
      os << os.Indent() << "if (!num_results_" << id << ") {\n";
      os.PushIndent();
      empty_body->Accept(*this);
      os.PopIndent();
      os << os.Indent() << "}\n";
    }
  }

  void Visit(ProgramInductionRegion region) override {
    os << Comment(os, region, "ProgramInductionRegion");

    // Base case
    if (auto init_region = region.Initializer(); init_region) {
      init_region->Accept(*this);
    }

    // Fixpoint
    os << Comment(os, region, "Induction Fixpoint Loop Region");
    os << os.Indent() << "bool changed_" << region.Id() << " = true;\n";
    os << os.Indent() << "while (changed_" << region.Id() << ") {\n";

    os.PushIndent();
    region.FixpointLoop().Accept(*this);

    // Update the entry condition on the back-edge.
    os << os.Indent() << "changed_" << region.Id() << " = ";
    auto sep = "";
    for (auto vec : region.Vectors()) {
      os << sep << Vector(os, vec) << ".size() != 0";
      sep = " || ";
    }
    os << ";\n";

    os.PopIndent();
    os << os.Indent() << "}\n";

    // Output
    if (auto output = region.Output(); output) {
      os << Comment(os, region, "Induction Output Region");
      output->Accept(*this);
    }
  }

  void Visit(ProgramLetBindingRegion region) override {
    os << Comment(os, region, "ProgramLetBindingRegion");
    auto i = 0u;
    const auto used_vars = region.UsedVariables();
    for (auto var : region.DefinedVariables()) {
      os << os.Indent() << "auto " << Var(os, var) << " = "
         << Var(os, used_vars[i++]) << ";\n";
    }

    if (auto body = region.Body(); body) {
      body->Accept(*this);
    } else {
      os << os.Indent() << "{}\n";
    }
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
    auto message = region.Message();
    os << os.Indent() << "log." << message.Name() << '_' << message.Arity();

    auto sep = "(";
    for (auto var : region.VariableArguments()) {
      os << sep << Var(os, var);
      sep = ", ";
    }

    if (region.IsRemoval()) {
      os << sep << "false";
    } else {
      os << sep << "true";
    }

    os << ");\n";
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

    os << os.Indent() << Vector(os, region.Vector()) << ".emplace_back(";
    auto sep = "";
    for (auto var : tuple_vars) {
      os << sep << Var(os, var) << ReifyVar(os, var);
      sep = ", ";
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
    os << os.Indent() << VectorIndex(os, vec) << " = 0;\n"
       << os.Indent() << "while (" << VectorIndex(os, vec) << " < "
       << Vector(os, vec) << ".size()) {\n";
    os.PushIndent();

    os << os.Indent() << "auto [";
    const auto tuple_vars = region.TupleVariables();
    auto sep = "";
    for (auto var : tuple_vars) {
      os << sep << Var(os, var);
      sep = ", ";
    }
    // Need to differentiate between our SerializedVector and regular
    bool serial_vec = region.Usage() == VectorUsage::kProcedureInputVector;
    if (serial_vec) {
      os << ", tmp_" << region.UniqueId() << "_index";
    }
    os << "] = " << Vector(os, vec) << "[" << VectorIndex(os, vec) << "];\n";

    os << os.Indent() << VectorIndex(os, vec);
    if (serial_vec) {
      os << " = tmp_" << region.UniqueId() << "_index;\n";
    } else {
      os << " += 1;\n";
    }

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
      if (var.DefiningRegion()) {
        if (!foreign_type->IsReferentiallyTransparent(Language::kCxx)) {
          os << os.Indent() << "auto reified_" << Var(os, var) << " = _resolve<"
             << TypeName(*foreign_type) << ">(" << Var(os, var)
             << ReifyVar(os, var) << ");\n";
        }
      } else {
        switch (var.DefiningRole()) {
          case VariableRole::kConditionRefCount:
          case VariableRole::kInitGuard:
          case VariableRole::kConstantZero:
          case VariableRole::kConstantOne:
          case VariableRole::kConstantFalse:
          case VariableRole::kConstantTrue: assert(false); break;
          case VariableRole::kConstant:
          case VariableRole::kConstantTag:
          default: break;
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

    auto print_state_enum = [&](TupleState state) {
      switch (state) {
        case TupleState::kAbsent: os << "TupleState::kAbsent"; break;
        case TupleState::kPresent: os << "TupleState::kPresent"; break;
        case TupleState::kUnknown: os << "TupleState::kUnknown"; break;
        case TupleState::kAbsentOrUnknown:
          os << "TupleState::kAbsentOrUnknown";
          break;
      }
    };

    os << os.Indent() << "bool did_transition_" << region.UniqueId() << " = "
       << Table(os, region.Table()) << ".TransitionState(::hyde::rt::";
    print_state_enum(region.FromState());
    os << ", ::hyde::rt::";
    print_state_enum(region.ToState());
    auto sep = ", ";
    for (auto var : tuple_vars) {
      os << sep << Var(os, var) << ReifyVar(os, var);
    }
    os << ");\n";

    os << os.Indent() << "if (did_transition_" << region.UniqueId() << ") {\n";
    os.PushIndent();

    if (auto succeeded_body = region.BodyIfSucceeded(); succeeded_body) {
      succeeded_body->Accept(*this);
    } else {
      os << os.Indent() << "{}\n";
    }

    os.PopIndent();
    os << os.Indent() << "}\n";
  }

  void Visit(ProgramCheckStateRegion region) override {
    os << Comment(os, region, "ProgramCheckStateRegion");
    const auto table = region.Table();
    const auto vars = region.TupleVariables();
    os << os.Indent() << "state = " << Table(os, table) << ".GetState(";
    if (vars.size() == 1u) {
      os << Var(os, vars[0]);
    } else {
      auto sep = "";
      for (auto var : vars) {
        os << sep << Var(os, var) << ReifyVar(os, var);
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

    // Nested loop join
    auto vec = region.PivotVector();
    os << os.Indent() << VectorIndex(os, vec) << " = 0;\n"
       << os.Indent() << "while (" << VectorIndex(os, vec) << " < "
       << Vector(os, vec) << ".size()) {\n";
    os.PushIndent();

    std::vector<std::string> var_names;
    os << os.Indent() << "auto [";
    auto sep = "";
    for (auto var : region.OutputPivotVariables()) {
      std::stringstream var_name;
      (void) Var(var_name, var);
      var_names.emplace_back(var_name.str());
      os << sep << var_names.back();
      sep = ", ";
    }
    os << "] = " << Vector(os, vec) << "[" << VectorIndex(os, vec) << "];\n";

    os << os.Indent() << VectorIndex(os, vec) << " += 1;\n";

    auto tables = region.Tables();
    for (auto i = 0u; i < tables.size(); ++i) {
      const auto table = tables[i];
      const auto index = region.Index(i);
      const auto index_keys = index.KeyColumns();
      const auto index_vals = index.ValueColumns();

      (void) table;

      // The index is a set of key column values/tuples.
      if (index_vals.empty()) {
        os << os.Indent() << "if (" << TableIndex(os, index) << ".KeyExists(";

        // Print out key columns
        auto key_columns = [&]() {
          sep = "";
          for (auto index_col : index_keys) {
            auto j = 0u;
            for (auto used_col : region.IndexedColumns(i)) {
              if (used_col == index_col) {
                os << sep << var_names[j];
                sep = ", ";
              }
              ++j;
            }
          }
        };

        key_columns();
        os << ")";

        // The index aliases the underlying table; lets double check that the
        // state isn't `absent`.
        assert(index.KeyColumns().size() == table.Columns().size());
        os << " && (" << TableIndex(os, index) << ".Get(";
        key_columns();
        os << ") & " << kStateMask << ") != " << kStateAbsent << ") {\n";

        // We increase indentation here, and the corresponding `PopIndent()`
        // only comes *after* visiting the `region.Body()`.
        os.PushIndent();

        // Should have no output variables
        assert(region.OutputVariables(i).empty());

      // The index is a default dict mapping key columns to a list of value
      // columns/tuples.
      } else {

        // We don't want to have to make a temporary copy of the current state
        // of the index, so instead what we do is we capture a reference to the
        // list of tuples in the index, and we also create an index variable
        // that tracks which tuple we can next look at. This allows us to
        // observe writes into the index as they happen.
        os << os.Indent() << "int tuple_" << region.Id() << "_" << i
           << "_index = 0;\n"
           << os.Indent() << "auto tuple_" << region.Id() << "_" << i
           << "_vec = " << TableIndex(os, index) << ".Get(";

        // This is a bit ugly, but basically: we want to index into the
        // Python representation of this index, e.g. via `index_10[(a, b)]`,
        // where `a` and `b` are pivot variables. However, the pivot vector
        // might have the tuple entries in the order `(b, a)`. To easy matching
        // between pivot variables and indexed columns, `region.IndexedColumns`
        // exposes columns in the same order as the pivot variables, which as we
        // see, might not match the order of the columns in the index. Thus we
        // need to re-order our usage of variables so that they match the
        // order expected by `index_10[...]`.
        sep = "";
        for (auto index_col : index_keys) {
          auto j = 0u;
          for (auto used_col : region.IndexedColumns(i)) {
            if (used_col == index_col) {
              os << sep << var_names[j];
              sep = ", ";
            }
            ++j;
          }
        }

        os << ");\n";

        os << os.Indent() << "while (tuple_" << region.Id() << "_" << i
           << "_index < tuple_" << region.Id() << "_" << i
           << "_vec.size()) {\n";

        // We increase indentation here, and the corresponding `PopIndent()`
        // only comes *after* visiting the `region.Body()`.
        os.PushIndent();

        auto out_vars = region.OutputVariables(i);
        assert(out_vars.size() == region.SelectedColumns(i).size());

        os << os.Indent() << "auto [";
        sep = "";
        for (auto var : out_vars) {
          os << sep << Var(os, var);
          sep = ", ";
        }

        os << ", tmp_" << region.Id() << "_" << i << "_index] = tuple_"
           << region.Id() << "_" << i << "_vec[tuple_" << region.Id() << "_"
           << i << "_index];\n";

        os << os.Indent() << "tuple_" << region.Id() << "_" << i
           << "_index = tmp_" << region.Id() << "_" << i << "_index;\n";
      }
    }

    if (auto body = region.Body(); body) {
      body->Accept(*this);
    } else {
      os << os.Indent() << "{}\n";
    }

    // Outdent for each nested for loop over an index.
    for (auto table : tables) {
      (void) table;
      os.PopIndent();
      os << os.Indent() << "}\n";
    }

    // Output of the loop over the pivot vector.
    os.PopIndent();
    os << os.Indent() << "}\n";
  }

  void Visit(ProgramTableProductRegion region) override {
    os << Comment(os, region, "ProgramTableProductRegion");

    os << os.Indent();

    auto i = 0u;
    os << "::hyde::rt::Vector<StorageT";
    for (auto table : region.Tables()) {
      (void) table;
      for (auto var : region.OutputVariables(i++)) {
        os << ", " << TypeName(module, var.Type());
      }
    }

    os << "> "
       << "vec_" << region.Id() << ";\n";

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
      auto sep = "";
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
      sep = ".emplace_back(";
      auto k = 0u;
      for (auto table : region.Tables()) {
        (void) table;
        for (auto var : region.OutputVariables(k++)) {
          os << sep << Var(os, var);
          sep = ", ";
        }
      }
      os << ");\n";

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
    auto sep = "for (auto [";
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

    const auto input_vars = region.InputVariables();
    const auto filled_vec = region.FilledVector();

    // Make sure to resolve to the correct reference of the foreign object.
    ResolveReferences(input_vars);

    // TODO(pag): Do we need to watch out for the index aliasing the key space
    //            of the table, and having some columns in the absent state?

    // Index scan :-D
    if (auto maybe_index = region.Index(); maybe_index) {
      const auto index = *maybe_index;

      os << os.Indent() << "auto scan_tuple_" << filled_vec.Id()
         << "_vec = " << TableIndex(os, index) << ".Get(";
      auto sep = "";
      for (auto var : input_vars) {
        os << sep << Var(os, var) << ReifyVar(os, var);
        sep = ", ";
      }
      os << ");\n";

      os << os.Indent() << "int scan_index_" << filled_vec.Id() << " = 0;\n";
      os << os.Indent() << "while (scan_index_" << filled_vec.Id()
         << " < scan_tuple_" << filled_vec.Id() << "_vec.size()) {\n";
      os.PushIndent();

      os << os.Indent() << "auto [";
      sep = "";
      for (auto var : index.ValueColumns()) {
        os << sep << "scan_tuple_" << filled_vec.Id() << "_" << var.Id();
        sep = ", ";
      }
      os << ", offset_" << region.UniqueId() << "] = scan_tuple_"
         << filled_vec.Id() << "_vec["
         << "scan_index_" << filled_vec.Id() << "];\n"
         << os.Indent() << "scan_index_" << filled_vec.Id() << " = offset_"
         << region.UniqueId() << ";\n";

      os << os.Indent() << Vector(os, filled_vec) << ".emplace_back(";
      sep = "";
      for (auto var : index.ValueColumns()) {
        os << sep << "scan_tuple_" << filled_vec.Id() << "_" << var.Id()
           << ".Reify()";
        sep = ", ";
      }
      os << ");\n";

    // Full table scan.
    } else {
      assert(input_vars.empty());
      os << os.Indent() << "for (auto ";
      os << "scan_tuple_" << filled_vec.Id() << " : "
         << Table(os, region.Table()) << ".Keys()) {\n";
      os.PushIndent();

      os << os.Indent() << "auto [";
      auto table = region.Table();
      auto sep = "";
      for (auto i = 0; i < table.Columns().size(); i++) {
        os << sep << "scan_var_" << filled_vec.Id() << "_" << i;
        sep = ", ";
      }
      os << ", _] = scan_tuple_" << filled_vec.Id() << ".GetReified();\n";

      os << os.Indent() << Vector(os, filled_vec) << ".emplace_back(";
      sep = "";
      for (unsigned int i = 0; i < table.Columns().size(); i++) {
        os << sep << "scan_var_" << filled_vec.Id() << "_" << i;
        sep = ", ";
      }
      os << ");\n";
    }

    os.PopIndent();
    os << os.Indent() << "}\n";
  }

  void Visit(ProgramTupleCompareRegion region) override {
    os << Comment(os, region, "ProgramTupleCompareRegion");

    const auto lhs_vars = region.LHS();
    const auto rhs_vars = region.RHS();

    if (lhs_vars.size() == 1u) {
      os << os.Indent() << "if (" << Var(os, lhs_vars[0])
         << ReifyVar(os, lhs_vars[0]) << ' '
         << OperatorString(region.Operator()) << ' ' << Var(os, rhs_vars[0])
         << ReifyVar(os, rhs_vars[0]) << ") {\n";

    } else {
      os << os.Indent() << "if (";

      auto cond = "";
      for (size_t i = 0; i < region.LHS().size(); i++) {
        os << cond << '(' << Var(os, lhs_vars[i]) << ReifyVar(os, lhs_vars[i])
           << ' ' << OperatorString(region.Operator()) << ' '
           << Var(os, rhs_vars[i]) << ReifyVar(os, rhs_vars[i]) << ')';
        cond = " && ";
      }
      os << ") {\n";
    }

    os.PushIndent();
    if (auto true_body = region.BodyIfTrue(); true_body) {
      true_body->Accept(*this);
    } else {
      os << os.Indent() << "{}\n";
    }
    os.PopIndent();
    os << os.Indent() << "} else {\n";
    os.PushIndent();
    if (auto false_body = region.BodyIfFalse(); false_body) {
      false_body->Accept(*this);
    } else {
      os << os.Indent() << "{}\n";
    }
    os.PopIndent();
    os << os.Indent() << "}\n";
  }

  void Visit(ProgramWorkerIdRegion region) override {
    os << Comment(os, region, "Program WorkerId Region");
    if (auto body = region.Body(); body) {
      body->Accept(*this);
    } else {
      os << os.Indent() << "{}";
    }
  }

 private:
  OutputStream &os;
  const ParsedModule module;
};

static void DeclareFunctor(OutputStream &os, ParsedModule module,
                           ParsedFunctor func, bool user_fwd_declare = false) {
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
        os << "std::optional<std::vector<" << tuple_prefix << return_tuple.str()
           << tuple_suffix << ">>";
        break;
      case FunctorRange::kOneToOne:
        os << tuple_prefix << return_tuple.str() << tuple_suffix;
        break;
      case FunctorRange::kZeroOrOne:
        os << "std::optional<" << tuple_prefix << return_tuple.str()
           << tuple_suffix << ">";
    }
  }

  os << " " << (user_fwd_declare ? "_" : "") << func.Name() << '_'
     << ParsedDeclaration(func).BindingPattern() << "(";

  auto arg_sep = "";
  for (ParsedParameter arg : args) {
    os << arg_sep << "const " << TypeName(module, arg.Type().Kind()) << "& "
       << arg.Name();
    arg_sep = ", ";
  }

  os << ")";
}

static void DefineFunctor(OutputStream &os, ParsedModule module,
                          ParsedFunctor func) {
  DeclareFunctor(os, module, func);
  os << " {\n";
  os.PushIndent();
  os << os.Indent() << "return _" << func.Name() << '_'
     << ParsedDeclaration(func).BindingPattern() << "(";
  auto sep_ret = "";
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

  for (auto module : ParsedModuleIterator(root_module)) {
    for (auto first_func : module.Functors()) {
      for (auto func : first_func.Redeclarations()) {
        std::stringstream ss;
        ss << func.Id() << ':' << ParsedDeclaration(func).BindingPattern();
        if (auto [it, inserted] = seen.emplace(ss.str()); inserted) {
          DefineFunctor(os, module, func);

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
  os << "void " << message.Name() << "_" << message.Arity() << "(";

  auto sep = "";
  for (auto param : message.Parameters()) {
    os << sep << TypeName(module, param.Type()) << " " << param.Name();
    sep = ", ";
  }

  os << ", bool added) ";
  if (interface) {
    os << " {}\n\n";
  } else {
    os << " {\n";
    os.PushIndent();
    os << os.Indent() << impl << "\n";
    os.PopIndent();
    os << os.Indent() << "}\n\n";
  }
}

static void DeclareMessageLog(OutputStream &os, Program program,
                              ParsedModule root_module) {
  os << os.Indent() << "class " << gClassName << "Log {\n";
  os.PushIndent();
  os << os.Indent() << "public:\n";
  os.PushIndent();

  for (auto message : Messages(root_module)) {
    if (message.IsPublished()) {
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

  // First, declare all vector parameters.
  auto sep = "";
  for (auto vec : vec_params) {
    os << sep;
    auto vec_kind = vec.Kind();
    if (vec_kind == VectorKind::kParameter) {
      os << "const ";
    }
    if (proc.Kind() == ProcedureKind::kMessageHandler ||
        proc.Kind() == ProcedureKind::kEntryDataFlowFunc) {
      os << "::hyde::rt::SerializedVector<StorageT";
    } else {
      os << "::hyde::rt::Vector<StorageT";
    }
    const auto &col_types = vec.ColumnTypes();
    for (auto type : col_types) {
      os << ", " << TypeName(module, type);
    }
    os << "> & ";

    os << Vector(os, vec);
    sep = ", ";
  }

  // Then, declare all variable parameters.
  for (auto param : var_params) {
    os << sep << TypeName(module, param.Type()) << ' ' << Var(os, param);
    sep = ", ";
  }

  os << ") {\n";
  os.PushIndent();
  os << os.Indent() << "int state = " << kStateUnknown << ";\n"
     << os.Indent() << "int prev_state = " << kStateUnknown << ";\n"
     << os.Indent() << "int present_bit = 0;\n"
     << os.Indent() << "bool ret = false;\n"
     << os.Indent() << "bool found = false;\n";

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
    if (proc.Kind() == ProcedureKind::kMessageHandler ||
        proc.Kind() == ProcedureKind::kInitializer) {
      os << os.Indent() << "::hyde::rt::SerializedVector<StorageT";
    } else {
      os << os.Indent() << "::hyde::rt::Vector<StorageT";
    }

    for (auto type : vec.ColumnTypes()) {
      os << ", " << TypeName(module, type);
    }

    os << "> " << Vector(os, vec) << ";\n";

    // Tracking variable for the vector.
    os << os.Indent() << "int " << VectorIndex(os, vec) << " = 0;\n";
  }

  // Visit the body of the procedure. Procedure bodies are never empty; the
  // most trivial procedure body contains a `return False`.
  CPPCodeGenVisitor visitor(os, module);
  proc.Body().Accept(visitor);

  // From a codegen perspective, we guarantee that all paths through all
  // functions return, but mypy isn't always smart enough, mostly because we
  // have our returns inside of conditionals that mypy doesn't know are
  // complete.
  os << os.Indent() << "assert(false);\n" << os.Indent() << "return false;\n";

  os.PopIndent();
  os << os.Indent() << "}\n\n";
}

static void DefineQueryEntryPoint(OutputStream &os, ParsedModule module,
                                  const ProgramQuery &spec) {
  const ParsedDeclaration decl(spec.query);

  auto num_bound_params = 0u;
  auto num_free_params = 0u;
  const auto params = decl.Parameters();
  const auto num_params = decl.Arity();

  for (auto param : params) {
    if (param.Binding() == ParameterBinding::kBound) {
      ++num_bound_params;
    } else {
      ++num_free_params;
    }
  }

  os << os.Indent();
  if (num_free_params) {
    os << "std::vector<";
    if (1u < num_free_params) {
      os << "std::tuple<";
    }
    auto sep = "";
    for (auto param : params) {
      if (param.Binding() != ParameterBinding::kBound) {
        os << sep << TypeName(module, param.Type());
        sep = ", ";
      }
    }
    if (1u < num_free_params) {
      os << '>';
    }
    os << "> ";
  } else {
    os << "bool ";
  }
  os << decl.Name() << '_' << decl.BindingPattern() << "(";

  auto sep = "";
  for (auto param : params) {
    if (param.Binding() == ParameterBinding::kBound) {
      os << sep << TypeName(module, param.Type()) << " param_" << param.Index();
      sep = ", ";
    }
  }
  os << ") {\n";

  assert(num_params == (num_bound_params + num_free_params));

  os.PushIndent();
  os << os.Indent() << "int state = 0;\n";

  // Return vector
  if (num_free_params) {
    os << os.Indent() << "std::vector<";
    if (1u < num_free_params) {
      os << "std::tuple<";
    }
    auto sep = "";
    for (auto param : params) {
      if (param.Binding() != ParameterBinding::kBound) {
        os << sep << TypeName(module, param.Type());
        sep = ", ";
      }
    }
    if (1u < num_free_params) {
      os << '>';
    }
    os << "> ret;\n";
  }

  if (spec.forcing_function) {
    os << os.Indent() << Procedure(os, *(spec.forcing_function)) << '(';
    auto sep = "";
    for (auto param : params) {
      if (param.Binding() == ParameterBinding::kBound) {
        os << sep << "param_" << param.Index();
        sep = ", ";
      }
    }
    os << ");\n";
  }

  os << os.Indent() << "int tuple_index = 0;\n";

  // This is an index scan.
  if (num_bound_params && num_bound_params < num_params) {
    assert(spec.index.has_value());
    const auto index = *(spec.index);

    os << os.Indent() << "auto tuple_vec = " << TableIndex(os, index)
       << ".Get(";

    sep = "";
    for (auto param : decl.Parameters()) {
      if (param.Binding() == ParameterBinding::kBound) {
        os << sep << "param_" << param.Index();
        sep = ", ";
      }
    }

    os << ");\n";

    os << os.Indent() << "while (tuple_index < tuple_vec.size()) {\n";
    os.PushIndent();
    os << os.Indent() << "auto [";
    sep = "";
    for (auto param : params) {
      if (param.Binding() != ParameterBinding::kBound) {
        os << sep << "param_" << param.Index();
        sep = ", ";
      }
    }

    os << ", offset] = tuple_vec[tuple_index];\n"
       << os.Indent() << "tuple_index = offset;\n";

  // This is a full table scan.
  } else if (num_free_params) {
    assert(0u < num_free_params);

    os << os.Indent() << "for (auto &tuple : " << Table(os, spec.table)
       << ".Keys()"
       << ") {\n";
    os.PushIndent();

    os << os.Indent() << "auto [";
    sep = "";
    for (auto param : params) {
      if (param.Binding() != ParameterBinding::kBound) {
        os << sep << "param_" << param.Index();
        sep = ", ";
      }
    }
    os << ", _] = tuple.Get();\n";

  // Either the tuple checker will figure out of the tuple is present, or our
  // state check on the full tuple will figure it out.
  } else {
    os << os.Indent() << "if (true) {\n";
    os.PushIndent();
  }


  if (spec.tuple_checker) {
    os << os.Indent() << "if (!" << Procedure(os, *(spec.tuple_checker)) << '(';
    auto sep = "";
    for (auto param : params) {
      bool reify_param = param.Binding() == ParameterBinding::kFree;
      os << sep << "param_" << param.Index() << (reify_param ? ".Reify()" : "");
      sep = ", ";
    }
    os << ")) {\n";
    os.PushIndent();
    if (num_free_params) {
      os << os.Indent() << "continue;\n";
    } else {
      os << os.Indent() << "return false;\n";
    }
    os.PopIndent();
    os << os.Indent() << "}\n";

  // Double check the tuple's state.
  } else {
    os << os.Indent() << "state = " << Table(os, spec.table) << ".GetState(";

    sep = "";
    for (auto param : params) {
      os << sep << "param_" << param.Index();
      if (param.Binding() != ParameterBinding::kBound) {
        os << ".Reify()";
      }
      sep = ", ";
    }
    os << ") & " << kStateMask << ";\n"
       << os.Indent() << "if (state != " << kStatePresent << ") {\n";
    os.PushIndent();
    if (num_free_params) {
      os << os.Indent() << "continue;\n";
    } else {
      os << os.Indent() << "return false;\n";
    }
    os.PopIndent();
    os << os.Indent() << "}\n";
  }

  if (num_free_params) {
    os << os.Indent() << "ret.emplace_back(";
    auto sep = "";
    for (auto param : params) {
      if (param.Binding() != ParameterBinding::kBound) {
        os << sep << "param_" << param.Index() << ".Reify()";
        sep = ", ";
      }
    }
    os << ");\n";

  } else {
    os << os.Indent() << "return true;\n";
  }

  os.PopIndent();
  os << os.Indent() << "}\n";

  if (num_free_params) {
    os << os.Indent() << "return ret;\n";
  }

  os.PopIndent();
  os << os.Indent() << "}\n\n";
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

  os << "#include <algorithm>\n"
     << "#include <optional>\n"
     << "#include <tuple>\n"
     << "#include <unordered_map>\n"
     << "#include <vector>\n\n";

  // Forward-declare user-provided functors
  std::unordered_set<std::string> seen;
  for (auto module : ParsedModuleIterator(module)) {
    for (auto first_func : module.Functors()) {
      for (auto func : first_func.Redeclarations()) {
        std::stringstream ss;
        ss << func.Id() << ':' << ParsedDeclaration(func).BindingPattern();
        if (auto [it, inserted] = seen.emplace(ss.str()); inserted) {
          DeclareFunctor(os, module, func, true);
          os << ";\n";
          (void) it;
        }
      }
    }
  }
  os << "\n";

  DeclareDescriptors(os, program, module);

  os << "namespace {\n\n";

  DeclareFunctors(os, program, module);
  DeclareMessageLog(os, program, module);

  // A program gets its own class
  os << "template <typename StorageT, typename LogT, typename FunctorsT>\n";
  os << "class " << gClassName << " {\n";
  os.PushIndent();  // class

  os << os.Indent() << "public:\n";
  os.PushIndent();  // public:

  os << os.Indent() << "LogT &log;\n";
  os << os.Indent() << "FunctorsT &functors;\n";

  os << os.Indent()
     << "std::unordered_map<std::size_t, std::vector<::hyde::rt::Any*>> _refs;\n";

  os << "\n";

  os << os.Indent() << "explicit " << gClassName
     << "(StorageT &storage, LogT &l, FunctorsT &f)\n";
  os.PushIndent();  // constructor
  os << os.Indent() << ": log(l),\n" << os.Indent() << "  functors(f)";

  for (auto table : program.Tables()) {
    for (auto index : table.Indices()) {

      // If value columns are empty, then we've already mapped it to the table
      // itself as an accessor function
      if (!index.ValueColumns().empty()) {
        os << ",\n"
           << os.Indent() << "  " << TableIndex(os, index) << "(storage)";
      }
    }

    os << ",\n" << os.Indent() << "  " << Table(os, table) << "(storage";
    for (auto index : table.Indices()) {

      // NOTE(ekilmer): If value columns are empty, then we don't need to let the table know about it (...probably) because it's already a reference to the table itself
      if (!index.ValueColumns().empty()) {
        os << ", " << TableIndex(os, index);
      }
    }
    os << ")";
  }

  for (auto global : program.GlobalVariables()) {
    if (!global.IsConstant()) {
      os << ",\n"
         << os.Indent() << "  " << Var(os, global)
         << TypeValueOrDefault(module, global.Type(), global);
    }
  }
  os << " {\n";

  // Invoke the init procedure. Always first
  auto init_procedure = program.Procedures()[0];
  assert(init_procedure.Kind() == ProcedureKind::kInitializer);
  os << os.Indent() << Procedure(os, init_procedure) << "();\n";

  os.PopIndent();  // constructor
  os << os.Indent() << "}\n\n";

  for (const auto &query_spec : program.Queries()) {
    DefineQueryEntryPoint(os, module, query_spec);
  }

  for (auto proc : program.Procedures()) {
    if (proc.Kind() == ProcedureKind::kMessageHandler) {
      DefineProcedure(os, module, proc);
    }
  }

  os.PopIndent();  // public:

  os << os.Indent() << "private:\n";
  os.PushIndent();  // private:

  for (auto table : program.Tables()) {
    DeclareTable(os, module, table);
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
    if (proc.Kind() != ProcedureKind::kMessageHandler) {
      DefineProcedure(os, module, proc);
    }
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
