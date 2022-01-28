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

#include "Util.h"

namespace hyde {
namespace cxx {
namespace {

// Declare Table Descriptors that contain additional metadata about columns,
// indexes, and tables. The output of this code looks roughly like this:
//
//      template <>
//      struct ColumnDescriptor<12> {
//        static constexpr bool kIsNamed = false;
//        static constexpr unsigned kId = 12;
//        static constexpr unsigned kTableId = 10;
//        static constexpr unsigned kOffset = 1;
//        using Type = uint64_t;
//      };
//      template <>
//      struct IndexDescriptor<141> {
//        static constexpr unsigned kId = 141;
//        static constexpr unsigned kTableId = 10;
//        using Columns = TypeList<KeyColumn<11>, ValueColumn<12>>;
//        using KeyColumnIds = IdList<11>;
//        using ValueColumnIds = IdList<12>;
//      };
//      template <>
//      struct TableDescriptor<10> {
//        using ColumnIds = IdList<11, 12>;
//        using IndexIds = IdList<141>;
//        static constexpr unsigned kNumColumns = 2;
//      };
//
// We use the IDs of columns/indices/tables in place of type names so that we
// can have circular references.
static void DeclareDescriptors(OutputStream &os, Program program,
                               ParsedModule module) {

  // Table and column descriptors
  os << "namespace hyde::rt {\n";
  for (auto table : program.Tables()) {
    for (auto col : table.Columns()) {
      os << os.Indent() << "template <>\n"
         << os.Indent() << "struct ColumnDescriptor<" << col.Id() << "> {\n";
      os.PushIndent();
      os << os.Indent() << "static constexpr bool kIsNamed = false;\n"
         << os.Indent() << "static constexpr unsigned kId = " << col.Id()
         << ";\n"
         << os.Indent() << "static constexpr unsigned kTableId = " << table.Id()
         << ";\n"
         << os.Indent() << "static constexpr unsigned kOffset = " << col.Index()
         << ";\n"
         << os.Indent() << "using Type = ";

      TypeLoc col_type(col.Type());
      os << TypeName(os, module, col_type) << ";\n";
      os.PopIndent();
      os << os.Indent() << "};\n";
    }

    auto sep = "";

    std::vector<DataIndex> indexes;
    for (DataIndex index : table.Indices()) {
      indexes.push_back(index);
    }

    std::sort(indexes.begin(), indexes.end(),
              [] (DataIndex a, DataIndex b) {
                return a.ValueColumns().size() < b.ValueColumns().size();
              });

    unsigned i = 0u;
    for (auto index : indexes) {

      const auto key_cols = index.KeyColumns();
      const auto val_cols = index.ValueColumns();

      os << os.Indent() << "template <>\n"
         << os.Indent() << "struct IndexDescriptor<" << index.Id() << "> {\n";
      os.PushIndent();
      os << os.Indent() << "static constexpr unsigned kId = " << index.Id()
         << ";\n"
         << os.Indent() << "static constexpr unsigned kTableId = " << table.Id()
         << ";\n"
         << os.Indent() << "static constexpr unsigned kOffset = "
         << (i++) << ";\n"
         << os.Indent() << "static constexpr unsigned kNumKeyColumns = "
         << key_cols.size() << ";\n"
         << os.Indent() << "static constexpr unsigned kNumValueColumns = "
         << val_cols.size() << ";\n"
         << os.Indent() << "static constexpr bool kCoversAllColumns = "
         << (0 == val_cols.size() ? "true" : "false") << ";\n"
         << os.Indent() << "using Columns = TypeList<";

      // In C++ codegen, the Index knows which columns are keys/values, but they
      // need to be ordered as they were in the table
      auto key_col_iter = key_cols.begin();
      auto val_col_iter = val_cols.begin();

      // Assumes keys and values are sorted by index within their respective lists

      sep = "";
      while (key_col_iter != key_cols.end() || val_col_iter != val_cols.end()) {
        os << sep;
        if (val_col_iter == val_cols.end() ||
            (key_col_iter != key_cols.end() &&
             (*key_col_iter).Index() < (*val_col_iter).Index())) {
          os << "KeyColumn<" << (*key_col_iter).Id() << ">";
          key_col_iter++;
        } else {
          os << "ValueColumn<" << (*val_col_iter).Id() << ">";
          val_col_iter++;
        }
        sep = ", ";
      }

      os << ">;\n" << os.Indent() << "using KeyColumnIds = IdList<";
      sep = "";
      for (auto key_col : key_cols) {
        os << sep << key_col.Id();
        sep = ", ";
      }

      os << ">;\n" << os.Indent() << "using ValueColumnIds = IdList<";
      sep = "";
      for (auto val_col : val_cols) {
        os << sep << val_col.Id();
        sep = ", ";
      }

      os << ">;\n"
         << os.Indent() << "using KeyColumnOffsets = IdList<";
      sep = "";
      for (auto key_col : key_cols) {
        os << sep << key_col.Index();
        sep = ", ";
      }
      os << ">;\n"
          << os.Indent() << "using ValueColumnOffsets = IdList<";
       sep = "";
       for (auto val_col : val_cols) {
         os << sep << val_col.Index();
         sep = ", ";
       }
       os << ">;\n";
      os.PopIndent();
      os << os.Indent() << "};\n";
    }

    os << os.Indent() << "template <>\n"
       << os.Indent() << "struct TableDescriptor<" << table.Id() << "> {\n";
    os.PushIndent();

    // Does this table have an index that fully covers it?
    os << os.Indent() << "static constexpr bool kHasCoveringIndex = ";
    for (auto index : table.Indices()) {
      if (index.ValueColumns().empty()) {
        os << "true;\n";
        goto print_rest;
      }
    }
    os << "false;\n";

  print_rest:
    os << os.Indent() << "using ColumnIds = IdList<";
    sep = "";
    for (auto col : table.Columns()) {
      os << sep << col.Id();
      sep = ", ";
    }
    os << ">;\n" << os.Indent() << "using IndexIds = IdList<";
    sep = "";

    // Print out the indexes in order of the number of decreasing number of
    // key columns. Thus, if there's an index over all columns, then it appears
    // first.
    assert(!indexes.empty());
    for (auto index : indexes) {
      os << sep << index.Id();
      sep = ", ";
    }
    os << ">;\n"
        << os.Indent() << "static constexpr unsigned kFirstIndexId = "
        << indexes[0].Id() << ";\n"
        << os.Indent() << "static constexpr unsigned kNumColumns = "
        << table.Columns().size() << ";\n";

    os.PopIndent();
    os << "};\n";

    os << "\n";
  }
  os << "}  // namepace hyde::rt\n\n";
}

static void DefineGlobal(OutputStream &os, ParsedModule module,
                         DataVariable global) {
  TypeLoc type = global.Type();

  os << os.Indent();
  if (global.IsConstant()) {
    switch (global.Type().Kind()) {
      case TypeKind::kBoolean:
      case TypeKind::kSigned8:
      case TypeKind::kSigned16:
      case TypeKind::kSigned32:
      case TypeKind::kSigned64:
      case TypeKind::kUnsigned8:
      case TypeKind::kUnsigned16:
      case TypeKind::kUnsigned32:
      case TypeKind::kUnsigned64:
      case TypeKind::kDouble:
      case TypeKind::kFloat:
        os << "static constexpr ";
        break;
      default:
        os << "static const ";
        break;
    }
  }

  os << TypeName(os, module, type) << " " << Var(os, global) << ";\n";
}

static bool CanInlineDefineConstant(ParsedModule module, DataVariable global) {
  switch (global.DefiningRole()) {
    case VariableRole::kConstantZero:
    case VariableRole::kConstantOne:
    case VariableRole::kConstantFalse:
    case VariableRole::kConstantTrue: return true;
    default: break;
  }

  auto type = global.Type();
  return type.IsReferentiallyTransparent(module, Language::kCxx);
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
  if (CanInlineDefineConstant(module, global)) {
    os << os.Indent() << "static constexpr " << TypeName(module, type) << " "
       << Var(os, global) << " = "
       << TypeValueOrDefault(module, type, global) << ";\n";
  } else {
    os << os.Indent() << "const " << TypeName(os, module, type)
       << Var(os, global) << ";\n";
  }
}
//
//// We want to enable referential transparency in the code, so that if an Nth
//// value is produced by some mechanism that is equal to some prior value, then
//// we replace its usage with the prior value.
//static void DefineTypeRefResolver(OutputStream &os) {
//
//  // Has merge_into method
//  os << os.Indent() << "template<typename T>\n"
//     << os.Indent()
//     << "typename ::hyde::rt::enable_if<::hyde::rt::has_merge_into<T,\n"
//     << os.Indent() << "         void(T::*)(T &)>::value, T>::type\n"
//     << os.Indent() << "_resolve(T &obj) {\n";
//  os.PushIndent();
//  os << os.Indent() << "auto ref_list = _refs[std::hash<T>(obj)];\n"
//     << os.Indent() << "for (auto maybe_obj : ref_list) {\n";
//  os.PushIndent();
//  os << os.Indent() << "// TODO(ekilmer): This isn't going to work...\n";
//  os << os.Indent() << "if (&obj == &maybe_obj) {\n";
//  os.PushIndent();
//  os << os.Indent() << "return obj;\n";
//  os.PopIndent();
//  os << os.Indent() << "} else if (obj == maybe_obj) {\n";
//  os.PushIndent();
//  os << os.Indent() << "T prior_obj = static_cast<T>(maybe_obj);\n"
//     << os.Indent() << "obj.merge_into(prior_obj);\n"
//     << os.Indent() << "return prior_obj;\n";
//  os.PopIndent();
//  os << os.Indent() << "}\n";
//  os.PopIndent();
//  os << os.Indent() << "}\n"
//     << os.Indent() << "ref_list.push_back(obj);\n"
//     << os.Indent() << "return obj;\n";
//  os.PopIndent();
//  os << os.Indent() << "}\n\n";
//
//  // Does not have merge_into
//  os << os.Indent() << "template<typename T>\n"
//     << os.Indent() << os.Indent() << "auto _resolve(T &&obj) {\n";
//  os.PushIndent();
//  os << os.Indent() << "return obj;\n";
//  os.PopIndent();
//  os << os.Indent() << "}\n\n";
//  os << os.Indent() << "template<typename T>\n"
//     << os.Indent() << os.Indent() << "auto _resolve(T &obj) {\n";
//  os.PushIndent();
//  os << os.Indent() << "return obj;\n";
//  os.PopIndent();
//  os << os.Indent() << "}\n\n";
//}

class CPPCodeGenVisitor final : public ProgramVisitor {
 public:
  explicit CPPCodeGenVisitor(OutputStream &os_, ParsedModule module_)
      : os(os_),
        module(module_) {}

  void Visit(ProgramModeSwitchRegion region) override {
    os << Comment(os, region, "ProgramModeSwitchRegion");
    if (auto body = region.Body()) {
      body->Accept(*this);
    }
  }

  void Visit(ProgramCallRegion region) override {
    os << Comment(os, region, "ProgramCallRegion");

    const auto called_proc = region.CalledProcedure();

    os << os.Indent() << "if (" << Procedure(os, called_proc) << "(";

    auto sep = "";

    // Pass in the vector parameters, or the references to the vectors.
    for (auto vec : region.VectorArguments()) {
      os << sep << "std::move(" << Vector(os, vec) << ")";
      sep = ", ";
    }

    // Pass in the variable parameters, or the references to the variables.
    for (auto var : region.VariableArguments()) {
      os << sep << Var(os, var);
      sep = ", ";
    }

    os << ")) {\n";
    os.PushIndent();

    if (auto true_body = region.BodyIfTrue(); true_body) {
      true_body->Accept(*this);
    }

    os.PopIndent();
    os << os.Indent() << '}';

    if (auto false_body = region.BodyIfFalse(); false_body) {
      os << " else {\n";
      os.PushIndent();
      false_body->Accept(*this);
      os.PopIndent();
      os << os.Indent() << "}\n";
    } else {
      os << '\n';
    }
  }

  void Visit(ProgramReturnRegion region) override {
    os << Comment(os, region, "ProgramReturnRegion");
    os << os.Indent() << "return "
       << (region.ReturnsFalse() ? "false;\n" : "true;\n");
  }

  void Visit(ProgramTestAndSetRegion region) override {
    os << Comment(os, region, "ProgramTestAndSetRegion");
    const auto acc = region.Accumulator();
    const auto disp = region.Displacement();
    const auto cmp = region.Comparator();

    auto body = region.Body();

    os << os.Indent();
    if (body) {
      os << "if ((";
    }

    os << Var(os, acc);
    if (region.IsAdd()) {
      os << " += ";
    } else if (region.IsSubtract()) {
      os << " -= ";
    } else {
      assert(false);
    }
    os << Var(os, disp);

    if (body) {
      os << ") == " << Var(os, cmp) << ") {\n";
      os.PushIndent();
      body->Accept(*this);
      os.PopIndent();
      os << os.Indent() << "}\n";
    } else {
      os << ";\n";
    }
  }

  void Visit(ProgramGenerateRegion region) override {
    os << Comment(os, region, "ProgramGenerateRegion");

    const auto functor = region.Functor();
    const auto id = region.Id();

    os << os.Indent() << "::hyde::rt::index_t num_results_" << id << " = 0;\n";

    auto output_vars = region.OutputVariables();

    auto call_functor = [&](void) {
      Functor(os, functor) << "(";
      auto sep = "";
      for (auto in_var : region.InputVariables()) {
        if (in_var.Type().IsReferentiallyTransparent(module, Language::kCxx)) {
          os << sep << Var(os, in_var);
        } else {
          os << sep << "*" << Var(os, in_var);
        }
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
          auto out_var = output_vars[0];
          const auto out_type = out_var.Type();
          const auto out_type_is_transparent =
              out_type.IsReferentiallyTransparent(module, Language::kCxx);
          os << os.Indent() << "for (auto tmp_" << id << " : ";
          call_functor();
          os << ") {\n";
          os.PushIndent();

          os << os.Indent() << TypeName(os, module, out_type)
             << " " << Var(os, out_var) << " = ";
          if (!out_type_is_transparent) {
            os << "storage.Intern(";
          }
          os << "std::move(tmp_" << id << ")";
          if (!out_type_is_transparent) {
            os << ")";
          }
          os << ";\n";
          do_body();
          os.PopIndent();
          os << os.Indent() << "}\n";

        } else {
          assert(!output_vars.empty());

          os << os.Indent() << "for (auto tmp_" << id << " : ";
          call_functor();
          os << ") {\n";
          os.PushIndent();
          auto out_var_index = 0u;
          for (auto out_var : output_vars) {
            const auto out_type = out_var.Type();
            const auto out_type_is_transparent =
                out_type.IsReferentiallyTransparent(module, Language::kCxx);
            os << os.Indent() << TypeName(os, module, out_type)
               << " " << Var(os, out_var) << " = ";
            if (!out_type_is_transparent) {
              os << "storage.Intern(";
            }
            os << "std::move(std::get<" << (out_var_index++) << ">(tmp_" << id << "))";
            if (!out_type_is_transparent) {
              os << ")";
            }
            os << ";\n";
          }
          do_body();
          os.PopIndent();
          os << os.Indent() << "}\n";
        }

        break;
      }

      // These behave like returns of tuples/values.
      case FunctorRange::kOneToOne:
        assert(!functor.IsFilter());

        // Produces a single value.
        if (output_vars.size() == 1u) {

          const auto out_var = output_vars[0];
          const auto out_type = out_var.Type();
          const auto out_type_is_transparent =
              out_type.IsReferentiallyTransparent(module, Language::kCxx);
          os << os.Indent() << TypeName(os, module, out_type)
             << " " << Var(os, out_var) << " = ";
          if (!out_type_is_transparent) {
            os << "storage.Intern(";
          }
          call_functor();
          if (!out_type_is_transparent) {
            os << ")";
          }
          os << ";\n";
          do_body();

        // Produces a tuple of values.
        } else {

          os << os.Indent() << "auto tmp_" << id << " = ";
          call_functor();
          os << ";\n";
          auto out_var_index = 0u;
          for (auto out_var : output_vars) {
            const auto out_type = out_var.Type();
            const auto out_type_is_transparent =
                out_type.IsReferentiallyTransparent(module, Language::kCxx);
            os << os.Indent() << TypeName(os, module, out_type)
               << " " << Var(os, out_var) << " = ";
            if (!out_type_is_transparent) {
              os << "storage.Intern(";
            }
            os << "std::move(std::get<" << (out_var_index++) << ">(tmp_" << id << "))";
            if (!out_type_is_transparent) {
              os << ")";
            }
            os << ";\n";
          }
          do_body();
        }
        break;

      // These behave like returns of optional tuples/values.
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
          const auto out_type = out_var.Type();
          const auto out_type_is_transparent =
              out_type.IsReferentiallyTransparent(module, Language::kCxx);
          os << os.Indent() << "auto tmp_" << id << " = ";
          call_functor();
          os << ";\n";
          os << os.Indent() << "if (tmp_" << id << ") {\n";
          os.PushIndent();
          os << os.Indent() << TypeName(os, module, out_type)
             << " " << Var(os, out_var) << " = ";
          if (!out_type_is_transparent) {
            os << "storage.Intern(";
          }
          os << "std::move(tmp_" << id << ".value())";
          if (!out_type_is_transparent) {
            os << ")";
          }
          os << ";\n";
          do_body();
          os.PopIndent();
          os << os.Indent() << "}\n";

        // Produces a tuple of values.
        } else {
          assert(!functor.IsFilter());

          os << os.Indent() << "auto tmp_" << id << " = ";
          call_functor();
          os << ";\n" << os.Indent() << "if (tmp_" << id << ") {\n";
          os.PushIndent();
          auto out_var_index = 0u;
          for (auto out_var : output_vars) {
            const auto out_type = out_var.Type();
            const auto out_type_is_transparent =
                out_type.IsReferentiallyTransparent(module, Language::kCxx);
            os << os.Indent() << TypeName(os, module, out_type)
               << " " << Var(os, out_var) << " = ";
            if (!out_type_is_transparent) {
              os << "storage.Intern(";
            }
            os << "std::move(std::get<" << (out_var_index++) << ">(tmp_" << id << "))";
            if (!out_type_is_transparent) {
              os << ")";
            }
            os << ";\n";
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
    os << os.Indent() << "for (auto changed_" << region.Id()
       << " = true; changed_" << region.Id() << "; changed_" << region.Id()
       << " = !!(";
    auto sep = "";
    for (auto vec : region.Vectors()) {
      os << sep << Vector(os, vec) << ".Size()";
      sep = " | ";
    }
    os << ")) {\n";

    os.PushIndent();

    os << os.Indent() << "DumpStats();\n"
       << os.Indent() << "if constexpr (false) {\n";
    os.PushIndent();
    os << os.Indent() << "fprintf(stderr, \"";

    sep = "";
    for (auto vec : region.Vectors()) {
      os << sep << "vec_" << vec.Id() << " = %\" PRIu64 \"";
      sep = " ";
    }
    sep = "\\n\", ";
    for (auto vec : region.Vectors()) {
      os << sep << Vector(os, vec) << ".Size()";
      sep = ", ";
    }
    os << ");\n";
    os.PopIndent();
    os << os.Indent() << "}\n\n";

    region.FixpointLoop().Accept(*this);

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
    }
  }

  void Visit(ProgramParallelRegion region) override {
    os << Comment(os, region, "ProgramParallelRegion");
    for (auto sub_region : region.Regions()) {
      sub_region.Accept(*this);
    }
  }

  // Should never be reached; defined below.
  void Visit(ProgramProcedure) override {
    assert(false);
  }

  void Visit(ProgramPublishRegion region) override {
    os << Comment(os, region, "ProgramPublishRegion");
    auto message = region.Message();

    os << os.Indent() << "log." << message.Name() << '_' << message.Arity();

    auto sep = "(";
    for (auto var : region.VariableArguments()) {
      if (var.Type().IsReferentiallyTransparent(module, Language::kCxx)) {
        os << sep << Var(os, var);
      } else {
        os << sep << "*" << Var(os, var);
      }
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

    for (auto sub_region : region.Regions()) {
      sub_region.Accept(*this);
    }
  }

  void Visit(ProgramVectorAppendRegion region) override {
    os << Comment(os, region, "ProgramVectorAppendRegion");

    const auto tuple_vars = region.TupleVariables();

    os << os.Indent() << Vector(os, region.Vector());
    auto sep = ".Add(";
    for (DataVariable var : tuple_vars) {
      os << sep << Var(os, var);
      sep = ", ";
    }
    os << ");\n";
  }

  void Visit(ProgramVectorClearRegion region) override {
    os << Comment(os, region, "ProgramVectorClearRegion");
    os << os.Indent() << Vector(os, region.Vector()) << ".Clear();\n";
  }

  void Visit(ProgramVectorSwapRegion region) override {
    os << Comment(os, region, "Program VectorSwap Region");
    os << os.Indent() << Vector(os, region.LHS()) << ".Swap("
       << Vector(os, region.RHS()) << ");\n";
  }

  void Visit(ProgramVectorLoopRegion region) override {
    auto body = region.Body();
    if (!body) {
      os << Comment(os, region, "Empty ProgramVectorLoopRegion");
      return;
    }

    os << Comment(os, region, "ProgramVectorLoopRegion");
    auto vec = region.Vector();
    os << os.Indent() << "for (auto [";

    const auto tuple_vars = region.TupleVariables();
    auto sep = "";
    for (auto var : tuple_vars) {
      os << sep << Var(os, var);
      sep = ", ";
    }
    // Need to differentiate between our Vector and regular
    os << "] : " << Vector(os, vec) << ") {\n";

    os.PushIndent();
    body->Accept(*this);
    os.PopIndent();
    os << os.Indent() << "}\n";
  }

  void Visit(ProgramVectorUniqueRegion region) override {
    os << Comment(os, region, "ProgramVectorUniqueRegion");
    os << os.Indent() << Vector(os, region.Vector()) << ".SortAndUnique();\n";
  }

  void Visit(ProgramChangeTupleRegion region) override {
    os << Comment(os, region, "ProgramChangeTupleRegion");
    const auto tuple_vars = region.TupleVariables();

    auto print_state_enum = [&](TupleState state) {
      switch (state) {
        case TupleState::kAbsent: os << "Absent"; break;
        case TupleState::kPresent: os << "Present"; break;
        case TupleState::kUnknown: os << "Unknown"; break;
        case TupleState::kAbsentOrUnknown: os << "AbsentOrUnknown"; break;
      }
    };

    os << os.Indent() << "if (" << Table(os, region.Table())
       << ".TryChangeTupleFrom";

    print_state_enum(region.FromState());
    os << "To";
    print_state_enum(region.ToState());

    auto sep = "(";
    for (auto var : tuple_vars) {
      os << sep << Var(os, var);
      sep = ", ";
    }

    os << ")) {\n";
    os.PushIndent();

    if (auto succeeded_body = region.BodyIfSucceeded(); succeeded_body) {
      succeeded_body->Accept(*this);
    }

    os.PopIndent();
    os << os.Indent() << "}";
    if (auto failed_body = region.BodyIfFailed(); failed_body) {
      os << " else {\n";
      os.PushIndent();
      failed_body->Accept(*this);
      os.PopIndent();
      os << os.Indent() << "}\n";
    } else {
      os << '\n';
    }
  }

  void Visit(ProgramCheckTupleRegion region) override {
    os << Comment(os, region, "ProgramCheckTupleRegion");
    const auto table = region.Table();
    const auto vars = region.TupleVariables();
    os << os.Indent() << "switch (" << Table(os, table) << ".GetState(";
    auto sep = "";
    for (auto var : vars) {
      os << sep << Var(os, var);
      sep = ", ";
    }
    os << ")) {\n";

    os.PushIndent();

    if (auto absent_body = region.IfAbsent(); absent_body) {
      os << os.Indent() << "case ::hyde::rt::TupleState::kAbsent: {\n";
      os.PushIndent();
      absent_body->Accept(*this);
      os << os.Indent() << "break;\n";
      os.PopIndent();
      os << os.Indent() << "}\n";
    } else {
      os << os.Indent() << "case ::hyde::rt::TupleState::kAbsent: break;\n";
    }

    if (auto present_body = region.IfPresent(); present_body) {
      os << os.Indent() << "case ::hyde::rt::TupleState::kPresent: {\n";
      os.PushIndent();
      present_body->Accept(*this);
      os << os.Indent() << "break;\n";
      os.PopIndent();
      os << os.Indent() << "}\n";
    } else {
      os << os.Indent() << "case ::hyde::rt::TupleState::kPresent: break;\n";
    }

    if (auto unknown_body = region.IfUnknown(); unknown_body) {
      os << os.Indent() << "case ::hyde::rt::TupleState::kUnknown: {\n";
      os.PushIndent();
      unknown_body->Accept(*this);
      os << os.Indent() << "break;\n";
      os.PopIndent();
      os << os.Indent() << "}\n";
    } else {
      os << os.Indent() << "case ::hyde::rt::TupleState::kUnknown: break;\n";
    }

    os.PopIndent();
    os << os.Indent() << "}\n";
  }

  void Visit(ProgramTableJoinRegion region) override {
    auto body = region.Body();
    if (!body) {
      os << Comment(os, region, "Empty ProgramTableJoinRegion");
      return;
    }

    const auto id = region.Id();

    os << Comment(os, region, "ProgramTableJoinRegion");

    // Nested loop join
    auto vec = region.PivotVector();
    os << os.Indent() << "for (auto [";

    std::vector<std::string> var_names;
    auto sep = "";
    for (auto var : region.OutputPivotVariables()) {
      std::stringstream var_name;
      (void) Var(var_name, var);
      var_names.emplace_back(var_name.str());
      os << sep << var_names.back();
      sep = ", ";
    }
    os << "] : " << Vector(os, vec) << ") {\n";
    os.PushIndent();

    auto tables = region.Tables();

    // First, build the scans.
    for (auto i = 0u; i < tables.size(); ++i) {
      auto maybe_index = region.Index(i);
      assert(maybe_index.has_value());

      const auto table = tables[i];
      const auto index = *maybe_index;
      const auto index_keys = index.KeyColumns();

      os << os.Indent() << "::hyde::rt::Scan<StorageT, ::hyde::rt::IndexTag<"
         << index.Id() << ">> scan_" << id << '_' << i << "(storage, "
         << Table(os, table);

      for (auto index_col : index_keys) {
        auto j = 0u;
        for (auto used_col : region.IndexedColumns(i)) {
          if (used_col == index_col) {
            os << ", " << var_names[j];
          }
          ++j;
        }
      }

      os << ");\n";
    }

    // Now, iterate over the scans over the tables where we do use an index.
    for (auto i = 0u; i < tables.size(); ++i) {
      auto out_vars = region.OutputVariables(i);
      assert(out_vars.size() == region.SelectedColumns(i).size());
      os << os.Indent() << "for (auto [";
      sep = "";
      for (auto var : out_vars) {
        os << sep << Var(os, var);
        sep = ", ";
      }

      os << "] : scan_" << id << '_' << i << ") {\n";

      // We increase indentation here, and the corresponding `PopIndent()`
      // only comes *after* visiting the `region.Body()`.
      os.PushIndent();
    }

    body->Accept(*this);

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
    auto body = region.Body();
    if (!body) {
      os << Comment(os, region, "Empty ProgramTableProductRegion");
      return;
    }

    auto id = region.Id();
    os << Comment(os, region, "ProgramTableProductRegion");

    os << os.Indent();

    auto i = 0u;
    os << "::hyde::rt::Vector<StorageT";
    for (auto table : region.Tables()) {
      (void) table;
      for (auto var : region.OutputVariables(i++)) {
        os << ", " << TypeName(os, module, var.Type());
      }
    }

    os << "> "
       << "vec_" << region.Id() << "(storage, " << region.Id() << ");\n";

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

        os << os.Indent() << "::hyde::rt::Scan<StorageT, ::hyde::rt::TableTag<"
           << inner_table.Id() << ">> scan_" << id << "_" << i << "_" << j
           << "(storage, " << Table(os, inner_table) << ");\n";

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
        os << " : scan_" << id << "_" << i << "_" << j << ") {\n";
        os.PushIndent();
        ++indents;
      }

      // Collect all product things into a vector.
      os << os.Indent() << "vec_" << region.Id();
      sep = ".Add(";
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
      for (auto table : region.Tables()) {
        os.PopIndent();
        os << os.Indent() << "}\n";
        assert(0u < indents);
        indents--;
        (void) table;
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
    body->Accept(*this);
    os.PopIndent();
    os << os.Indent() << "}\n";
  }

  void Visit(ProgramTableScanRegion region) override {
    os << Comment(os, region, "ProgramTableScanRegion");
    const auto body = region.Body();
    if (!body) {
      return;
    }

    const auto id = region.Id();
    const auto table = region.Table();
    const auto input_vars = region.InputVariables();
    os << os.Indent() << "{\n";
    os.PushIndent();
    os << os.Indent() << "::hyde::rt::Scan<StorageT, ::hyde::rt::";
    if (auto maybe_index = region.Index(); maybe_index) {
      os << "IndexTag<" << maybe_index->Id() << ">";
    } else {
      os << "TableTag<" << table.Id() << ">";
    }

    os << "> scan_" << id << "(storage, " << Table(os, table);
    for (auto var : input_vars) {
      os << ", " << Var(os, var);
    }
    os << ");\n";

    os << os.Indent() << "for (auto [";
    auto sep = "";
    for (auto var : region.OutputVariables()) {
      os << sep << Var(os, var);
      sep = ", ";
    }
    os << "] : scan_" << id << ") {\n";

    os.PushIndent();
    body->Accept(*this);
    os.PopIndent();
    os << os.Indent() << "}\n";
    os.PopIndent();
    os << os.Indent() << "}\n";
  }

  void Visit(ProgramTupleCompareRegion region) override {
    os << Comment(os, region, "ProgramTupleCompareRegion");

    const auto lhs_vars = region.LHS();
    const auto rhs_vars = region.RHS();

    os << os.Indent() << "if (std::make_tuple(";

    auto sep = "";
    for (auto var : lhs_vars) {
      os << sep << Var(os, var);
      sep = ", ";
    }

    os << ") " << OperatorString(region.Operator()) << " std::make_tuple(";
    sep = "";
    for (auto var : rhs_vars) {
      os << sep << Var(os, var);
      sep = ", ";
    }
    os << ")) {\n";

    os.PushIndent();
    if (auto true_body = region.BodyIfTrue(); true_body) {
      true_body->Accept(*this);
    }
    os.PopIndent();
    os << os.Indent() << '}';

    if (auto false_body = region.BodyIfFalse(); false_body) {
      os << " else {\n";
      os.PushIndent();
      false_body->Accept(*this);
      os.PopIndent();
      os << os.Indent() << "}\n";
    } else {
      os << '\n';
    }
  }

  void Visit(ProgramWorkerIdRegion region) override {
    os << Comment(os, region, "Program WorkerId Region");
    if (auto body = region.Body(); body) {
      body->Accept(*this);
    }
  }

 private:
  OutputStream &os;
  const ParsedModule module;
};

static void DeclareFunctor(OutputStream &os, ParsedModule module,
                           ParsedFunctor func) {
  ParsedDeclaration decl(func);
  std::stringstream return_tuple;
  std::vector<ParsedParameter> args;
  auto sep_ret = "";
  auto num_ret_types = 0u;
  for (auto param : decl.Parameters()) {
    if (param.Binding() == ParameterBinding::kBound) {
      args.push_back(param);
    } else {
      ++num_ret_types;
      return_tuple << sep_ret << TypeName(module, param.Type());
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

  os << " " << func.Name() << '_'
     << ParsedDeclaration(func).BindingPattern() << '(';

  auto arg_sep = "";
  for (ParsedParameter arg : args) {
    const TypeLoc type = arg.Type();
    os << arg_sep;
    if (type.IsReferentiallyTransparent(module, Language::kCxx)) {
      os << TypeName(module, type) << ' ';
    } else {
      os << "const " << TypeName(module, type) << " &";
    }
    os << arg.Name();
    arg_sep = ", ";
  }

  os << ')';
}

static void DefineFunctor(OutputStream &os, ParsedModule module,
                          ParsedFunctor func, const std::string &ns_name) {
  ParsedDeclaration decl(func);

  os << os.Indent() << "virtual\n";
  DeclareFunctor(os, module, func);
  os << " = 0;\n";
}

static void DeclareFunctors(OutputStream &os, Program program,
                            ParsedModule root_module,
                            const std::string &ns_name,
                            const std::vector<ParsedInline> &inlines) {
  os << os.Indent() << "template <typename StorageT>\n"
     << os.Indent() << "class " << gClassName << "Functors {\n";
  os.PushIndent();
  os << os.Indent() << "public:\n";
  os << os.Indent() << " virtual ~" << gClassName
     << "Functors(void) = default;\n";
  os.PushIndent();

  for (auto code : inlines) {
    if (code.Stage() == "c++:database:functors:prologue") {
      os << code.CodeToInline() << "\n\n";
    }
  }

  for (ParsedFunctor func : Functors(root_module)) {
    if (!func.IsInline(Language::kCxx)) {
      DefineFunctor(os, root_module, func, ns_name);
    }
  }

  for (auto code : inlines) {
    if (code.Stage() == "c++:database:functors:epilogue") {
      os << code.CodeToInline() << "\n\n";
    }
  }

  os.PopIndent();

  os.PopIndent();
  os << os.Indent() << "};\n\n";
}

static void DeclareMessageLogger(OutputStream &os, ParsedModule module,
                                 ParsedMessage message) {
  ParsedDeclaration decl(message);
  os << os.Indent() << "void " << message.Name() << "_" << message.Arity()
     << "(";

  auto sep = "";
  for (auto param : decl.Parameters()) {
    os << sep;
    if (param.Type().IsReferentiallyTransparent(module, Language::kCxx)) {
      os << TypeName(module, param.Type()) << " p";
    } else {
      os << "const " << TypeName(module, param.Type()) << " &p";
    }
    os << param.Index() << " /* " << param.Name() << " */";
    sep = ", ";
  }

  os << sep << "bool added) {}\n";
}

static void DeclareMessageLog(OutputStream &os, Program program,
                              ParsedModule root_module,
                              const std::vector<ParsedInline> &inlines) {
  os << os.Indent() << "template <typename StorageT>\n"
     << os.Indent() << "class " << gClassName << "Log {\n";
  os.PushIndent();
  os << os.Indent() << "public:\n";
  os.PushIndent();

  for (auto code : inlines) {
    if (code.Stage() == "c++:database:log:prologue") {
      os << code.CodeToInline() << "\n\n";
    }
  }

  for (auto message : Messages(root_module)) {
    if (message.IsPublished()) {
      DeclareMessageLogger(os, root_module, message);
    }
  }

  for (auto code : inlines) {
    if (code.Stage() == "c++:database:log:epilogue") {
      os << code.CodeToInline() << "\n\n";
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
    os << sep << "::hyde::rt::Vector<StorageT";
    const auto &col_types = vec.ColumnTypes();
    for (auto type : col_types) {
      auto type_loc = TypeLoc(type);
      os << ", " << TypeName(os, module, type_loc);
    }
    os << "> ";

    if (proc.Kind() != ProcedureKind::kMessageHandler &&
        proc.Kind() != ProcedureKind::kEntryDataFlowFunc &&
        proc.Kind() != ProcedureKind::kPrimaryDataFlowFunc) {
      os << '&';
    }
    os << Vector(os, vec);
    sep = ", ";
  }

  // Then, declare all variable parameters.
  for (auto param : var_params) {
    os << sep << TypeName(os, module, param.Type()) << ' ' << Var(os, param);
    sep = ", ";
  }

  os << ") {\n";
  os.PushIndent();

  // Define the vectors that will be created and used within this procedure.
  // These vectors exist to support inductions, joins (pivot vectors), etc.
  for (auto vec : proc.DefinedVectors()) {
    os << os.Indent() << "::hyde::rt::Vector<StorageT";

    for (auto type : vec.ColumnTypes()) {
      auto type_loc = TypeLoc(type);
      os << ", " << TypeName(os, module, type_loc);
    }

    os << "> " << Vector(os, vec) << "(storage, " << vec.Id() << "u);\n";
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
  (void) num_params;

  for (auto param : params) {
    if (param.Binding() == ParameterBinding::kBound) {
      ++num_bound_params;
    } else {
      ++num_free_params;
    }
  }
  if (num_free_params) {
    os << os.Indent() << "template <typename _Generator>\n";
  }

  os << os.Indent() << "::hyde::rt::index_t " << decl.Name() << '_'
     << decl.BindingPattern() << "(";

  auto sep = "";
  auto has_refs = false;
  for (auto param : params) {
    if (param.Binding() == ParameterBinding::kBound) {
      os << sep << TypeName(module, param.Type()) << " ";
      if (!param.Type().IsReferentiallyTransparent(module, Language::kCxx)) {
        has_refs = true;
        os << "val_";
      }
      os << "param_" << param.Index();
      sep = ", ";
    }
  }

  if (num_free_params) {
    os << sep << "_Generator _generator";
  }
  os << ") {\n";

  assert(num_params == (num_bound_params + num_free_params));

  os.PushIndent();
  if (has_refs) {
    for (auto param : params) {
      if (param.Binding() != ParameterBinding::kBound ||
          param.Type().IsReferentiallyTransparent(module, Language::kCxx)) {
        continue;
      }

      os << os.Indent()
         << "::hyde::rt::InternRef<" << TypeName(module, param.Type())
         << "> param_" << param.Index()
         << "(storage.Intern(std::move(val_param_" << param.Index() << ")));\n";
    }
  }
  os << os.Indent() << "::hyde::rt::index_t num_generated = 0;\n"
     << os.Indent() << "(void) num_generated;\n";

  if (spec.forcing_function) {
    os << os.Indent() << Procedure(os, *(spec.forcing_function)) << '(';
    sep = "";
    for (auto param : params) {
      if (param.Binding() == ParameterBinding::kBound) {
        os << sep << "param_" << param.Index();
        sep = ", ";
      }
    }
    os << ");\n";
  }

  // This is either a table or index scan.
  if (num_free_params) {
    os << os.Indent() << "::hyde::rt::Scan<StorageT, ::hyde::rt::";

    // This is an index scan.
    if (num_bound_params) {
      assert(spec.index.has_value());
      os << "IndexTag<" << spec.index->Id() << ">";

    // This is a full table scan.
    } else {
      os << "TableTag<" << spec.table.Id() << ">";
    }

    os << "> scan(storage, " << Table(os, spec.table);
    for (auto param : params) {
      if (param.Binding() == ParameterBinding::kBound) {
        if (param.Type().IsReferentiallyTransparent(module, Language::kCxx)) {
          os << ", param_" << param.Index();
        } else {
          os << ", std::move(param_" << param.Index() << ")";
        }
      }
    }
    os << ");\n"
       << os.Indent() << "for (auto [";
    sep = "";
    for (auto param : params) {
      if (param.Binding() != ParameterBinding::kBound) {
        os << sep << "param_" << param.Index();
      } else {
        os << sep << "shadow_param_" << param.Index();
      }
      sep = ", ";
    }

    os << "] : scan) {\n";
    os.PushIndent();

    // We have to double-check the tuples from index scans, as they can be
    // probabilistically stored.
    if (num_bound_params) {
      os << os.Indent() << "if (std::make_tuple(";
      sep = "";
      for (auto param : params) {
        if (param.Binding() == ParameterBinding::kBound) {
          os << sep << "param_" << param.Index();
          sep = ", ";
        }
      }

      os << ") != std::make_tuple(";
      sep = "";
      for (auto param : params) {
        if (param.Binding() == ParameterBinding::kBound) {
          os << sep << "shadow_param_" << param.Index();
          sep = ", ";
        }
      }

      os << ")) {\n";
      os.PushIndent();
      os << os.Indent() << "continue;\n";
      os.PopIndent();
      os << os.Indent() << "}\n";
    }

  // This is an existence check.
  } else {
    os << os.Indent() << "if (true) {\n";
    os.PushIndent();
  }

  // Check the tuple's state using a finder function.
  if (spec.tuple_checker) {
    os << os.Indent() << "if (!" << Procedure(os, *(spec.tuple_checker)) << '(';
    sep = "";
    for (auto param : params) {
      os << sep << "param_" << param.Index();
      sep = ", ";
    }
    os << ")) {\n";

  // Check the tuple's state directly.
  } else {
    os << os.Indent() << "if (" << Table(os, spec.table) << ".GetState(";

    sep = "";
    for (auto param : params) {
      os << sep << "param_" << param.Index();
      sep = ", ";
    }
    os << ") != ::hyde::rt::TupleState::kPresent) {\n";
  }

  os.PushIndent();
  if (num_free_params) {
    os << os.Indent() << "continue;\n";
  } else {
    os << os.Indent() << "return num_generated;\n";
  }
  os.PopIndent();
  os << os.Indent() << "}\n";

  os << os.Indent() << "num_generated += 1u;\n";

  if (num_free_params) {
    os << os.Indent() << "if (!_generator(";
    sep = "";
    for (auto param : params) {
      if (param.Type().IsReferentiallyTransparent(module, Language::kCxx)) {
        os << sep << "param_" << param.Index();
      } else {
        os << sep << "*param_" << param.Index();
      }
      sep = ", ";
    }
    os << ")) {\n";
    os.PushIndent();
    os << os.Indent() << "return num_generated;\n";
    os.PopIndent();
    os << os.Indent() << "}\n";
  }

  os.PopIndent();
  os << os.Indent() << "}\n"
     << os.Indent() << "return num_generated;\n";
  os.PopIndent();
  os << os.Indent() << "}\n\n";
}

}  // namespace

// Emits C++ code for the given program to `os`.
void GenerateDatabaseCode(const Program &program, OutputStream &os) {
  const auto module = program.ParsedModule();
  const auto inlines = Inlines(module, Language::kCxx);

  std::string file_name = "datalog";
  std::string ns_name;
  std::string macro_name;
  if (const auto db_name = module.DatabaseName()) {
    ns_name = db_name->NamespaceName(Language::kCxx);
    file_name = db_name->FileName();
    for (auto ch : ns_name) {
      if (std::isalnum(ch)) {
        macro_name += ch;
      } else {
        macro_name += '_';
      }
    }
  } else {
    macro_name = gClassName;
  }

  os << "/* Auto-generated file */\n\n"
     << "#pragma once\n\n"
     << "#define DRLOJEKYLL_DATABASE_CODE\n\n"
     << "#include <drlojekyll/Runtime/Runtime.h>\n\n"
     << "#include \"" << file_name << "_generated.h\"\n"
     << "#include <algorithm>\n"
     << "#include <cstdio>\n"
     << "#include <cinttypes>\n"
     << "#include <optional>\n"
     << "#include <tuple>\n"
     << "#include <unordered_map>\n"
     << "#include <vector>\n\n";

  for (auto code : inlines) {
    if (code.Stage() == "c++:database:prologue") {
      os << code.CodeToInline() << "\n\n";
    }
  }

  if (!ns_name.empty()) {
    os << "namespace " << ns_name << " {\n";
  }

  for (auto code : inlines) {
    if (code.Stage() == "c++:database:prologue:namespace") {
      os << code.CodeToInline() << "\n\n";
    }
  }

  // Declare these up here so that prologue code can specialize them.
  DeclareFunctors(os, program, module, ns_name, inlines);
  DeclareMessageLog(os, program, module, inlines);

  if (!ns_name.empty()) {
    os << "}  // namespace " << ns_name << "\n\n";
  }

  os << "#ifndef __DRLOJEKYLL_SERIALIZER_CODE_" << macro_name << "\n"
     << "#  define __DRLOJEKYLL_SERIALIZER_CODE_" << macro_name << "\n";

  for (ParsedEnumType type : module.EnumTypes()) {
    os << "DRLOJEKYLL_MAKE_ENUM_SERIALIZER("
       << TypeName(module, type.Type()) << ", "
       << TypeName(module, type.UnderlyingType()) << ")\n";
  }

  os << "#endif  // __DRLOJEKYLL_SERIALIZER_CODE_" << macro_name << "\n\n";

  DeclareDescriptors(os, program, module);

  if (!ns_name.empty()) {
    os << "namespace " << ns_name << " {\n";
  }
  // A program gets its own class
  os << "template <typename StorageT, typename LogT=" << gClassName
     << "Log<StorageT>, typename FunctorsT=" << gClassName << "Functors<StorageT>>\n";
  os << "class " << gClassName << " {\n";
  os.PushIndent();  // class

  os << os.Indent() << "public:\n";
  os.PushIndent();  // public:

  os << os.Indent() << "StorageT &storage;\n"
     << os.Indent() << "LogT &log;\n"
     << os.Indent() << "FunctorsT &functors;\n"
     << "\n";

  for (auto table : program.Tables()) {
    os << os.Indent() << "::hyde::rt::Table<StorageT, " << table.Id() << "> "
       << Table(os, table) << ";\n";
  }

  for (auto global : program.GlobalVariables()) {
    DefineGlobal(os, module, global);
  }
  os << "\n";

  for (auto constant : program.Constants()) {
    DefineConstant(os, module, constant);
  }

  os << "\n"
     << os.Indent() << "explicit " << gClassName
     << "(StorageT &s, LogT &l, FunctorsT &f)\n";
  os.PushIndent();  // constructor
  os << os.Indent() << ": storage(s),\n"
     << os.Indent() << "  log(l),\n"
     << os.Indent() << "  functors(f)";

  for (auto table : program.Tables()) {
    os << ",\n" << os.Indent() << "  " << Table(os, table) << "(s)";
  }

  for (auto global : program.GlobalVariables()) {
    if (!global.IsConstant()) {
      os << ",\n"
         << os.Indent() << "  " << Var(os, global)
         << TypeValueOrDefault(module, global.Type(), global);
    }
  }
  for (auto constant : program.Constants()) {
    if (!CanInlineDefineConstant(module, constant)) {
      os << ",\n"
         << os.Indent() << "  " << Var(os, constant)
         << TypeValueOrDefault(module, constant.Type(), constant);
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

//  os.PopIndent();
//  os << os.Indent() << "private:\n";
//  os.PushIndent();

  //  for (auto table : program.Tables()) {
  //    DeclareTable(os, module, table);
  //  }

  os << "\n"
     << os.Indent() << "template <typename Printer>\n"
     << os.Indent() << "void DumpSizes(Printer _print) const {\n";
  os.PushIndent();

  for (auto table : program.Tables()) {
    os << os.Indent() << "_print(" << table.Id() << ", "
       << Table(os, table) << ".Size());\n";
  }

  os.PopIndent();
  os << os.Indent() << "}\n\n"
     << os.Indent() << "void DumpStats(void) const {\n";
  os.PushIndent();
  os << os.Indent() << "if constexpr (true) {\n";
  os.PushIndent();
  os << os.Indent() << "return;  /* change to false to enable */\n";
  os.PopIndent();
  os << os.Indent() << "}\n"
     << os.Indent() << "static FILE *tables = nullptr;\n"
     << os.Indent() << "if (!tables) {\n";
  os.PushIndent();
  os << os.Indent() << "tables = fopen(\"/tmp/tables.csv\", \"w\");\n"
     << os.Indent() << "fprintf(tables, \"";

  auto sep = "";
  for (auto table : program.Tables()) {
    os << sep << "table " << table.Id();
    sep = ",";
  }
  os << "\\n\");\n";
  os.PopIndent();
  os << os.Indent() << "}\n";
  os << os.Indent() << "fprintf(tables, \"";
  sep = "";
  for (auto table : program.Tables()) {
    (void) table;
    os << sep << "%\" PRIu64 \"";
    sep = ",";
  }
  os << "\\n\"";
  for (auto table : program.Tables()) {
    os << ", " << Table(os, table) << ".Size()";
  }
  os << ");\n";

  os.PopIndent();
  os << os.Indent() << "}\n\n";

  for (auto proc : program.Procedures()) {
    if (proc.Kind() != ProcedureKind::kMessageHandler) {
      DefineProcedure(os, module, proc);
    }
  }

  os.PopIndent();  // private:
  os.PopIndent();  // class:
  os << "};\n\n";

  for (auto code : inlines) {
    if (code.Stage() == "c++:database:epilogue:namespace") {
      os << code.CodeToInline() << "\n\n";
    }
  }

  if (!ns_name.empty()) {
    os << "}  // namespace " << ns_name << "\n\n";
  }

  for (auto code : inlines) {
    if (code.Stage() == "c++:database:epilogue") {
      os << code.CodeToInline() << "\n\n";
    }
  }
}

}  // namespace cxx
}  // namespace hyde
