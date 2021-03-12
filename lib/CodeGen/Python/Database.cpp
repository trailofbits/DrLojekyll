// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/CodeGen/CodeGen.h>
#include <drlojekyll/Parse/ModuleIterator.h>

#include <algorithm>
#include <sstream>
#include <unordered_set>
#include <vector>

#include "Util.h"

namespace hyde {
namespace {

static OutputStream &Functor(OutputStream &os, const ParsedFunctor func) {
  return os << "self._functors." << func.Name() << '_'
            << ParsedDeclaration(func).BindingPattern();
}

static OutputStream &Table(OutputStream &os, const DataTable table) {
  return os << "self.table_" << table.Id();
}

static OutputStream &TableIndex(OutputStream &os, const DataIndex index) {
  return os << "self.index_" << index.Id();
}

static OutputStream &Vector(OutputStream &os, const DataVector vec) {
  return os << "vec_" << vec.Id();
}

static OutputStream &VectorIndex(OutputStream &os, const DataVector vec) {
  return os << "vec_index" << vec.Id();
}

// Declare a set to hold the table.
static void DefineTable(OutputStream &os, ParsedModule module, DataTable table) {
  os << os.Indent() << Table(os, table) << ": DefaultDict[";
  auto sep = "";
  const auto cols = table.Columns();
  if (cols.size() == 1u) {
    os << TypeName(module, cols[0].Type());

  } else {
    os << "Tuple[";
    for (auto col : cols) {
      os << sep << TypeName(module, col.Type());
      sep = ", ";
    }
    os << "]";
  }

  os << ", int] = defaultdict(int)\n";

  // We represent indices as mappings to vectors so that we can concurrently
  // write to them while iterating over them (via an index and length check).
  for (auto index : table.Indices()) {
    os << os.Indent() << TableIndex(os, index);
    const auto key_cols = index.KeyColumns();
    const auto val_cols = index.ValueColumns();

    // The index can be implemented with the keys in the table's `defaultdict`.
    // In this case, the index lookup will be an `if ... in ...`.
    if (key_cols.size() == cols.size()) {
      assert(val_cols.empty());
      os << " = " << Table(os, table) << "\n";
      continue;
    }

    auto key_prefix = "Tuple[";
    auto key_suffix = "]";
    if (key_cols.size() == 1u) {
      key_prefix = "";
      key_suffix = "";
    }

    auto val_prefix = "Tuple[";
    auto val_suffix = "]";
    if (val_cols.size() == 1u) {
      val_prefix = "";
      val_suffix = "";
    }

    // If we have no value columns, then use a set for our index.
    if (val_cols.empty()) {
      os << ": Set[" << key_prefix;
      sep = "";
      for (auto col : index.KeyColumns()) {
        os << sep << TypeName(module, col.Type());
        sep = ", ";
      }
      os << key_suffix << "] = set()\n";

    // If we do have value columns, then use a defaultdict mapping to a
    // list.
    } else {
      os << ": DefaultDict[" << key_prefix;
      sep = "";
      for (auto col : index.KeyColumns()) {
        os << sep << TypeName(module, col.Type());
        sep = ", ";
      }
      os << key_suffix << ", List[" << val_prefix;
      sep = "";
      for (auto col : index.ValueColumns()) {
        os << sep << TypeName(module, col.Type());
        sep = ", ";
      }
      os << val_suffix << "]] = defaultdict(list)\n";
    }
  }
  os << "\n";
}

static void DefineGlobal(OutputStream &os, ParsedModule module,
                         DataVariable global) {
  auto type = global.Type();
  os << os.Indent() << Var(os, global);
  if (global.IsConstant()) {
    os << ": Final[" << TypeName(module, type) << "] = ";
  } else {
    os << ": " << TypeName(module, type) << " = ";
  }
  os << TypeValueOrDefault(module, type, global) << "\n\n";
}

// Similar to DefineGlobal except has type-hint to enforce const-ness
static void DefineConstant(OutputStream &os, ParsedModule module,
                           DataVariable global) {
  switch (global.DefiningRole()) {
    case VariableRole::kConstantZero:
    case VariableRole::kConstantOne:
    case VariableRole::kConstantFalse:
    case VariableRole::kConstantTrue:
      return;
    default:
      break;
  }
  auto type = global.Type();
  os << os.Indent() << Var(os, global) << ": " << TypeName(module, type)
     << " = " << TypeValueOrDefault(module, type, global) << "\n";
}

// We want to enable referential transparency in the code, so that if an Nth
// value is produced by some mechanism that is equal to some prior value, then
// we replace its usage with the prior value.
static void DefineTypeRefResolver(OutputStream &os, ParsedModule module,
                                  ParsedForeignType type) {
  if (type.IsBuiltIn()) {
    return;
  }
  os << os.Indent() << "_HAS_MERGE_METHOD_" << type.Name()
     << ": Final[bool] = hasattr(" << TypeName(type) << ", 'merge_into')\n"
     << os.Indent() << "_MERGE_METHOD_" << type.Name()
     << ": Final[Callable[[" << TypeName(type) << ", " << TypeName(type)
     << "], None]] = getattr(" << TypeName(type)
     << ", 'merge_into', lambda a, b: None)\n\n"
     << os.Indent() << "def _resolve_" << type.Name() << "(self, obj: "
     << TypeName(type) << ") -> " << TypeName(type) << ":\n";

  os.PushIndent();
  os << os.Indent() << "if " << gClassName << "._HAS_MERGE_METHOD_"
     << type.Name() << ":\n";
  os.PushIndent();

  os << os.Indent() << "ref_list = self._refs[hash(obj)]\n"
     << os.Indent() << "for maybe_obj in ref_list:\n";
  os.PushIndent();

  // The proposed object is identical (referentially) to the old one.
  os << os.Indent() << "if obj is maybe_obj:\n";
  os.PushIndent();
  os << os.Indent() << "return obj\n";
  os.PopIndent();

  // The proposed object is structurally equivalent to the old one.
  os << os.Indent() << "elif obj == maybe_obj:\n";
  os.PushIndent();
  os << os.Indent() << "prior_obj: " << TypeName(type)
     << " = cast(" << TypeName(type) << ", maybe_obj)\n";

  // Merge the new value `obj` into the prior value, `prior_obj`.
  os << os.Indent() << gClassName << "._MERGE_METHOD_"
     << type.Name() << "(obj, prior_obj)\n";
  os << os.Indent() << "return prior_obj\n";

  os.PopIndent();
  os.PopIndent();

  // We didn't find a prior version of the object; add our object in.
  os << os.Indent() << "ref_list.append(obj)\n";
  os.PopIndent();

  os << os.Indent() << "return obj\n\n";

  os.PopIndent();
}

class PythonCodeGenVisitor final : public ProgramVisitor {
 public:
  explicit PythonCodeGenVisitor(OutputStream &os_, ParsedModule module_)
      : os(os_),
        module(module_) {}

  void Visit(ProgramCallRegion region) override {
    os << Comment(os, region, "Program Call Region");

    auto param_index = 0u;
    const auto id = region.Id();

    const auto called_proc = region.CalledProcedure();
    const auto vec_params = called_proc.VectorParameters();
    const auto var_params = called_proc.VariableParameters();

    // Create the by-reference vector parameters, if any.
    for (auto vec : region.VectorArguments()) {
      const auto param = vec_params[param_index];
      if (param.Kind() == VectorKind::kInputOutputParameter) {
        os << os.Indent() << "param_" << region.Id() << '_' << param_index
           << " = [" << Vector(os, vec) << "]\n";
      }
      ++param_index;
    }

    const auto num_vec_params = param_index;

    // Create the by-reference variable parameters, if any.
    for (auto var : region.VariableArguments()) {
      const auto param = var_params[param_index - num_vec_params];
      if (param.DefiningRole() == VariableRole::kInputOutputParameter) {
        os << os.Indent() << "param_" << region.Id() << '_' << param_index
           << " = [" << Var(os, var) << "]\n";
      }
      ++param_index;
    }

    os << os.Indent() << "ret_" << id << ": bool = self."
       << Procedure(os, called_proc) << "(";

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

    os << ")\n";

    param_index = 0u;

    // Pull out the updated version of the referenced vectors.
    for (auto vec : region.VectorArguments()) {
      const auto param = vec_params[param_index];
      if (param.Kind() == VectorKind::kInputOutputParameter) {
        os << os.Indent() << Vector(os, vec) << " = param_" << region.Id()
           << '_' << param_index << "[0]\n";
      }
      ++param_index;
    }

    // Pull out the updated version of the referenced variables.
    for (auto var : region.VariableArguments()) {
      const auto param = var_params[param_index - num_vec_params];
      if (param.DefiningRole() == VariableRole::kInputOutputParameter) {
        os << os.Indent() << Var(os, var) << " = param_" << region.Id()
           << '_' << param_index << "[0]\n";
      }
      ++param_index;
    }

    if (auto true_body = region.BodyIfTrue(); true_body) {
      os << os.Indent() << "if ret_" << id << ":\n";
      os.PushIndent();
      true_body->Accept(*this);
      os.PopIndent();
    }

    if (auto false_body = region.BodyIfFalse(); false_body) {
      os << os.Indent() << "if not ret_" << id << ":\n";
      os.PushIndent();
      false_body->Accept(*this);
      os.PopIndent();
    }
  }

  void Visit(ProgramReturnRegion region) override {
    os << Comment(os, region, "Program Return Region");

    auto proc = ProgramProcedure::Containing(region);
    auto param_index = 0u;

    // Update any vectors in the caller by reference.
    for (auto vec : proc.VectorParameters()) {
      if (vec.Kind() == VectorKind::kInputOutputParameter) {
        os << os.Indent() << "param_" << param_index << "[0] = "
           << Vector(os, vec) << '\n';
      }
      ++param_index;
    }

    // Update any vectors in the caller by reference.
    for (auto var : proc.VariableParameters()) {
      if (var.DefiningRole() == VariableRole::kInputOutputParameter) {
        os << os.Indent() << "param_" << param_index << "[0] = "
           << Var(os, var) << '\n';
      }
      ++param_index;
    }

    os << os.Indent() << "return " << (region.ReturnsFalse() ? "False" : "True")
       << "\n";
  }

  void Visit(ProgramTestAndSetRegion region) override {
    os << Comment(os, region, "Program ExistenceAssertion Region");
    const auto vars = region.Operands();
    for (auto var : vars) {
      if (region.IsAdd()) {
        os << os.Indent() << Var(os, var) << " += 1\n";
      } else {
        os << os.Indent() << Var(os, var) << " -= 1\n";
      }
    }

    if (auto body = region.Body(); body) {
      assert(vars.size() == 1u);
      if (region.IsAdd()) {
        os << os.Indent() << "if " << Var(os, vars[0]) << " == 1:\n";
      } else {
        os << os.Indent() << "if " << Var(os, vars[0]) << " == 0:\n";
      }
      os.PushIndent();
      body->Accept(*this);
      os.PopIndent();
    }
  }

  void Visit(ProgramGenerateRegion region) override {
    const auto functor = region.Functor();
    const auto id = region.Id();
    os << Comment(os, region, "Program Generate Region");

    os << os.Indent() << "num_results_" << id << ": int = 0\n";

    switch (functor.Range()) {
      case FunctorRange::kZeroOrOne:
      case FunctorRange::kZeroOrMore:
        for (auto var : region.OutputVariables()) {
          os << os.Indent() << Var(os, var) << ": Optional["
             << TypeName(module, var.Type()) << "] = None\n";
        }
        break;
      default:
        break;
    }

    auto output_vars = region.OutputVariables();

    auto call_functor = [&] (void) {
      Functor(os, functor) << "(";
      auto sep = "";
      for (auto in_var : region.InputVariables()) {
        os << sep << Var(os, in_var);
        sep = ", ";
      }
      os << ")";
    };

    auto do_body = [&] (void) {
      os << os.Indent() << "num_results_" << id << " += 1\n";
      if (auto body = region.BodyIfResults(); body) {
        body->Accept(*this);

      // Break out of the body early if there is nothing to do, and if we've
      // already counted at least one instance of results (in the case of the
      // functor possibly producing more than one result tuples), then that
      // is sufficient information to be able to enter into the "empty" body.
      } else if (const auto range = functor.Range();
                 FunctorRange::kOneOrMore == range ||
                 FunctorRange::kZeroOrMore == range) {
        os << os.Indent() << "break\n";
      }
    };

    switch (const auto range = functor.Range()) {
      // These behave like iterators.
      case FunctorRange::kOneOrMore:
      case FunctorRange::kZeroOrMore: {
        if (output_vars.size() == 1u) {
          os << os.Indent() << "for " << Var(os, output_vars[0]) << " in ";
          call_functor();
          os << ":\n";
          os.PushIndent();
          do_body();
          os.PopIndent();

        } else {
          assert(!output_vars.empty());

          os << os.Indent() << "tmp_" << id;
          auto sep = ": Tuple[";
          for (auto out_var : output_vars) {
            os << sep << TypeName(module, out_var.Type());
            sep = ", ";
          }
          os << "]\n"
             << os.Indent() << "for tmp_" << id << " in ";
          call_functor();
          os << ":\n";
          os.PushIndent();
          auto out_var_index = 0u;
          for (auto out_var : output_vars) {
            os << os.Indent() << Var(os, out_var) << " = tmp_" << id
               << '[' << (out_var_index++) << "]\n";
          }
          do_body();
          os.PopIndent();
        }

        break;
      }

      // These behave like returns of tuples/values/optionals.
      case FunctorRange::kOneToOne:
      case FunctorRange::kZeroOrOne:
        // Only takes bound inputs, acts as a filter functor.
        if (output_vars.empty()) {
          assert(functor.IsFilter());

          os << os.Indent() << "if ";
          call_functor();
          os << ":\n";
          os.PushIndent();
          do_body();
          os.PopIndent();

        // Produces a single value. This returns an `Optional` value.
        } else if (output_vars.size() == 1u) {
          assert(!functor.IsFilter());

          const auto out_var = output_vars[0];
          os << os.Indent() << "tmp_" << id << ": ";
          if (range == FunctorRange::kZeroOrOne) {
            os << "Optional[";
          }
          os << TypeName(module, out_var.Type());
          if (range == FunctorRange::kZeroOrOne) {
            os << ']';
          }
          os << " = ";
          call_functor();
          os << "\n";
          if (range == FunctorRange::kZeroOrOne) {
            os << os.Indent() << "if tmp_" << id << " is not None:\n";
            os.PushIndent();
          }
          os << os.Indent() << Var(os, out_var) << " = tmp_" << region.Id()
             << '\n';
          do_body();
          if (range == FunctorRange::kZeroOrOne) {
            os.PopIndent();
          }

        // Produces a tuple of values.
        } else {
          assert(!functor.IsFilter());

          os << os.Indent() << "tmp_" << region.Id();
          auto sep = ": Optional[Tuple[";
          for (auto out_var : output_vars) {
            os << sep << TypeName(module, out_var.Type());
            sep = ", ";
          }
          os << "]] = ";
          call_functor();
          os << "\n"
             << os.Indent() << "if tmp_" << id << " is not None:\n";
          os.PushIndent();
          auto out_var_index = 0u;
          for (auto out_var : output_vars) {
            os << os.Indent() << Var(os, out_var) << " = tmp_" << id
               << '[' << (out_var_index++) << "]\n";
          }
          do_body();
          os.PopIndent();
        }
        break;
    }

    if (auto empty_body = region.BodyIfEmpty(); empty_body) {
      os << os.Indent() << "if not num_results" << id << ":\n";
      os.PushIndent();
      empty_body->Accept(*this);
      os.PopIndent();
    }
  }

  void Visit(ProgramInductionRegion region) override {
    os << Comment(os, region, "Program Induction Init Region");

    // Base case
    region.Initializer().Accept(*this);

    // Fixpoint
    os << Comment(os, region, "Induction Fixpoint Loop Region");
    os << os.Indent() << "while ";
    auto sep = "";
    for (auto vec : region.Vectors()) {
      os << sep << "len(" << Vector(os, vec) << ")";
      sep = " or ";
    }
    os << ":\n";

    os.PushIndent();
    region.FixpointLoop().Accept(*this);
    os.PopIndent();

    // Output
    if (auto output = region.Output(); output) {
      for (auto vec : region.Vectors()) {
        os << os.Indent() << VectorIndex(os, vec) << " = 0\n";
      }
      os << Comment(os, region, "Induction Output Region");
      output->Accept(*this);
    }
  }

  void Visit(ProgramLetBindingRegion region) override {
    os << Comment(os, region, "Program LetBinding Region");
    auto i = 0u;
    const auto used_vars = region.UsedVariables();
    for (auto var : region.DefinedVariables()) {
      os << os.Indent() << Var(os, var) << ": "
         << TypeName(module, var.Type()) << " = "
         << Var(os, used_vars[i++]) << '\n';
    }

    if (auto body = region.Body(); body) {
      body->Accept(*this);
    } else {
      os << os.Indent() << "pass\n";
    }
  }

  void Visit(ProgramParallelRegion region) override {
    os << Comment(os, region, "Program Parallel Region");

    // Same as SeriesRegion
    auto any = false;
    for (auto sub_region : region.Regions()) {
      sub_region.Accept(*this);
      any = true;
    }

    if (!any) {
      os << os.Indent() << "pass\n";
    }
  }

  // Should never be reached; defined below.
  void Visit(ProgramProcedure region) override {
    assert(false);
  }

  void Visit(ProgramPublishRegion region) override {
    os << Comment(os, region, "Program Publish Region");
    auto message = region.Message();
    os << os.Indent() << "self._log." << message.Name() << '_'
       << message.Arity();

    auto sep = "(";
    for (auto var : region.VariableArguments()) {
      os << sep << Var(os, var);
      sep = ", ";
    }

    if (region.IsRemoval()) {
      os << sep << "False";
    } else {
      os << sep << "True";
    }

    os << ")\n";
  }

  void Visit(ProgramSeriesRegion region) override {
    os << Comment(os, region, "Program Series Region");

    auto any = false;
    for (auto sub_region : region.Regions()) {
      sub_region.Accept(*this);
      any = true;
    }

    if (!any) {
      os << os.Indent() << "pass\n";
    }
  }

  void Visit(ProgramVectorAppendRegion region) override {
    os << Comment(os, region, "Program VectorAppend Region");

    const auto tuple_vars = region.TupleVariables();

    // Make sure to resolve to the correct reference of the foreign object.
    switch (region.Usage()) {
      case VectorUsage::kInductionVector:
      case VectorUsage::kJoinPivots:
        ResolveReferences(tuple_vars);
        break;
      default:
        break;
    }

    os << os.Indent() << Vector(os, region.Vector()) << ".append(";
    if (tuple_vars.size() == 1u) {
      os << Var(os, tuple_vars[0]);

    } else {
      os << '(';
      auto sep = "";
      for (auto var : tuple_vars) {
        os << sep << Var(os, var);
        sep = ", ";
      }
      os << ')';
    }
    os << ")\n";
  }

  void Visit(ProgramVectorClearRegion region) override {
    os << Comment(os, region, "Program VectorClear Region");

    os << os.Indent() << "del " << Vector(os, region.Vector()) << "[:]\n";
    os << os.Indent() << VectorIndex(os, region.Vector()) << " = 0\n";
  }

  void Visit(ProgramVectorSwapRegion region) override {
    os << Comment(os, region, "Program VectorSwap Region");

    os << os.Indent() << Vector(os, region.LHS()) << ", "
       << Vector(os, region.RHS()) << " = " << Vector(os, region.RHS())
       << ", " << Vector(os, region.LHS()) << '\n';
  }

  void Visit(ProgramVectorLoopRegion region) override {
    os << Comment(os, region, "Program VectorLoop Region");
    auto vec = region.Vector();
    if (region.Usage() != VectorUsage::kInductionVector) {
      os << os.Indent() << VectorIndex(os, vec) << " = 0\n";
    }
    os << os.Indent() << "while " << VectorIndex(os, vec) << " < len("
       << Vector(os, vec) << "):\n";
    os.PushIndent();
    os << os.Indent();

    const auto tuple_vars = region.TupleVariables();
    if (tuple_vars.size() == 1u) {
      os << Var(os, tuple_vars[0]);

    } else {
      auto sep = "";
      for (auto var : tuple_vars) {
        os << sep << Var(os, var);
        sep = ", ";
      }
    }
    os << " = " << Vector(os, vec) << "[" << VectorIndex(os, vec) << "]\n";

    os << os.Indent() << VectorIndex(os, vec) << " += 1\n";

    if (auto body = region.Body(); body) {
      body->Accept(*this);
    }
    os.PopIndent();
  }

  void Visit(ProgramVectorUniqueRegion region) override {
    os << Comment(os, region, "Program VectorUnique Region");

    os << os.Indent() << Vector(os, region.Vector()) << " = list(set("
       << Vector(os, region.Vector()) << "))\n";
    os << os.Indent() << VectorIndex(os, region.Vector()) << " = 0\n";
  }

  void ResolveReference(DataVariable var) {
    if (auto foreign_type = module.ForeignType(var.Type()); foreign_type) {
      if (!foreign_type->IsReferentiallyTransparent(Language::kPython)) {
        os << os.Indent() << Var(os, var)
           << " = self._resolve_" << foreign_type->Name()
           << '(' << Var(os, var) << ")\n";
      } else {
        switch (var.DefiningRole()) {
          case VariableRole::kConditionRefCount:
          case VariableRole::kConstant:
          case VariableRole::kConstantZero:
          case VariableRole::kConstantOne:
          case VariableRole::kConstantFalse:
          case VariableRole::kConstantTrue:
            break;
          default:
            assert(false);
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
    os << Comment(os, region, "Program TransitionState Region");

    const auto tuple_vars = region.TupleVariables();

    // Make sure to resolve to the correct reference of the foreign object.
    ResolveReferences(tuple_vars);

    std::stringstream tuple;
    tuple << "tuple";
    for (auto tuple_var : tuple_vars) {
      tuple << "_" << tuple_var.Id();
    }

    auto tuple_prefix = "(";
    auto tuple_suffix = ")";
    if (tuple_vars.size() == 1u) {
      tuple_prefix = "";
      tuple_suffix = "";
    }

    auto sep = "";
    auto tuple_var = tuple.str();
    os << os.Indent() << tuple_var << " = " << tuple_prefix;
    for (auto var : tuple_vars) {
      os << sep << Var(os, var);
      sep = ", ";
    }
    os << tuple_suffix << "\n"
       << os.Indent() << "prev_state = " << Table(os, region.Table())
       << "[" << tuple_var << "]\n"
       << os.Indent() << "state = prev_state & " << kStateMask << "\n"
       << os.Indent() << "present_bit = prev_state & "
       << kPresentBit << "\n";

    os << os.Indent() << "if ";
    switch (region.FromState()) {
      case TupleState::kAbsent:
        os << "state == " << kStateAbsent << ":\n";
        break;
      case TupleState::kPresent:
        os << "state == " << kStatePresent << ":\n";
        break;
      case TupleState::kUnknown:
        os << "state == " << kStateUnknown << ":\n";
        break;
      case TupleState::kAbsentOrUnknown:
        os << "state == " << kStateAbsent
           << " or state == " << kStateUnknown << ":\n";
        break;
    }
    os.PushIndent();
    os << os.Indent() << Table(os, region.Table()) << "[" << tuple_var
       << "] = ";

    switch (region.ToState()) {
      case TupleState::kAbsent:
        os << kStateAbsent << " | " << kPresentBit << "\n"; break;
      case TupleState::kPresent:
        os << kStatePresent << " | " << kPresentBit << "\n"; break;
      case TupleState::kUnknown:
        os << kStateUnknown << " | " << kPresentBit << "\n"; break;
      case TupleState::kAbsentOrUnknown:
        os << kStateUnknown << " | " << kPresentBit << "\n";
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
      os << os.Indent() << "if not present_bit:\n";
      os.PushIndent();

      auto has_indices = false;
      for (auto index : indices) {
        const auto key_cols = index.KeyColumns();
        const auto val_cols = index.ValueColumns();

        auto key_prefix = "(";
        auto key_suffix = ")";
        auto val_prefix = "(";
        auto val_suffix = ")";

        if (key_cols.size() == 1u) {
          key_prefix = "";
          key_suffix = "";
        }

        if (val_cols.size() == 1u) {
          val_prefix = "";
          val_suffix = "";
        }

        // The index is the set of keys in the table's `defaultdict`. Thus, we
        // don't need to add anything because adding to the table will have done
        // it.
        if (tuple_vars.size() == key_cols.size()) {
          continue;
        }

        has_indices = true;
        os << os.Indent() << TableIndex(os, index);

        // The index is implemented with a `set`.
        if (val_cols.empty()) {
          os << ".add(";
          sep = "";
          if (key_cols.size() == 1u) {
            os << tuple_var;
          } else {
            os << '(';
            for (auto indexed_col : index.KeyColumns()) {
              os << sep << tuple_var << "[" << indexed_col.Index() << "]";
              sep = ", ";
            }
            os << ')';
          }

          os << ")\n";

        // The index is implemented with a `defaultdict`.
        } else {
          os << "[" << key_prefix;
          sep = "";
          for (auto indexed_col : index.KeyColumns()) {
            os << sep << tuple_var << "[" << indexed_col.Index() << "]";
            sep = ", ";
          }
          os << key_suffix << "].append(" << val_prefix;
          sep = "";
          for (auto mapped_col : index.ValueColumns()) {
            os << sep << tuple_var << "[" << mapped_col.Index() << "]";
            sep = ", ";
          }
          os << val_suffix << ")\n";
        }
      }

      if (!has_indices) {
        os << os.Indent() << "pass\n";
      }

      os.PopIndent();
    }

    if (auto body = region.Body(); body) {
      body->Accept(*this);
    }
    os.PopIndent();
  }

  void Visit(ProgramCheckStateRegion region) override {
    os << Comment(os, region, "Program CheckState Region");
    const auto table = region.Table();
    const auto vars = region.TupleVariables();
    os << os.Indent() << "state = " << Table(os, table) << "[";
    if (vars.size() == 1u) {
      os << Var(os, vars[0]);
    } else {
      auto sep = "(";
      for (auto var : vars) {
        os << sep << Var(os, var);
        sep = ", ";
      }
      os << ')';
    }
    os << "] & " << kStateMask << '\n';

    auto sep = "if ";

    if (auto absent_body = region.IfAbsent(); absent_body) {
      os << os.Indent() << sep << "state == " << kStateAbsent << ":\n";
      os.PushIndent();
      absent_body->Accept(*this);
      os.PopIndent();
      sep = "elif ";
    }

    if (auto present_body = region.IfPresent(); present_body) {
      os << os.Indent() << sep << "state == " << kStatePresent << ":\n";
      os.PushIndent();
      present_body->Accept(*this);
      os.PopIndent();
      sep = "elif ";
    }

    if (auto unknown_body = region.IfUnknown(); unknown_body) {
      os << os.Indent() << sep << "state == " << kStateUnknown << ":\n";
      os.PushIndent();
      unknown_body->Accept(*this);
      os.PopIndent();
    }
  }

  void Visit(ProgramTableJoinRegion region) override {
    const auto id = region.Id();
    os << Comment(os, region, "Program TableJoin Region");

    // Nested loop join
    auto vec = region.PivotVector();
    os << os.Indent() << "while " << VectorIndex(os, vec) << " < len("
       << Vector(os, vec) << "):\n";
    os.PushIndent();
    os << os.Indent();

    std::vector<std::string> var_names;
    auto sep = "";
    for (auto var : region.OutputPivotVariables()) {
      std::stringstream var_name;
      (void) Var(var_name, var);
      var_names.emplace_back(var_name.str());
      os << sep << var_names.back();
      sep = ", ";
    }
    os << " = " << Vector(os, vec) << "[" << VectorIndex(os, vec) << "]\n";

    os << os.Indent() << VectorIndex(os, vec) << " += 1\n";

    auto tables = region.Tables();
    for (auto i = 0u; i < tables.size(); ++i) {
      const auto table = tables[i];
      const auto index = region.Index(i);
      const auto index_keys = index.KeyColumns();
      const auto index_vals = index.ValueColumns();

      (void) table;

      auto key_prefix = "(";
      auto key_suffix = ")";
      if (index_keys.size() == 1u) {
        key_prefix = "";
        key_suffix = "";
      }

      // The index is a set of key column values/tuples.
      if (index_vals.empty()) {


        os << os.Indent() << "key_" << id << '_' << i << " = "
           << key_prefix;

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

        os << key_suffix << "\n";

        os << os.Indent() << "if key_" << id << '_' << i
           << " in " << TableIndex(os, index);

        // The index aliases the underlying table; lets double check that the
        // state isn't `absent`.
        assert(index.KeyColumns().size() == table.Columns().size());
        os << " and (" << TableIndex(os, index) << "[key_" << id << '_' << i
           << "] & " << kStateMask << ") != " << kStateAbsent << ":\n";

        // We increase indentation here, and the corresponding `PopIndent()`
        // only comes *after* visiting the `region.Body()`.
        os.PushIndent();

      // The index is a default dict mapping key columns to a list of value
      // columns/tuples.
      } else {

        // We don't want to have to make a temporary copy of the current state
        // of the index, so instead what we do is we capture a reference to the
        // list of tuples in the index, and we also create an index variable
        // that tracks which tuple we can next look at. This allows us to
        // observe writes into the index as they happen.
        os << os.Indent() << "tuple_" << region.Id() << "_" << i
           << "_index: int = 0\n"
           << os.Indent() << "tuple_" << region.Id() << "_" << i
           << "_vec: List[";

        if (1u < index_vals.size()) {
          os << "Tuple[";
        }

        auto sep = "";
        for (auto col : index_vals) {
          os << sep << TypeName(module, col.Type());
          sep = ", ";
        }
        if (1u < index_vals.size()) {
          os << "]";
        }
        os << "] = " << TableIndex(os, index) << "[" << key_prefix;

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

        os << key_suffix << "]\n";

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
      }

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
             << i;

          if (1u < out_vars.size()) {
            os << "[" << select_col_idx - tuple_col_idx_offset << "]";
          }
          os << '\n';

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
    os << Comment(os, region, "Program TableProduct Region");

    os << os.Indent() << "vec_" << region.Id();

    auto i = 0u;
    auto sep = ": List[Tuple[";
    for (auto table : region.Tables()) {
      (void) table;
      for (auto var : region.OutputVariables(i++)) {
        os << sep << TypeName(module, var.Type());
        sep = ", ";
      }
    }

    os << "]] = []\n";

    i = 0u;
    // Products work by having tables and vectors for each proposer. We want
    // to take the product of each proposer's vector against all other tables.
    // The outer loop deals with the vectors.
    for (auto outer_table : region.Tables()) {
      const auto outer_vars = region.OutputVariables(i);
      const auto outer_vec = region.Vector(i++);
      (void) outer_table;

      os << os.Indent();
      sep = "for ";

      for (auto var : outer_vars) {
        os << sep << Var(os, var);
        sep = ", ";
      }

      os << " in " << Vector(os, outer_vec) << ":\n";
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

        os << os.Indent();
        sep = "for ";
        for (auto var : inner_vars) {
          os << sep << Var(os, var);
          sep = ", ";
        }
        os << " in " << Table(os, inner_table) << ":\n";
        os.PushIndent();
        ++indents;
      }

      // Collect all product things into a vector.
      os << os.Indent() << "vec_" << region.Id();
      sep = ".append((";
      auto k = 0u;
      for (auto table : region.Tables()) {
        (void) table;
        for (auto var : region.OutputVariables(k++)) {
          os << sep << Var(os, var);
          sep = ", ";
        }
      }
      os << "))\n";

      // De-dent everything.
      for (auto outer_vec : region.Tables()) {
        os.PopIndent();
        assert(0u < indents);
        indents--;
        (void) outer_vec;
      }
    }

    os << os.Indent();
    sep = "for ";
    auto k = 0u;
    for (auto table : region.Tables()) {
      (void) table;
      for (auto var : region.OutputVariables(k++)) {
        os << sep << Var(os, var);
        sep = ", ";
      }
    }

    os << " in vec_" << region.Id() << ":\n";
    os.PushIndent();
    if (auto body = region.Body(); body) {
      body->Accept(*this);
    } else {
      os << os.Indent() << "pass\n";
    }
    os.PopIndent();
  }

  void Visit(ProgramTableScanRegion region) override {
    os << Comment(os, region, "Program TableScan Region");

    const auto input_vars = region.InputVariables();
    const auto output_cols = region.SelectedColumns();
    const auto filled_vec = region.FilledVector();

    // Make sure to resolve to the correct reference of the foreign object.
    ResolveReferences(input_vars);

    os << os.Indent() << "scan_tuple_" << filled_vec.Id()
       << ": ";

    if (output_cols.size() == 1u) {
      os << TypeName(module, output_cols[0].Type()) << '\n';
    } else {
      os << "Tuple[";
      auto sep = "";
      for (auto col : output_cols) {
        os << sep << TypeName(module, col.Type());
        sep = ", ";
      }
      os << "]\n";
    }

    // TODO(pag): Do we need to watch out for the index aliasing the key space
    //            of the table, and having some columns in the absent state?

    // Index scan :-D
    if (auto maybe_index = region.Index(); maybe_index) {
      const auto index = *maybe_index;

      os << os.Indent() << "scan_index_" << filled_vec.Id()
         << ": int = 0\n"
         << os.Indent() << "scan_tuple_" << filled_vec.Id() << "_vec: List[";

      // Types mapped by this index.
      if (output_cols.size() == 1u) {
        os << TypeName(module, output_cols[0].Type());
      } else {
        os << "Tuple[";
        auto sep = "";
        for (auto col : output_cols) {
          os << sep << TypeName(module, col.Type());
          sep = ", ";
        }
        os << "]";
      }

      os << "] = " << TableIndex(os, index) << "[";
      auto sep = "";
      for (auto var : input_vars) {
        os << sep << Var(os, var);
        sep = ", ";
      }
      os << "]\n";
      os << os.Indent() << "while scan_index_" << filled_vec.Id()
         << " < len(scan_tuple_" << filled_vec.Id() << "_vec):\n";
      os.PushIndent();

      os << os.Indent() << "scan_tuple_" << filled_vec.Id()
         << " = scan_tuple_" << filled_vec.Id() << "_vec["
         << "scan_index_" << filled_vec.Id() << "]\n"
         << os.Indent() << "scan_index_" << filled_vec.Id()
         << " += 1\n";

    // Full table scan.
    } else {
      assert(input_vars.empty());
      os << os.Indent() << "for scan_tuple_" << filled_vec.Id()
         << " in " << Table(os, region.Table()) << ":\n";
      os.PushIndent();
    }

    os << os.Indent() << Vector(os, filled_vec)
       << ".append(scan_tuple_" << filled_vec.Id() << ")\n";
    os.PopIndent();
  }

  void Visit(ProgramTupleCompareRegion region) override {
    os << Comment(os, region, "Program TupleCompare Region");

    const auto lhs_vars = region.LHS();
    const auto rhs_vars = region.RHS();

    if (lhs_vars.size() == 1u) {
      os << os.Indent() << "if " << Var(os, lhs_vars[0]) << ' '
         << OperatorString(region.Operator())
         << ' ' << Var(os, rhs_vars[0]) << ":\n";

    } else {
      os << os.Indent() << "if (";
      for (auto lhs : region.LHS()) {
        os << Var(os, lhs) << ", ";
      }

      os << ") " << OperatorString(region.Operator()) << " (";

      for (auto rhs : region.RHS()) {
        os << Var(os, rhs) << ", ";
      }
      os << "):\n";
    }

    os.PushIndent();
    if (auto body = region.Body(); body) {
      body->Accept(*this);
    } else {
      os << os.Indent() << "pass";
    }
    os.PopIndent();
  }

 private:
  OutputStream &os;
  const ParsedModule module;
};

static void DeclareFunctor(OutputStream &os, ParsedModule module,
                          ParsedFunctor func) {
  os << os.Indent() << "def " << func.Name() << '_'
     << ParsedDeclaration(func).BindingPattern() << "(self";

  std::stringstream return_tuple;
  auto sep_ret = "";
  auto num_ret_types = 0u;
  for (auto param : func.Parameters()) {
    if (param.Binding() == ParameterBinding::kBound) {
      os << ", " << param.Name() << ": "
         << TypeName(module, param.Type().Kind());
    } else {
      ++num_ret_types;
      return_tuple << sep_ret << TypeName(module, param.Type().Kind());
      sep_ret = ", ";
    }
  }

  os << ") -> ";

  if (func.IsFilter()) {
    assert(func.Range() == FunctorRange::kZeroOrOne);
    os << "bool";

  } else {
    auto tuple_prefix = "";
    auto tuple_suffix = "";
    if (1u < num_ret_types) {
      tuple_prefix = "Tuple[";
      tuple_suffix = "]";
    } else {
      assert(0u < num_ret_types);
    }

    switch (func.Range()) {
      case FunctorRange::kOneOrMore:
      case FunctorRange::kZeroOrMore:
        os << "Iterator[" << tuple_prefix << return_tuple.str()
           << tuple_suffix << "]";
        break;
      case FunctorRange::kOneToOne:
        os << tuple_prefix << return_tuple.str()<< tuple_suffix;
        break;
      case FunctorRange::kZeroOrOne:
        os << "Optional[" << tuple_prefix << return_tuple.str()
           << tuple_suffix << "]";
    }
  }

  os << ":\n";

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
  os << ")  # type: ignore\n\n";
  os.PopIndent();
}


static void DeclareFunctors(OutputStream &os, Program program,
                            ParsedModule root_module) {
  os << os.Indent() << "class " << gClassName << "Functors:\n";
  os.PushIndent();

  std::unordered_set<std::string> seen;

  auto has_functors = false;
  for (auto module : ParsedModuleIterator(root_module)) {
    for (auto first_func : module.Functors()) {
      for (auto func : first_func.Redeclarations()) {
        std::stringstream ss;
        ss << func.Id() << ':'
           << ParsedDeclaration(func).BindingPattern();
        if (auto [it, inserted] = seen.emplace(ss.str()); inserted) {
          DeclareFunctor(os, module, func);
          has_functors = true;
          (void) it;
        }
      }
    }
  }

  if (!has_functors) {
    os << os.Indent() << "pass\n";
  }
  os.PopIndent();
}

static void DeclareMessageLogger(OutputStream &os, ParsedModule module,
                                 ParsedMessage message, const char *impl) {
  os << os.Indent() << "def " << message.Name() << "_" << message.Arity()
     << "(self";

  for (auto param : message.Parameters()) {
    os << ", " << param.Name() << ": " << TypeName(module, param.Type());
  }

  os << ", added: bool):\n";
  os.PushIndent();
  os << os.Indent() << impl << "\n\n";
  os.PopIndent();
}

static void DeclareMessageLog(OutputStream &os, Program program,
                              ParsedModule root_module) {
  os << os.Indent() << "class " << gClassName << "LogInterface(Protocol):\n";
  os.PushIndent();

  const auto messages = Messages(root_module);

  if (messages.empty()) {
    os << os.Indent() << "pass\n\n";
  } else {
    for (auto message : messages) {
      DeclareMessageLogger(os, root_module, message, "...");
    }
  }
  os.PopIndent();

  os << '\n';
  os << os.Indent() << "class " << gClassName << "Log:\n";
  os.PushIndent();

  if (messages.empty()) {
    os << os.Indent() << "pass\n\n";
  } else {
    for (auto message : messages) {
      DeclareMessageLogger(os, root_module, message, "pass");
    }
  }
  os.PopIndent();
}

static void DefineProcedure(OutputStream &os, ParsedModule module,
                            ProgramProcedure proc) {
  os << os.Indent() << "def " << Procedure(os, proc) << "(self";

  const auto vec_params = proc.VectorParameters();
  const auto var_params = proc.VariableParameters();
  auto param_index = 0u;

  // First, declare all vector parameters.
  for (auto vec : vec_params) {
    const auto is_byref = vec.Kind() == VectorKind::kInputOutputParameter;
    os << ", ";
    if (is_byref) {
      os << "param_" << param_index << ": List[";
    } else {
      os << Vector(os, vec) << ": ";
    }
    os << "List[";
    const auto &col_types = vec.ColumnTypes();
    if (1u < col_types.size()) {
      os << "Tuple[";
    }
    auto type_sep = "";
    for (auto type : col_types) {
      os << type_sep << TypeName(module, type);
      type_sep = ", ";
    }
    if (1u < col_types.size()) {
      os << ']';
    }
    if (is_byref) {
      os << ']';
    }
    os << ']';
    ++param_index;
  }

  // Then, declare all variable parameters.
  for (auto param : var_params) {
    if (param.DefiningRole() == VariableRole::kInputOutputParameter) {
      os << ", param_" << param_index << ": List["
         << TypeName(module, param.Type()) << "]";
    } else {
      os << ", " << Var(os, param) << ": " << TypeName(module, param.Type());
    }
    ++param_index;
  }

  // Every procedure has a boolean return type. A lot of the time the return
  // type is not used, but for top-down checkers (which try to prove whether or
  // not a tuple in an unknown state is either present or absent) it is used.
  os << ") -> bool:\n";

  os.PushIndent();
  os << os.Indent() << "state: int = " << kStateUnknown << '\n'
     << os.Indent() << "prev_state: int = " << kStateUnknown << '\n'
     << os.Indent() << "present_bit: int = 0\n"
     << os.Indent() << "ret: bool = False\n"
     << os.Indent() << "found: bool = False\n";

  param_index = 0u;

  // Pull out the referenced vectors.
  for (auto vec : vec_params) {
    if (vec.Kind() == VectorKind::kInputOutputParameter) {
      os << os.Indent() << Vector(os, vec) << " = param_"
         << param_index << "[0]\n";
    }
    ++param_index;
  }

  // Pull out the referenced variables.
  for (auto var : var_params) {
    if (var.DefiningRole() == VariableRole::kInputOutputParameter) {
      os << os.Indent() << Var(os, var) << " = param_"
         << param_index << "[0]\n";
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
    os << os.Indent() << VectorIndex(os, vec) << ": int = 0\n";
  }

  // Define the vectors that will be created and used within this procedure.
  // These vectors exist to support inductions, joins (pivot vectors), etc.
  for (auto vec : proc.DefinedVectors()) {
    os << os.Indent() << Vector(os, vec) << ": List[";

    const auto &col_types = vec.ColumnTypes();
    if (1u < col_types.size()) {
      os << "Tuple[";
    }

    auto type_sep = "";
    for (auto type : col_types) {
      os << type_sep << TypeName(module, type);
      type_sep = ", ";
    }

    if (1u < col_types.size()) {
      os << "]";
    }

    os << "] = list()\n";

    // Tracking variable for the vector.
    os << os.Indent() << VectorIndex(os, vec) << ": int = 0\n";
  }

  // Visit the body of the procedure. Procedure bodies are never empty; the
  // most trivial procedure body contains a `return False`.
  PythonCodeGenVisitor visitor(os, module);
  proc.Body().Accept(visitor);

  os.PopIndent();
  os << '\n';
}

static void DefineQueryEntryPoint(OutputStream &os, ParsedModule module,
                                  const ProgramQuery &spec) {
  const ParsedDeclaration decl(spec.query);
  os << os.Indent() << "def " << decl.Name() << '_' << decl.BindingPattern()
     << "(self";

  auto num_bound_params = 0u;
  auto num_free_params = 0u;
  const auto params = decl.Parameters();
  const auto num_params = decl.Arity();
  for (auto param : params) {
    if (param.Binding() == ParameterBinding::kBound) {
      os << ", param_" << param.Index() << ": "
         << TypeName(module, param.Type());
      ++num_bound_params;
    } else {
      ++num_free_params;
    }
  }

  assert(num_params == (num_bound_params + num_free_params));

  if (num_free_params) {
    os << ") -> Iterator[";
    if (1u < num_free_params) {
      os << "Tuple[";
    }
    auto sep = "";
    for (auto param : params) {
      if (param.Binding() != ParameterBinding::kBound) {
        os << sep << TypeName(module, param.Type());
        sep = ", ";
      }
    }
    if (1u < num_free_params) {
      os << ']';
    }
    os << "]:\n";
  } else {
    os << ") -> bool:\n";
  }
  os.PushIndent();
  os << os.Indent() << "state: int = 0\n";

  if (spec.forcing_function) {
    os << os.Indent() << "self." << Procedure(os, *(spec.forcing_function))
       << '(';
    auto sep = "";
    for (auto param : params) {
      if (param.Binding() == ParameterBinding::kBound) {
        os << sep << "param_" << param.Index();
        sep = ", ";
      }
    }
    os << ")\n";
  }

  os << os.Indent() << "tuple_index: int = 0\n";

  // This is an index scan.
  if (num_bound_params && num_bound_params < num_params) {
    assert(spec.index.has_value());
    const auto index = *(spec.index);
    const auto index_vals = index.ValueColumns();
    auto key_prefix = "(";
    auto key_suffix = ")";

    if (num_free_params == 1u) {
      key_prefix = "";
      key_suffix = "";
    }

    os << os.Indent() << "tuple_vec: List[";

    if (1u < num_free_params) {
      os << "Tuple[";
    }

    auto sep = "";
    for (auto col : index_vals) {
      os << sep << TypeName(module, col.Type());
      sep = ", ";
    }
    if (1u < index_vals.size()) {
      os << "]";
    }
    os << "] = " << TableIndex(os, index) << "[" << key_prefix;

    sep = "";
    for (auto param : decl.Parameters()) {
      if (param.Binding() == ParameterBinding::kBound) {
        os << sep << "param_" << param.Index();
        sep = ", ";
      }
    }

    os << key_suffix << "]\n";

    os << os.Indent() << "while tuple_index < len(tuple_vec):\n";
    os.PushIndent();
    os << os.Indent() << "tuple = tuple_vec[tuple_index]\n"
       << os.Indent() << "tuple_index += 1\n";

  // This is a full table scan.
  } else if (num_free_params) {
    assert(0u < num_free_params);

    os << os.Indent() << "for tuple in " << Table(os, spec.table)
       << ":\n";
    os.PushIndent();
    os << os.Indent() << "tuple_index += 1\n";

  // Either the tuple checker will figure out of the tuple is present, or our
  // state check on the full tuple will figure it out.
  } else {
    os << os.Indent() << "if True:\n";
    os.PushIndent();
  }

  auto col_index = 0u;
  for (auto param : params) {
    if (param.Binding() != ParameterBinding::kBound) {
      os << os.Indent() << "param_" << param.Index() << ": "
         << TypeName(module, param.Type()) << " = tuple";
      if (num_free_params != 1u) {
        os << '[' << col_index << ']';
      }
      ++col_index;
      os << '\n';
    }
  }

  if (spec.tuple_checker) {
    os << os.Indent() << "if not self." << Procedure(os, *(spec.tuple_checker))
       << '(';
    auto sep = "";
    for (auto param : params) {
      os << sep << "param_" << param.Index();
      sep = ", ";
    }
    os << "):\n";
    os.PushIndent();
    if (num_free_params) {
      os << os.Indent() << "continue\n";
    } else {
      os << os.Indent() << "return False\n";
    }
    os.PopIndent();

  // Double check the tuple's state.
  } else {
    os << os.Indent() << "full_tuple = ";
    if (1 < num_params) {
      os << '(';
    }

    auto sep = "";
    for (auto param : params) {
      os << sep << "param_" << param.Index();
      sep = ", ";
    }

    if (1 < num_params) {
      os << ')';
    }

    os << '\n'
       << os.Indent() << "state = " << Table(os, spec.table)
       << "[full_tuple] & " << kStateMask << '\n'
       << os.Indent() << "if state != " << kStatePresent << ":\n";
    os.PushIndent();
    if (num_free_params) {
      os << os.Indent() << "continue\n";
    } else {
      os << os.Indent() << "return False\n";
    }
    os.PopIndent();
  }

  if (num_free_params) {
    os << os.Indent() << "yield ";
    auto sep = "";
    for (auto param : params) {
      if (param.Binding() != ParameterBinding::kBound) {
        os << sep << "param_" << param.Index();
        sep = ", ";
      }
    }
    os << "\n";

  } else {
    os << os.Indent() << "return True\n";
  }

  os.PopIndent();

  os.PopIndent();
  os << '\n';
}

}  // namespace

// Emits Python code for the given program to `os`.
void GeneratePythonDatabaseCode(const Program &program, OutputStream &os) {
  os << "# Auto-generated file\n\n"
     << "from __future__ import annotations\n"
     << "import sys\n"
     << "from dataclasses import dataclass\n"
     << "from collections import defaultdict, namedtuple\n"
     << "from typing import Callable, cast, DefaultDict, Final, Iterator, "
     << "List, NamedTuple, Optional, Sequence, Set, Tuple, Union\n"
     << "try:\n";
  os.PushIndent();
  os << os.Indent() << "from typing import Protocol\n";
  os.PopIndent();
  os << "except ImportError:\n";
  os.PushIndent();
  os << os.Indent() << "from typing_extensions import Protocol #type: ignore\n\n";
  os.PopIndent();

  const auto module = program.ParsedModule();

  // Output prologue code.
  for (auto sub_module : ParsedModuleIterator(module)) {
    for (auto code : sub_module.Inlines()) {
      switch (code.Language()) {
        case Language::kUnknown:
        case Language::kPython:
          if (code.IsPrologue()) {
            os << code.CodeToInline() << "\n\n";
          }
          break;
        default:
          break;
      }
    }
  }

  DeclareFunctors(os, program, module);
  DeclareMessageLog(os, program, module);

  // A program gets its own class
  os << "class " << gClassName << ":\n\n";
  os.PushIndent();

  os << os.Indent() << "def __init__(self, log: " << gClassName
     << "LogInterface, functors: " << gClassName << "Functors):\n";
  os.PushIndent();
  os << os.Indent() << "self._log: " << gClassName << "LogInterface = log\n"
     << os.Indent() << "self._functors: " << gClassName
     << "Functors = functors\n"
     << os.Indent() << "self._refs: DefaultDict[int, List[object]] "
     << "= defaultdict(list)\n\n";

  for (auto table : program.Tables()) {
    DefineTable(os, module, table);
  }

  for (auto global : program.GlobalVariables()) {
    DefineGlobal(os, module, global);
  }

  for (auto constant : program.Constants()) {
    DefineConstant(os, module, constant);
  }

  // Invoke the init procedure. Always first
  auto init_procedure = program.Procedures()[0];
  assert(init_procedure.Kind() == ProcedureKind::kInitializer);
  os << os.Indent() << "self." << Procedure(os, init_procedure) << "()\n\n";

  os.PopIndent();

  for (auto type : module.ForeignTypes()) {
    DefineTypeRefResolver(os, module, type);
  }

  for (auto proc : program.Procedures()) {
    DefineProcedure(os, module, proc);
  }

  for (const auto &query_spec : program.Queries()) {
    DefineQueryEntryPoint(os, module, query_spec);
  }

  os.PopIndent();

  // Output epilogue code.
  for (auto sub_module : ParsedModuleIterator(module)) {
    for (auto code : sub_module.Inlines()) {
      switch (code.Language()) {
        case Language::kUnknown:
        case Language::kPython:
          if (code.IsEpilogue()) {
            os << code.CodeToInline() << "\n\n";
          }
          break;
        default:
          break;
      }
    }
  }
}

}  // namespace hyde
