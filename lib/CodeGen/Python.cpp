// Copyright 2020, Trail of Bits. All rights reserved.

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

namespace hyde {
namespace {

constexpr auto kStateAbsent = 0u;
constexpr auto kStatePresent = 1u;
constexpr auto kStateUnknown = 2u;

// NOTE(pag): We store an extra bit besides present/absent/unknown
//            to track whether or not the data had ever been in our
//            index before, and thus doesn't need to be re-added.
constexpr auto kStateMask = 0x3u;
constexpr auto kPresentBit = 0x4u;

// NOTE(ekilmer): Classes are named all the same for now.
constexpr auto gClassName = "Database";

// Make a comment in code for debugging purposes
static OutputStream &Comment(OutputStream &os, const char *message) {
#ifndef NDEBUG
  os << os.Indent() << "# " << message << "\n";
#else
  (void) message;
#endif
  return os;
}

static OutputStream &Procedure(OutputStream &os, ProgramProcedure proc) {
  switch (proc.Kind()) {
    case ProcedureKind::kInitializer: return os << "init_" << proc.Id() << "_";
    case ProcedureKind::kMessageHandler:
      return os << proc.Message()->Name() << "_"
                << proc.Message()->Arity();
    case ProcedureKind::kTupleFinder:
    case ProcedureKind::kTupleRemover:
    case ProcedureKind::kInductionCycleHandler:
    case ProcedureKind::kInductionOutputHandler:
    default: return os << "proc_" << proc.Id() << "_";
  }
}

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
static const std::string_view TypeName(ParsedForeignType type) {
  if (auto code = type.CodeToInline(Language::kPython)) {
    return *code;
  }
  assert(false);
  return "Any";
}

// Python representation of TypeKind
static const std::string_view TypeName(ParsedModule module, TypeLoc kind) {
  switch (kind.UnderlyingKind()) {
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
    case TypeKind::kForeignType:
      if (auto type = module.ForeignType(kind); type) {
        return TypeName(*type);
      }
      [[clang::fallthrough]];
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

static std::string TypeValueOrDefault(ParsedModule module, TypeLoc loc,
                                      std::optional<ParsedLiteral> val) {
  std::string_view prefix = "";
  std::string_view suffix = "";

  // Default value
  switch (loc.UnderlyingKind()) {
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
      prefix = "b";
      break;
    case TypeKind::kASCII:
    case TypeKind::kUTF8:
    case TypeKind::kUUID:
      break;
    case TypeKind::kForeignType:
      if (auto type = module.ForeignType(loc); type) {
        if (auto constructor = type->Constructor(Language::kPython);
            constructor) {
          prefix = constructor->first;
          suffix = constructor->second;
        }
        break;
      }
      [[clang::fallthrough]];
    default: assert(false); prefix = "None  #";
  }

  std::stringstream value;
  value << prefix;
  if (val) {
    if (auto spelling = val->Spelling(Language::kPython); spelling) {
      value << *spelling;
    }
  }
  value << suffix;
  return value.str();
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
  os << os.Indent() << Var(os, global)
     << ": Final[" << TypeName(module, type) << "] = "
     << TypeValueOrDefault(module, type, global.Value()) << "\n\n";
}

// Similar to DefineGlobal except has type-hint to enforce const-ness
static void DefineConstant(OutputStream &os, ParsedModule module,
                           DataVariable global) {
  auto type = global.Type();
  os << os.Indent() << Var(os, global) << ": " << TypeName(module, type)
     << " = " << TypeValueOrDefault(module, type, global.Value()) << "\n";
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
    os << Comment(os, "Program Call Region");

    auto param_index = 0u;
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

    os << os.Indent() << "ret = self."
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

    // Check the return value.
    bool is_cond = true;
    if (region.ExecuteBodyIfReturnIsTrue()) {
      os << os.Indent() << "if ret:\n";
    } else if (region.ExecuteBodyIfReturnIsFalse()) {
      os << os.Indent() << "if not ret:\n";
    } else {
      is_cond = false;
    }

    if (is_cond) {
      os.PushIndent();
      if (auto body = region.Body(); body) {
        body->Accept(*this);
      } else {
        os << os.Indent() << "pass\n";
      }
      os.PopIndent();
    } else {
      os << '\n';
    }
  }

  void Visit(ProgramReturnRegion region) override {
    os << Comment(os, "Program Return Region");

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

  void Visit(ProgramExistenceAssertionRegion region) override {
    os << Comment(os, "Program ExistenceAssertion Region");
    const auto vars = region.ReferenceCounts();
    for (auto var : vars) {
      if (region.IsIncrement()) {
        os << os.Indent() << Var(os, var) << " += 1\n";
      } else {
        os << os.Indent() << Var(os, var) << " -= 1\n";
      }
    }

    if (auto body = region.Body(); body) {
      assert(vars.size() == 1u);
      if (region.IsIncrement()) {
        os << os.Indent() << "if " << Var(os, vars[0]) << " == 1:\n";
      } else {
        os << os.Indent() << "if " << Var(os, vars[0]) << " == 0:\n";
      }
      os.PushIndent();
      body->Accept(*this);
      os.PopIndent();
    }
  }

  void Visit(ProgramExistenceCheckRegion region) override {
    os << Comment(os, "Program ExistenceCheck Region");
    const auto vars = region.ReferenceCounts();
    auto sep = "if ";
    os << os.Indent();
    auto seen = false;
    for (auto var : vars) {
      os << sep << Var(os, var);
      if (region.CheckForZero()) {
        os << " == 0";
      } else if (region.CheckForNotZero()) {
        os << " != 0";
      } else {
        assert(false);
      }
      sep = " and ";
      seen = true;
    }

    if (seen) {
      os << ":\n";
    } else {
      os << "if True:\n";
    }
    os.PushIndent();
    if (auto body = region.Body(); body) {
      body->Accept(*this);
    } else {
      os << os.Indent() << "pass\n";
    }
    os.PopIndent();
  }

  void Visit(ProgramGenerateRegion region) override {
    const auto functor = region.Functor();
    os << Comment(os, "Program Generate Region");

    if (!region.IsPositive()) {
      assert(functor.Range() != FunctorRange::kOneToOne);
      assert(functor.Range() != FunctorRange::kOneOrMore);
      os << os.Indent() << "found = False\n";
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
      if (!region.IsPositive()) {
        os << os.Indent() << "found = True\n";
        os.PopIndent();
        os << os.Indent() << "if not found:\n";
        os.PushIndent();
      }

      if (auto body = region.Body(); body) {
        body->Accept(*this);
      } else {
        os << os.Indent() << "pass";
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

          os << os.Indent() << "tmp_" << region.Id();
          auto sep = ": Tuple[";
          for (auto out_var : output_vars) {
            os << sep << TypeName(module, out_var.Type());
            sep = ", ";
          }
          os << "]\n"
             << os.Indent() << "for tmp_" << region.Id() << " in ";
          call_functor();
          os << ":\n";
          os.PushIndent();
          auto out_var_index = 0u;
          for (auto out_var : output_vars) {
            os << os.Indent() << Var(os, out_var) << " = tmp_" << region.Id()
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
          os << os.Indent() << "tmp_" << region.Id() << ": ";
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
            os << os.Indent() << "if tmp_" << region.Id() << " is not None:\n";
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
             << os.Indent() << "if tmp_" << region.Id() << " is not None:\n";
          os.PushIndent();
          auto out_var_index = 0u;
          for (auto out_var : output_vars) {
            os << os.Indent() << Var(os, out_var) << " = tmp_" << region.Id()
               << '[' << (out_var_index++) << "]\n";
          }
          do_body();
          os.PopIndent();
        }
        break;
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
      os << Comment(os, "Induction Output Region");
      output->Accept(*this);
    }
  }

  void Visit(ProgramLetBindingRegion region) override {
    os << Comment(os, "Program LetBinding Region");
    auto i = 0u;
    const auto used_vars = region.UsedVariables();
    for (auto var : region.DefinedVariables()) {
      os << os.Indent() << Var(os, var) << ": "
         << TypeName(module, var.Type()) << " = "
         << Var(os, used_vars[i++]) << '\n';
    }

    if (auto body = region.Body(); body) {
      body->Accept(*this);
    }
  }

  void Visit(ProgramParallelRegion region) override {
    os << Comment(os, "Program Parallel Region");

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
    os << Comment(os, "Program Publish Region");
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
    os << Comment(os, "Program Series Region");

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
    os << Comment(os, "Program VectorAppend Region");

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
    os << Comment(os, "Program VectorClear Region");

    os << os.Indent() << "del " << Vector(os, region.Vector()) << "[:]\n";
    os << os.Indent() << VectorIndex(os, region.Vector()) << " = 0\n";
  }

  void Visit(ProgramVectorSwapRegion region) override {
    os << Comment(os, "Program VectorSwap Region");

    os << os.Indent() << Vector(os, region.LHS()) << ", "
       << Vector(os, region.RHS()) << " = " << Vector(os, region.RHS())
       << ", " << Vector(os, region.LHS()) << '\n';
  }

  void Visit(ProgramVectorLoopRegion region) override {
    os << Comment(os, "Program VectorLoop Region");
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
    os << Comment(os, "Program VectorUnique Region");

    os << os.Indent() << Vector(os, region.Vector()) << " = list(set("
       << Vector(os, region.Vector()) << "))\n";
    os << os.Indent() << VectorIndex(os, region.Vector()) << " = 0\n";
  }

  void ResolveReference(DataVariable var) {
    if (auto foreign_type = module.ForeignType(var.Type()); foreign_type) {
      if (var.DefiningRegion()) {
        os << os.Indent() << Var(os, var)
           << " = self._resolve_" << foreign_type->Name()
           << '(' << Var(os, var) << ")\n";
      }
    }
  }

  void ResolveReferences(UsedNodeRange<DataVariable> vars) {
    for (auto var : vars) {
      ResolveReference(var);
    }
  }

  void Visit(ProgramTransitionStateRegion region) override {
    os << Comment(os, "Program TransitionState Region");

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
    os << Comment(os, "Program CheckState Region");
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
    os << Comment(os, "Program TableJoin Region");

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
      const auto index = region.Index(i);
      const auto index_keys = index.KeyColumns();
      const auto index_vals = index.ValueColumns();
      auto key_prefix = "(";
      auto key_suffix = ")";
      if (index_keys.size() == 1u) {
        key_prefix = "";
        key_suffix = "";
      }

      // The index is a set of key column values/tuples.
      if (index_vals.empty()) {

        os << os.Indent() << "if " << key_prefix;

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

        os << key_suffix << " in " << TableIndex(os, index) << ":\n";

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
    os << Comment(os, "Program TableProduct Region");
    os << os.Indent() << "pass  # TODO(ekilmer): ProgramTableProductRegion\n";
  }

  void Visit(ProgramTableScanRegion region) override {
    os << Comment(os, "Program TableScan Region");

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

    // Index scan :-D
    if (region.Index()) {
      const auto index = *(region.Index());

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
    os << Comment(os, "Program TupleCompare Region");

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
  os << os.Indent() << "return sys.modules[__name__]." << func.Name() << '_'
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
                                 ParsedMessage message) {
  os << os.Indent() << "def " << message.Name() << "_" << message.Arity()
     << "(self";

  for (auto param : message.Parameters()) {
    os << ", " << param.Name() << ": " << TypeName(module, param.Type());
  }

  os << ", added: bool):\n";
  os.PushIndent();
  os << os.Indent() << "pass\n\n";
  os.PopIndent();
}

static void DeclareMessageLog(OutputStream &os, Program program,
                              ParsedModule root_module) {
  os << os.Indent() << "class " << gClassName << "Log:\n";
  os.PushIndent();

  std::unordered_set<ParsedMessage> seen;

  bool has_messages = false;
  for (auto module : ParsedModuleIterator(root_module)) {
    for (auto message : module.Messages()) {
      if (auto [it, inserted] = seen.emplace(message);
          inserted && message.IsPublished()) {
        DeclareMessageLogger(os, module, message);
        has_messages = true;
        (void) it;
      }
    }
  }
  if (!has_messages) {
    os << os.Indent() << "pass\n\n";
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
      os << os.Indent() << "continue;\n";
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
      os << os.Indent() << "continue;\n";
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
void GeneratePythonCode(Program &program, OutputStream &os) {
  os << "# Auto-generated file\n\n"
     << "from __future__ import annotations\n"
     << "import sys\n"
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

  DeclareFunctors(os, program, module);
  DeclareMessageLog(os, program, module);

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

  // A program gets its own class
  os << "class " << gClassName << ":\n\n";
  os.PushIndent();

  os << os.Indent() << "def __init__(self, log: " << gClassName
     << "Log, functors: " << gClassName << "Functors):\n";
  os.PushIndent();
  os << os.Indent() << "self._log: " << gClassName << "Log = log\n"
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
