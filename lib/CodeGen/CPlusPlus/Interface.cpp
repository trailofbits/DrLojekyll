// Copyright 2021, Trail of Bits. All rights reserved.

#include <set>
#include <sstream>
#include <string>
#include <unordered_map>

#include <drlojekyll/CodeGen/CodeGen.h>
#include <drlojekyll/ControlFlow/Format.h>
#include <drlojekyll/ControlFlow/Program.h>
#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Lex/Format.h>
#include <drlojekyll/Parse/Format.h>
#include <drlojekyll/Parse/ModuleIterator.h>

#include "Util.h"

namespace hyde {
namespace cxx {

namespace {

static void DeclareMessageVector(OutputStream &os, ParsedModule module,
                                 DataVector vector) {
  os << os.Indent() << "::hyde::rt::SerializedVector<StorageT";
  for (auto type : vector.ColumnTypes()) {
    os << ", " << TypeName(module, type);
  }
  os << "> vec_" << vector.Id() << ";\n";
}

static void DeclareAppendMessageMethod(OutputStream &os, ParsedModule module,
                                       DataVector vec, ParsedMessage message,
                                       bool added) {
  os << os.Indent() << "void ";
  if (added) {
    os << "produce_";
  } else {
    os << "retract_";
  }

  os << message.Name() << '_' << message.Arity();
  auto sep = "(";
  for (auto param : message.Parameters()) {
    os << sep << TypeName(module, param.Type()) << ' ';
    if (!param.Type().IsReferentiallyTransparent(module, Language::kCxx)) {
      os << "&&";
    }
    os << param.Name();
    sep = ", ";
  }
  os << ") noexcept {\n";
  os.PushIndent();

  os << os.Indent() << "size += 1u;\n"
     << os.Indent() << "vec_" << vec.Id();
  sep = ".Add(";
  for (auto param : message.Parameters()) {
    os << sep;
    const auto is_transparent = param.Type().IsReferentiallyTransparent(
        module, Language::kCxx);
    if (!is_transparent) {
      os << "std::forward<" << TypeName(module, param.Type()) << ">(";
    }
    os << param.Name();
    if (!is_transparent) {
      os << ')';
    }
    sep = ", ";
  }
  os << ");\n";

  os.PopIndent();
  os << os.Indent() << "}\n\n";
}

}  // namespace

// Emits C++ code to build up and collect messages to send to a database,
// or to collect messages published by the database and aggregate them into
// a single object.
void GenerateInterfaceCode(const Program &program, OutputStream &os) {
  os << "/* Auto-generated file */\n\n"
     << "#pragma once\n\n"
     << "#include <memory>\n"
     << "#include <string>\n"
     << "#include <tuple>\n"
     << "#include <utility>\n"
     << "#include <drlojekyll/Runtime/Runtime.h>\n\n"
     << "#ifndef __DRLOJEKYLL_PROLOGUE_CODE_" << gClassName << "\n"
     << "#  define __DRLOJEKYLL_PROLOGUE_CODE_" << gClassName << "\n";

  // Output prologue code.
  ParsedModule module = program.ParsedModule();
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

  os << "#endif  // __DRLOJEKYLL_PROLOGUE_CODE_" << gClassName << "\n\n";

  std::vector<std::tuple<DataVector, ParsedMessage, bool>> message_vecs;

  std::optional<ProgramProcedure> entry_proc;
  for (ProgramProcedure proc : program.Procedures()) {
    if (ProcedureKind::kEntryDataFlowFunc == proc.Kind()) {
      assert(!entry_proc.has_value());
      entry_proc.emplace(proc);
      for (DataVector param_vec : proc.VectorParameters()) {
        if (auto added_message = param_vec.AddedMessage()) {
          message_vecs.emplace_back(param_vec, *added_message, true);
        } else if (auto removed_message = param_vec.RemovedMessage()) {
          message_vecs.emplace_back(param_vec, *removed_message, false);
        }
      }
      break;
    }
  }

  const auto messages = Messages(module);

  // Create a mapping of names to actual messages.
  unsigned next_input_message_id = 0u;
  unsigned next_output_message_id = 0u;
  std::unordered_map<std::string, unsigned> name_to_id;
  std::unordered_map<ParsedMessage, std::string> message_to_name;

  for (ParsedMessage message : messages) {
    std::stringstream ss;
    OutputStream ss_os(os.display_manager, ss);
    ss_os << message.Name() << '_' << message.Arity();
    ss_os.Flush();
    std::string name = ss.str();
    if (!name_to_id.count(name)) {
      message_to_name.emplace(message, name);
      if (message.IsPublished()) {
        name_to_id.emplace(std::move(name), next_output_message_id++);
      } else {
        name_to_id.emplace(std::move(name), next_input_message_id++);
      }
    }
  }

  assert(entry_proc.has_value());

  os << os.Indent()
     << "template <typename StorageT, typename LogT, typename FunctorsT>\n"
     << os.Indent() << "class " << gClassName << ";\n\n"
     << os.Indent() << "struct " << gClassName << "MessageVisitor;\n\n"
     << os.Indent() << "template <typename StorageT>\n"
     << os.Indent() << "class " << gClassName << "InputMessage {\n";
  os.PushIndent();
  os << os.Indent() << "private:\n";
  os.PushIndent();
  os << os.Indent() << "friend struct " << gClassName << "MessageVisitor;\n\n"
     << os.Indent() << "unsigned long size{0u};\n\n";

  for (auto [vec, message, added] : message_vecs) {
    DeclareMessageVector(os, module, vec);
  }

  os.PopIndent();  // private
  os << os.Indent() << "public:\n";
  os.PushIndent();
  os << os.Indent() << gClassName << "InputMessage(StorageT &storage_)";
  auto sep1 = "\n";
  auto sep2 = "    : ";
  for (auto [vec, message, added] : message_vecs) {
    if (!message.IsPublished()) {
      os << sep1 << os.Indent() << sep2 << "vec_" << vec.Id()
         << "(storage_, " << vec.Id() << "u)";
      sep1 = ",\n";
      sep2 = "      ";
    }
  }
  os << " {}\n\n"
     << os.Indent() << "void Clear(void) {\n";
  os.PushIndent();
  os << os.Indent() << "size = 0u;\n";
  for (auto [vec, message, added] : message_vecs) {
    os << os.Indent() << "vec_" << vec.Id() << ".Clear();\n";
  }
  os.PopIndent();
  os << os.Indent() << "}\n\n"  // Clear
     << os.Indent() << "unsigned long Size(void) const noexcept {\n";
  os.PushIndent();
  os << os.Indent() << "return size;\n";
  os.PopIndent();
  os << os.Indent() << "}\n\n"  // Size
     << os.Indent() << "bool Empty(void) const noexcept {\n";
  os.PushIndent();
  os << os.Indent() << "return !size;\n";
  os.PopIndent();
  os << os.Indent() << "}\n\n";  // Empty

  for (auto [vec, message, added] : message_vecs) {
    DeclareAppendMessageMethod(os, module, vec, message, added);
  }

  // Make a method that applies the vectors to the database.
  os << os.Indent() << "template <typename LogT, typename FunctorsT>\n"
     << os.Indent() << "void Apply(" << gClassName
     << "<StorageT, LogT, FunctorsT> &db_) {\n";

  os.PushIndent();
  os << os.Indent() << "size = 0u;\n"
     << os.Indent() << "db_.proc_" << entry_proc->Id();
  auto sep = "_(";
  for (auto [vec, message, added] : message_vecs) {
    os << sep << "std::move(vec_" << vec.Id() << ")";
    sep = ", ";
  }
  os << ");\n";
  os.PopIndent();
  os << os.Indent() << "}\n";  // Apply.

  os.PopIndent();  // public
  os.PopIndent();
  os << os.Indent() << "};\n\n";

  for (ParsedMessage message : messages) {
    const auto &name = message_to_name[message];
    const auto id = name_to_id[name];

    os << os.Indent() << "struct " << gClassName;
    if (message.IsPublished()) {
      os << "OutputMessage";
    } else {
      os << "InputMessage";
    }
    os << id << " {\n";

    os.PushIndent();
    os << os.Indent() << "static constexpr auto kId = " << id << ";\n"
       << os.Indent() << "static constexpr auto kName = \"" << name << "\";\n"
       << os.Indent() << "static constexpr auto kNameLength = "
       << name.size() << ";\n"
       << os.Indent() << "static constexpr auto kNumParams = "
       << message.Arity() << "u;\n"
       << os.Indent() << "static constexpr bool kIsDifferential = ";
    if (message.IsDifferential()) {
      os << "true;\n";
    } else {
      os << "false;\n";
    }
    os << os.Indent() << "static constexpr bool kIsPublished = ";
    if (message.IsPublished()) {
      os << "true;\n";
    } else {
      os << "false;\n";
    }
    os << os.Indent() << "using TupleType = std::tuple<";
    sep = "";
    for (auto param : message.Parameters()) {
      os << sep << TypeName(module, param.Type().Kind());
      sep = ", ";
    }
    os << ">;\n\n"
       << os.Indent() << "template <typename StorageT>\n"
       << os.Indent() << "inline static bool AppendTupleToInputMessage("
       << gClassName << "InputMessage<StorageT> &msg, TupleType tuple, "
       << "bool added) {\n";
    os.PushIndent();
    if (!message.IsPublished()) {
      os << os.Indent() << "if (added) {\n";
      os.PushIndent();
      os << os.Indent() << "msg.produce_" << name;
      sep = "(";
      for (auto param : message.Parameters()) {
        os << sep;
        if (param.Type().IsReferentiallyTransparent(module, Language::kCxx)) {
          os << "std::get<" << param.Index() << ">(tuple)";
        } else {
          os << "std::move(std::get<" << param.Index() << ">(tuple))";
        }
        sep = ", ";
      }
      os << ");\n";
      os << os.Indent() << "return true;\n";
      os.PopIndent();
      os << os.Indent() << "} else {\n";  // !added
      os.PushIndent();
      if (message.IsDifferential()) {
        os << os.Indent() << "msg.retract_" << name;
        sep = "(";
        for (auto param : message.Parameters()) {
          os << sep;
          if (param.Type().IsReferentiallyTransparent(module, Language::kCxx)) {
            os << "std::get<" << param.Index() << ">(tuple)";
          } else {
            os << "std::move(std::get<" << param.Index() << ">(tuple))";
          }
          sep = ", ";
        }
        os << ");\n";
      } else {
        os << os.Indent() << "return false;\n";
      }

      os.PopIndent();
      os << os.Indent() << "}\n";  // !added.
    } else {
      os << os.Indent() << "(void) tuple;\n"
         << os.Indent() << "(void) msg;\n"
         << os.Indent() << "(void) added;\n"
         << os.Indent() << "return false;\n";
    }
    os.PopIndent();
    os << os.Indent() << "}\n";  // End of `AppendToInputMessage`.
    os.PopIndent();
    os << os.Indent() << "};\n\n";  // End of InputMessageN
  }

  os << os.Indent() << "template <typename Visitor, typename... Args>\n"
     << os.Indent() << "inline static void Visit" << gClassName
     << "Messages(Visitor &visitor, Args&... args) {\n";
  os.PushIndent();
  for (ParsedMessage message : messages) {
    const auto &name = message_to_name[message];
    const auto id = name_to_id[name];
    os << os.Indent() << "visitor.template Visit<" << gClassName;
    if (message.IsPublished()) {
      os << "OutputMessage";

    } else {
      os << "InputMessage";
    }
    os << id << ">(args...);\n";
  }
  os.PopIndent();
  os << "}\n\n";  // End of `VisitMessages`.

  os << os.Indent() << "template <typename Visitor>\n"
     << os.Indent() << "class " << gClassName << "LogVisitor {\n";
  os.PushIndent();
  os << os.Indent() << "private:\n";
  os.PushIndent();
  os << os.Indent() << "Visitor &vis;\n\n";
  os.PopIndent();  // private
  os << os.Indent() << "public:\n";
  os.PushIndent();

  os << os.Indent() << gClassName << "LogVisitor(Visitor &vis_)\n"
     << os.Indent() << "    : vis(vis_) {}\n\n";

  for (ParsedMessage message : messages) {
    if (!message.IsPublished()) {
      continue;
    }

    os << os.Indent() << "void " << message.Name() << "_" << message.Arity()
       << "(";

    sep = "";
    for (auto param : message.Parameters()) {
      os << sep;
      if (param.Type().IsReferentiallyTransparent(module, Language::kCxx)) {
        os << TypeName(module, param.Type())
           << " p";
      } else {
        os << "const " << TypeName(module, param.Type()) << " &p";
      }
      os << param.Index() << " /* " << param.Name() << " */";
      sep = ", ";
    }

    os << ", bool added) {\n";
    os.PushIndent();
    os << os.Indent() << "if (added) {\n";
    os.PushIndent();
    os << os.Indent() << "vis.template AcceptAdd<" << gClassName
       << "OutputMessage" << name_to_id[message_to_name[message]];

    sep = ">(";
    for (auto param : message.Parameters()) {
      os << sep << "p" << param.Index();
      sep = ", ";
    }

    os << ");\n";
    os.PopIndent();
    os << os.Indent() << "} else {\n";
    os.PushIndent();
    if (message.IsDifferential()) {
      os << os.Indent() << "vis.template AcceptRemove<" << gClassName
         << "OutputMessage" << name_to_id[message_to_name[message]];

      sep = ">(";
      for (auto param : message.Parameters()) {
        os << sep << "p" << param.Index();
        sep = ", ";
      }

      os << ");\n";
    }
    os.PopIndent();
    os << os.Indent() << "}\n";  // !added

    os.PopIndent();
    os << os.Indent() << "}\n";  // message logger
  }

  os.PopIndent();  // public:
  os.PopIndent();
  os << os.Indent() << "};\n\n";  // End of LogVisitor

  // Make a proxy message logger.

  os << os.Indent() << "template <typename L, typename LPtr=L *>\n"
     << os.Indent() << "class Proxy" << gClassName << "Log {\n";
  os.PushIndent();
  os << os.Indent() << "public:\n";
  os.PushIndent();
  os << os.Indent() << "LPtr logger;\n"
     << os.Indent() << "Proxy" << gClassName << "Log(LPtr logger_)\n"
     << os.Indent() << "    : logger(std::move(logger_)) {}\n";

  for (ParsedMessage message : messages) {
    if (!message.IsPublished()) {
      continue;
    }

    os << "\n"
       << os.Indent() << "void " << message.Name() << "_" << message.Arity();

    sep = "(";
    for (ParsedParameter param : message.Parameters()) {
      os << sep;
      if (param.Type().IsReferentiallyTransparent(module, Language::kCxx)) {
        os << TypeName(module, param.Type()) << " p";
      } else {
        os << "const " << TypeName(module, param.Type()) << " &p";
      }
      os << param.Index();
      sep = ", ";
    }

    os << sep << "bool added) {\n";
    os.PushIndent();
    os << os.Indent() << "if (logger) {\n";
    os.PushIndent();
    os << os.Indent() << "logger->" << message.Name() << "_" << message.Arity();
    sep = "(";
    for (ParsedParameter param : message.Parameters()) {
      os << sep << "p" << param.Index();
      sep = ", ";
    }
    os << sep << "added);\n";
    os.PopIndent();
    os << os.Indent() << "}\n";
    os.PopIndent();
    os << os.Indent() << "}\n";
  }

  os.PopIndent();  // public
  os.PopIndent();
  os << os.Indent() << "};\n\n";  // Proxy*Log

  os << os.Indent() << "class Virtual" << gClassName << "Log {\n";
  os.PushIndent();
  os << os.Indent() << "public:\n";
  os.PushIndent();

  for (ParsedMessage message : messages) {
    if (!message.IsPublished()) {
      continue;
    }

    os << "\n"
       << os.Indent() << "virtual void " << message.Name()
       << "_" << message.Arity();

    sep = "(";
    for (ParsedParameter param : message.Parameters()) {
      os << sep;
      if (param.Type().IsReferentiallyTransparent(module, Language::kCxx)) {
        os << TypeName(module, param.Type()) << " p";
      } else {
        os << "const " << TypeName(module, param.Type()) << " &p";
      }
      os << param.Index();
      sep = ", ";
    }

    os << sep << "bool added) = 0;\n";
  }

  os.PopIndent();  // public
  os.PopIndent();
  os << os.Indent() << "};\n\n";  // VirtualLog

  // Make a virtual proxy message logger.

  os << os.Indent() << "template <typename L, typename LPtr=L *>\n"
     << os.Indent() << "class VirtualProxy" << gClassName
     << "Log final : public Virtual" << gClassName << "Log {\n";
  os.PushIndent();
  os << os.Indent() << "public:\n";
  os.PushIndent();
  os << os.Indent() << "LPtr logger;\n"
     << os.Indent() << "VirtualProxy" << gClassName << "Log(LPtr logger_)\n"
     << os.Indent() << "    : logger(std::move(logger_)) {}\n";

  for (ParsedMessage message : messages) {
    if (!message.IsPublished()) {
      continue;
    }

    os << "\n"
       << os.Indent() << "void " << message.Name() << "_" << message.Arity();

    sep = "(";
    for (ParsedParameter param : message.Parameters()) {
      os << sep;
      if (param.Type().IsReferentiallyTransparent(module, Language::kCxx)) {
        os << TypeName(module, param.Type()) << " p";
      } else {
        os << "const " << TypeName(module, param.Type()) << " &p";
      }
      os << param.Index();
      sep = ", ";
    }

    os << sep << "bool added) final {\n";
    os.PushIndent();
    os << os.Indent() << "if (logger) {\n";
    os.PushIndent();
    os << os.Indent() << "logger->" << message.Name() << "_" << message.Arity();
    sep = "(";
    for (ParsedParameter param : message.Parameters()) {
      os << sep << "p" << param.Index();
      sep = ", ";
    }
    os << sep << "added);\n";
    os.PopIndent();
    os << os.Indent() << "}\n";
    os.PopIndent();
    os << os.Indent() << "}\n";
  }

  os.PopIndent();  // public
  os.PopIndent();
  os << os.Indent() << "};\n\n";  // VirtualProxy*Log

  os
     << os.Indent() << "class " << gClassName << "QueryGenerator {\n";
  os.PushIndent();
  os << os.Indent() << "public:\n";
  os.PushIndent();
  os << os.Indent() << "virtual ~" << gClassName
     << "QueryGenerator(void) = default;\n"
     << os.Indent() << "virtual unsigned QueryId(void) const noexcept = 0;\n"
     << os.Indent() << "virtual void *TryGetNextOpaque(void) noexcept = 0;\n";
  os.PopIndent();  // public
  os.PopIndent();
  os << os.Indent() << "};\n\n"  // QueryGenerator
     << os.Indent() << "template <typename RetTupleType>\n"
     << os.Indent() << "class " << gClassName << "RowGenerator : public "
     << gClassName << "QueryGenerator {\n";
  os.PushIndent();
  os << os.Indent() << "public:\n";
  os.PushIndent();

  os << os.Indent() << "virtual ~" << gClassName
     << "RowGenerator(void) = default;\n"
     << os.Indent()
     << "virtual RetTupleType *TryGetNext(void) noexcept = 0;\n";

  os.PopIndent();  // public
  os.PopIndent();
  os << "};\n\n";  // RowGenerator

  unsigned next_query_id = 0u;
  for (const ProgramQuery &query_info : program.Queries()) {

    unsigned num_params = 0u;
    unsigned num_rets = 0u;
    ParsedDeclaration decl(query_info.query);
    std::stringstream ss;
    OutputStream ss_os(os.display_manager, ss);
    ss_os << decl.Name() << '_' << decl.BindingPattern();
    ss_os.Flush();
    std::string name = ss.str();

    for (ParsedParameter param : decl.Parameters()) {
      if (param.Binding() == ParameterBinding::kBound) {
        ++num_params;
      } else {
        ++num_rets;
      }
    }

    os << os.Indent() << "using " << gClassName << "Query" << next_query_id
       << "ParamTupleType = std::tuple<";
    sep = "";
    for (ParsedParameter param : decl.Parameters()) {
      if (param.Binding() == ParameterBinding::kBound) {
        os << sep << TypeName(module, param.Type());
        sep = ", ";
      }
    }
    os << ">;\n"
       << os.Indent() << "using " << gClassName << "Query" << next_query_id
       << "RetTupleType = std::tuple<";
    sep = "";
    for (ParsedParameter param : decl.Parameters()) {
      os << sep << TypeName(module, param.Type());
      sep = ", ";
    }
    os << ">;\n"
       << os.Indent() << "template <typename StorageT, typename LogT, "
       << "typename FunctorsT>\n"
       << os.Indent() << "class " << gClassName << "Query"
       << next_query_id << "Generator final : public " << gClassName
       << "RowGenerator<" << gClassName << "Query" << next_query_id
       << "RetTupleType> {\n";
    os.PushIndent();
    os << os.Indent() << "private:\n";
    os.PushIndent();

    os << os.Indent() << "using ParamTupleType = " << gClassName << "Query"
       << next_query_id << "ParamTupleType;\n"
       << os.Indent() << "using RetTupleType = " << gClassName << "Query"
       << next_query_id << "RetTupleType;\n"
       << os.Indent() << gClassName << "<StorageT, LogT, FunctorsT> &db;\n\n"
       << os.Indent() << "ParamTupleType params;\n\n";

    // This is either a table or index scan.
    if (num_rets) {
      os << os.Indent() << "RetTupleType ret;\n"
         << os.Indent()
         << "using ScanType = ::hyde::rt::Scan<StorageT, ::hyde::rt::";

      // This is an index scan.
      if (num_params) {
        assert(query_info.index.has_value());
        os << "IndexTag<" << query_info.index->Id() << ">";

      // This is a full table scan.
      } else {
        os << "TableTag<" << query_info.table.Id() << ">";
      }

      os << ">;\n"
         << os.Indent() << "ScanType scan;\n"
         << os.Indent() << "std::remove_reference_t<"
         << "decltype(reinterpret_cast<ScanType *>(NULL)->begin())> it;\n"
         << os.Indent() << "std::remove_reference_t<"
         << "const decltype(reinterpret_cast<ScanType *>(NULL)->end())> end;\n";

    } else {
      os << os.Indent() << gClassName << "Query" << next_query_id
         << "ParamTupleType *found{nullptr};\n\n";
    }

    os.PopIndent();  // private
    os << os.Indent() << "public:\n";
    os.PushIndent();

    os << os.Indent() << "virtual ~" << gClassName << "Query"
       << next_query_id << "Generator(void) = default;\n"
       << os.Indent() << gClassName << "Query" << next_query_id
       << "Generator(" << gClassName << "<StorageT, LogT, FunctorsT> &db_, "
       << "ParamTupleType params_)\n"
       << os.Indent() << "    : db(db_),\n"
       << os.Indent() << "      params(std::move(params_))";
    if (num_rets) {
      os << ",\n"
         << os.Indent() << "      scan(db_.storage, db."
         << Table(os, query_info.table);

      auto i = 0u;
      for (auto param : decl.Parameters()) {
        if (param.Binding() == ParameterBinding::kBound) {
          os << ", std::get<" << i << ">(params)";
          ++i;
        }
      }
      os << "),\n"
         << os.Indent() << "      it(scan.begin()),\n"
         << os.Indent() << "      end(scan.end()) {}\n\n";

    // If we don't need to generate, then look for the tuple.
    } else {
      os << " {\n";
      os.PushIndent();
      os << os.Indent() << "if (db_." << name;
      sep = "(";
      auto i = 0u;
      for (ParsedParameter param : decl.Parameters()) {
        (void) param;
        os << sep << "std::get<" << (i++) << ">(params)";
        sep = ", ";
      }
      os << ")) {\n";
      os.PushIndent();
      os << os.Indent() << "found = &params;\n";
      os.PopIndent();
      os << os.Indent() << "}\n";
      os.PopIndent();
      os << os.Indent() << "}\n\n";  // constructor
    }

    os << os.Indent() << "RetTupleType *TryGetNext(void) noexcept final {\n";
    os.PushIndent();
    if (num_rets) {
      os << os.Indent() << "while (it != end) {\n";
      os.PushIndent();
      os << os.Indent() << "ret = *it;\n"
         << os.Indent() << "++it;\n";

      // Index scans are over-approximate -- they may include unrelated data, so
      // we need to double check individual results.
      if (num_params) {
        sep = "if (";
        os << os.Indent();

        auto i = 0u;
        auto j = 0u;
        for (ParsedParameter param : decl.Parameters()) {
          if (param.Binding() == ParameterBinding::kBound) {
            os << sep << "std::get<" << i << ">(params) != std::get<"
               << j << ">(ret)";
            sep = " || ";
            ++i;
          }
          ++j;
        }
        os << ") {\n";
        os.PushIndent();
        os << os.Indent() << "continue;\n";
        os.PopIndent();
        os << os.Indent() << "}\n";
      }

      // This is a differential message; we need to double check that records
      // are valid.
      if (query_info.forcing_function) {
        os << os.Indent() << "if (!db."
           << Procedure(os, *(query_info.forcing_function));
        sep = "(";
        for (ParsedParameter param : decl.Parameters()) {
          os << sep << "std::get<" << param.Index() << ">(ret)";
          sep = ", ";
        }
        os << ")) {\n";
        os.PushIndent();
        os << os.Indent() << "continue;\n";
        os.PopIndent();
        os << os.Indent() << "}\n";
      }

      os << os.Indent() << "return &ret;\n";
      os.PopIndent();
      os << os.Indent() << "}\n"
         << os.Indent() << "return nullptr;\n";
    } else {
      os << os.Indent() << "const auto ret = found;\n"
         << os.Indent() << "found = nullptr;\n"
         << os.Indent() << "return ret;\n";
    }
    os.PopIndent();
    os << os.Indent() << "}\n"  // TryGetNext.
       << os.Indent() << "unsigned QueryId(void) const noexcept final {\n";
    os.PushIndent();
    os << os.Indent() << "return " << next_query_id << "u;\n";
    os.PopIndent();
    os << os.Indent() << "}\n"
       << os.Indent() << "void *TryGetNextOpaque(void) noexcept final {\n";
    os.PushIndent();
    os << os.Indent() << "return TryGetNext();\n";
    os.PopIndent();
    os << os.Indent() << "}\n";  // TryGetNextOpaque.

    os.PopIndent();  // public
    os.PopIndent();
    os << os.Indent() << "};\n\n";  // Query*Generator

    os << os.Indent() << "struct " << gClassName << "Query" << next_query_id
       << " {\n";
    os.PushIndent();
    os << os.Indent() << "using ParamTupleType = " << gClassName << "Query"
       << next_query_id << "ParamTupleType;\n"
       << os.Indent() << "using RetTupleType = " << gClassName << "Query"
       << next_query_id << "RetTupleType;\n"
       << os.Indent() << "static constexpr auto kId = " << next_query_id
       << ";\n"
       << os.Indent() << "static constexpr auto kName = \"" << name << "\";\n"
       << os.Indent() << "static constexpr auto kNameLength = "
       << name.size() << "u;\n"
       << os.Indent() << "static constexpr auto kNumParams = "
       << num_params << "u;\n"
       << os.Indent() << "static constexpr auto kNumReturns = "
       << num_rets << "u + kNumParams;\n\n";

    // Make a method that can invoke the query on a database instance.
    os << os.Indent() << "template <typename StorageT, typename LogT, "
       << "typename FunctorsT";
    if (num_rets) {
      os << ", typename Generator";
    }
    os << ">\n"
       << os.Indent() << "inline static ::hyde::rt::index_t Apply("
       << gClassName << "<StorageT, LogT, FunctorsT> &db_, ParamTupleType params_";

    if (num_rets) {
      os << ", Generator gen_";
    }

    os << ") {\n";

    os.PushIndent();

    if (num_rets) {
      os << os.Indent() << "return db_.template " << name << "<Generator>(";
    } else {
      os << os.Indent() << "return db_." << name << "(";
    }
    sep = "";
    auto i = 0u;
    for (ParsedParameter param : decl.Parameters()) {
      if (param.Binding() == ParameterBinding::kBound) {
        if (param.Type().IsReferentiallyTransparent(module, Language::kCxx)) {
          os << sep << "std::get<" << i << ">(params_)";
        } else {
          os << sep << "std::move(std::get<" << i << ">(params_))";
        }
        sep = ", ";
      }
    }

    if (num_rets) {
      os << sep << "std::move(gen_)";
    }
    os << ");\n";
    os.PopIndent();
    os << os.Indent() << "}\n";  // Apply.


    // Make a method that can return a generator for this functor.
    os << os.Indent() << "template <typename StorageT, typename LogT, "
       << "typename FunctorsT>\n"
       << os.Indent() << "inline static std::unique_ptr<" << gClassName
       << "Query" << next_query_id << "Generator<StorageT, LogT, FunctorsT>> Generate("
       << gClassName << "<StorageT, LogT, FunctorsT> &db_, "
       << "ParamTupleType params_) {\n";

    os.PushIndent();
    os << os.Indent() << "return std::make_unique<" << gClassName
       << "Query" << next_query_id << "Generator<StorageT, LogT, FunctorsT>>"
       << "(db_, std::move(params_));\n";
    os.PopIndent();
    os << os.Indent() << "}\n";  // Generate

    os.PopIndent();
    os << os.Indent() << "};\n\n";  // QueryN

    ++next_query_id;
  }

  os << os.Indent() << "template <typename Visitor, typename... Args>\n"
     << os.Indent() << "inline static void Visit" << gClassName
     << "Queries(Visitor &visitor, Args&... args) {\n";
  os.PushIndent();
  next_query_id = 0u;
  for (const ProgramQuery &query_info : program.Queries()) {
    (void) query_info;
    os << os.Indent() << "visitor.template Visit<" << gClassName
       << "Query" << (next_query_id++) << ">(args...);\n";
  }
  os.PopIndent();
  os << "}\n\n";  // End of `VisitQueries`.
}

}  // namespace cxx
}  // namespace hyde
