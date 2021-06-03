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
     << "#include <string>\n"
     << "#include <drlojekyll/Runtime/Runtime.h>\n\n"
     << "\n";

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

  ParsedModule module = program.ParsedModule();
  const auto messages = Messages(module);

  // Create a mapping of names to actual messages.
  unsigned next_input_message_id = 1u;
  unsigned next_output_message_id = 1u;
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
  os << os.Indent() << "}\n";

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
    os << os.Indent() << "using ParamTypes = ::hyde::rt::TypeList<";
    auto sep = "";
    for (auto param : message.Parameters()) {
      os << sep << TypeName(module, param.Type().Kind());
      sep = ", ";
    }
    os << ">;\n";
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
     << "Messages(Visitor &visitor, const Args&... args) {\n";
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
      os << sep << TypeName(module, param.Type()) << " ";
      os << "p" << param.Index() << " /* " << param.Name() << " */";
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
      os << sep;
      if (param.Type().IsReferentiallyTransparent(module, Language::kCxx)) {
        os << "p" << param.Index();
      } else {
        os << "std::move(p" << param.Index() << ")";
      }
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
        os << sep;
        if (param.Type().IsReferentiallyTransparent(module, Language::kCxx)) {
          os << "p" << param.Index();
        } else {
          os << "std::move(p" << param.Index() << ")";
        }
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

    auto sep = "(";
    for (ParsedParameter param : message.Parameters()) {
      os << sep << TypeName(module, param.Type()) << " p" << param.Index();
      sep = ", ";
    }

    os << sep << "bool added) {\n";
    os.PushIndent();
    os << os.Indent() << "if (logger) {\n";
    os.PushIndent();
    os << os.Indent() << "logger->" << message.Name() << "_" << message.Arity();
    sep = "(";
    for (ParsedParameter param : message.Parameters()) {
      os << sep;
      if (param.Type().IsReferentiallyTransparent(module, Language::kCxx)) {
        os << "p" << param.Index();
      } else {
        os << "std::move(p" << param.Index() << ")";
      }
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

    auto sep = "(";
    for (ParsedParameter param : message.Parameters()) {
      os << sep << TypeName(module, param.Type()) << " p" << param.Index();
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

    auto sep = "(";
    for (ParsedParameter param : message.Parameters()) {
      os << sep << TypeName(module, param.Type()) << " p" << param.Index();
      sep = ", ";
    }

    os << sep << "bool added) final {\n";
    os.PushIndent();
    os << os.Indent() << "if (logger) {\n";
    os.PushIndent();
    os << os.Indent() << "logger->" << message.Name() << "_" << message.Arity();
    sep = "(";
    for (ParsedParameter param : message.Parameters()) {
      os << sep;
      if (param.Type().IsReferentiallyTransparent(module, Language::kCxx)) {
        os << "p" << param.Index();
      } else {
        os << "std::move(p" << param.Index() << ")";
      }
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
}

}  // namespace cxx
}  // namespace hyde
