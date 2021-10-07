#include <drlojekyll/CodeGen/CodeGen.h>
#include <drlojekyll/ControlFlow/Format.h>
#include <drlojekyll/ControlFlow/Program.h>
#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Lex/Format.h>
#include <drlojekyll/Parse/Format.h>
#include <drlojekyll/Parse/ModuleIterator.h>

#include <algorithm>
#include <vector>

#include "Util.h"

namespace hyde {
namespace cxx {
namespace {

// Determines if all parameteres to a declaration are `bound`-attributed.
static bool AllParametersAreBound(ParsedDeclaration decl) {
  auto all_bound = true;
  for (ParsedParameter param : decl.Parameters()) {
    if (param.Binding() != ParameterBinding::kBound) {
      all_bound = false;
      break;
    }
  }
  return all_bound;
}

// Define the `Build` method of the `DatalogMessageBuilder` class, which
// goes and packages up all messages into flatbuffer vectors and into
// added/removed messages. Normally, the offsets to the messages-to-be-published
// are held in `std::vector`s.
static void DefineBuilderBuilder(const std::vector<ParsedMessage> &messages,
                                 OutputStream &os) {
  auto has_added = false;
  auto has_removed = false;
  for (ParsedMessage message : messages) {
    if (message.IsReceived()) {
      has_added = true;
      if (message.IsDifferential()) {
        has_removed = true;
        break;
      }
    }
  }

  os << os.Indent() << "flatbuffers::grpc::Message<InputMessage> Build(void) {\n";
  os.PushIndent();

  auto do_message = [&os] (ParsedMessage message, const char *suffix) {
    os << os.Indent() << "flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<Message_"
       << message.Name() << "_" << message.Arity() << ">>> "
       << message.Name() << "_" << message.Arity() << suffix << "_offset;\n"
       << os.Indent() << "if (!" << message.Name() << "_"
       << message.Arity() << suffix << ".empty()) {\n";
    os.PushIndent();

    os << os.Indent() << "" << message.Name() << "_"
       << message.Arity() << suffix << "_offset = mb.CreateVector<Message_"
       << message.Name() << "_" << message.Arity() << ">("
       << message.Name() << "_" << message.Arity() << suffix << ".data(), "
       << message.Name() << "_" << message.Arity() << suffix << ".size());\n"
       << os.Indent() << message.Name() << "_"
       << message.Arity() << suffix << ".clear();\n";
    os.PopIndent();
    os << os.Indent() << "}\n";
  };

  if (has_added) {
    os << os.Indent() << "flatbuffers::Offset<AddedInputMessage> added_offset;\n";
    if (has_removed) {
      os << os.Indent() << "flatbuffers::Offset<RemovedInputMessage> removed_offset;\n";
    }

    os << os.Indent() << "if (has_added) {\n";
    os.PushIndent();

    for (ParsedMessage message : messages) {
      if (message.IsReceived()) {
        do_message(message, "_added");
      }
    }
    os << os.Indent() << "added_offset = CreateAddedInputMessage(mb";
    for (ParsedMessage message : messages) {
      if (message.IsReceived()) {
        os << ", " << message.Name() << "_" << message.Arity() << "_added_offset";
      }
    }
    os << ");\n";

    os.PopIndent();
    os << os.Indent() << "}\n";  // has_added
  }

  if (has_removed) {
    os << os.Indent() << "if (has_removed) {\n";
    os.PushIndent();
    for (ParsedMessage message : messages) {
      if (message.IsReceived() && message.IsDifferential()) {
        do_message(message, "_removed");
      }
    }
    os << os.Indent() << "removed_offset = CreateRemovedInputMessage(mb";
    for (ParsedMessage message : messages) {
      if (message.IsReceived()) {
        os << ", " << message.Name() << "_" << message.Arity()
           << "_removed_offset";
      }
    }
    os << ");\n";
    os.PopIndent();
    os << os.Indent() << "}\n";  // has_removed
  }

  os << os.Indent() << "has_added = false;\n";
  if (has_removed) {
    os << os.Indent() << "has_removed = false;\n";
  }
  os << os.Indent() << "mb.Finish(CreateInputMessage(mb";
  if (has_added) {
    os << ", added_offset";
  }
  if (has_removed) {
    os << ", removed_offset";
  }
  os << "));\n"
     << os.Indent() << "return mb.ReleaseMessage<InputMessage>();\n";
  os.PopIndent();
  os << os.Indent() << "}";
}

// Define the `DatalogMessageBuilder` class, which has one method per
// received message. The role of this message builder is to accumulate
// messages into a flatbuffer to be sent to the backend.
static void DefineMessageBuilder(ParsedModule module,
                                 const std::vector<ParsedMessage> &messages,
                                 OutputStream &os) {

  os << "class DatalogMessageBuilder final {\n";
  os.PushIndent();
  os << os.Indent() << "private:\n";
  os.PushIndent();
  os << os.Indent() << "flatbuffers::grpc::MessageBuilder mb;";

  // Create vectors for holding offsets.
  bool has_differential = false;
  for (ParsedMessage message : messages) {
    if (!message.IsReceived()) {
      continue;
    }

    auto declare_vec = [&] (const char *suffix) {
      os << "\n"
         << os.Indent() << "std::vector<flatbuffers::Offset<Message_" << message.Name()
         << "_" << message.Arity() << ">> " << message.Name() << "_"
         << message.Arity() << suffix << ";";
    };

    declare_vec("_added");

    if (message.IsDifferential()) {
      has_differential = true;
      declare_vec("_removed");
    }
  }

  os << "\n" << os.Indent() << "bool has_added{false};";
  if (has_differential) {
    os << "\n" << os.Indent() << "bool has_removed{false};";
  }

  os.PopIndent();  // private
  os << "\n\n"
     << os.Indent() << "public:";
  os.PushIndent();

  // Define a function that builds up the flatbuffer message and clears out
  // all other empty buffers.
  os << "\n\n"
     << os.Indent() << "inline bool HasAnyMessages(void) const noexcept {\n";
  os.PushIndent();
  os << os.Indent() << "return has_added";
  if (has_differential) {
    os << " || has_removed";
  }
  os << ";\n";
  os.PopIndent();
  os << os.Indent() << "}\n\n";

  DefineBuilderBuilder(messages, os);

  // Define the message logging function for each message.
  for (ParsedMessage message : messages) {
    if (!message.IsReceived()) {
      continue;
    }

    ParsedDeclaration decl(message);
    os << "\n\n"
       << os.Indent() << "void " << message.Name() << "_" << message.Arity();

    auto sep = "(";
    for (auto param : decl.Parameters()) {
      os << sep;
      if (param.Type().IsReferentiallyTransparent(module, Language::kCxx)) {
        os << TypeName(module, param.Type()) << " ";
      } else {
        os << "const " << TypeName(module, param.Type()) << " &";
      }
      os << param.Name();
      sep = ", ";
    }

    if (message.IsDifferential()) {
      os << sep << "bool added=true) {\n";
      os.PushIndent();

    } else {
      os << ") {\n";
      os.PushIndent();
      os << os.Indent() << "constexpr auto added = true;\n";
    }

    os << os.Indent() << "auto offset = CreateMessage_"
       << message.Name() << "_" << message.Arity() << "(mb";

    for (auto param : decl.Parameters()) {
      os << ", " << param.Name();
    }

    os << ");\n"
       << os.Indent() << "if (added) {\n";
    os.PushIndent();

    os << os.Indent() << "has_added = true;\n"
       << os.Indent() << message.Name() << "_"
       << message.Arity() << "_added.emplace_back(std::move(offset));\n";

    os.PopIndent();
    os << os.Indent() << "}";
    if (message.IsDifferential()) {
      os << " else {\n";
      os.PushIndent();

      os << os.Indent() << "has_removed = true;\n"
         << os.Indent() << message.Name() << "_"
         << message.Arity() << "_removed.emplace_back(std::move(offset));\n";

      os.PopIndent();
      os << os.Indent() << "}\n";
    } else {
      os << "\n";
    }

    os.PopIndent();
    os << os.Indent() << "}";
  }

  os.PopIndent();  // public
  os.PopIndent();
  os << "\n};\n\n";
}

//// Define the `DatalogMessageVisitor` class, which has one method per
//// received message. The role of this message visitor is to call methods for
//// each received message.
//static void DefineMessageVisitor(ParsedModule module,
//                                 const std::vector<ParsedMessage> &messages,
//                                 OutputStream &os) {
//
//  os << "class DatalogMessageVisitor final {\n";
//  os.PushIndent();
//  os << os.Indent() << "public:\n";
//  os.PushIndent();
//
//  // Create vectors for holding offsets.
//  bool has_differential = false;
//  for (ParsedMessage message : messages) {
//    if (!message.IsPublished()) {
//      continue;
//    }
//
//    ParsedDeclaration decl(message);
//
//    os << os.Indent() << "inline void " << decl.Name() << "_" << decl.Arity()
//       << "(std::shared_ptr<Message_" << decl.Name() << "_"
//       << decl.Arity() << ">";
//
//    if (message.IsDifferential()) {
//      os << ", bool added";
//    }
//    os << ") {}\n";
//  }
//}

static void DeclareQuery(ParsedModule module, ParsedQuery query,
                         OutputStream &os, const char *prefix) {
  ParsedDeclaration decl(query);
  os << os.Indent();

  if (AllParametersAreBound(decl)) {
    os << "std::shared_ptr<" << decl.Name() << "_" << decl.Arity()
       << "> ";
  } else {

    os << "::hyde::rt::ClientResultStream<"
       << decl.Name() << "_" << decl.Arity() << "> ";
  }

  os << prefix << decl.Name() << "_"
     << decl.BindingPattern();

  auto sep = "(";
  for (ParsedParameter param : decl.Parameters()) {
    if (param.Binding() == ParameterBinding::kBound) {
      TypeLoc type = param.Type();
      os << sep << TypeName(module, type) << " " << param.Name();
      sep = ", ";
    }
  }

  os << ") const";
}

void GenerateClientHeader(Program program, ParsedModule module,
                          const std::string &file_name,
                          const std::string &ns_name,
                          const std::string &ns_name_prefix,
                          const std::vector<ParsedQuery> &queries,
                          const std::vector<ParsedMessage> &messages,
                          bool has_inputs, bool has_outputs,
                          bool has_removed_inputs, bool has_removed_outputs,
                          OutputStream &os) {
  os << "/* Auto-generated file */\n\n"
     << "#pragma once\n\n"
     << "#include <cstddef>\n"
     << "#include <functional>\n"
     << "#include <memory>\n"
     << "#include <string>\n"
     << "#include <vector>\n\n"
     << "#include <flatbuffers/flatbuffers.h>\n"
     << "#include <flatbuffers/grpc.h>\n"
     << "#include <drlojekyll/Runtime/ClientConnection.h>\n"
     << "#include <drlojekyll/Runtime/ClientResultStream.h>\n"
     << "#include \"" << file_name << "_generated.h\"\n\n";

  if (!ns_name.empty()) {
    os << "namespace " << ns_name << " {\n\n";
  }

  // Declare the message builder, which accumulates messages for publication.
  DefineMessageBuilder(module, messages, os);

  // Declare the client interface to the database.
  os << "class DatalogClientImpl;\n"
     << "class DatalogClient final : public hyde::rt::ClientConnection {\n";
  os.PushIndent();
  os << os.Indent() << "private:\n";
  os.PushIndent();

  for (ParsedQuery query : queries) {
    ParsedDeclaration decl(query);
    os << os.Indent() << "const grpc::internal::RpcMethod method_Query_"
       << decl.Name() << "_" << decl.BindingPattern()
       << ";\n";
  }

  os << os.Indent() << "const grpc::internal::RpcMethod method_Publish;\n"
     << os.Indent() << "const grpc::internal::RpcMethod method_Subscribe;\n\n";

  os.PopIndent();  // private

  os << os.Indent() << "public:\n";
  os.PushIndent();

  os << os.Indent() << "DatalogClient(const DatalogClient &) = delete;\n"
     << os.Indent() << "DatalogClient(DatalogClient &&) noexcept = delete;\n"
     << os.Indent() << "DatalogClient &operator=(const DatalogClient &) = delete;\n"
     << os.Indent() << "DatalogClient &operator=(DatalogClient &&) noexcept = delete;\n\n"
     << os.Indent() << "~DatalogClient(void);\n"
     << os.Indent() << "explicit DatalogClient(const std::shared_ptr<grpc::Channel> &channel_);\n\n";

  // Print out methods for each query.
  for (ParsedQuery query : queries) {
    DeclareQuery(module, query, os, "");
    os << ";\n\n";
  }

  os << os.Indent() << "bool Publish(DatalogMessageBuilder &messages) const;\n"
     << os.Indent() << "::hyde::rt::ClientResultStream<OutputMessage> Subscribe(const std::string &client_name) const;\n";

  os.PopIndent();  // public
  os.PopIndent();  // class
  os << "};\n\n";

  if (!ns_name.empty()) {
    os << "}  // namespace " << ns_name << "\n\n";
  }
}

void GenerateClientImpl(Program program, ParsedModule module,
                        const std::string &file_name,
                        const std::string &ns_name,
                        const std::string &ns_name_prefix,
                        const std::vector<ParsedQuery> &queries,
                        const std::vector<ParsedMessage> &messages,
                        bool has_inputs, bool has_outputs,
                        bool has_removed_inputs, bool has_removed_outputs,
                        OutputStream &os) {
  os << "/* Auto-generated file */\n\n"
     << "#include <grpcpp/grpcpp.h>\n"
     << "#include <flatbuffers/flatbuffers.h>\n"
     << "#include \"" << file_name << "_generated.h\"\n"
     << "#include \"" << file_name << ".grpc.fb.h\"\n"
     << "#include \"" << file_name << ".client.h\"\n\n";

  if (!ns_name.empty()) {
    os << "namespace " << ns_name << " {\n\n";
  }

  os << "DatalogClient::~DatalogClient(void) {}\n\n"
     << "DatalogClient::DatalogClient(const std::shared_ptr<grpc::Channel> &channel_)\n";
  os.PushIndent();
  os << os.Indent() << ": hyde::rt::ClientConnection(channel_)";

  for (ParsedQuery query : queries) {
    ParsedDeclaration decl(query);
    os << ",\n"
       << os.Indent() << "  method_Query_"
       << decl.Name() << "_" << decl.BindingPattern()
       << "(\"/" << file_name << ".Datalog/Query_"
       << decl.Name() << "_" << decl.BindingPattern()
       << "\", ";

    if (AllParametersAreBound(decl)) {
      os << "::grpc::internal::RpcMethod::NORMAL_RPC, channel_)";
    } else {
      os << "::grpc::internal::RpcMethod::SERVER_STREAMING, channel_)";
    }
  }

  os << ",\n"
     << os.Indent() << "  method_Publish(\"/" << file_name
     << ".Datalog/Publish\", ::grpc::internal::RpcMethod::NORMAL_RPC, channel_)"
     << ",\n"
     << os.Indent() << "  method_Subscribe(\"/" << file_name
     << ".Datalog/Subscribe\", ::grpc::internal::RpcMethod::SERVER_STREAMING, channel_) {}\n\n";

  os.PopIndent();

  // Print out methods for each query.
  for (ParsedQuery query : queries) {
    ParsedDeclaration decl(query);
    DeclareQuery(module, query, os, "DatalogClient::");
    os << " {\n";
    os.PushIndent();
    os << os.Indent() << "flatbuffers::grpc::MessageBuilder mb;\n";

    // TODO(pag): Eventually create flatbuffer offsets for non-trivial
    //            types.
    //  for (ParsedParameter param : decl.Parameters()) {
    //    switch (param.Type().UnderlyingKind()) {
    //
    //    }
    //  }

    os << os.Indent() << "mb.Finish(Create" << query.Name() << "_"
       << decl.Arity() << "(mb";

    for (ParsedParameter param : decl.Parameters()) {
      if (param.Binding() == ParameterBinding::kBound) {
        os << ", " << param.Name();
      }
    }

    os << "));\n"
       << os.Indent() << "PumpActiveStreams();\n"
       << os.Indent() << "auto message = mb.ReleaseMessage<"
       << query.Name() << "_" << decl.Arity() << ">();\n";

    if (AllParametersAreBound(decl)) {
      os << os.Indent() << "return this->CallResult<"
         << decl.Name() << "_" << decl.Arity()
         << ">(method_Query_"
         << decl.Name() << "_" << decl.BindingPattern()
         << ", message.BorrowSlice());\n";

    } else {
      os << os.Indent() << "return ::hyde::rt::ClientResultStream<"
         << decl.Name() << "_" << decl.Arity()
         << ">(this->impl, method_Query_"
         << decl.Name() << "_" << decl.BindingPattern()
         << ", message.BorrowSlice());\n";
    }
    os.PopIndent();
    os << "}\n\n";
  }

  os << "bool DatalogClient::Publish(DatalogMessageBuilder &messages) const {\n";
  os.PushIndent();
  os << os.Indent() << "if (messages.HasAnyMessages()) {\n";
  os.PushIndent();
  os << os.Indent() << "auto message = messages.Build();\n"
     << os.Indent() << "return this->hyde::rt::ClientConnection::Publish(method_Publish, message.BorrowSlice());\n";
  os.PopIndent();  // if
  os << os.Indent() << "}\n"
     << os.Indent() << "return false;\n";
  os.PopIndent();  // Publish
  os << "}\n\n"
     << "::hyde::rt::ClientResultStream<OutputMessage> DatalogClient::Subscribe(const std::string &client_name) const {\n";
  os.PushIndent();

  os << os.Indent() << "flatbuffers::grpc::MessageBuilder mb;\n"
     << os.Indent() << "mb.Finish(CreateClient(mb, mb.CreateString(client_name)));\n"
     << os.Indent() << "PumpActiveStreams();\n"
     << os.Indent() << "auto message = mb.ReleaseMessage<Client>();\n"
     << os.Indent() << "return ::hyde::rt::ClientResultStream<OutputMessage>(this->impl, method_Subscribe, message.BorrowSlice());\n";

  os.PopIndent();  // Subscribe
  os << "}\n\n";

  if (!ns_name.empty()) {
    os << "}  // namespace " << ns_name << "\n\n";
  }
}

}  // namespace

// Emits C++ RPC code for the given program to `header_os` and `impl_os`.
void GenerateClientCode(const Program &program, OutputStream &header_os,
                        OutputStream &impl_os) {
  const auto module = program.ParsedModule();
  const auto queries = Queries(module);
  const auto messages = Messages(module);

  std::string file_name = "datalog";
  std::string ns_name;
  std::string ns_name_prefix;
  if (const auto db_name = module.DatabaseName()) {
    ns_name = db_name->NameAsString();
    file_name = ns_name;
    ns_name_prefix = ns_name + "::";
  }

  auto has_inputs = false;
  auto has_outputs = false;
  auto has_removed_inputs = false;
  auto has_removed_outputs = false;
  for (auto message : messages) {
    if (message.IsDifferential()) {
      if (message.IsPublished()) {
        has_removed_outputs = true;
      } else if (message.IsReceived()) {
        has_removed_inputs = true;
      } else {
        assert(false);
      }
    } else if (message.IsPublished()) {
      has_outputs = true;
    } else if (message.IsReceived()) {
      has_inputs = true;
    }
  }

  GenerateClientHeader(program, module, file_name, ns_name, ns_name_prefix,
                       queries, messages, has_inputs, has_outputs,
                       has_removed_inputs, has_removed_outputs, header_os);
  GenerateClientImpl(program, module, file_name, ns_name, ns_name_prefix,
                     queries, messages, has_inputs, has_outputs,
                     has_removed_inputs, has_removed_outputs, impl_os);
}

}  // namespace cxx
}  // namespace hyde
