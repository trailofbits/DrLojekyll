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

static void DeclareQuery(ParsedQuery query, OutputStream &os) {
  ParsedDeclaration decl(query);


  os << "\n\n"
     << os.Indent() << "::grpc::Status Query_" << query.Name() << "_"
     << decl.BindingPattern() << "(\n";
  os.PushIndent();
  os << os.Indent() << "::grpc::ServerContext *context,\n"
     << os.Indent() << "const flatbuffers::grpc::Message<"
     << query.Name() << "_"  << decl.BindingPattern() << "> *request,\n";

  if (AllParametersAreBound(decl)) {
    os << os.Indent() << "flatbuffers::grpc::Message<"
       << query.Name() << "_" << query.Arity() << "> *response";
  } else {
    os << os.Indent() << "::grpc::ServerWriter< flatbuffers::grpc::Message<"
       << "flatbuffers::grpc::Message<"
       << query.Name() << "_" << query.Arity() << ">> *writer";
  }

  os << ")";

  os.PopIndent();
}

static void DeclareServiceMethods(const std::vector<ParsedQuery> &queries,
                                  OutputStream &os) {

  os << os.Indent() << "virtual ~DatabaseService(void) = default;";

  for (ParsedQuery query : queries) {
    DeclareQuery(query, os);
    os << " final;";
  }
  os << "\n\n"
     << os.Indent() << "::grpc::Status Update(\n";
  os.PushIndent();
  os << os.Indent() << "::grpc::ServerContext *context,\n"
     << os.Indent() << "::grpc::ServerReaderWriter<flatbuffers::grpc::Message<OutputMessage>,\n"
     << os.Indent() << "                           flatbuffers::grpc::Message<InputMessage>> *stream) final;\n";
  os.PopIndent();
}

static void DefineDatabaseLog(ParsedModule module,
                              const std::vector<ParsedMessage> &messages,
                              OutputStream &os) {
  os << "class FlatBufferMessageBuilder final {\n";
  os.PushIndent();
  os << os.Indent() << "private:\n";
  os.PushIndent();
  os << os.Indent() << "flatbuffers::grpc::MessageBuilder mb;";

  // Create vectors for holding offsets.
  bool has_differential = false;
  for (ParsedMessage message : messages) {
    if (!message.IsPublished()) {
      continue;
    }

    os << "\n"
       << os.Indent() << "std::vector<flatbuffers::Offset<Message_" << message.Name()
       << "_" << message.Arity() << ">> " << message.Name() << "_"
       << message.Arity() << "_added;";

    if (message.IsDifferential()) {
      has_differential = true;
      os << "\n"
         << os.Indent() << "std::vector<flatbuffers::Offset<Message_" << message.Name()
         << "_" << message.Arity() << ">> " << message.Name() << "_"
         << message.Arity() << "_removed;";
    }
  }

  os << "\n" << os.Indent() << "has_added{false};";
  if (has_differential) {
    os << "\n" << os.Indent() << "has_removed{false};";
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
  os << os.Indent() << "}\n\n"
     << "if (has_added) {\n";
  os.PushIndent();

  for (ParsedMessage message : messages) {
    if (!message.IsPublished()) {
      continue;
    }
  }
  os.PopIndent();
  os << os.Indent() << "}\n";

  if (has_differential) {
    os << os.Indent() << "if (has_removed) {\n";
    os.PushIndent();

    os.PopIndent();
    os << os.Indent() << "}\n";
  }
  // Define the message logging function for each message.
  for (ParsedMessage message : messages) {
    if (!message.IsPublished()) {
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

    os << sep << "bool added) {\n";
    os.PushIndent();
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

}  // namespace

// Emits C++ code for the given program to `os`.
void GenerateServiceCode(const Program &program, OutputStream &os) {
  os << "/* Auto-generated file */\n\n"
     << "#pragma once\n\n"
     << "#include <cstdlib>\n"
     << "#include <cstdio>\n"
     << "#include <cstring>\n"
     << "#include <sstream>\n"
     << "#include <drlojekyll/Runtime/Runtime.h>\n\n";

  const auto module = program.ParsedModule();
  const auto db_name = module.DatabaseName();

  std::string file_name = "datalog";
  std::string ns_name;
  std::string ns_name_prefix;
  if (db_name) {
    file_name = db_name->NameAsString();
    ns_name = file_name;
    ns_name_prefix = ns_name + "::";
  }

  // Include auto-generated files.
  os << "#include <grpcpp/grpcpp.h>\n"
     << "#include <flatbuffers/flatbuffers.h>\n"
     << "#include \"" << file_name << "_generated.h\"\n"
     << "#include \"" << file_name << ".grpc.fb.h\"\n"
     << "#include \"" << file_name << ".interface.h\"\n"
     << "#include \"" << file_name << ".db.h\"\n\n";

  auto queries = Queries(module);
  auto messages = Messages(module);

  // Output prologue code.
  auto inlines = Inlines(module, Language::kCxx);
  for (ParsedInline code : inlines) {
    if (code.IsPrologue()) {
      os << code.CodeToInline() << "\n\n";
    }
  }

  if (!ns_name.empty()) {
    os << "namespace " << ns_name << " {\n\n";
  }

  DefineDatabaseLog(module, messages, os);

  os << "class DatabaseService final : public Database::Service {\n";
  os.PushIndent();
  os << os.Indent() << "public:\n";
  os.PushIndent();
  DeclareServiceMethods(queries, os);
  os.PopIndent();  // public
  os.PopIndent();
  os << "};\n\n";  // DatabaseImpl

  if (!ns_name.empty()) {
    os << "}  // namespace " << ns_name << "\n\n";
  }

  // Output epilogue code.
  for (ParsedInline code : inlines) {
    if (code.IsEpilogue()) {
      os << code.CodeToInline() << "\n\n";
    }
  }

  os << "extern \"C\" int main(int argc, char *argv[]) {\n";
  os.PushIndent();

  // Start with some YOLO-style argument parsing using `sscanf`.
  os << os.Indent() << "std::stringstream ss;\n"
     << os.Indent() << "for (auto i = 0; i < argc; ++i) {\n";
  os.PushIndent();
  os << os.Indent() << "ss << ' ' << argv[i];\n";
  os.PopIndent();
  os << os.Indent() << "}\n"
     << os.Indent() << "std::string args = ss.str();\n"
     << os.Indent() << "args.resize(args.size() + 10);\n"
     << os.Indent() << "const char *args_str = args.c_str();\n"
     << os.Indent() << "std::string host(\"localhost\");\n"
     << os.Indent() << "int host_len = 9;  // `localhost`\n"
     << os.Indent() << "unsigned long port = 50052u;\n"

  // Parse the host name.
     << os.Indent() << "if (sscanf(args_str, \"--host %*s\", &(host[0]), &host_len) == 2) {\n";
  os.PushIndent();
  os << os.Indent() << "host.resize(static_cast<unsigned>(host_len));\n";
  os.PopIndent();
  os << os.Indent() << "}\n"

  // Parse the port.
     << os.Indent() << "(void) sscanf(args_str, \"--port %lu\", &port);\n"
     << os.Indent() << "std::stringstream address_ss;\n"
     << os.Indent() << "address_ss << host << ':' << port;\n"

     << os.Indent() << ns_name_prefix << "DatabaseService service;\n"

  // Build a gRPC server builder, configuring it with host/port.
     << os.Indent() << "grpc::ServerBuilder builder;\n"
     << os.Indent() << "builder.SetMaxReceiveMessageSize(std::numeric_limits<int>::max());\n"
     << os.Indent() << "builder.SetCompressionAlgorithmSupportStatus(GRPC_COMPRESS_GZIP, true);\n"
     << os.Indent() << "builder.SetCompressionAlgorithmSupportStatus(GRPC_COMPRESS_STREAM_GZIP, true);\n"
     << os.Indent() << "builder.SetDefaultCompressionAlgorithm(GRPC_COMPRESS_GZIP);\n"
     << os.Indent() << "builder.AddListeningPort(address_ss.str(), grpc::InsecureServerCredentials());\n"
     << os.Indent() << "builder.RegisterService(&service);\n"

  // Build the actual server.
     << os.Indent() << "auto server = builder.BuildAndStart();\n";

  os << os.Indent() << "return EXIT_SUCCESS;\n";
  os.PopIndent();
  os << "}\n\n";
}

}  // namespace cxx
}  // namespace hyde
