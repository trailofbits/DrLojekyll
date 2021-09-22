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

//     << "#ifndef __DRLOJEKYLL_PROLOGUE_CODE_" << gClassName << "\n"
//     << "#  define __DRLOJEKYLL_PROLOGUE_CODE_" << gClassName << "\n";

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

  os << "class DatabaseService : public Database::Service {\n";
  os.PushIndent();
  os << "public:\n";
  os.PushIndent();
  os << os.Indent() << "virtual ~DatabaseImpl(void);\n";
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

  os << "return EXIT_SUCCESS;\n";
  os.PopIndent();
  os << "}\n\n";
}

}  // namespace cxx
}  // namespace hyde
