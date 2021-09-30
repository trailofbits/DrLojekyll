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
     << "#include <flatbuffers/flatbuffers.h>\n"
     << "#include \"" << file_name << "_generated.h\"\n\n";

  if (!ns_name.empty()) {
    os << "namespace " << ns_name << " {\n\n";
  }

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
     << "#include \"" << file_name << ".grpc.fb.h\"\n\n";

  if (!ns_name.empty()) {
    os << "namespace " << ns_name << " {\n\n";
  }

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
