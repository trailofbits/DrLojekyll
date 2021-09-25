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

// Define structures for holding the messages that need to be sent back to
// clients.
static void DefineOutboxes(OutputStream &os) {
  os << "struct Outbox {\n";
  os.PushIndent();
  os << os.Indent() << "Outbox **prev_next{nullptr};\n"
     << os.Indent() << "Outbox *next{nullptr};\n"
     << os.Indent() << "moodycamel::LightweightSemaphore messages_sem;\n"
     << os.Indent() << "std::mutex messages_lock;\n"
     << os.Indent() << "std::vector<std::shared_ptr<flatbuffers::grpc::Message<OutputMessage>>> messages;\n\n"
     << os.Indent() << "inline Outbox(void) {\n";
  os.PushIndent();
  os << os.Indent() << "messages.reserve(4u);\n";
  os.PopIndent();
  os << os.Indent() << "}\n";  // constructor.
  os.PopIndent();
  os << "};\n\n"
     << "static Outbox *gFirstOutbox{nullptr};\n"
     << "static std::mutex gOutboxesLock;\n\n";
}

// Declare a `Query_*` method on the service, which corresponds with a
// `#query` in the code. Query methods are suffixed by the binding parameters.
//
// A query whose parameters are all bound is treated as an existence check,
// and returns a single message, or absent that, a cancelled status.
//
// A query that has at least one free parameter streams back the found tuples
// to the requester.
static void DeclareQuery(ParsedQuery query, OutputStream &os, bool out_of_line) {
  ParsedDeclaration decl(query);

  os << "\n\n"
     << os.Indent() << "::grpc::Status ";
  if (out_of_line) {
    os << "DatabaseService::";
  }
  os << "Query_" << query.Name() << "_"
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

// Declare the prototypes of all query methods on the `DatabaseService`
// class. We'll define the methods out-of-line.
static void DeclareServiceMethods(const std::vector<ParsedQuery> &queries,
                                  OutputStream &os) {

  os << os.Indent() << "virtual ~DatabaseService(void) = default;";

  for (ParsedQuery query : queries) {
    DeclareQuery(query, os, false);
    os << " final;";
  }
  os << "\n\n"
     << os.Indent() << "::grpc::Status Publish(\n";
  os.PushIndent();
  os << os.Indent() << "::grpc::ServerContext *context,\n"
     << os.Indent() << "flatbuffers::grpc::Message<InputMessage> *request,\n"
     << os.Indent() << "flatbuffers::grpc::Message<Empty> *response) final;";
  os.PopIndent();

  os << "\n\n"
     << os.Indent() << "::grpc::Status Subscribe(\n";
  os.PushIndent();
  os << os.Indent() << "::grpc::ServerContext *context,\n"
     << os.Indent() << "flatbuffers::grpc::Message<Empty> *request,\n"
     << os.Indent() << "::grpc::ServerWriter<flatbuffers::grpc::Message<OutputMessage>> *writer) final;\n";
  os.PopIndent();
}

static void DefineQuery(ParsedQuery query, OutputStream &os) {
  ParsedDeclaration decl(query);
  os << os.Indent() << "std::shared_lock<std::shared_mutex> locker(gDatabaseLock);\n"
     << os.Indent() << "auto status = grpc::GRPC_STATUS_NOT_FOUND;\n"
     << os.Indent() << "const auto num_generated = gDatabase." << query.Name()
     << "_" << decl.BindingPattern();

  auto sep = "(";
  auto has_free_params = false;
  for (ParsedParameter param : decl.Parameters()) {
    if (param.Binding() == ParameterBinding::kBound) {
      os << sep << param.Name();
      sep = ", ";
    } else if (param.Binding() == ParameterBinding::kFree) {
      has_free_params = true;
    }
  }

  os << sep;
  sep = "[&status, =] (";
  for (ParsedParameter param : decl.Parameters()) {
    os << sep << "auto p" << param.Index();
    sep = ", ";
  }
  os << ") -> bool {\n";
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
    os << ", p" << param.Index();
  }

  os << "));\n"
     << os.Indent() << "auto message = mb.ReleaseMessage<"
     << query.Name() << "_" << decl.Arity() << ">();\n";

  // If there are free parameters, then we're doing server-to-client streaming
  // using `writer`.
  if (has_free_params) {
    os << os.Indent() << "if (!writer->Write(message)) {\n";
    os.PushIndent();
    os << os.Indent() << "status = grpc::CANCELLED;\n"
       << os.Indent() << "return false;\n";
    os.PopIndent();
    os << os.Indent() << "} else {\n";
    os.PushIndent();
    os << os.Indent() << "status = grpc::Status::OK;\n"
       << os.Indent() << "return true;\n";
    os.PopIndent();
    os << os.Indent() << "}\n";

  // If there are not any free parameters, then we're sending back a message
  // to the client using `response`.
  } else {
    os << os.Indent() << "status = grpc::Status::OK;\n"
       << os.Indent() << "*response = std::move(message);\n"
       << os.Indent() << "return true;\n";
  }

  os.PopIndent();  // End of lambda to query callback.
  os << os.Indent() << "});\n";

  os << os.Indent() << "return grpc::Status(status, kQuery_"
     << query.Name() << "_" << query.Arity()
     << ");\n";
}

// Define the out-of-line method bodies for each of the `Query_*`methods.
static void DefineQueryMethods(const std::vector<ParsedQuery> &queries,
                               OutputStream &os) {
  if (queries.empty()) {
    return;
  }

  os << "\n";

  for (ParsedQuery query : queries) {
    ParsedDeclaration decl(query);
    if (decl.IsFirstDeclaration()) {
      os << "\nstatic const std::string kQuery_" << decl.Name()
         << "_" << decl.Arity() << "{\"" << decl.Name()
         << "_" << decl.Arity() << "\"};";
    }
  }

  for (ParsedQuery query : queries) {
    DeclareQuery(query, os, true);
    os << " {\n";
    os.PushIndent();
    DefineQuery(query, os);
    os.PopIndent();
    os << "}";
  }
}

// Define a method that clients invoke to subscribe to messages from the server.
static void DefineSubscribeMethod(OutputStream &os) {
  os << "\n\n::grpc::Status DatabaseService::Subscribe(\n";
  os.PushIndent();
  os << os.Indent() << "::grpc::ServerContext *context,\n"
     << os.Indent() << "flatbuffers::grpc::Message<Empty> *request,\n"
     << os.Indent() << "::grpc::ServerWriter<flatbuffers::grpc::Message<OutputMessage>> *writer) {\n\n"
     << os.Indent() << "alignas(64) Outbox outbox;\n"
     << os.Indent() << "alignas(64) std::vector<std::shared_ptr<flatbuffers::grpc::Message<OutputMessage>>> messages;\n"
     << os.Indent() << "messages.reserve(4u);\n\n"
     << os.Indent() << "{\n";
  os.PushIndent();
  os << os.Indent() << "std::unique_lock<std::mutex> locker(gOutboxesLock);\n"
     << os.Indent() << "outbox.next = gFirstOutbox;\n"
     << os.Indent() << "outbox.prev_next = &gFirstOutbox;\n"
     << os.Indent() << "if (gFirstOutbox) {\n";
  os.PushIndent();
  os << os.Indent() << "gFirstOutbox->prev_next = &(outbox.next);\n";
  os.PopIndent();
  os << os.Indent() << "}\n";  // gFirstOutbox
  os.PopIndent();
  os << os.Indent() << "}\n\n";  // Link it in.

  // Busy loop.
  os << os.Indent() << "for (auto failed = false; !failed && !context->IsCancelled(); ) {\n";
  os.PushIndent();
  os << os.Indent() << "if (outbox.messages_sem.waitMany()) {\n";
  os.PushIndent();
  os << os.Indent() << "std::unique_lock<std::mutex> locker(outbox.messages_lock);\n"
     << os.Indent() << "messages.swap(outbox.messages);\n";
  os.PopIndent();
  os << os.Indent() << "}\n\n"  // waitMany
     << os.Indent() << "for (const auto &message : messages) {\n";
  os.PushIndent();
  os << os.Indent() << "if (!writer->Write(*message)) {\n";
  os.PushIndent();
  os << os.Indent() << "failed = true;\n"
     << os.Indent() << "break;\n";
  os.PopIndent();
  os << os.Indent() << "}\n";  // Write
  os.PopIndent();
  os << os.Indent() << "}\n\n"  // for
     << os.Indent() << "messages.clear();\n";
  os.PopIndent();
  os << os.Indent() << "}\n\n";  // busy loop.

  // Unlink the stack-allocated `outbox`.
  os << os.Indent() << "{\n";
  os.PushIndent();
  os << os.Indent() << "std::unique_lock<std::mutex> locker(gOutboxesLock);\n"
     << os.Indent() << "*(outbox.prev_next) = outbox.next;\n";
  os.PopIndent();
  os << os.Indent() << "}\n"  // End of unlink.
     << os.Indent() << "return grpc::Status::OK;\n";
  os.PopIndent();
  os << "}";  // End of Subscribe.
}

// Define a method that clients invoke to publish messages to the server.
static void DefinePublishMethod(const std::vector<ParsedMessage> &messages,
                                OutputStream &os) {
  os << "\n\n::grpc::Status DatabaseService::Publish(\n";
  os.PushIndent();
  os << os.Indent() << "::grpc::ServerContext *context,\n"
     << os.Indent() << "flatbuffers::grpc::Message<InputMessage> *request,\n"
     << os.Indent() << "flatbuffers::grpc::Message<Empty> *response) {\n\n"
     << os.Indent() << "const auto req_msg = request->GetRoot();\n"
     << os.Indent() << "if (!req_msg) {\n";
  os.PushIndent();
  os << os.Indent() << "return grpc::Status::OK;\n";
  os.PopIndent();

  os << os.Indent() << "}\n\n"
     << os.Indent() << "auto input_msg = std::make_unique<DatabaseInputMessageType>();\n";

  bool has_differential = false;
  for (ParsedMessage message : messages) {
    if (message.IsReceived() && message.IsDifferential()) {
      has_differential = true;
      break;
    }
  }

  auto do_message = [&] (ParsedMessage message, const char *vector,
                         const char *method_prefix) {
    ParsedDeclaration decl(message);
    os << os.Indent() << "if (auto " << message.Name()
       << "_" << message.Arity() << " = " << vector << "->" << message.Name()
       << "_" << message.Arity() << "()) {\n";
    os.PushIndent();

    os << os.Indent() << "for (auto entry : *" << message.Name() << "_"
       << message.Arity() << ") {\n";
    os.PushIndent();
    os << os.Indent() << "input_msg->" << method_prefix << message.Name() << "_"
       << message.Arity();
    auto sep = "(";
    for (ParsedParameter param : decl.Parameters()) {
      os << sep << "entry->" << param.Name() << "()";
      sep = ", ";
    }
    os << ");\n";
    os.PopIndent();
    os << os.Indent() << "}\n";  // vector iteration.

    os.PopIndent();
    os << os.Indent() << "}\n";  // vector pointer.
  };

  // Handle added messages.
  os << os.Indent() << "if (auto added = req_msg->added()) {\n";
  os.PushIndent();
  for (ParsedMessage message : messages) {
    if (message.IsReceived()) {
      do_message(message, "added", "produce_");
    }
  }
  os.PopIndent();
  os << os.Indent() << "}\n";  // added

  // Handle removed messages.
  if (has_differential) {
    os << os.Indent() << "if (auto removed = req_msg->removed()) {\n";
    os.PushIndent();
    for (ParsedMessage message : messages) {
      if (message.IsReceived() && message.IsDifferential()) {
        do_message(message, "removed", "retract_");
      }
    }
    os.PopIndent();
    os << os.Indent() << "}\n";  // added
  }

  os << os.Indent() << "if (input_msg->Size()) {\n";
  os.PushIndent();
  os << os.Indent() << "std::unique_lock<std::mutex> locker(gInputMessagesLock);\n"
     << os.Indent() << "gInputMessages.push_back(std::move(input_msg));\n"
     << os.Indent() << "gInputMessagesSemaphore.signal();\n";
  os.PopIndent();
  os << os.Indent() << "}\n"
     << os.Indent() << "return grpc::Status::OK;\n";

  os.PopIndent();
  os << os.Indent() << "}";
}

// Define the `Build` method of the `FlatBufferMessageBuilder` class, which
// goes and packages up all messages into flatbuffer vectors and into
// added/removed messages. Normally, the offsets to the messages-to-be-published
// are held in `std::vector`s.
static void DefineDatabaseLogBuild(const std::vector<ParsedMessage> &messages,
                                   OutputStream &os) {

  auto has_differential = false;
  for (ParsedMessage message : messages) {
    if (message.IsPublished() && message.IsDifferential()) {
      has_differential = true;
    }
  }

  os << os.Indent() << "flatbuffers::grpc::Message<OutputMessage> Build(void) {\n";
  os.PushIndent();
  os << os.Indent() << "flatbuffers::Offset<AddedOutputMessage> added_offset;\n";
  if (has_differential) {
    os << os.Indent() << "flatbuffers::Offset<RemovedOutputMessage> removed_offset;\n";
  }

  os << os.Indent() << "if (has_added) {\n";
  os.PushIndent();

  auto do_message = [&os] (ParsedMessage message, const char *suffix) {
    os << os.Indent() << "flatbuffers::Offset<Message_"
       << message.Name() << "_" << message.Arity() << "> "
       << message.Name() << "_" << message.Arity() << "_added_offset;\n"
       << os.Indent() << "if (!" << message.Name() << "_"
       << message.Arity() << suffix << ".empty()) {\n";
    os.PushIndent();

    os << os.Indent() << "" << message.Name() << "_"
       << message.Arity() << suffix << "_offset = flatbuffers::CreateVector<Message_"
       << message.Name() << "_" << message.Arity() << ">("
       << message.Name() << "_" << message.Arity() << suffix << ".data(), "
       << message.Name() << "_" << message.Arity() << suffix << ".size());\n"
       << os.Indent() << message.Name() << "_"
       << message.Arity() << suffix << ".clear();\n";
    os.PopIndent();
    os << os.Indent() << "}\n";
  };

  for (ParsedMessage message : messages) {
    if (message.IsPublished()) {
      do_message(message, "_added");
    }
  }
  os << os.Indent() << "added_offset = CreateAddedOutputMessage(mb";
  for (ParsedMessage message : messages) {
    if (message.IsPublished()) {
      os << ", " << message.Name() << "_" << message.Arity() << "_added_offset";
    }
  }
  os << ");\n";

  os.PopIndent();
  os << os.Indent() << "}\n";  // has_added

  if (has_differential) {
    os << os.Indent() << "if (has_removed) {\n";
    os.PushIndent();
    for (ParsedMessage message : messages) {
      if (message.IsPublished() && message.IsDifferential()) {
        do_message(message, "_removed");
      }
    }
    os << os.Indent() << "removed_offset = CreateRemovedOutputMessage(mb";
    for (ParsedMessage message : messages) {
      if (message.IsPublished()) {
        os << ", " << message.Name() << "_" << message.Arity()
           << "_removed_offset";
      }
    }
    os << ");\n";
    os.PopIndent();
    os << os.Indent() << "}\n";  // has_removed
  }

  os << os.Indent() << "has_added = false;\n";
  if (has_differential) {
    os << os.Indent() << "has_removed = false;\n";
  }
  os << os.Indent() << "mb.Finish(CreateOutputMessage(mb, added_offset";
  if (has_differential) {
    os << ", removed_offset";
  }
  os << "));\n"
     << os.Indent() << "return mb.ReleaseMessage<OutputMessage>();\n";
  os.PopIndent();
  os << os.Indent() << "}";
}

// Define the `FlatBufferMessageBuilder` class, which has one method per
// published message. The role of this message builder is to accumulate
// messages into a flatbuffer to be published to all connected clients.
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

  DefineDatabaseLogBuild(messages, os);

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

// Defines some queues for reading/writing data.
static void DefineQueues(OutputStream &os) {

}

// Defines the function that runs the database.
static void DefineDatabaseThread(OutputStream &os) {
  // Make the main database thread.
  os << "static void DatabaseWriterThread(void) {\n";
  os.PushIndent();
  os << os.Indent() << "std::vector<std::unique_ptr<DatabaseInputMessageType>> inputs;\n"
     << os.Indent() << "inputs.reserve(128);\n"
     << os.Indent() << "while (true) {\n";
  os.PushIndent();
  os << os.Indent() << "if (gInputMessagesSemaphore.waitMany()) {\n";
  os.PushIndent();
  os << os.Indent() << "std::unique_lock<std::mutex> locker(gInputMessagesLock);\n"
     << os.Indent() << "inputs.swap(gInputMessagesLock);\n";
  os.PopIndent();
  os << os.Indent() << "}\n"  // waitMany
     << os.Indent() << "for (const auto &input : inputs) {\n";
  os.PushIndent();
  os << os.Indent() << "std::unique_lock<std::shared_mutex> locker(gDatabaseLock);\n"
     << os.Indent() << "input->Apply(gDatabase);\n";
  os.PopIndent();
  os << os.Indent() << "}\n"  // for
     << os.Indent() << "inputs.clear();\n"
     << os.Indent() << "if (gDatabaseLog.HasAnyMessages()) {\n";
  os.PushIndent();
  os << os.Indent() << "auto output = std::make_shared<flatbuffers::grpc::Message<OutputMessage>>(gDatabaseLog.Build());\n"
     << os.Indent() << "std::unique_lock<std::mutex> locker(gOutboxesLock);\n"
     << os.Indent() << "for (auto output = gFirstOutbox; outbox;) {\n";
  os.PushIndent();
  os << os.Indent() << "std::unique_lock<std::mutex> outbox_locker(outbox->messages_lock);\n"
     << os.Indent() << "outbox->messages.push_back(output);\n"
     << os.Indent() << "outbox->messages_sem.signal();\n"
     << os.Indent() << "output = outbox->next;\n";
  os.PopIndent();
  os << os.Indent() << "}\n";  // for
  os.PopIndent();
  os << os.Indent() << "}\n";  // HasAnyMessages
  os.PopIndent();
  os << os.Indent() << "}\n";  // while true
  os.PopIndent();
  os << "}\n\n";
}

}  // namespace

// Emits C++ code for the given program to `os`.
void GenerateServiceCode(const Program &program, OutputStream &os) {
  os << "/* Auto-generated file */\n\n"
     << "#pragma once\n\n"
     << "#include <algorithm>\n"
     << "#include <cstdlib>\n"
     << "#include <cstdio>\n"
     << "#include <cstring>\n"
     << "#include <iostream>\n"
     << "#include <memory>\n"
     << "#include <mutex>\n"
     << "#include <shared_mutex>\n"
     << "#include <sstream>\n"
     << "#include <string>\n"
     << "#include <vector>\n\n"
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
//     << "DEFINE_string(host, \"localhost\", \"Server host name\");\n";

  DefineOutboxes(os);
  DefineQueues(os);

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

  DefineOutboxes(os);

  DefineDatabaseLog(module, messages, os);

  // Define the main gRPC service class, and declare each of its methods.
  os << "class DatabaseService final : public Database::Service {\n";
  os.PushIndent();
  os << os.Indent() << "public:\n";
  os.PushIndent();
  DeclareServiceMethods(queries, os);
  os.PopIndent();  // public
  os.PopIndent();
  os << "};";  // DatabaseService

  os << "\n\n"
     << "using DatabaseStorageType = hyde::rt::StdStorage;\n"
     << "using DatabaseInputMessageType = DatabaseInputMessage<DatabaseStorageType>;\n"
     << "static std::vector<std::unique_ptr<DatabaseInputMessageType>> gInputMessages;\n"
     << "static std::mutex gInputMessagesLock;\n"
     << "static moodycamel::LightweightSemaphore gInputMessagesSemaphore;\n"
     << "static FlatBufferMessageBuilder gDatabaseLog;\n"
     << "static DatabaseStorageType gStorage;\n"
     << "static std::shared_mutex gDatabaseLock;\n"
     << "static DatabaseFunctors gFunctors;\n"
     << "static Database<DatabaseStorageType, FlatBufferMessageBuilder> gDatabase(\n"
     << "    gStorage, gDatabaseLog, gFunctors);\n";

  // Define the query methods out-of-line.
  DefineQueryMethods(queries, os);
  DefinePublishMethod(messages, os);
  DefineSubscribeMethod(os);

  os << "\n\n";

  DefineDatabaseThread(os);

  if (!ns_name.empty()) {
    os << "}  // namespace " << ns_name << "\n\n";
  }

  // Output epilogue code.
  for (ParsedInline code : inlines) {
    if (code.IsEpilogue()) {
      os << code.CodeToInline() << "\n\n";
    }
  }

  os << "extern \"C\" int main(int argc, const char *argv[]) {\n";
  os.PushIndent();

  // Make some vectors reasonably big to avoid allocations at runtime, and start
  // the database thread.
  os << os.Indent() << "gInputMessages.reserve(128);\n"
     << os.Indent() << "std::thread db_thread(" << ns_name_prefix << "DatabaseWriterThread);\n\n";
  // Default argument values.
  os << os.Indent() << "std::string host = \"localhost\";\n"
     << os.Indent() << "unsigned port = 50052u;\n"

  // Make a vector of arguments with a bit of fudge space.
     << os.Indent() << "std::vector<const char *> args;\n"
     << os.Indent() << "for (auto i = 0; i < argc; ++i) {\n";
  os.PushIndent();
  os << os.Indent() << "args.push_back(argv[i]);\n";

  os.PopIndent();
  os << os.Indent() << "}\n"
     << os.Indent() << "args.push_back(\"\");\n"

  // Parse the arguments.
     << os.Indent() << "for (auto i = 0ul; i < args.size() - 1ul; ) {\n";
  os.PushIndent();
  os << os.Indent() << "const auto arg = args[i++];\n"
     << os.Indent() << "const auto val = args[i++];\n"
     << os.Indent() << "     if (!strcmp(arg, \"--host\")) host = val;\n"
     << os.Indent() << "else if (!strcmp(arg, \"--port\")) port = static_cast<unsigned>(atol(val));\n"
     << os.Indent() << "else {\n";
  os.PushIndent();
  os << os.Indent() << "std::cerr << \"Unrecognized option: \" << arg << ' ' << val << std::endl;\n"
     << os.Indent() << "return EXIT_FAILURE;\n";
  os.PopIndent();
  os << os.Indent() << "}\n";
  os.PopIndent();
  os << os.Indent() << "}\n\n"
     << os.Indent() << "std::stringstream address_ss;\n"
     << os.Indent() << "address_ss << host << ':' << port;\n\n"
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
     << os.Indent() << "auto server = builder.BuildAndStart();\n"
     << os.Indent() << "server->Wait();\n"
     << os.Indent() << "db_thread.join();\n"
     << os.Indent() << "return EXIT_SUCCESS;\n";
  os.PopIndent();
  os << "}\n\n";
}

}  // namespace cxx
}  // namespace hyde
