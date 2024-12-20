// Copyright 2021, Trail of Bits. All rights reserved.

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

// Define structures for holding the messages that need to be sent back to
// clients.
static void DefineOutboxes(OutputStream &os) {
  os << "\n\nstruct Outbox {\n";
  os.PushIndent();
  os << os.Indent() << "Outbox **prev_next{nullptr};\n"
     << os.Indent() << "Outbox *next{nullptr};\n"
     << os.Indent() << "std::string name;\n"
     << os.Indent() << "hyde::rt::Semaphore messages_sem;\n"
     << os.Indent() << "std::mutex messages_lock;\n"
     << os.Indent() << "std::vector<std::shared_ptr<flatbuffers::grpc::Message<DatalogClientMessage>>> messages;\n\n"
     << os.Indent() << "inline Outbox(void) {\n";
  os.PushIndent();
  os << os.Indent() << "messages.reserve(4u);\n";
  os.PopIndent();
  os << os.Indent() << "}\n";  // constructor.
  os.PopIndent();
  os << "};\n\n"
     << "static Outbox *gFirstOutbox{nullptr};\n"
     << "static std::mutex gOutboxesLock;\n";
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
    os << "DatalogService::";
  }
  os << "Query_" << query.Name() << "_"
     << decl.BindingPattern() << "(\n";
  os.PushIndent();
  os << os.Indent() << "::grpc::ServerContext *context,\n"
     << os.Indent() << "const flatbuffers::grpc::Message<"
     << query.Name() << "_"  << decl.BindingPattern() << "> *request,\n";

  if (AllParametersAreBound(decl) || query.ReturnsAtMostOneResult()) {
    os << os.Indent() << "flatbuffers::grpc::Message<"
       << query.Name() << "_" << query.Arity() << "> *response";
  } else {
    os << os.Indent() << "::grpc::ServerWriter<flatbuffers::grpc::Message<"
       << query.Name() << "_" << query.Arity() << ">> *writer";
  }

  os << ")";

  os.PopIndent();
}

// Declare the prototypes of all query methods on the `DatalogService`
// class. We'll define the methods out-of-line.
static void DeclareServiceMethods(const std::vector<ParsedQuery> &queries,
                                  OutputStream &os) {

  os << os.Indent() << "virtual ~DatalogService(void) = default;";

  for (ParsedQuery query : queries) {
    DeclareQuery(query, os, false);
    os << " final;";
  }
  os << "\n\n"
     << os.Indent() << "::grpc::Status Publish(\n";
  os.PushIndent();
  os << os.Indent() << "::grpc::ServerContext *context,\n"
     << os.Indent() << "const flatbuffers::grpc::Message<DatalogServerMessage> *request,\n"
     << os.Indent() << "flatbuffers::grpc::Message<Empty> *response) final;";
  os.PopIndent();

  os << "\n\n"
     << os.Indent() << "::grpc::Status Subscribe(\n";
  os.PushIndent();
  os << os.Indent() << "::grpc::ServerContext *context,\n"
     << os.Indent() << "const flatbuffers::grpc::Message<Client> *request,\n"
     << os.Indent() << "::grpc::ServerWriter<flatbuffers::grpc::Message<DatalogClientMessage>> *writer) final;\n";
  os.PopIndent();
}

static void DefineQuery(ParsedModule module, ParsedQuery query,
                        OutputStream &os, const std::string &ns_prefix) {
  ParsedDeclaration decl(query);
  os << os.Indent() << "auto status = grpc::StatusCode::NOT_FOUND;\n"
     << os.Indent() << "if (auto params = request->GetRoot()) {\n";
  os.PushIndent();

  auto forcing_message = query.ForcingMessage();
  if (forcing_message) {
    os << os.Indent()
       << "std::unique_lock<std::shared_mutex> locker(gDatabaseLock);\n";
  } else {
    os << os.Indent()
       << "std::shared_lock<std::shared_mutex> locker(gDatabaseLock);\n";
  }

  os << os.Indent() << "const auto num_generated = gDatabase->" << query.Name()
     << "_" << decl.BindingPattern();

  auto sep = "(";
  auto has_free_params = false;
  for (ParsedParameter param : decl.Parameters()) {
    if (param.Binding() == ParameterBinding::kBound) {
      os << sep << "::hyde::rt::FBCast<" << TypeName(module, param.Type())
         << ">::From(params->" << param.Name() << "())";
      sep = ", ";
    } else if (param.Binding() == ParameterBinding::kFree) {
      has_free_params = true;
    }
  }

  if (has_free_params) {
    os << sep;
    sep = "[=, &status] (";
    for (ParsedParameter param : decl.Parameters()) {
      os << sep << "auto p" << param.Index();
      sep = ", ";
    }
    os << ") -> bool {\n";
    os.PushIndent();

    os << os.Indent() << "flatbuffers::grpc::MessageBuilder mb;\n";

    // TODO(pag): Eventually create flatbuffer offsets for non-trivial
    //            types.

    os << os.Indent() << "mb.Finish(::hyde::rt::CreateFB<::" << ns_prefix
       << query.Name() << "_" << decl.Arity() << ">::Create(mb";

    for (ParsedParameter param : decl.Parameters()) {
      os << ", p" << param.Index();
    }

    os << "));\n"
       << os.Indent() << "auto message = mb.ReleaseMessage<::" << ns_prefix
       << query.Name() << "_" << decl.Arity() << ">();\n";

    // If there are free parameters, then we're doing server-to-client streaming
    // using `writer`.
    if (!query.ReturnsAtMostOneResult()) {
      os << os.Indent() << "if (!writer->Write(message)) {\n";
      os.PushIndent();
      os << os.Indent() << "status = grpc::StatusCode::CANCELLED;\n"
         << os.Indent() << "return false;\n";
      os.PopIndent();
      os << os.Indent() << "} else {\n";
      os.PushIndent();
      os << os.Indent() << "status = grpc::StatusCode::OK;\n"
         << os.Indent() << "return true;\n";
      os.PopIndent();
      os << os.Indent() << "}\n";

    // We want to write back only our first found result.
    } else {
      os << os.Indent() << "*response = std::move(message);\n";
      os << os.Indent() << "status = grpc::StatusCode::OK;\n"
         << os.Indent() << "return false;\n";
    }

    os.PopIndent();  // End of lambda to query callback.
    os << os.Indent() << "});\n"
       << os.Indent() << "(void) num_generated;\n";

  // If there are not any free parameters, then we're sending back a message
  // to the client using `response`.
  } else {
    os << ");\n"
       << "if (num_generated) {\n";
    os.PushIndent();
    os << os.Indent() << "flatbuffers::grpc::Message<" <<  query.Name() << "_"
        << decl.Arity() << "> message(request->BorrowSlice());\n"
        << "*response = std::move(message);\n"
        << "status = grpc::StatusCode::OK;\n";
    os.PopIndent();
    os << os.Indent() << "}\n";
  }

  // The query internally sent a message into the system, so there might
  // be an update that we need to broadcast to clients.
  if (forcing_message) {
    os << os.Indent() << "PublishMessages();\n";
  }

  os.PopIndent();
  os << os.Indent() << "}\n\n"  // GetRoot
     << os.Indent() << "return grpc::Status(status, kQuery_"
     << query.Name() << "_" << query.Arity()
     << ");\n";
}

// Define the out-of-line method bodies for each of the `Query_*`methods.
static void DefineQueryMethods(ParsedModule module,
                               const std::vector<ParsedQuery> &queries,
                               const std::string &ns_prefix,
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
    DefineQuery(module, query, os, ns_prefix);
    os.PopIndent();
    os << "}";
  }
}

// Define a method that clients invoke to subscribe to messages from the server.
static void DefineSubscribeMethod(const std::vector<ParsedMessage> &messages,
                                  OutputStream &os) {
  os << "\n\n::grpc::Status DatalogService::Subscribe(\n";
  os.PushIndent();
  os << os.Indent() << "::grpc::ServerContext *context,\n"
     << os.Indent() << "const flatbuffers::grpc::Message<Client> *request,\n"
     << os.Indent() << "::grpc::ServerWriter<flatbuffers::grpc::Message<DatalogClientMessage>> *writer) {\n\n";

  os << os.Indent() << "writer->SendInitialMetadata();\n\n"
     << os.Indent() << "grpc::WriteOptions options;\n"
     << os.Indent() << "options.set_write_through();\n"
     << os.Indent() << "const auto client = request->GetRoot();\n"
     << os.Indent() << "if (!client) {\n";
  os.PushIndent();
  os << os.Indent() << "return grpc::Status::CANCELLED;\n";
  os.PopIndent();

  os << os.Indent() << "}\n\n"
     << os.Indent() << "alignas(64) Outbox outbox;\n"
     << os.Indent() << "if (auto client_name = client->name()) {\n";
  os.PushIndent();
  os << os.Indent() << "outbox.name = client_name->str();\n";
  os.PopIndent();
  os << os.Indent() << "}\n\n"
     << os.Indent() << "LOG(INFO) << \"Client '\" << outbox.name << \"' connected\";\n"
     << os.Indent() << "alignas(64) std::vector<std::shared_ptr<flatbuffers::grpc::Message<DatalogClientMessage>>> messages;\n"
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
  os << os.Indent() << "}\n"  // gFirstOutbox
     << os.Indent() << "gFirstOutbox = &outbox;\n";
  os.PopIndent();
  os << os.Indent() << "}\n\n";  // Link it in.

  // Busy loop.
  os << os.Indent() << "for (auto failed = false; !failed; ) {\n";
  os.PushIndent();
  os << os.Indent() << "if (outbox.messages_sem.Wait()) {\n";
  os.PushIndent();
  os << os.Indent() << "std::unique_lock<std::mutex> locker(outbox.messages_lock);\n"
     << os.Indent() << "messages.swap(outbox.messages);\n";
  os.PopIndent();
  os << os.Indent() << "}\n\n"  // wait
     << os.Indent() << "if (messages.empty()) {\n";
  os.PushIndent();
  os << os.Indent() << "continue;\n";
  os.PopIndent();
  os << os.Indent() << "}\n\n"
     << os.Indent() << "LOG(INFO) << \"Sending \" << messages.size() << \" outputs to client '\" << outbox.name << \"'\";\n"
     << os.Indent() << "auto num_sent = 0ul;\n"
     << os.Indent() << "auto num_failed = 0ul;\n"
     << os.Indent() << "for (const auto &message : messages) {\n";
  os.PushIndent();
  os << os.Indent() << "LOG(INFO) << \"Message size is \" << message->BorrowSlice().size() << \" bytes\";\n\n"
     << os.Indent() << "if (!writer->Write(*message, options)) {\n";
  os.PushIndent();
  os << os.Indent() << "++num_failed;\n";
  os.PopIndent();
  os << os.Indent() << "} else {\n";
  os.PushIndent();
  os << os.Indent() << "++num_sent;\n";
  os.PopIndent();
  os << os.Indent() << "}\n";  // Write
  os.PopIndent();
  os << os.Indent() << "}\n\n"  // for
     << os.Indent() << "messages.clear();\n"
     << os.Indent() << "LOG(INFO) << \"Sent \" << num_sent << \"/\" << (num_sent+num_failed) << \" outputs to client '\" << outbox.name << \"'\";\n\n"
     << os.Indent() << "if (num_failed) {\n";
  os.PushIndent();
  os << os.Indent() << "break;\n";
  os.PopIndent();
  os << os.Indent() << "}\n";
  os.PopIndent();
  os << os.Indent() << "}\n\n";  // busy loop.

  // Unlink the stack-allocated `outbox`.
  os << os.Indent() << "{\n";
  os.PushIndent();
  os << os.Indent() << "std::unique_lock<std::mutex> locker(gOutboxesLock);\n"
     << os.Indent() << "if (outbox.next) {\n";
  os.PushIndent();
  os << os.Indent() << "outbox.next->prev_next = outbox.prev_next;\n";
  os.PopIndent();
  os << os.Indent() << "}\n"
     << os.Indent() << "*(outbox.prev_next) = outbox.next;\n";
  os.PopIndent();
  os << os.Indent() << "}\n"  // End of unlink.
     << os.Indent() << "LOG(INFO) << \"Client '\" << outbox.name << \"' disconnected\";\n\n"
     << os.Indent() << "return grpc::Status::OK;\n";
  os.PopIndent();
  os << "}";  // End of Subscribe.
}

// Define a method that clients invoke to publish messages to the server.
static void DefinePublishMethod(ParsedModule module,
                                const std::vector<ParsedMessage> &messages,
                                OutputStream &os) {
  os << "\n\n::grpc::Status DatalogService::Publish(\n";
  os.PushIndent();
  os << os.Indent() << "::grpc::ServerContext *context,\n"
     << os.Indent() << "const flatbuffers::grpc::Message<DatalogServerMessage> *request,\n"
     << os.Indent() << "flatbuffers::grpc::Message<Empty> *response) {\n\n"
     << os.Indent() << "const auto req_msg = request->GetRoot();\n"
     << os.Indent() << "if (!req_msg) {\n";
  os.PushIndent();
  os << os.Indent() << "return grpc::Status::OK;\n";
  os.PopIndent();

  os << os.Indent() << "}\n\n"
     << os.Indent() << "LOG(INFO) << \"Received message size is \" << request->BorrowSlice().size() << \" bytes\";\n"
     << os.Indent() << "auto input_msg = std::make_unique<DatabaseInputMessageType>(*gStorage);\n";

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
      os << sep << "::hyde::rt::FBCast<" << TypeName(module, param.Type())
         << ">::From(entry->" << param.Name() << "())";
      sep = ", ";
    }
    os << ");\n";
    os.PopIndent();
    os << os.Indent() << "}\n";  // vector iteration.

    os.PopIndent();
    os << os.Indent() << "}\n";  // vector pointer.
  };

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

  // Handle added messages.
  if (has_added) {
    os << os.Indent() << "if (auto added = req_msg->added()) {\n";
    os.PushIndent();
    for (ParsedMessage message : messages) {
      if (message.IsReceived()) {
        do_message(message, "added", "produce_");
      }
    }
    os.PopIndent();
    os << os.Indent() << "}\n";  // added
  }

  // Handle removed messages.
  if (has_removed) {
    os << os.Indent() << "if (auto removed = req_msg->removed()) {\n";
    os.PushIndent();
    for (ParsedMessage message : messages) {
      if (message.IsReceived() && message.IsDifferential()) {
        do_message(message, "removed", "retract_");
      }
    }
    os.PopIndent();
    os << os.Indent() << "}\n";  // removed
  }

  os << os.Indent() << "if (auto size = input_msg->Size()) {\n";
  os.PushIndent();
  os << os.Indent() << "LOG(INFO) << \"Received \" << size << \" messages\";\n\n"
     << os.Indent() << "std::unique_lock<std::mutex> locker(gInputMessagesLock);\n"
     << os.Indent() << "gInputMessages.push_back(std::move(input_msg));\n"
     << os.Indent() << "gInputMessagesSemaphore.Signal();\n";
  os.PopIndent();
  os << os.Indent() << "}\n"
     << os.Indent() << "return grpc::Status::OK;\n";

  os.PopIndent();
  os << os.Indent() << "}";
}

// Define the `Build` method of the `PublishedMessageBuilder` class, which
// goes and packages up all messages into flatbuffer vectors and into
// added/removed messages. Normally, the offsets to the messages-to-be-published
// are held in `std::vector`s.
static void DefineDatabaseLogBuild(const std::vector<ParsedMessage> &messages,
                                   OutputStream &os) {
  auto has_added = false;
  auto has_removed = false;
  for (ParsedMessage message : messages) {
    if (message.IsPublished()) {
      has_added = true;
      if (message.IsDifferential()) {
        has_removed = true;
        break;
      }
    }
  }

  os << os.Indent() << "flatbuffers::grpc::Message<DatalogClientMessage> Build(void) {\n";
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
    os << os.Indent() << "flatbuffers::Offset<AddedOutputMessage> added_offset;\n";
    if (has_removed) {
      os << os.Indent() << "flatbuffers::Offset<RemovedOutputMessage> removed_offset;\n";
    }

    os << os.Indent() << "if (has_added) {\n";
    os.PushIndent();

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
  }

  if (has_removed) {
    os << os.Indent() << "if (has_removed) {\n";
    os.PushIndent();
    for (ParsedMessage message : messages) {
      if (message.IsPublished() && message.IsDifferential()) {
        do_message(message, "_removed");
      }
    }
    os << os.Indent() << "removed_offset = CreateRemovedOutputMessage(mb";
    for (ParsedMessage message : messages) {
      if (message.IsPublished() && message.IsDifferential()) {
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
  os << os.Indent() << "mb.Finish(CreateDatalogClientMessage(mb";
  if (has_added) {
    os << ", added_offset";
  }
  if (has_removed) {
    os << ", removed_offset";
  }
  os << "));\n"
     << os.Indent() << "return mb.ReleaseMessage<DatalogClientMessage>();\n";
  os.PopIndent();
  os << os.Indent() << "}";
}

// Define the `PublishedMessageBuilder` class, which has one method per
// published message. The role of this message builder is to accumulate
// messages into a flatbuffer to be published to all connected clients.
static void DefineDatabaseLog(ParsedModule module,
                              const std::vector<ParsedMessage> &messages,
                              OutputStream &os) {

  os << "class PublishedMessageBuilder final : public grpc::GrpcLibraryCodegen {\n";
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
    os << os.Indent() << "auto offset = ::hyde::rt::CreateFB<Message_"
       << message.Name() << "_" << message.Arity() << ">::Create(mb";

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

// Defines the function that runs the database.
static void DefineDatabaseThread(const std::vector<ParsedMessage> &messages,
                                 OutputStream &os) {

  // Make a function to publish messages.
  os << "void PublishMessages(void) {\n";
  os.PushIndent();
  os << os.Indent() << "auto output = std::make_shared<flatbuffers::grpc::Message<DatalogClientMessage>>(gDatabaseLog->Build());\n"
     << os.Indent() << "std::unique_lock<std::mutex> locker(gOutboxesLock);\n"
     << os.Indent() << "for (auto outbox = gFirstOutbox; outbox;) {\n";
  os.PushIndent();
  os << os.Indent() << "LOG(INFO) << \"Sending updates to client subscriber '\" << outbox->name << \"'\";\n\n"  // if
     << os.Indent() << "std::unique_lock<std::mutex> outbox_locker(outbox->messages_lock);\n"
     << os.Indent() << "outbox->messages.push_back(output);\n"
     << os.Indent() << "outbox->messages_sem.Signal();\n"
     << os.Indent() << "outbox = outbox->next;\n";
  os.PopIndent();
  os << os.Indent() << "}\n";  // for
  os.PopIndent();
  os << "}\n\n";

  // Make the main database thread.
  os << "static void *DatabaseWriterThread(void *) {\n";
  os.PushIndent();
  os << os.Indent() << "std::vector<std::unique_ptr<DatabaseInputMessageType>> inputs;\n"
     << os.Indent() << "inputs.reserve(128);\n"
     << os.Indent() << "while (true) {\n";
  os.PushIndent();
  os << os.Indent() << "if (gInputMessagesSemaphore.Wait()) {\n";
  os.PushIndent();
  os << os.Indent() << "std::unique_lock<std::mutex> locker(gInputMessagesLock);\n"
     << os.Indent() << "inputs.swap(gInputMessages);\n";
  os.PopIndent();
  os << os.Indent() << "}\n"  // wait
     << os.Indent() << "if (inputs.empty()) {\n";
  os.PushIndent();
  os << os.Indent() << "continue;\n";
  os.PopIndent();
  os << os.Indent() << "}\n\n"
     << os.Indent() << "uint64_t total_num_applied = 0u;\n"
     << os.Indent() << "for (const auto &input : inputs) {\n";
  os.PushIndent();
  os << os.Indent() << "total_num_applied += input->Size();\n"
     << os.Indent() << "LOG(INFO) << \"Applying \" << input->Size() << \" messages to the database\";\n\n"
     << os.Indent() << "std::unique_lock<std::shared_mutex> locker(gDatabaseLock);\n"
     << os.Indent() << "input->Apply(*gDatabase);\n";
  os.PopIndent();
  os << os.Indent() << "}\n"  // for
     << os.Indent() << "inputs.clear();\n"
     << os.Indent() << "LOG(INFO) << \"Applied \" << total_num_applied << \" messages to the database\";\n\n"
     << os.Indent() << "PublishMessages();\n";

  os.PopIndent();
  os << os.Indent() << "}\n"  // while true
     << os.Indent() << "return nullptr;\n";
  os.PopIndent();
  os << "}\n\n";
}

}  // namespace

// Emits C++ code for the given program to `os`.
void GenerateServerCode(const Program &program, OutputStream &os) {
  os << "/* Auto-generated file */\n\n"
     << "#include <algorithm>\n"
     << "#include <cstdlib>\n"
     << "#include <cstdio>\n"
     << "#include <cstring>\n"
     << "#include <iostream>\n"
     << "#include <memory>\n"
     << "#include <mutex>\n"
     << "#include <pthread.h>\n"
     << "#include <shared_mutex>\n"
     << "#include <sstream>\n"
     << "#include <string>\n"
     << "#include <thread>\n"
     << "#include <vector>\n\n"
     << "#define DRLOJEKYLL_SERVER_CODE\n\n"
     << "#include <drlojekyll/Runtime/FlatBuffers.h>\n"
     << "#include <drlojekyll/Runtime/StdRuntime.h>\n\n";

  const auto module = program.ParsedModule();
  const auto inlines = Inlines(module, Language::kCxx);
  const auto db_name = module.DatabaseName();

  std::string file_name = "datalog";
  std::string ns_name;
  std::string ns_name_prefix;
  if (db_name) {
    file_name = db_name->FileName();
    ns_name = db_name->NamespaceName(Language::kCxx);
    ns_name_prefix = ns_name + "::";
  }

  // Include auto-generated files.
  os << "#include <drlojekyll/Runtime/Semaphore.h>\n"
     << "#include <grpcpp/grpcpp.h>\n"
     << "#include <grpcpp/impl/grpc_library.h>\n"
     << "#include <flatbuffers/flatbuffers.h>\n"
     << "#include <glog/logging.h>\n"
     << "#include <gflags/gflags.h>\n\n"
     << "#include \"" << file_name << "_generated.h\"\n"
     << "#include \"" << file_name << ".grpc.fb.h\"\n"
     << "#include \"" << file_name << ".interface.h\"\n"
     << "#include \"" << file_name << ".db.h\"\n\n";

  for (auto code : inlines) {
    if (code.Stage() == "c++:server:prologue") {
      os << code.CodeToInline() << "\n\n";
    }
  }

  if (db_name) {
    os << "DEFINE_string(host, \"localhost\", \"Hostname of the " << db_name.value() << " server\");\n"
        << "DEFINE_uint32(port, 50051, \"Port of the " << db_name.value() << " server\");\n\n";
  } else {
    os << "DEFINE_string(host, \"localhost\", \"Hostname of this server\");\n"
       << "DEFINE_uint32(port, 50051, \"Port of this server\");\n\n";
  }
  auto queries = Queries(module);
  auto messages = Messages(module);

  if (!ns_name.empty()) {
    os << "namespace " << ns_name << " {\n\n";
    for (auto code : inlines) {
      if (code.Stage() == "c++:server:prologue:namespace") {
        os << code.CodeToInline() << "\n\n";
      }
    }
  }

  DefineDatabaseLog(module, messages, os);

  // Define the main gRPC service class, and declare each of its methods.
  os << "class DatalogService final\n";
  os.PushIndent();
  os.PushIndent();
  os << os.Indent() << ": public grpc::GrpcLibraryCodegen, public Datalog::Service {\n";
  os.PopIndent();
  os << os.Indent() << "public:\n";
  os.PushIndent();
  DeclareServiceMethods(queries, os);
  os.PopIndent();  // public
  os.PopIndent();
  os << "};"  // DatalogService
     << "\n\n"
     << "using DatabaseStorageType = hyde::rt::StdStorage;\n"
     << "using DatabaseInputMessageType = DatabaseInputMessage<DatabaseStorageType>;\n"
     << "[[gnu::used]] static grpc::internal::GrpcLibraryInitializer gInitializer;\n"
     << "static std::vector<std::unique_ptr<DatabaseInputMessageType>> gInputMessages;\n"
     << "static std::mutex gInputMessagesLock;\n"
     << "static hyde::rt::Semaphore gInputMessagesSemaphore;\n"
     << "static PublishedMessageBuilder *gDatabaseLog = nullptr;\n"
     << "static DatabaseStorageType *gStorage = nullptr;\n"
     << "static std::shared_mutex gDatabaseLock;\n"
     << "static Database<DatabaseStorageType, PublishedMessageBuilder> *gDatabase = nullptr;\n\n"
     << "static void PublishMessages(void);\n";

  // Define the query methods out-of-line.
  DefineQueryMethods(module, queries, ns_name_prefix, os);
  DefineOutboxes(os);
  DefinePublishMethod(module, messages, os);
  DefineSubscribeMethod(messages, os);

  os << "\n\n";

  DefineDatabaseThread(messages, os);

  if (!ns_name.empty()) {
    for (auto code : inlines) {
      if (code.Stage() == "c++:server:epilogue:namespace") {
        os << code.CodeToInline() << "\n\n";
      }
    }

    os << "}  // namespace " << ns_name << "\n\n";
  }

  for (auto code : inlines) {
    if (code.Stage() == "c++:server:epilogue") {
      os << code.CodeToInline() << "\n\n";
    }
  }

  os << "extern \"C\" int main(int argc, char *argv[]) {\n";
  os.PushIndent();

  for (auto code : inlines) {
    if (code.Stage() == "c++:server:prologue:main") {
      os << code.CodeToInline() << "\n\n";
    }
  }

  os << os.Indent() << "google::ParseCommandLineFlags(&argc, &argv, false);\n"
     << os.Indent() << "google::InitGoogleLogging(argv[0]);\n\n";

  // Start by locking the database. We haven't actually constructed it, so this
  // is a way of making sure that nothing else tries to access it.
  os << os.Indent() << ns_name_prefix << "gDatabaseLock.lock();\n"

  // Make some vectors reasonably big to avoid allocations at runtime, and start
  // the database thread.
     << os.Indent() << ns_name_prefix << "gInputMessages.reserve(128);\n"
     << os.Indent() << "pthread_t db_thread;\n"
     << os.Indent() << "pthread_attr_t attr;\n"
     << os.Indent() << "pthread_attr_init(&attr);\n"
     << os.Indent() << "pthread_attr_setstacksize(&attr, (1ull << 20) * 128ull);\n"
     << os.Indent() << "pthread_create(&db_thread, &attr, &" << ns_name_prefix
                    << "DatabaseWriterThread, nullptr);\n"
     << os.Indent() << "pthread_attr_destroy(&attr);\n"
     << os.Indent() << "std::stringstream address_ss;\n"
     << os.Indent() << "address_ss << FLAGS_host << ':' << FLAGS_port;\n\n"
     << os.Indent() << ns_name_prefix << "DatalogService service;\n"

  // Build a gRPC server builder, configuring it with host/port.
     << os.Indent() << "grpc::ServerBuilder builder;\n"
     << os.Indent() << "builder.SetMaxReceiveMessageSize(std::numeric_limits<int>::max());\n"
     << os.Indent() << "builder.SetMaxSendMessageSize(std::numeric_limits<int>::max());\n"
     << os.Indent() << "builder.SetCompressionAlgorithmSupportStatus(GRPC_COMPRESS_GZIP, true);\n"
     << os.Indent() << "builder.SetCompressionAlgorithmSupportStatus(GRPC_COMPRESS_STREAM_GZIP, true);\n"
     << os.Indent() << "builder.SetDefaultCompressionAlgorithm(GRPC_COMPRESS_GZIP);\n"
     << os.Indent() << "builder.AddListeningPort(address_ss.str(), grpc::InsecureServerCredentials());\n"
     << os.Indent() << "builder.RegisterService(&service);\n"

  // Create the database.
     << os.Indent() << ns_name_prefix << "DatabaseStorageType storage;\n"
     << os.Indent() << ns_name_prefix << "gStorage = &storage;\n"
     << os.Indent() << ns_name_prefix << "PublishedMessageBuilder log;\n"
     << os.Indent() << ns_name_prefix << "gDatabaseLog = &log;\n"
     << os.Indent() << ns_name_prefix << "DatabaseFunctors<"
                    << ns_name_prefix << "DatabaseStorageType> functors;\n"
     << os.Indent() << ns_name_prefix << "Database<"
                    << ns_name_prefix << "DatabaseStorageType, "
                    << ns_name_prefix << "PublishedMessageBuilder> db(storage, log, functors);\n"
     << os.Indent() << ns_name_prefix << "gDatabase = &db;\n"

  // Build the actual server.
     << os.Indent() << "auto server = builder.BuildAndStart();\n"

  // Initializing the database may have triggered message publication, so handle that.
     << os.Indent() << ns_name_prefix << "PublishMessages();\n"
     << os.Indent() << ns_name_prefix << "gDatabaseLock.unlock();\n"

  // Wait for the server to stop.
     << os.Indent() << "server->Wait();\n"
     << os.Indent() << "void *res = nullptr;\n"
     << os.Indent() << "pthread_join(db_thread, &res);\n";


  for (auto code : inlines) {
    if (code.Stage() == "c++:server:epilogue:main") {
      os << code.CodeToInline() << "\n\n";
    }
  }

  os << os.Indent() << "return EXIT_SUCCESS;\n";
  os.PopIndent();
  os << "}\n\n";
}

}  // namespace cxx
}  // namespace hyde
