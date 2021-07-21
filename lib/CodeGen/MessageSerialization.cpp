// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/CodeGen/MessageSerialization.h>
#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Lex/Format.h>
#include <drlojekyll/Parse/ErrorLog.h>
#include <drlojekyll/Parse/Format.h>
#include <drlojekyll/Parse/Parse.h>

#include <cassert>
#include <sstream>
#include <vector>

namespace hyde {
namespace {

[[nodiscard]] auto ParseParameterType(const TypeLoc &type,
                                      const ErrorLog &) {
  switch (type.UnderlyingKind()) {
    case TypeKind::kSigned8:
    case TypeKind::kUnsigned8:
    case TypeKind::kSigned16:
    case TypeKind::kUnsigned16:
    case TypeKind::kSigned32:
    case TypeKind::kUnsigned32: return "int";
    case TypeKind::kFloat: return "float";
    case TypeKind::kSigned64:
    case TypeKind::kUnsigned64: return "long";
    case TypeKind::kDouble: return "double";
    case TypeKind::kBytes: return "bytes";
    case TypeKind::kForeignType: return "record";
    case TypeKind::kInvalid: return "null";
    case TypeKind::kBoolean: return "boolean";
    default: assert(false); return "null";
  }
}

AvroMessageInfo GenerateMessageSchema(const DisplayManager &display_manager,
                                      const ParsedMessage &message,
                                      const ErrorLog &err) {
  std::stringstream ss;
  hyde::OutputStream os(display_manager, ss);
  os << "{\"type\":\"record\",\"namespace\":\""
     << AVRO_DRLOG_NAMESPACE << "\",\"name\":\"" << message.Name() << "\","
     << "\"fields\":[";

  std::string_view message_name;
  display_manager.TryReadData(message.Name().SpellingRange(), &message_name);
  std::string name(message_name.data(), message_name.size());

  auto sep = "";
  for (auto parameter : message.Parameters()) {
    os << sep << "{\"name\":\"" << parameter.Name() << "\",\"type\":\""
       << ParseParameterType(parameter.Type(), err) << "\"}";
    sep = ",";
  }
  os << "]}";
  os.Flush();

  return {name, ss.str()};
}

}  // namespace

[[nodiscard]] std::vector<AvroMessageInfo>
GenerateAvroMessageSchemas(const DisplayManager &display_manager,
                           const ParsedModule &module, const ErrorLog &err) {
  std::vector<AvroMessageInfo> avro_schemas{};

  for (auto message : module.Messages()) {
    avro_schemas.emplace_back(
        GenerateMessageSchema(display_manager, message, err));
  }

  return avro_schemas;
}

}  // namespace hyde
