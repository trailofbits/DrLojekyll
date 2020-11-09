// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/CodeGen/MessageSerialization.h>
#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Parse/ErrorLog.h>
#include <drlojekyll/Parse/Parse.h>

#include <nlohmann/json.hpp>
#include <sstream>
#include <vector>

using json = nlohmann::json;

namespace hyde {
namespace {

[[nodiscard]] auto BuildAvroRecord(std::string_view name,
                                   json::array_t &fields) {
  return AvroMessageInfo{std::string(name),
                         json{
                             {"type", "record"},
                             {"namespace", AVRO_DRLOG_NAMESPACE},
                             {"name", name},
                             {"fields", fields},
                         }};
}

[[nodiscard]] auto ParseParameterType(const TypeLoc &type,
                                      const ErrorLog &err) {
  switch (type.Kind()) {
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
    case TypeKind::kASCII:
    case TypeKind::kUTF8:
    case TypeKind::kUUID: return "string";
    case TypeKind::kInvalid: return "null";
    default: assert(false); return "null";
  }
}

[[nodiscard]] auto ParseMessageParameter(const DisplayManager &display_manager,
                                         const ParsedParameter &parameter,
                                         const ErrorLog &err) {
  std::string_view msg_str;
  (void) display_manager.TryReadData(parameter.Name().SpellingRange(),
                                     &msg_str);
  return json{{"name", std::string(msg_str)},
              {"type", ParseParameterType(parameter.Type(), err)}};
}

[[nodiscard]] auto GenerateMessageSchema(const DisplayManager &display_manager,
                                         const ParsedMessage &message,
                                         const ErrorLog &err) {
  json::array_t fields = json::array();
  for (auto parameter : message.Parameters()) {
    fields.emplace_back(ParseMessageParameter(display_manager, parameter, err));
  }
  std::string_view msg_str;
  (void) display_manager.TryReadData(message.Name().SpellingRange(), &msg_str);
  return BuildAvroRecord(msg_str, fields);
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
