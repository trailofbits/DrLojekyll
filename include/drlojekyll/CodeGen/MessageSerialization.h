// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

#include <nlohmann/json.hpp>

namespace hyde {

class DisplayManager;
class ParsedModule;
class ErrorLog;

// Avro message namespace prefix for all Dr. Lojekyll serialization schemas
constexpr auto AVRO_DRLOG_NAMESPACE = "drlog.avro";

// Struct to hold metadata about the Avro schema and the schema itself
struct AvroMessageInfo {
  std::string message_name;
  nlohmann::json schema;
};

// Generates Avro schemas corresponding to all messages in the Datalog module.
[[nodiscard]] std::vector<AvroMessageInfo>
GenerateAvroMessageSchemas(DisplayManager display_manager, const ParsedModule &,
                           const ErrorLog &);

}  // namespace hyde
