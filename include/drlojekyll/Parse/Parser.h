// Copyright 2019, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/Parse/Parse.h>

#include <memory>
#include <optional>
#include <string_view>
#include <filesystem>

namespace hyde {

class DisplayConfiguration;
class DisplayManager;
class ErrorLog;

class ParserImpl;

// Manages all module parse trees and their memory.
class Parser {
 public:
  ~Parser(void);

  explicit Parser(const DisplayManager &display_manager,
                  const ErrorLog &error_log);

  // Parse a buffer.
  //
  // NOTE(pag): `data` must remain valid for the lifetime of the parser's
  //            `display_manager`.
  std::optional<ParsedModule>
  ParseBuffer(std::string_view data, const DisplayConfiguration &config) const;

  // Parse a file, specified by its path.
  std::optional<ParsedModule>
  ParsePath(std::string_view path, const DisplayConfiguration &config) const;

  // Parse an input stream.
  //
  // NOTE(pag): `is` must remain a valid reference for the lifetime of the
  //            parser's `display_manager`.
  std::optional<ParsedModule>
  ParseStream(std::istream &is, const DisplayConfiguration &config) const;

  // Add a directory as a search path for modules.
  void AddModuleSearchPath(std::filesystem::path path) const;

  // Return a copy of the list of search paths used by the parser.
  std::vector<std::filesystem::path> SearchPaths(void) const noexcept;

 private:
  Parser(void) = delete;

  std::unique_ptr<ParserImpl> impl;
};

}  // namespace hyde
