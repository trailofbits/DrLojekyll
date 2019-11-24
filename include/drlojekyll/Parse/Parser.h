// Copyright 2019, Trail of Bits. All rights reserved.

#pragma once

#include <memory>
#include <string_view>

#include <drlojekyll/Parse/Parse.h>

namespace hyde {

class DisplayConfiguration;
class DisplayManager;
class ErrorLog;

class ParserImpl;

// Manages all module parse trees and their memory.
class Parser {
 public:
  ~Parser(void);

  explicit Parser(
      const DisplayManager &display_manager,
      const ErrorLog &error_log);

  // Parse a buffer.
  //
  // NOTE(pag): `data` must remain valid for the lifetime of the parser's
  //            `display_manager`.
  ParsedModule ParseBuffer(
      std::string_view data,
      const DisplayConfiguration &config) const;

  // Parse a file, specified by its path.
  ParsedModule ParsePath(
      std::string_view path,
      const DisplayConfiguration &config) const;

  // Parse an input stream.
  //
  // NOTE(pag): `is` must remain a valid reference for the lifetime of the
  //            parser's `display_manager`.
  ParsedModule ParseStream(
      std::istream &is,
      const DisplayConfiguration &config) const;

  // Add a directory as a search path for files.
  void AddSearchPath(std::string_view path) const;

 private:
  Parser(void) = delete;

  std::unique_ptr<ParserImpl> impl;
};

}  // namespace hyde
