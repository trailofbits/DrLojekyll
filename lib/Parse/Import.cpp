// Copyright 2020, Trail of Bits. All rights reserved.

#include "Parser.h"

namespace hyde {

// Try to parse `sub_range` as an import. We eagerly parse imported modules
// before continuing the parse of our current module. This is so that we
// can make sure all dependencies on exported rules, messages, etc. are
// visible. This is partially enforced by ensuring that imports must precede
// and declarations, and declarations must precede their uses. The result is
// that we can built up a semantically meaningful parse tree in a single pass.
void ParserImpl::ParseImport(Node<ParsedModule> *module) {
  Token tok;
  if (!ReadNextSubToken(tok)) {
    assert(false);
  }

  assert(tok.Lexeme() == Lexeme::kHashImportModuleStmt);

  std::unique_ptr<Node<ParsedImport>> imp(
      new Node<ParsedImport>);
  imp->directive_pos = tok.Position();

  if (!ReadNextSubToken(tok)) {
    Error err(context->display_manager, SubTokenRange(), imp->directive_pos);
    err << "Expected string literal of file path here for import statement";
    context->error_log.Append(std::move(err));
    return;
  }

  if (Lexeme::kLiteralString != tok.Lexeme()) {
    Error err(context->display_manager, SubTokenRange(),
              tok.SpellingRange());
    err << "Expected string literal of file path here for import "
        << "statement, got '" << tok << "' instead";
    context->error_log.Append(std::move(err));
    return;
  }

  imp->path = tok;

  // This should work...
  std::string_view path_str;
  if (!context->string_pool.TryReadString(tok.StringId(), tok.StringLength(),
                                          &path_str) ||
      path_str.empty()) {
    Error err(context->display_manager, SubTokenRange(),
              tok.SpellingRange());
    err << "Unknown error when trying to read data associatd with import "
        << "path '" << tok << "'";
    context->error_log.Append(std::move(err));
    return;
  }

  std::error_code ec;
  std::string_view full_path;

  for (auto search_path : context->import_search_paths) {
    full_path = std::string_view();

    ec = context->file_manager.PushDirectory(search_path);
    if (ec) {
      continue;
    }

    Path path(context->file_manager, path_str);
    context->file_manager.PopDirectory();

    ec = path.RealPath(&full_path);

    if (ec) {
      continue;
    }

    break;
  }

  if (ec || full_path.empty()) {
    Error err(context->display_manager, SubTokenRange());
    err << "Unable to locate module '" << tok
        << "' requested by import statement";
    context->error_log.Append(std::move(err));
    return;
  }

  // Save the old first search path, and put in the directory containing the
  // about-to-be parsed module as the new first search path.
  Path prev_search0 = context->import_search_paths[0];
  context->import_search_paths[0] =
      Path(context->file_manager, full_path).DirName();

  DisplayConfiguration sub_config = module->config;
  sub_config.name = full_path;

  // Go and parse the module.
  ParserImpl sub_impl(context);
  auto sub_mod = sub_impl.ParseDisplay(
      context->display_manager.OpenPath(full_path, sub_config),
      sub_config);

  // Restore the old first search path.
  context->import_search_paths[0] = prev_search0;

  imp->imported_module = sub_mod.impl.get();

  if (!module->imports.empty()) {
    module->imports.back()->next = imp.get();
  }

  module->imports.push_back(std::move(imp));
}

}  // namespace hyde
