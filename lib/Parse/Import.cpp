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

  std::unique_ptr<Node<ParsedImport>> imp(new Node<ParsedImport>);
  imp->directive_pos = tok.Position();

  if (!ReadNextSubToken(tok)) {
    context->error_log.Append(scope_range, imp->directive_pos)
        << "Expected string literal of file path here for import statement";
    return;
  }

  const auto tok_range = tok.SpellingRange();

  if (Lexeme::kLiteralString != tok.Lexeme()) {
    context->error_log.Append(scope_range, tok_range)
        << "Expected string literal of file path here for import "
        << "statement, got '" << tok << "' instead";
    return;
  }

  imp->path = tok;

  // This should work...
  std::string_view path_str;
  if (!context->string_pool.TryReadString(tok.StringId(), tok.StringLength(),
                                          &path_str) ||
      path_str.empty()) {
    context->error_log.Append(scope_range, tok_range)
        << "Unknown error when trying to read data associatd with import "
        << "path '" << tok << "'";
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
    context->error_log.Append(scope_range, tok_range)
        << "Unable to locate module '" << tok
        << "' requested by import statement";
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
  auto sub_mod_opt = sub_impl.ParseDisplay(
      context->display_manager.OpenPath(full_path, sub_config), sub_config);

  // Restore the old first search path.
  context->import_search_paths[0] = prev_search0;

  if (sub_mod_opt) {
    imp->imported_module = sub_mod_opt->impl.get();

    if (!module->imports.empty()) {
      module->imports.back()->next = imp.get();
    }

    module->imports.push_back(std::move(imp));

  } else {
    context->error_log.Append(scope_range, tok_range)
        << "Failed to parse '" << full_path
        << "' requested by import statement";
  }
}

}  // namespace hyde
