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

  const auto path_range = tok.SpellingRange();

  if (Lexeme::kLiteralString != tok.Lexeme()) {
    context->error_log.Append(scope_range, path_range)
        << "Expected string literal of file path here for import "
        << "statement, got '" << tok << "' instead";
    return;
  }

  imp->path = tok;

  if (!ReadNextSubToken(tok)) {
    context->error_log.Append(scope_range, imp->path.NextPosition())
        << "Expected period to end the import statement";
    return;

  } else if (tok.Lexeme() != Lexeme::kPuncPeriod) {
    auto err = context->error_log.Append(scope_range, imp->path.NextPosition());
    err << "Expected period here to end the import statement";

    err.Note(scope_range, tok.SpellingRange()) << "Got '" << tok << "' instead";
    return;
  }

  imp->dot = tok;

  // This should work...
  std::string_view path_str;
  if (!context->string_pool.TryReadString(
          imp->path.StringId(), imp->path.StringLength(), &path_str) ||
      path_str.empty()) {
    context->error_log.Append(scope_range, path_range)
        << "Unknown error when trying to read data associated with import "
        << "path '" << imp->path << "'";
    return;
  }

  std::filesystem::path resolved_path;
  std::error_code ec =
      ResolvePath(path_str, context->import_search_paths, resolved_path);

  if (ec || resolved_path.empty()) {

    // TODO(blarsen): Fix up the error reporting here to include the source
    //                information, like in `ParseInclude`
    context->error_log.Append(scope_range, path_range)
        << "Unable to locate module '" << imp->path
        << "' requested by import statement";
    return;
  }

  imp->resolved_path = resolved_path;

  // Save the old first search path, and put in the directory containing the
  // about-to-be parsed module as the new first search path.
  std::filesystem::path prev_search0 = context->import_search_paths[0];
  context->import_search_paths[0] = resolved_path.parent_path();

  DisplayConfiguration sub_config = module->config;
  sub_config.name = resolved_path.string();

  // Go and parse the module.
  ParserImpl sub_impl(context);
  auto sub_mod_opt = sub_impl.ParseDisplay(
      context->display_manager.OpenPath(resolved_path.string(), sub_config),
      sub_config);

  // Restore the old first search path.
  context->import_search_paths[0] = prev_search0;

  if (sub_mod_opt) {
    imp->imported_module = sub_mod_opt->impl.get();

    if (!module->imports.empty()) {
      module->imports.back()->next = imp.get();
    }

    module->imports.push_back(std::move(imp));

  } else {
    context->error_log.Append(scope_range, path_range)
        << "Failed to parse '" << resolved_path
        << "' requested by import statement";
  }
}

}  // namespace hyde
