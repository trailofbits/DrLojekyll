// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/Display/DisplayManager.h>
#include <drlojekyll/Display/DisplayReader.h>
#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Lex/Format.h>
#include <drlojekyll/Lex/Lexer.h>
#include <drlojekyll/Lex/StringPool.h>
#include <drlojekyll/Lex/Token.h>
#include <drlojekyll/Parse/ErrorLog.h>
#include <drlojekyll/Parse/Format.h>
#include <drlojekyll/Parse/Parser.h>
#include <drlojekyll/Util/DefUse.h>

#include <cassert>
#include <cstring>
#include <filesystem>
#include <sstream>
#include <unordered_map>
#include <vector>

#include "Parse.h"

namespace hyde {

// Information shared by multiple parsers.
class SharedParserContext {
 public:
  SharedParserContext(const DisplayManager &display_manager_,
                      const ErrorLog &error_log_)
      : display_manager(display_manager_),
        error_log(error_log_) {

    // TODO(blarsen): Grabbing the current path in parser construction
    //                is a hidden dependency.
    std::filesystem::path cwd = std::filesystem::current_path();
    import_search_paths.push_back(cwd);
  }

  // Search paths for looking for imports.
  std::vector<std::filesystem::path> import_search_paths;

  // All parsed modules.
  ParsedModuleImpl *root_module{nullptr};

  // Mapping of display IDs to parsed modules. This exists to prevent the same
  // module from being parsed multiple times.
  //
  // NOTE(pag): Cyclic module imports are valid.
  std::unordered_map<unsigned, std::weak_ptr<ParsedModuleImpl>>
      parsed_modules;

  const DisplayManager display_manager;
  const ErrorLog error_log;
  const StringPool string_pool;

  // Keeps track of the global locals. All parsed modules shared this.
  std::unordered_map<uint64_t, ParsedDeclarationImpl *> declarations;

  // Maps identifier IDs to foreign types.
  std::unordered_map<uint32_t, ParsedForeignTypeImpl *> foreign_types;

  // Maps identifier IDs to foreign constants.
  std::unordered_map<uint32_t, ParsedForeignConstantImpl *> foreign_constants;
};

class ParserImpl {
 public:
  ParserImpl(const std::shared_ptr<SharedParserContext> &context_)
      : context(context_),
        lexer() {}

  std::shared_ptr<SharedParserContext> context;

  Lexer lexer;

  // All of the tokens.
  std::vector<Token> tokens;

  // All of the tokens on the current line, or up to the next period, depending
  // on what is needed by the context. Used when we need to parse
  // directives/clauses, which terminate at newlines/periods, respectively.
  std::vector<Token> sub_tokens;

  // Range of tokens in `sub_tokens`.
  DisplayRange scope_range;

  // Set of previously named variables in the current clause.
  std::unordered_map<unsigned, ParsedVariableImpl *> prev_named_var;

  // The index of the next token to read from `tokens`.
  size_t next_tok_index{0};
  size_t next_sub_tok_index{0};

  // Lex all the tokens from a display. This fills up `tokens` with the tokens.
  // There should always be at least one token in the display, e.g. for EOF or
  // error.
  void LexAllTokens(Display display);

  // Read the next token.
  bool ReadNextToken(Token &tok_out);
  bool ReadNextSubToken(Token &tok_out);

  // Unread the last read token.
  void UnreadToken(void);
  void UnreadSubToken(void);

  // Return the display range of all the sub tokens.
  DisplayRange SubTokenRange(void) const;

  // Read until the next period. This fill sup `sub_tokens` with all
  // read tokens. Returns `false` if a period is not found.
  bool ReadStatement(void);

  // Add a declaration or redeclaration to the module.
  template <typename T>
  T *AddDecl(ParsedModuleImpl *module, DeclarationKind kind, Token name,
             size_t arity);

  // Remove a declaration.
  void RemoveDecl(ParsedDeclarationImpl *decl);

  void FinalizeDeclAndCheckConsistency(ParsedDeclarationImpl *decl);

  // Try to parse an inline predicate.
  bool ParseAggregatedPredicate(ParsedModuleImpl *module,
                                ParsedClauseImpl *clause,
                                ParsedAggregateImpl *agg,
                                Token &tok, DisplayPosition &next_pos);

  // Try to parse all of the tokens.
  void ParseAllTokens(ParsedModuleImpl *module);

  // Try to parse `sub_range` as a functor, adding it to `module` if successful.
  void ParseFunctor(ParsedModuleImpl *module);

  // Try to parse `sub_range` as a query, adding it to `module` if successful.
  void ParseQuery(ParsedModuleImpl *module);

  // Try to parse `sub_range` as a message, adding it to `module` if successful.
  void ParseMessage(ParsedModuleImpl *module);

  // Try to parse `sub_range` as a local or export rule, adding it to `module`
  // if successful.
  template <typename NodeTypeImpl, DeclarationKind kDeclKind,
            Lexeme kIntroducerLexeme>
  void ParseLocalExport(ParsedModuleImpl *module,
                        UseList<NodeTypeImpl, ParsedDeclarationImpl> &out_vec);

  // Try to parse `sub_range` as a foreign type declaration, adding it to
  // module if successful.
  void ParseForeignTypeDecl(ParsedModuleImpl *module);

  // Try to parse `sub_range` as a foreign constant declaration, adding it to
  // module if successful.
  void ParseForeignConstantDecl(ParsedModuleImpl *module);

  // Try to parse `sub_range` as an import.
  void ParseImport(ParsedModuleImpl *module);

  // Try to resolve the given path to a file on the filesystem, searching the
  // provided directories in order.
  // TODO(blarsen): fix up the filesystem error-handling behavior here
  static std::error_code
  ResolvePath(const std::filesystem::path &path,
              const std::vector<std::filesystem::path> &search_dirs,
              std::filesystem::path &out_resolved_path);

  // Try to parse `sub_range` as an inlining of of C/C++ code into the Datalog
  // module.
  void ParseInlineCode(ParsedModuleImpl *module);

  // Try to parse `sub_range` as a database name declaration.
  void ParseDatabase(ParsedModuleImpl *module);

  // Try to match a clause with a declaration.
  bool TryMatchClauseWithDecl(ParsedModuleImpl *module,
                              ParsedClauseImpl *clause);

  // Try to match a predicate with a declaration.
  ParsedDeclarationImpl *TryMatchPredicateWithDecl(
      ParsedModuleImpl *module, Token pred_name,
      const std::vector<ParsedVariableImpl *> &pred_vars,
      Token pred_end_tok);

  // Try to parse `sub_range` as a clause.
  void ParseClause(ParsedModuleImpl *module,
                   ParsedDeclarationImpl *decl = nullptr);

  // Create a variable.
  ParsedVariableImpl *CreateVariable(ParsedClauseImpl *clause, Token name,
                                       bool is_param, bool is_arg);

  // Create a variable to name a literal.
  ParsedVariableImpl *CreateLiteralVariable(ParsedClauseImpl *clause,
                                              Token tok, bool is_param,
                                              bool is_arg);

  // Perform type checking/assignment. Returns `false` if there was an error.
  bool AssignTypes(ParsedModuleImpl *module);

  // Parse a display, returning the parsed module.
  //
  // NOTE(pag): Due to display caching, this may return a prior parsed module,
  //            so as to avoid re-parsing a module.
  std::optional<ParsedModule> ParseDisplay(Display display,
                                           const DisplayConfiguration &config);
};

// Add a declaration or redeclaration to the module. This makes sure that
// all locals in a redecl list have the same kind.
template <typename T>
T *ParserImpl::AddDecl(ParsedModuleImpl *module, DeclarationKind kind,
                       Token name, size_t arity) {
  const auto scope_range = SubTokenRange();

  if (arity > kMaxArity) {
    context->error_log.Append(scope_range, name.SpellingRange())
        << "Too many arguments to predicate '" << name
        << "; maximum number of arguments is " << kMaxArity;
    return nullptr;
  }

#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Wconversion"
#endif
  parse::IdInterpreter interpreter = {};
  interpreter.info.atom_name_id = name.IdentifierId();
  interpreter.info.arity = arity;
#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic pop
#endif

  const auto id = interpreter.flat;
  auto first_decl_it = context->declarations.find(id);

  // This is the first time we've seen this declaration.
  if (first_decl_it == context->declarations.end()) {
    auto decl = module->declarations.CreateDerived<T>(module, kind);
    context->declarations.emplace(id, decl);
    return decl;

  // We've seen this declaration before.
  } else {
    const auto first_decl = first_decl_it->second;

    const auto &decl_context = first_decl->context;

    // This is a re-declaration of the wrong kind.
    if (decl_context->kind != kind) {
      auto err = context->error_log.Append(scope_range, name.SpellingRange());
      err << "Cannot re-declare '" << first_decl->name << "' as a "
          << first_decl->KindName();

      DisplayRange first_decl_range(
          ParsedDeclaration(first_decl).SpellingRange());
      err.Note(first_decl_range) << "Original declaration is here";

      return nullptr;

    // This is a valid re-declaration.
    } else {
      return module->declarations.CreateDerived<T>(module, decl_context);
    }
  }
}

}  // namespace hyde
