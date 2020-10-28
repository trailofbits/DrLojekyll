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

    // FIXME(blarsen): grabbing the current path in parser construction is a hidden dependency
    std::filesystem::path cwd = std::filesystem::current_path();
    import_search_paths.push_back(cwd);
    include_search_paths[1].push_back(cwd);
  }

  // Search paths for looking for imports.
  std::vector<std::filesystem::path> import_search_paths;

  // Search paths for looking for includes.
  std::vector<std::filesystem::path> include_search_paths[2];

  // All parsed modules.
  Node<ParsedModule> *root_module{nullptr};

  // Mapping of display IDs to parsed modules. This exists to prevent the same
  // module from being parsed multiple times.
  //
  // NOTE(pag): Cyclic module imports are valid.
  std::unordered_map<unsigned, std::weak_ptr<Node<ParsedModule>>>
      parsed_modules;

  const DisplayManager display_manager;
  const ErrorLog error_log;
  const StringPool string_pool;

  // Keeps track of the global locals. All parsed modules shared this.
  std::unordered_map<uint64_t, Node<ParsedDeclaration> *> declarations;
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
  std::unordered_map<unsigned, Node<ParsedVariable> *> prev_named_var;

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

  // Read until the next new line token. This fill sup `sub_tokens` with all
  // read tokens.
  void ReadLine(void);

  // Read until the next period. This fill sup `sub_tokens` with all
  // read tokens. Returns `false` if a period is not found.
  bool ReadStatement(void);

  // Add a declaration or redeclaration to the module.
  template <typename T>
  Node<T> *AddDecl(Node<ParsedModule> *module, DeclarationKind kind, Token name,
                   size_t arity);

  // Remove a declaration.
  template <typename T>
  void RemoveDecl(std::unique_ptr<Node<T>> decl);

  template <typename T>
  void FinalizeDeclAndCheckConsistency(
      std::vector<std::unique_ptr<Node<T>>> &decl_list,
      std::unique_ptr<Node<T>> decl);

  // Try to parse an inline predicate.
  bool ParseAggregatedPredicate(Node<ParsedModule> *module,
                                Node<ParsedClause> *clause,
                                std::unique_ptr<Node<ParsedPredicate>> functor,
                                Token &tok, DisplayPosition &next_pos);

  // Try to parse all of the tokens.
  void ParseAllTokens(Node<ParsedModule> *module);

  // Try to parse `sub_range` as a functor, adding it to `module` if successful.
  void ParseFunctor(Node<ParsedModule> *module);

  // Try to parse `sub_range` as a query, adding it to `module` if successful.
  void ParseQuery(Node<ParsedModule> *module);

  // Try to parse `sub_range` as a message, adding it to `module` if successful.
  void ParseMessage(Node<ParsedModule> *module);

  // Try to parse `sub_range` as a local or export rule, adding it to `module`
  // if successful.
  template <typename NodeType, DeclarationKind kDeclKind,
            Lexeme kIntroducerLexeme>
  void ParseLocalExport(Node<ParsedModule> *module,
                        std::vector<std::unique_ptr<Node<NodeType>>> &out_vec);

  // Try to parse `sub_range` as an import.
  void ParseImport(Node<ParsedModule> *module);

  // Try to parse `sub_range` as an include of C/C++ code.
  void ParseInclude(Node<ParsedModule> *module);

  // Try to resolve the given path to a file on the filesystem, searching the
  // provided directories in order.
  // TODO(blarsen): fix up the filesystem error-handling behavior here
  static std::error_code
  ResolvePath(const std::filesystem::path &path,
              const std::vector<std::filesystem::path> &search_dirs,
              std::filesystem::path &out_resolved_path);

  // Try to parse `sub_range` as an inlining of of C/C++ code into the Datalog
  // module.
  void ParseInline(Node<ParsedModule> *module);

  // Try to match a clause with a declaration.
  bool TryMatchClauseWithDecl(Node<ParsedModule> *module,
                              Node<ParsedClause> *clause);

  // Try to match a predicate with a declaration.
  bool TryMatchPredicateWithDecl(Node<ParsedModule> *module,
                                 Node<ParsedPredicate> *pred);

  // Try to parse `sub_range` as a clause.
  void ParseClause(Node<ParsedModule> *module, Token negation_tok = Token(),
                   Node<ParsedDeclaration> *decl = nullptr);

  // Create a variable.
  Node<ParsedVariable> *CreateVariable(Node<ParsedClause> *clause, Token name,
                                       bool is_param, bool is_arg);

  // Create a variable to name a literal.
  Node<ParsedVariable> *CreateLiteralVariable(Node<ParsedClause> *clause,
                                              Token tok, bool is_param,
                                              bool is_arg);

  // Perform type checking/assignment. Returns `false` if there was an error.
  bool AssignTypes(Node<ParsedModule> *module);

  // Parse a display, returning the parsed module.
  //
  // NOTE(pag): Due to display caching, this may return a prior parsed module,
  //            so as to avoid re-parsing a module.
  std::optional<ParsedModule> ParseDisplay(Display display,
                                           const DisplayConfiguration &config);
};

#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Wconversion"
#endif

// Add a declaration or redeclaration to the module. This makes sure that
// all locals in a redecl list have the same kind.
template <typename T>
Node<T> *ParserImpl::AddDecl(Node<ParsedModule> *module, DeclarationKind kind,
                             Token name, size_t arity) {
  const auto scope_range = SubTokenRange();

  if (arity > kMaxArity) {
    context->error_log.Append(scope_range, name.SpellingRange())
        << "Too many arguments to predicate '" << name
        << "; maximum number of arguments is " << kMaxArity;
    return nullptr;
  }

  parse::IdInterpreter interpreter = {};
  interpreter.info.atom_name_id = name.IdentifierId();
  interpreter.info.arity = arity;

  const auto id = interpreter.flat;
  auto first_decl_it = context->declarations.find(id);

  std::shared_ptr<parse::DeclarationContext> decl_context;

  // This is the first time we've seen this declaration.
  if (first_decl_it == context->declarations.end()) {
    auto decl = new Node<T>(module, kind);
    decl_context = decl->context;
    context->declarations.emplace(id, decl);
    return decl;
  }

  // We've seen this declaration before.
  const auto first_decl = first_decl_it->second;

  decl_context = first_decl->context;
  if (decl_context->kind != kind) {
    auto err = context->error_log.Append(scope_range, name.SpellingRange());
    err << "Cannot re-declare '" << first_decl->name << "' as a "
        << first_decl->KindName();

    DisplayRange first_decl_range(first_decl->directive_pos,
                                  first_decl->rparen.NextPosition());
    err.Note(first_decl_range) << "Original declaration is here";

    return nullptr;

  } else {
    return new Node<T>(module, decl_context);
  }
}

// Remove a declaration.
template <typename T>
void ParserImpl::RemoveDecl(std::unique_ptr<Node<T>> decl) {
  if (!decl) {
    return;
  }

  parse::IdInterpreter interpreter = {};
  interpreter.info.atom_name_id = decl->name.IdentifierId();
  interpreter.info.arity = decl->parameters.size();
  const auto id = interpreter.flat;

  assert(decl.get() == decl->context->redeclarations.back());
  decl->context->redeclarations.pop_back();

  if (!decl->context->redeclarations.empty()) {
    decl->context->redeclarations.back()->next_redecl = nullptr;

  } else {
    context->declarations.erase(id);
  }
}

#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic pop
#endif

// Add `decl` to the end of `decl_list`, and make sure `decl` is consistent
// with any prior declarations of the same name.
template <typename T>
void ParserImpl::FinalizeDeclAndCheckConsistency(
    std::vector<std::unique_ptr<Node<T>>> &decl_list,
    std::unique_ptr<Node<T>> decl) {

  const auto scope_range = SubTokenRange();
  const auto num_params = decl->parameters.size();

  const parse::DeclarationContext *decl_context = decl->context.get();
  auto &redecls = decl_context->redeclarations;

  if (1 < redecls.size()) {
    const auto prev_decl = reinterpret_cast<Node<T> *>(redecls.front());
    assert(prev_decl->parameters.size() == num_params);

    // The first usage of a functor in a `mutable` attribute marks it as
    // being a merge functor and forces a `1:1` range.
    if (prev_decl->range != decl->range &&
        prev_decl->range == FunctorRange::kOneToOne && prev_decl->is_merge) {
      assert(!decl->is_merge);
      decl->is_merge = true;
      decl->range = FunctorRange::kOneToOne;
    }

    // The inferred range specifications don't match.
    if (prev_decl->range != decl->range) {
      DisplayRange prev_range_spec(prev_decl->range_begin_opt.Position(),
                                   prev_decl->range_end_opt.NextPosition());

      DisplayRange curr_range_spec(decl->range_begin_opt.Position(),
                                   decl->range_end_opt.NextPosition());

      // Examine the concrete syntax to produce a meaningful error message.
      if (prev_decl->range_begin_opt.IsValid() &&
          decl->range_begin_opt.IsValid()) {
        auto err = context->error_log.Append(scope_range, curr_range_spec);
        err << "Functor range specifier differs from prior range specifier";

        auto note = err.Note(T(prev_decl).SpellingRange(), prev_range_spec);
        note << "Previous range specifier is here";

      } else if (prev_decl->range_begin_opt.IsValid()) {
        auto err = context->error_log.Append(scope_range);
        err << "Functor uses default zero-or-more range specifier, but prior "
            << "declaration explicitly changes the range";

        auto note = err.Note(T(prev_decl).SpellingRange(), prev_range_spec);
        note << "Previous range specifier is here";

      } else if (decl->range_begin_opt.IsValid()) {
        auto err = context->error_log.Append(scope_range, curr_range_spec);
        err << "Functor explicitly specifies a non-default range specifier "
            << "that is different than the implicit zero-or-more specification";

        auto note = err.Note(T(prev_decl).SpellingRange());
        note << "Previous declaration uses the implicit zero-or-more range "
             << "specification";

      // Neither functor has explicit `range` syntax, and they disagree.
      } else {
        auto err = context->error_log.Append(scope_range);
        err << "Inferred functor range differs from prior inferred range";

        auto note = err.Note(T(prev_decl).SpellingRange());
        note << "Previous declaration is here";
      }

      RemoveDecl(std::move(decl));
      return;
    }

    // Make sure all parameters bindings, types, merge declarations, etc. match
    // across all re-declarations.
    for (size_t i = 0; i < num_params; ++i) {
      const auto prev_param = prev_decl->parameters[i].get();
      const auto curr_param = decl->parameters[i].get();
      if (prev_param->opt_binding.Lexeme() !=
          curr_param->opt_binding.Lexeme()) {
        auto err = context->error_log.Append(
            scope_range, curr_param->opt_binding.SpellingRange());
        err << "Parameter binding attribute differs";

        auto note = err.Note(T(prev_decl).SpellingRange(),
                             prev_param->opt_binding.SpellingRange());
        note << "Previous parameter binding attribute is here";

        RemoveDecl(std::move(decl));
        return;
      }

      if (prev_param->opt_merge != curr_param->opt_merge) {
        auto err = context->error_log.Append(
            scope_range, curr_param->opt_binding.SpellingRange());
        err << "Mutable parameter's merge operator differs";

        auto note = err.Note(T(prev_decl).SpellingRange(),
                             prev_param->opt_binding.SpellingRange());
        note << "Previous mutable attribute declaration is here";

        RemoveDecl(std::move(decl));
        return;
      }

      if (prev_param->opt_type.Kind() != curr_param->opt_type.Kind()) {
        auto err = context->error_log.Append(
            scope_range, curr_param->opt_type.SpellingRange());
        err << "Parameter type specification differs";

        auto note = err.Note(T(prev_decl).SpellingRange(),
                             prev_param->opt_type.SpellingRange());
        note << "Previous type specification is here";

        RemoveDecl(std::move(decl));
        return;
      }
    }

    // Make sure this inline attribute matches the prior one.
    if (prev_decl->inline_attribute.Lexeme() !=
        decl->inline_attribute.Lexeme()) {
      auto err = context->error_log.Append(
          scope_range, decl->inline_attribute.SpellingRange());
      err << "Inline attribute differs";

      auto note = err.Note(T(prev_decl).SpellingRange(),
                           prev_decl->inline_attribute.SpellingRange());
      note << "Previous inline attribute is here";
      RemoveDecl(std::move(decl));
      return;
    }
  }

  // We've made it without errors; add it in.
  if (!decl_list.empty()) {
    decl_list.back()->next = decl.get();
  }
  decl_list.emplace_back(std::move(decl));
}

}  // namespace hyde
