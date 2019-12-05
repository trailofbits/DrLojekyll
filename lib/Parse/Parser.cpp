// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include <drlojekyll/Parse/Parser.h>

#include <cstring>
#include <cassert>
#include <sstream>
#include <unordered_map>
#include <vector>

#include <drlojekyll/Display/DisplayManager.h>
#include <drlojekyll/Display/DisplayReader.h>
#include <drlojekyll/Lex/Lexer.h>
#include <drlojekyll/Lex/StringPool.h>
#include <drlojekyll/Lex/Token.h>
#include <drlojekyll/Parse/ErrorLog.h>
#include <drlojekyll/Util/FileManager.h>

#include "Parse.h"

namespace hyde {
namespace {

// Information shared by multiple parsers.
class SharedParserContext {
 public:
  SharedParserContext(const DisplayManager &display_manager_,
                      const ErrorLog &error_log_)
      : display_manager(display_manager_),
        error_log(error_log_) {
    search_paths.push_back(file_manager.CurrentDirectory());
  }

  // Search paths for looking for imports.
  std::vector<Path> search_paths;

  // All parsed modules.
  std::vector<std::unique_ptr<parse::Impl<ParsedModule>>> modules;

  // Mapping of display IDs to parsed modules. This exists to prevent the same
  // module from being parsed multiple times.
  //
  // NOTE(pag): Cyclic module imports are valid.
  std::unordered_map<unsigned, parse::Impl<ParsedModule> *> parsed_modules;

  FileManager file_manager;
  const DisplayManager display_manager;
  const ErrorLog error_log;
  StringPool string_pool;

  // Keeps track of the global locals. All parsed modules shared this.
  std::unordered_map<uint64_t, parse::Impl<ParsedDeclaration> *> declarations;
};

}  // namespace

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

  // Set of previously named variables in the current clause.
  std::unordered_map<unsigned, parse::Impl<ParsedVariable> *> prev_named_var;

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
  parse::Impl<T> *AddDecl(
      parse::Impl<ParsedModule> *module, DeclarationKind kind, Token name,
      size_t arity);

  // Remove a declaration.
  template <typename T>
  void RemoveDecl(std::unique_ptr<parse::Impl<T>> decl);

  template <typename T>
  void AddDeclAndCheckConsistency(
      std::vector<std::unique_ptr<parse::Impl<T>>> &decl_list,
      std::unique_ptr<parse::Impl<T>> decl);

  // Try to parse an inline predicate.
  bool ParseAggregatedPredicate(
      parse::Impl<ParsedModule> *module, parse::Impl<ParsedClause> *clause,
      std::unique_ptr<parse::Impl<ParsedPredicate>> functor,
      Token &tok, DisplayPosition &next_pos);

  // Try to parse all of the tokens.
  void ParseAllTokens(parse::Impl<ParsedModule> *module);

  // Try to parse `sub_range` as a functor, adding it to `module` if successful.
  void ParseFunctor(parse::Impl<ParsedModule> *module);

  // Try to parse `sub_range` as a query, adding it to `module` if successful.
  void ParseQuery(parse::Impl<ParsedModule> *module);

  // Try to parse `sub_range` as a message, adding it to `module` if successful.
  void ParseMessage(parse::Impl<ParsedModule> *module);

  // Try to parse `sub_range` as an exported rule, adding it to `module`
  // if successful.
  void ParseExport(parse::Impl<ParsedModule> *module);

  // Try to parse `sub_range` as a local rule, adding it to `module`
  // if successful.
  void ParseLocal(parse::Impl<ParsedModule> *module);

  // Try to parse `sub_range` as an import.
  void ParseImport(parse::Impl<ParsedModule> *module);

  // Try to match a clause with a declaration.
  bool TryMatchClauseWithDecl(parse::Impl<ParsedModule> *module,
                              parse::Impl<ParsedClause> *clause);

  // Try to match a predicate with a declaration.
  bool TryMatchPredicateWithDecl(parse::Impl<ParsedModule> *module,
                                 parse::Impl<ParsedPredicate> *pred);

  // Try to parse `sub_range` as a clause.
  void ParseClause(parse::Impl<ParsedModule> *module,
                   parse::Impl<ParsedDeclaration> *decl=nullptr);

  // Create a variable.
  parse::Impl<ParsedVariable> *CreateVariable(
      parse::Impl<ParsedClause> *clause,
      Token name, bool is_param, bool is_arg);

  // Create a variable to name a literal.
  parse::Impl<ParsedVariable> *CreateLiteralVariable(
      parse::Impl<ParsedClause> *clause, Token tok);

  // Parse a display, returning the parsed module.
  //
  // NOTE(pag): Due to display caching, this may return a prior parsed module,
  //            so as to avoid re-parsing a module.
  ParsedModule ParseDisplay(
      Display display, const DisplayConfiguration &config);
};

// Add a declaration or redeclaration to the module. This makes sure that
// all locals in a redecl list have the same kind.
template <typename T>
parse::Impl<T> *ParserImpl::AddDecl(
    parse::Impl<ParsedModule> *module, DeclarationKind kind, Token name,
    size_t arity) {

  parse::IdInterpreter interpreter = {};
  interpreter.info.atom_name_id = name.IdentifierId();
  interpreter.info.arity = arity;

  const auto id = interpreter.flat;
  auto first_decl_it = context->declarations.find(id);

  if (first_decl_it != context->declarations.end()) {
    const auto first_decl = first_decl_it->second;
    auto &decl_context = first_decl->context;
    if (decl_context->kind == kind) {
      Error err(context->display_manager, SubTokenRange(),
                name.SpellingRange());
      err << "Cannot re-declare '" << first_decl->name
          << "' as a " << first_decl->KindName();

      DisplayRange first_decl_range(
          first_decl->directive_pos, first_decl->rparen.NextPosition());
      auto note = err.Note(context->display_manager, first_decl_range);
      note << "Original declaration is here";

      context->error_log.Append(std::move(err));
      return nullptr;

    } else {
      auto decl = new parse::Impl<T>(module, decl_context);
      decl_context->redeclarations.back()->next_redecl = decl;
      decl_context->redeclarations.push_back(decl);
      return decl;
    }
  } else {
    auto decl = new parse::Impl<T>(module, kind);
    auto &decl_context = decl->context;
    decl_context->redeclarations.push_back(decl);
    context->declarations.emplace(id, decl);
    return decl;
  }
}

// Lex all the tokens from a display. This fills up `tokens` with the tokens.
// There should always be at least one token in the display, e.g. for EOF or
// error.
void ParserImpl::LexAllTokens(Display display) {
  DisplayReader reader(display);
  lexer.ReadFromDisplay(reader);
  tokens.clear();

  Token tok;
  DisplayPosition first_pos;
  DisplayPosition prev_pos;
  auto ignore_line = false;

  while (lexer.TryGetNextToken(context->string_pool, &tok)) {
    const auto lexeme = tok.Lexeme();

    if (first_pos.IsInvalid()) {
      first_pos = tok.Position();
    }

    // Report lexing errors and fix up the tokens into non-errors.
    if (tok.IsInvalid()) {

      Error error;
      if (tok.ErrorPosition() == tok.Position()) {
        error = Error(
            context->display_manager, DisplayRange(first_pos, tok.NextPosition()),
            tok.SpellingRange());
      } else {
        error = Error(
            context->display_manager, DisplayRange(first_pos, tok.NextPosition()),
            tok.ErrorPosition());
      }

      switch (lexeme) {
        case Lexeme::kInvalid:
          assert(false);
          break;

        case Lexeme::kInvalidStreamOrDisplay: {
          std::stringstream error_ss;
          if (reader.TryGetErrorMessage(&error_ss)) {
            error << error_ss.str();
          } else {
            error << "Error reading data from stream";
          }
          tok = Token::FakeEndOfFile(tok.Position());
          break;
        }

        case Lexeme::kInvalidDirective:
          error << "Unrecognized declaration '" << tok << "'";
          ignore_line = true;
          break;

        case Lexeme::kInvalidNumber:
          error << "Invalid number literal '" << tok << "'";
          tok = Token::FakeNumberLiteral(tok.Position(), tok.SpellingWidth());
          break;

        case Lexeme::kInvalidNewLineInString:
          error << "Invalid new line character in string literal";
          tok = Token::FakeStringLiteral(tok.Position(), tok.SpellingWidth());
          break;

        case Lexeme::kInvalidEscapeInString:
          error << "Invalid escape character '" << tok.InvalidEscapeChar()
                << "' in string literal";
          tok = Token::FakeStringLiteral(tok.Position(), tok.SpellingWidth());
          break;

        case Lexeme::kInvalidTypeName:
          error << "Invalid type name '" << tok << "'";
          tok = Token::FakeType(tok.Position(), tok.SpellingWidth());
          break;

        case Lexeme::kInvalidUnterminatedString:
          error << "Unterminated string literal";
          ignore_line = true;
          // NOTE(pag): No recovery, i.e. exclude the token.
          break;

        case Lexeme::kInvalidUnknown:
          if (ignore_line) {
            error << "Unexpected character sequence '" << tok.InvalidChar()
                  << "' in stream";
            break;

          // There's no recovery, but we'll add it into the token list to
          // report a more useful error at a higher level (i.e. in the
          // context of parsing something).
          } else {
            tokens.push_back(tok);
            continue;
          }

        default:
          break;
      }

      context->error_log.Append(error);

      // If we're not skipping the rest of the line, and if we corrected the
      // token, then add it in.
      if (!ignore_line) {
        tokens.push_back(tok);
      }

      //  Ensures that an EOF token is always at the end of a file
      tokens.push_back(Token::FakeEndOfFile(tok.Position()));


    } else if (Lexeme::kEndOfFile == lexeme) {
      tokens.push_back(tok);
      prev_pos = tok.Position();
      ignore_line = false;

    // If we're in a mode where we're ignoring the rest of the line, then only
    // add in this token if it moves us to a new line. We get into the ignore
    // line mode when we encounter unrecognized directives.
    } else if (ignore_line) {

      int num_lines = 0;
      prev_pos.TryComputeDistanceTo(tok.NextPosition(), nullptr,
                                    &num_lines, nullptr);
      if (num_lines) {
        ignore_line = false;
        tokens.push_back(tok);
      }

    // All good, add the token in.
    } else {
      tokens.push_back(tok);
      prev_pos = tok.Position();
    }
  }
}

// Read the next token.
bool ParserImpl::ReadNextToken(Token &tok_out) {
  while (next_tok_index < tokens.size()) {
    const auto &next_tok = tokens[next_tok_index];
    ++next_tok_index;

    switch (next_tok.Lexeme()) {
      case Lexeme::kInvalid:
      case Lexeme::kInvalidDirective:
      case Lexeme::kInvalidNumber:
      case Lexeme::kInvalidNewLineInString:
      case Lexeme::kInvalidEscapeInString:
      case Lexeme::kInvalidUnterminatedString:
      case Lexeme::kComment:
        continue;

      // We pass these through so that we can report more meaningful
      // errors in locations relevant to specific parses.
      case Lexeme::kInvalidUnknown:
      default:
        tok_out = next_tok;
        return true;
    }
  }
  return false;
}

// Read the next sub token.
bool ParserImpl::ReadNextSubToken(Token &tok_out) {
  if (next_sub_tok_index < sub_tokens.size()) {
    tok_out = sub_tokens[next_sub_tok_index];
    ++next_sub_tok_index;
    return true;
  } else {
    return false;
  }
}

// Read until the next new line token. If a new line token appears inside of
// a parenthesis, then it is permitted.This fill sup `sub_tokens` with all
// read tokens, excluding any whitespace found along the way.
void ParserImpl::ReadLine(void) {
  Token tok;

  int paren_count = 0;
  DisplayPosition unmatched_paren;

  while (ReadNextToken(tok)) {
    switch (tok.Lexeme()) {
      case Lexeme::kEndOfFile:
        UnreadToken();
        return;
      case Lexeme::kPuncOpenParen:
        sub_tokens.push_back(tok);
        ++paren_count;
        continue;
      case Lexeme::kPuncCloseParen:
        sub_tokens.push_back(tok);
        if (!paren_count) {
          unmatched_paren = tok.Position();
        } else {
          --paren_count;
        }
        continue;
      case Lexeme::kWhitespace:
        if (tok.Line() < tok.NextPosition().Line()) {
          if (paren_count) {
            continue;
          } else {
            return;
          }
        } else {
          continue;
        }
      case Lexeme::kComment:
        continue;
      default:
        sub_tokens.push_back(tok);
        continue;
    }
  }

  if (unmatched_paren.IsValid()) {
    Error err(context->display_manager, SubTokenRange(),
              unmatched_paren);
    err << "Unmatched parenthesis";
    context->error_log.Append(std::move(err));
  }
}

// Read until the next period. This fill sup `sub_tokens` with all
// read tokens (excluding any whitespace found along the way).
// Returns `false` if a period is not found.
bool ParserImpl::ReadStatement(void) {
  Token tok;

  while (ReadNextToken(tok)) {
    switch (tok.Lexeme()) {
      case Lexeme::kEndOfFile:
        UnreadToken();
        return false;

      case Lexeme::kPuncPeriod:
        sub_tokens.push_back(tok);
        return true;

      case Lexeme::kWhitespace:
      case Lexeme::kComment:
        continue;

      default:
        sub_tokens.push_back(tok);
        continue;
    }
  }

  // We should have reached an EOF.
  assert(false);
  return false;
}

// Unread the last read token.
void ParserImpl::UnreadToken(void) {
  if (next_tok_index) {
    --next_tok_index;
  }
}

// Unread the last read sub token.
void ParserImpl::UnreadSubToken(void) {
  if (next_sub_tok_index) {
    --next_sub_tok_index;
  }
}

// Return the display range of all the sub tokens.
DisplayRange ParserImpl::SubTokenRange(void) const {
  assert(!sub_tokens.empty());
  return DisplayRange(sub_tokens.front().Position(),
                      sub_tokens.back().NextPosition());
}

// Remove a declaration.
template <typename T>
void ParserImpl::RemoveDecl(std::unique_ptr<parse::Impl<T>> decl) {
  if (!decl) {
    return;
  }

  parse::IdInterpreter interpreter = {};
  interpreter.info.atom_name_id = decl->name.IdentifierId();
  interpreter.info.arity = decl->parameters.size();
  const auto id = interpreter.flat;

  decl->context->redeclarations.pop_back();
  if (!decl->context->redeclarations.empty()) {
    decl->context->redeclarations.back()->next_redecl = nullptr;

  } else {
    context->declarations.erase(id);
  }
}

// Add `decl` to the end of `decl_list`, and make sure `decl` is consistent
// with any prior declarations of the same name.
template <typename T>
void ParserImpl::AddDeclAndCheckConsistency(
    std::vector<std::unique_ptr<parse::Impl<T>>> &decl_list,
    std::unique_ptr<parse::Impl<T>> decl) {

  if (1 < decl->context->redeclarations.size()) {
    const auto prev_decl = reinterpret_cast<parse::Impl<T> *>(
        decl->context->redeclarations.front());
    const auto num_params = decl->parameters.size();
    assert(prev_decl->parameters.size() == num_params);

    for (size_t i = 0; i < num_params; ++i) {
      const auto prev_param = prev_decl->parameters[i].get();
      const auto curr_param = decl->parameters[i].get();
      if (prev_param->opt_binding.Lexeme() != curr_param->opt_binding.Lexeme()) {
        Error err(context->display_manager, SubTokenRange(),
                  curr_param->opt_binding.SpellingRange());
        err << "Parameter binding attribute differs";

        auto note = err.Note(
            context->display_manager,
            T(prev_decl).SpellingRange(),
            prev_param->opt_binding.SpellingRange());
        note << "Previous parameter binding attribute is here";

        context->error_log.Append(std::move(err));
        RemoveDecl(std::move(decl));
        return;
      }

      if (prev_param->opt_type.Lexeme() != curr_param->opt_type.Lexeme()) {
        Error err(context->display_manager, SubTokenRange(),
                  curr_param->opt_type.SpellingRange());
        err << "Parameter type specification differs";

        auto note = err.Note(
            context->display_manager,
            T(prev_decl).SpellingRange(),
            prev_param->opt_type.SpellingRange());
        note << "Previous type specification is here";

        context->error_log.Append(std::move(err));
        RemoveDecl(std::move(decl));
        return;
      }
    }

    // Make sure this functor's complexity attribute matches the prior one.
    if (prev_decl->complexity_attribute.Lexeme() !=
        decl->complexity_attribute.Lexeme()) {
      Error err(context->display_manager, SubTokenRange(),
                decl->complexity_attribute.SpellingRange());
      err << "Complexity attribute differs";

      auto note = err.Note(
          context->display_manager,
          T(prev_decl).SpellingRange(),
          prev_decl->complexity_attribute.SpellingRange());
      note << "Previous complexity attribute is here";
      context->error_log.Append(std::move(err));
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

// Try to parse `sub_range` as a functor, adding it to `module` if successful.
void ParserImpl::ParseFunctor(parse::Impl<ParsedModule> *module) {
  Token tok;
  if (!ReadNextSubToken(tok)) {
    assert(false);
  }

  assert(tok.Lexeme() == Lexeme::kHashFunctorDecl);

  // State transition diagram for parsing functors.
  //
  //               .---------------<-------<------<-------.
  //     0      1  |        2         3       4       5   |
  // -- atom -- ( -+-> bound/free -> type -> var -+-> , --'
  //               aggregate/summary              |
  //                                              '-> ) -> trivial/complex
  //                                                  6           7

  int state = 0;
  std::unique_ptr<parse::Impl<ParsedFunctor>> functor;
  std::unique_ptr<parse::Impl<ParsedParameter>> param;
  std::vector<std::unique_ptr<parse::Impl<ParsedParameter>>> params;

  DisplayPosition next_pos;
  Token last_aggregate;
  Token last_summary;
  Token last_free;
  Token name;

  for (next_pos = tok.NextPosition();
       ReadNextSubToken(tok);
       next_pos = tok.NextPosition()) {

    const auto lexeme = tok.Lexeme();
    switch (state) {
      case 0:
        if (Lexeme::kIdentifierAtom == lexeme) {
          name = tok;
          state = 1;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected atom here (lower case identifier) for the name of "
              << "the functor being declared, got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }
      case 1:
        if (Lexeme::kPuncOpenParen == lexeme) {
          state = 2;
          continue;
        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected opening parenthesis here to begin parameter list of "
              << "functor '" << name << "', but got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 2:
        if (Lexeme::kKeywordBound == lexeme) {
          param.reset(new parse::Impl<ParsedParameter>);
          param->opt_binding = tok;
          state = 3;
          continue;

        } else if (Lexeme::kKeywordFree == lexeme) {
          last_free = tok;
          param.reset(new parse::Impl<ParsedParameter>);
          param->opt_binding = tok;
          state = 3;
          continue;

        } else if (Lexeme::kKeywordAggregate == lexeme) {
          last_aggregate = tok;
          param.reset(new parse::Impl<ParsedParameter>);
          param->opt_binding = tok;
          state = 3;
          continue;

        } else if (Lexeme::kKeywordSummary == lexeme) {
          last_summary = tok;
          param.reset(new parse::Impl<ParsedParameter>);
          param->opt_binding = tok;
          state = 3;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected binding specifier ('bound', 'free', 'aggregate', "
              << "or 'summary') in parameter "
              << "declaration of functor '" << name << "', " << "but got '"
              << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 3:
        if (tok.IsType()) {
          param->opt_type = tok;
          state = 4;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected type name here ('@'-prefixed identifier) for "
              << "parameter in functor '" << name << "', but got '"
              << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 4:
        if (Lexeme::kIdentifierVariable == lexeme) {
          param->name = tok;
          state = 5;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected named variable here (capitalized identifier) as a "
              << "parameter name of functor '" << name << "', but got '"
              << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 5:
        // Add the parameter in.
        if (!params.empty()) {
          params.back()->next = param.get();
        }
        params.emplace_back(std::move(param));

        if (Lexeme::kPuncComma == lexeme) {
          state = 2;
          continue;

        } else if (Lexeme::kPuncCloseParen == lexeme) {
          functor.reset(AddDecl<ParsedFunctor>(
              module, DeclarationKind::kFunctor, name, params.size()));
          
          if (last_aggregate.IsValid() || last_summary.IsValid()) {
              functor->is_aggregate = true;
          }

          if (!functor) {
            return;
          } else {
            functor->rparen = tok;
            functor->directive_pos = sub_tokens.front().Position();
            functor->name = name;
            functor->parameters.swap(params);
            state = 6;
            continue;
          }

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected either a comma or a closing parenthesis here, "
              << "but got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 6:
        if (Lexeme::kKeywordTrivial == lexeme) {
          functor->complexity_attribute = tok;
          state = 7;
          continue;

        } else if (Lexeme::kKeywordComplex == lexeme) {
          functor->complexity_attribute = tok;
          state = 7;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected either a 'trivial' or 'complex' attribute here, "
              << "but got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          RemoveDecl<ParsedFunctor>(std::move(functor));
          return;
        }

      case 7: {
        DisplayRange err_range(
            tok.Position(), sub_tokens.back().NextPosition());
        Error err(context->display_manager, SubTokenRange(),
                  err_range);
        err << "Unexpected tokens following declaration of the '"
            << name << "' functor";
        context->error_log.Append(std::move(err));
        state = 8;  // Ignore further errors, but add the functor in.
        continue;
      }

      case 8:
        continue;
    }
  }

  if (state == 6) {
    Error err(context->display_manager, SubTokenRange(), next_pos);
    err << "Expected either a 'trivial' or 'complex' attribute here";
    context->error_log.Append(std::move(err));
    RemoveDecl<ParsedFunctor>(std::move(functor));
    return;

  } else if (state < 7) {
    Error err(context->display_manager, SubTokenRange(), next_pos);
    err << "Incomplete functor declaration; the declaration must be "
        << "placed entirely on one line";
    context->error_log.Append(std::move(err));
    RemoveDecl<ParsedFunctor>(std::move(functor));

  } else if (last_summary.IsValid() && !last_aggregate.IsValid()) {
    Error err(context->display_manager, SubTokenRange(),
              last_summary.SpellingRange());
    err << "Functor '" << functor->name << "' produces a summary value without "
        << "any corresponding aggregate inputs";
    context->error_log.Append(std::move(err));
    RemoveDecl<ParsedFunctor>(std::move(functor));

  } else if (last_aggregate.IsValid() && !last_summary.IsValid()) {
    Error err(context->display_manager, SubTokenRange(),
              last_aggregate.SpellingRange());
    err << "Functor '" << functor->name << "' aggregates values without "
        << "producing any corresponding summary outputs";
    context->error_log.Append(std::move(err));
    RemoveDecl<ParsedFunctor>(std::move(functor));

  // Don't let us have both summary and free variables.
  //
  // NOTE(pag): For now we permit aggregate and bound variables.
  } else if (last_summary.IsValid() && last_free.IsValid()) {
    Error err(context->display_manager, SubTokenRange(),
              last_summary.SpellingRange());
    err << "Functor cannot bind both summary and free variables";
    auto note = err.Note(context->display_manager, SubTokenRange(),
                         last_free.SpellingRange());
    note << "Free variable is here";
    context->error_log.Append(std::move(err));
    RemoveDecl<ParsedFunctor>(std::move(functor));

  // If this is a redeclaration, check it for consistency against prior
  // declarations. Functors require special handling for things like aggregate/
  // summary parameters.
  } else if (1 < functor->context->redeclarations.size()) {

    const auto redecl = functor->context->redeclarations[0];
    auto i = 0u;

    for (auto &redecl_param : redecl->parameters) {
      const auto &param = functor->parameters[i++];
      const auto lexeme = param->opt_binding.Lexeme();
      const auto redecl_lexeme = redecl_param->opt_binding.Lexeme();

      // We can redeclare bound/free parameters with other variations of
      // bound/free, but the aggregation binding types must be equivalent.
      if (lexeme != redecl_lexeme &&
          ((lexeme == Lexeme::kKeywordAggregate ||
            lexeme == Lexeme::kKeywordSummary) ||
           (redecl_lexeme == Lexeme::kKeywordAggregate ||
            redecl_lexeme == Lexeme::kKeywordSummary))) {

        Error err(context->display_manager, SubTokenRange(),
                  ParsedParameter(param.get()).SpellingRange());
        err << "Aggregation functor '" << functor->name
            << "' cannot be re-declared with different aggregation semantics.";

        auto note = err.Note(
            context->display_manager,
            ParsedDeclaration(redecl).SpellingRange(),
            ParsedParameter(redecl_param.get()).SpellingRange());
        note << "Conflicting aggregation parameter is specified here";

        context->error_log.Append(std::move(err));
        RemoveDecl<ParsedFunctor>(std::move(functor));
        return;
      }
    }

    // Do generic consistency checking.
    AddDeclAndCheckConsistency<ParsedFunctor>(
        module->functors, std::move(functor));

  } else {
    module->functors.emplace_back(std::move(functor));
  }
}

// Try to parse `sub_range` as a query, adding it to `module` if successful.
void ParserImpl::ParseQuery(parse::Impl<ParsedModule> *module) {
  Token tok;
  if (!ReadNextSubToken(tok)) {
    assert(false);
  }

  assert(tok.Lexeme() == Lexeme::kHashQueryDecl);

  // State transition diagram for parsing queries.
  //
  //               .--------------<-------<-------<-------.
  //     0      1  |        2         3       4       5   |
  // -- atom -- ( -+-> bound/free -> type -> var -+-> , --'
  //                                              |
  //                                              '-> )
  //                                                  6

  int state = 0;
  std::unique_ptr<parse::Impl<ParsedQuery>> query;
  std::unique_ptr<parse::Impl<ParsedParameter>> param;
  std::vector<std::unique_ptr<parse::Impl<ParsedParameter>>> params;

  DisplayPosition next_pos;
  Token name;

  for (next_pos = tok.NextPosition();
       ReadNextSubToken(tok);
       next_pos = tok.NextPosition()) {

    const auto lexeme = tok.Lexeme();
    switch (state) {
      case 0:
        if (Lexeme::kIdentifierAtom == lexeme) {
          name = tok;
          state = 1;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected atom here (lower case identifier) for the name of "
              << "the query being declared, got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }
      case 1:
        if (Lexeme::kPuncOpenParen == lexeme) {
          state = 2;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected opening parenthesis here to begin parameter list of "
              << "query '" << name << "', but got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 2:
        if (Lexeme::kKeywordBound == lexeme) {
          param.reset(new parse::Impl<ParsedParameter>);
          param->opt_binding = tok;
          state = 3;
          continue;

        } else if (Lexeme::kKeywordFree == lexeme) {
          param.reset(new parse::Impl<ParsedParameter>);
          param->opt_binding = tok;
          state = 3;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected binding specifier ('bound' or 'free') in parameter "
              << "declaration of query '" << name << "', " << "but got '"
              << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 3:
        if (tok.IsType()) {
          param->opt_type = tok;
          state = 4;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected type name here ('@'-prefixed identifier) for "
              << "parameter in query '" << name << "', but got '"
              << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 4:
        if (Lexeme::kIdentifierVariable == lexeme) {
          param->name = tok;
          state = 5;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected named variable here (capitalized identifier) as a "
              << "parameter name of query '" << name << "', but got '"
              << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 5:
        // Add the parameter in.
        if (!params.empty()) {
          params.back()->next = param.get();
        }
        params.push_back(std::move(param));

        if (Lexeme::kPuncComma == lexeme) {
          state = 2;
          continue;

        } else if (Lexeme::kPuncCloseParen == lexeme) {
          query.reset(AddDecl<ParsedQuery>(
              module, DeclarationKind::kQuery, name, params.size()));
          if (!query) {
            return;

          } else {
            query->rparen = tok;
            query->name = name;
            query->parameters.swap(params);
            query->directive_pos = sub_tokens.front().Position();
            state = 6;
            continue;
          }

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected either a comma or a closing parenthesis here, "
              << "but got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 6: {
        DisplayRange err_range(
            tok.Position(), sub_tokens.back().NextPosition());
        Error err(context->display_manager, SubTokenRange(), err_range);
        err << "Unexpected tokens following declaration of the '"
            << name << "' query";
        context->error_log.Append(std::move(err));
        state = 7;  // Ignore further errors, but add the query in.
        continue;
      }

      case 7:
        continue;
    }
  }

  if (state < 6) {
    Error err(context->display_manager, SubTokenRange(), next_pos);
    err << "Incomplete query declaration; the declaration must be "
        << "placed entirely on one line";
    context->error_log.Append(std::move(err));

    RemoveDecl<ParsedQuery>(std::move(query));
  } else {
    AddDeclAndCheckConsistency<ParsedQuery>(
        module->queries, std::move(query));
  }
}

// Try to parse `sub_range` as a message, adding it to `module` if successful.
void ParserImpl::ParseMessage(parse::Impl<ParsedModule> *module) {
  Token tok;
  if (!ReadNextSubToken(tok)) {
    assert(false);
  }

  assert(tok.Lexeme() == Lexeme::kHashMessageDecl);

  // State transition diagram for parsing messages.
  //
  //               .---------<-------<-------.
  //     0      1  |    2        4       5   |
  // -- atom -- ( -+-> type --> var -.-> , --'
  //                                 |
  //                                 '-> )
  //                                     6

  int state = 0;
  std::unique_ptr<parse::Impl<ParsedMessage>> message;
  std::unique_ptr<parse::Impl<ParsedParameter>> param;
  std::vector<std::unique_ptr<parse::Impl<ParsedParameter>>> params;

  DisplayPosition next_pos;
  Token name;

  for (next_pos = tok.NextPosition();
       ReadNextSubToken(tok);
       next_pos = tok.NextPosition()) {

    const auto lexeme = tok.Lexeme();
    switch (state) {
      case 0:
        if (Lexeme::kIdentifierAtom == lexeme) {
          name = tok;
          state = 1;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected atom here (lower case identifier) for the name of "
              << "the message being declared, got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 1:
        if (Lexeme::kPuncOpenParen == lexeme) {
          state = 2;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected opening parenthesis here to begin parameter list of "
              << "message '" << name << "', but got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 2:
        if (tok.IsType()) {
          param.reset(new parse::Impl<ParsedParameter>);
          param->opt_type = tok;
          state = 3;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected type name here ('@'-prefixed identifier) for "
              << "parameter in message '" << name << "', but got '"
              << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 3:
        if (Lexeme::kIdentifierVariable == lexeme) {
          param->name = tok;
          state = 4;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected named variable here (capitalized identifier) as a "
              << "parameter name of message '" << name << "', but got '"
              << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 4:
        // Add the parameter in.
        if (!params.empty()) {
          params.back()->next = param.get();
        }
        params.push_back(std::move(param));

        if (Lexeme::kPuncComma == lexeme) {
          state = 2;
          continue;

        } else if (Lexeme::kPuncCloseParen == lexeme) {
          message.reset(AddDecl<ParsedMessage>(
              module, DeclarationKind::kMessage, name, params.size()));
          if (!message) {
            return;

          } else {
            message->rparen = tok;
            message->name = name;
            message->parameters.swap(params);
            message->directive_pos = sub_tokens.front().Position();
            state = 5;
            continue;
          }

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected either a comma or a closing parenthesis here, "
              << "but got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 5: {
        DisplayRange err_range(
            tok.Position(), sub_tokens.back().NextPosition());
        Error err(context->display_manager, SubTokenRange(), err_range);
        err << "Unexpected tokens following declaration of the '"
            << message->name << "' message";
        context->error_log.Append(std::move(err));
        state = 6;  // Ignore further errors, but add the message in.
        continue;
      }

      case 6:
        continue;
    }
  }

  if (state < 5) {
    Error err(context->display_manager, SubTokenRange(), next_pos);
    err << "Incomplete message declaration; the declaration must be "
        << "placed entirely on one line";
    context->error_log.Append(std::move(err));

    RemoveDecl<ParsedMessage>(std::move(message));

  } else {
    AddDeclAndCheckConsistency<ParsedMessage>(
        module->messages, std::move(message));
  }
}

// Try to parse `sub_range` as an exported rule, adding it to `module`
// if successful.
void ParserImpl::ParseExport(parse::Impl<ParsedModule> *module) {
  Token tok;
  if (!ReadNextSubToken(tok)) {
    assert(false);
  }

  assert(tok.Lexeme() == Lexeme::kHashExportDecl);

  // State transition diagram for parsing exports.
  //
  //               .---------<--------<-------.
  //     0      1  |      2       3       4   |
  // -- atom -- ( -+--> type-+-> var -.-> , --'
  //               '._______.'        |
  //                                  '-> )
  //                                      5

  int state = 0;
  std::unique_ptr<parse::Impl<ParsedExport>> exp;
  std::unique_ptr<parse::Impl<ParsedParameter>> param;
  std::vector<std::unique_ptr<parse::Impl<ParsedParameter>>> params;

  DisplayPosition next_pos;
  Token name;

  for (next_pos = tok.NextPosition();
       ReadNextSubToken(tok);
       next_pos = tok.NextPosition()) {

    const auto lexeme = tok.Lexeme();
    switch (state) {
      case 0:
        if (Lexeme::kIdentifierAtom == lexeme) {
          name = tok;
          state = 1;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected atom here (lower case identifier) for the name of "
              << "the export being declared, got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 1:
        if (Lexeme::kPuncOpenParen == lexeme) {
          state = 2;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected opening parenthesis here to begin parameter list of "
              << "export '" << name << "', but got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 2:
        param.reset(new parse::Impl<ParsedParameter>);
        if (tok.IsType()) {
          param->opt_type = tok;
          state = 3;
          continue;

        } else if (Lexeme::kIdentifierVariable == lexeme) {
          param->name = tok;
          state = 4;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected type name ('@'-prefixed identifier) or variable "
              << "name (capitalized identifier) for parameter in export '"
              << name << "', but got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 3:
        if (Lexeme::kIdentifierVariable == lexeme) {
          param->name = tok;
          state = 4;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected named variable here (capitalized identifier) as a "
              << "parameter name of export '" << name << "', but got '"
              << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 4:
        // Add the parameter in.
        if (!params.empty()) {
          params.back()->next = param.get();
        }
        params.push_back(std::move(param));

        if (Lexeme::kPuncComma == lexeme) {
          state = 2;
          continue;

        } else if (Lexeme::kPuncCloseParen == lexeme) {
          exp.reset(AddDecl<ParsedExport>(
              module, DeclarationKind::kExport, name, params.size()));
          if (!exp) {
            return;

          } else {
            exp->rparen = tok;
            exp->name = name;
            exp->parameters.swap(params);
            exp->directive_pos = sub_tokens.front().Position();
            state = 5;
            continue;
          }

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected either a comma or a closing parenthesis here, "
              << "but got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 5: {
        DisplayRange err_range(
            tok.Position(), sub_tokens.back().NextPosition());
        Error err(context->display_manager, SubTokenRange(), err_range);
        err << "Unexpected tokens following declaration of the '"
            << exp->name << "' export";
        context->error_log.Append(std::move(err));
        state = 6;  // Ignore further errors, but add the export in.
        continue;
      }

      case 6:
        continue;
    }
  }

  if (state < 5) {
    Error err(context->display_manager, SubTokenRange(), next_pos);
    err << "Incomplete export declaration; the declaration must be "
        << "placed entirely on one line";
    context->error_log.Append(std::move(err));
    RemoveDecl<ParsedExport>(std::move(exp));

  } else {
    AddDeclAndCheckConsistency<ParsedExport>(
        module->exports, std::move(exp));
  }
}

// Try to parse `sub_range` as an exported rule, adding it to `module`
// if successful.
void ParserImpl::ParseLocal(parse::Impl<ParsedModule> *module) {
  Token tok;
  if (!ReadNextSubToken(tok)) {
    assert(false);
  }

  assert(tok.Lexeme() == Lexeme::kHashLocalDecl);

  // State transition diagram for parsing locals.
  //
  //               .--------<--------<-------.
  //     0      1  |     2       3       4   |
  // -- atom -- ( -+-> type-+-> var -+-> , --'
  //               '.______.'        |
  //                                 '-> )
  //                                     5

  int state = 0;
  std::unique_ptr<parse::Impl<ParsedLocal>> local;
  std::unique_ptr<parse::Impl<ParsedParameter>> param;
  std::vector<std::unique_ptr<parse::Impl<ParsedParameter>>> params;

  DisplayPosition next_pos;
  Token name;

  for (next_pos = tok.NextPosition();
       ReadNextSubToken(tok);
       next_pos = tok.NextPosition()) {

    const auto lexeme = tok.Lexeme();
    switch (state) {
      case 0:
        if (Lexeme::kIdentifierAtom == lexeme) {
          name = tok;
          state = 1;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected atom here (lower case identifier) for the name of "
              << "the local being declared, got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }
      case 1:
        if (Lexeme::kPuncOpenParen == lexeme) {
          state = 2;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected opening parenthesis here to begin parameter list of "
              << "local '" << name << "', but got '"
              << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 2:
        param.reset(new parse::Impl<ParsedParameter>);
        if (tok.IsType()) {
          param->opt_type = tok;
          state = 3;
          continue;

        } else if (Lexeme::kIdentifierVariable == lexeme) {
          param->name = tok;
          state = 4;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected type name ('@'-prefixed identifier) or variable "
              << "name (capitalized identifier) for parameter in local '"
              << local->name << "', but got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 3:
        if (Lexeme::kIdentifierVariable == lexeme) {
          param->name = tok;
          state = 4;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected named variable here (capitalized identifier) as a "
              << "parameter name of local '" << name
              << "', but got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 4:
        // Add the parameter in.
        if (!params.empty()) {
          params.back()->next = param.get();
        }
        params.push_back(std::move(param));

        if (Lexeme::kPuncComma == lexeme) {
          state = 2;
          continue;

        } else if (Lexeme::kPuncCloseParen == lexeme) {
          local.reset(AddDecl<ParsedLocal>(
              module, DeclarationKind::kLocal, name, params.size()));
          if (!local) {
            return;

          } else {
            local->rparen = tok;
            local->name = name;
            local->parameters.swap(params);
            local->directive_pos = sub_tokens.front().Position();
            state = 5;
            continue;
          }

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected either a comma or a closing parenthesis here, "
              << "but got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 5: {
        DisplayRange err_range(
            tok.Position(), sub_tokens.back().NextPosition());
        Error err(context->display_manager, SubTokenRange(), err_range);
        err << "Unexpected tokens following declaration of the '"
            << local->name << "' local";
        context->error_log.Append(std::move(err));
        state = 6;  // Ignore further errors, but add the local in.
        continue;
      }

      case 6:
        continue;
    }
  }

  if (state < 5) {
    Error err(context->display_manager, SubTokenRange(), next_pos);
    err << "Incomplete local declaration; the declaration must be "
        << "placed entirely on one line";
    context->error_log.Append(std::move(err));
    RemoveDecl<ParsedLocal>(std::move(local));

  // Add the local to the module.
  } else {
    AddDeclAndCheckConsistency<ParsedLocal>(
        module->locals, std::move(local));
  }
}

// Try to parse `sub_range` as an import. We eagerly parse imported modules
// before continuing the parse of our current module. This is so that we
// can make sure all dependencies on exported rules, messages, etc. are
// visible. This is partially enforced by ensuring that imports must precede
// and declarations, and declarations must precede their uses. The result is
// that we can built up a semantically meaningful parse tree in a single pass.
void ParserImpl::ParseImport(parse::Impl<ParsedModule> *module) {
  Token tok;
  if (!ReadNextSubToken(tok)) {
    assert(false);
  }

  assert(tok.Lexeme() == Lexeme::kHashImportModuleStmt);

  std::unique_ptr<parse::Impl<ParsedImport>> imp(
      new parse::Impl<ParsedImport>);
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

  for (auto search_path : context->search_paths) {
    full_path = std::string_view();

    ec = context->file_manager.PushDirectory(search_path);
    if (ec) {
      continue;
    }

    Path path(context->file_manager, path_str);
    ec = path.RealPath(&full_path);
    if (ec) {
      context->file_manager.PopDirectory();
      continue;
    }
  }

  if (ec || full_path.empty()) {
    Error err(context->display_manager, SubTokenRange());
    err << "Unable to locate module '" << tok
        << "' requested by import declaration";
    context->error_log.Append(std::move(err));
    return;
  }

  context->file_manager.PopDirectory();

  // Save the old first search path, and put in the directory containing the
  // about-to-be parsed module as the new first search path.
  Path prev_search0 = context->search_paths[0];
  context->search_paths[0] =
      Path(context->file_manager, full_path).DirName();

  DisplayConfiguration sub_config = module->config;
  sub_config.name = full_path;

  // Go and parse the module.
  ParserImpl sub_impl(context);
  auto sub_mod = sub_impl.ParseDisplay(
      context->display_manager.OpenPath(full_path, sub_config),
      sub_config);

  // Restore the old first search path.
  context->search_paths[0] = prev_search0;

  imp->imported_module = sub_mod.impl;

  if (!module->imports.empty()) {
    module->imports.back()->next = imp.get();
  }

  module->imports.push_back(std::move(imp));
}

// Try to match a clause with a declaration.
bool ParserImpl::TryMatchClauseWithDecl(
    parse::Impl<ParsedModule> *module, parse::Impl<ParsedClause> *clause) {

  parse::IdInterpreter interpreter = {};
  interpreter.info.atom_name_id = clause->name.IdentifierId();
  interpreter.info.arity = clause->head_variables.size();
  const auto id = interpreter.flat;

  // There are no forward declarations associated with this ID.
  // We'll report an error, then invent one.
  if (!context->declarations.count(id)) {
    Error err(context->display_manager, SubTokenRange(),
              DisplayRange(
                  clause->name.Position(), clause->rparen.NextPosition()));
    err << "Missing declaration for '" << clause->name << "/"
        << clause->head_variables.size() << "'";
    context->error_log.Append(std::move(err));

    // Recover by adding a local declaration; this will let us keep
    // parsing.
    auto local = new parse::Impl<ParsedLocal>(module, DeclarationKind::kLocal);
    local->directive_pos = clause->name.Position();
    local->name = clause->name;
    local->rparen = clause->rparen;
    parse::Impl<ParsedParameter> *prev_param = nullptr;
    for (const auto &param_var : clause->head_variables) {
      auto param = new parse::Impl<ParsedParameter>;
      param->name = param_var->name;
      if (prev_param) {
        prev_param->next = param;
      }
      local->parameters.emplace_back(param);
      prev_param = param;
    }

    module->locals.emplace_back(local);
    context->declarations.emplace(id, local);
  }

  clause->declaration = context->declarations[id];

  DisplayRange directive_range(
      clause->declaration->directive_pos,
      clause->declaration->rparen.NextPosition());

  const auto &decl_context = clause->declaration->context;

  // Don't allow us to define clauses for functors.
  if (decl_context->kind == DeclarationKind ::kFunctor) {
    Error err(context->display_manager, SubTokenRange());
    err << "Cannot define a clause for the functor '"
        << clause->name << "'; functors are defined by native "
        << "code modules";

    auto note = err.Note(context->display_manager,
                         directive_range);
    note << "Functor '" << clause->name
         << "' is first declared here";

    context->error_log.Append(std::move(err));
    return false;

  // Don't allow us to define clauses for predicates exported by
  // other modules.
  } else if (decl_context->kind == DeclarationKind ::kExport &&
             module != clause->declaration->module) {
    Error err(context->display_manager, SubTokenRange());
    err << "Cannot define a clause '" << clause->name
        << "' for predicate exported by another module";

    auto note = err.Note(context->display_manager,
                         directive_range);
    note << "Predicate '" << clause->name << "' is declared here";

    context->error_log.Append(std::move(err));
    return false;
  }

  return true;
}

// Try to match a clause with a declaration.
bool ParserImpl::TryMatchPredicateWithDecl(
    parse::Impl<ParsedModule> *module, parse::Impl<ParsedPredicate> *pred) {

  parse::IdInterpreter interpreter = {};
  interpreter.info.atom_name_id = pred->name.IdentifierId();
  interpreter.info.arity = pred->argument_uses.size();
  const auto id = interpreter.flat;

  // There are no forward declarations associated with this ID.
  // We'll report an error and invent one.
  if (!context->declarations.count(id)) {
    Error err(context->display_manager, SubTokenRange(),
              DisplayRange(
                  pred->name.Position(), pred->rparen.NextPosition()));
    err << "Missing declaration for '" << pred->name << "/"
        << pred->argument_uses.size() << "'";
    context->error_log.Append(std::move(err));

    // Recover by adding a local declaration; this will let us keep
    // parsing.
    auto local = new parse::Impl<ParsedLocal>(module, DeclarationKind::kLocal);
    local->directive_pos = pred->name.Position();
    local->name = pred->name;
    local->rparen = pred->rparen;

    parse::Impl<ParsedParameter> *prev_param = nullptr;
    for (const auto &arg_use : pred->argument_uses) {
      auto param = new parse::Impl<ParsedParameter>;
      param->name = arg_use->used_var->name;
      if (prev_param) {
        prev_param->next = param;
      }
      local->parameters.emplace_back(param);
      prev_param = param;
    }

    module->locals.emplace_back(local);
    context->declarations.emplace(id, local);
  }

  pred->declaration = context->declarations[id];
  return true;
}

// Create a variable.
parse::Impl<ParsedVariable> *ParserImpl::CreateVariable(
    parse::Impl<ParsedClause> *clause,
    Token name, bool is_param, bool is_arg) {

  auto var = new parse::Impl<ParsedVariable>;
  if (is_param) {
    if (!clause->head_variables.empty()) {
      clause->head_variables.back()->next = var;
    }
    clause->head_variables.emplace_back(var);

  } else {
    if (!clause->body_variables.empty()) {
      clause->body_variables.back()->next = var;
    }
    clause->body_variables.emplace_back(var);
  }

  var->name = name;
  var->clause = clause;
  var->is_parameter = is_param;
  var->is_argument = is_arg;

  if (Lexeme::kIdentifierVariable == name.Lexeme()) {
    auto &prev = prev_named_var[name.IdentifierId()];
    if (prev) {
      var->first_use = prev->first_use;
      prev->next_use_in_clause = var;

      // All body_variables of the same name share the same set of assignment
      // and comparisons.
      var->assignment_uses = prev->assignment_uses;
      var->comparison_uses = prev->comparison_uses;
      var->parameters = prev->parameters;
      var->argument_uses = prev->argument_uses;

    } else {
      var->first_use = var;
      std::make_shared<parse::UseList<ParsedAssignment>>().swap(
          var->assignment_uses);
      std::make_shared<parse::UseList<ParsedComparison>>().swap(
          var->comparison_uses);
      std::make_shared<parse::UseList<ParsedClause>>().swap(
          var->parameters);
      std::make_shared<parse::UseList<ParsedPredicate>>().swap(
          var->argument_uses);
    }
    prev = var;

  // Unnamed variable.
  } else {
    var->first_use = var;
    std::make_shared<parse::UseList<ParsedAssignment>>().swap(
        var->assignment_uses);
    std::make_shared<parse::UseList<ParsedComparison>>().swap(
        var->comparison_uses);
    std::make_shared<parse::UseList<ParsedClause>>().swap(
        var->parameters);
    std::make_shared<parse::UseList<ParsedPredicate>>().swap(
        var->argument_uses);
  }

  return var;
}

// Create a variable to name a literal.
parse::Impl<ParsedVariable> *ParserImpl::CreateLiteralVariable(
    parse::Impl<ParsedClause> *clause, Token tok) {
  auto lhs = CreateVariable(
      clause,
      Token::Synthetic(Lexeme::kIdentifierUnnamedVariable, tok.SpellingRange()),
      false, false);

  auto assign = new parse::Impl<ParsedAssignment>(lhs);
  assign->rhs.literal = tok;
  assign->rhs.assigned_to = lhs;

  // Add to the clause's assignment list.
  if (!clause->assignment_uses.empty()) {
    clause->assignment_uses.back()->next = assign;
  }
  clause->assignment_uses.emplace_back(assign);

  // Add to the variable's assignment list. We support the list, but for
  // these auto-created variables, there can be only one use.
  lhs->assignment_uses->push_back(&(assign->lhs));
  return lhs;
}

// Try to parse `sub_range` as a clause.
void ParserImpl::ParseClause(parse::Impl<ParsedModule> *module,
                             parse::Impl<ParsedDeclaration> *decl) {

  auto clause = std::make_unique<parse::Impl<ParsedClause>>(module);
  prev_named_var.clear();

  Token tok;
  int state = 0;

  // Approximate state transition diagram for parsing clauses.
  //
  //               .--------<-------.
  //               |                |                      .-> var -->--.
  // -- atom -> ( -+-> var -+-> , --'       .-> var --> = -+           +-->---.
  //                        |               |              '-> literal -'      |
  //                        '-> ) ---> : -+-+                                  |
  //                                      | |                                  |
  //                                      | +------+-> atom -> ( -+-> var -+-. |
  //                                      | '-> ! -'-<-------.    '--- , <-' | |
  //                       .------->------'        .-> over -'               | |
  //                       |                       |                         | |
  //                       '-- , <--+-----+--------+--- ) <------------------' |
  //                                |     '------------------------------------'
  //                           . <--'
  //
  DisplayPosition next_pos;
  DisplayPosition negation_pos;
  parse::Impl<ParsedVariable> *arg = nullptr;
  parse::Impl<ParsedVariable> *lhs = nullptr;
  parse::Impl<ParsedVariable> *rhs = nullptr;
  Token compare_op;
  std::unique_ptr<parse::Impl<ParsedPredicate>> pred;

  for (next_pos = tok.NextPosition();
       ReadNextSubToken(tok);
       next_pos = tok.NextPosition()) {

    const auto lexeme = tok.Lexeme();
    switch (state) {
      case 0:
        if (Lexeme::kIdentifierAtom == lexeme ||
            Lexeme::kIdentifierUnnamedAtom == lexeme) {
          clause->name = tok;
          state = 1;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected atom here (lower case identifier) for the name of "
              << "the clause head being declared, got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 1:
        if (Lexeme::kPuncOpenParen == lexeme) {
          state = 2;
          continue;
        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected opening parenthesis here to begin parameter list of "
              << "clause head '" << clause->name << "', but got '"
              << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 2:
        if (Lexeme::kIdentifierVariable == lexeme) {
          (void) CreateVariable(clause.get(), tok, true, false);
          state = 3;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected variable name (capitalized identifier) for "
              << "parameter in clause '" << clause->name << "', but got '"
              << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 3:
        if (Lexeme::kPuncComma == lexeme) {
          state = 2;
          continue;

        } else if (Lexeme::kPuncCloseParen == lexeme) {
          if (decl) {
            clause->declaration = decl;
            state = 4;
            continue;

          } else if (!TryMatchClauseWithDecl(module, clause.get())) {
            return;

          } else {
            state = 4;
            continue;
          }

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected comma (to continue parameter list) or closing "
              << "parenthesis (to end paramater list) for clause head '"
              << clause->name << "', but got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 4:
        if (Lexeme::kPuncColon == lexeme) {
          state = 5;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected colon to denote the beginning of the body "
              << "of the clause '" << clause->name << "', but got '"
              << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 5:
        if (Lexeme::kIdentifierVariable == lexeme) {
          lhs = CreateVariable(clause.get(), tok, false, false);
          state = 6;
          continue;

        } else if (Lexeme::kLiteralString == lexeme ||
                   Lexeme::kLiteralNumber == lexeme) {
          lhs = CreateLiteralVariable(clause.get(), tok);
          state = 6;
          continue;

        } else if (Lexeme::kPuncExclaim == lexeme) {
          negation_pos = tok.Position();
          state = 11;
          continue;

        } else if (Lexeme::kIdentifierAtom == lexeme) {
          pred.reset(new parse::Impl<ParsedPredicate>(module, clause.get()));
          pred->name = tok;
          state = 12;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected variable name, atom, or exclamation point, but got '"
              << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 6:
        compare_op = tok;
        if (Lexeme::kPuncEqual == lexeme ||
            Lexeme::kPuncNotEqual == lexeme ||
            Lexeme::kPuncLess == lexeme ||
            Lexeme::kPuncGreater == lexeme) {
          state = 7;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected comparison operator, but got '" << tok
              << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 7:
        rhs = nullptr;

        // Allow comparisons with literals by converting the literals into
        // variables and assigning values to those variables.
        if (Lexeme::kLiteralString == lexeme ||
            Lexeme::kLiteralNumber == lexeme) {
          rhs = CreateLiteralVariable(clause.get(), tok);

        } else if (Lexeme::kIdentifierVariable == lexeme) {
          rhs = CreateVariable(clause.get(), tok, false, false);
        }

        if (rhs) {
          // Don't allow comparisons against the same named variable. This
          // simplifies later checks, and makes sure that iteration over the
          // comparisons containing a given variable are well-founded.
          if (lhs->name.IdentifierId() == rhs->name.IdentifierId()) {
            Error err(context->display_manager, SubTokenRange(),
                      DisplayRange(lhs->name.Position(),
                                   rhs->name.NextPosition()));
            err << "Variable '" << lhs->name
                << "' cannot appear on both sides of a comparison";
            context->error_log.Append(std::move(err));
            return;
          }

          const auto compare = new parse::Impl<ParsedComparison>(
              lhs, rhs, compare_op);

          // Add to the LHS variable's comparison use list.
          if (!lhs->comparison_uses->empty()) {
            lhs->comparison_uses->back()->next = &(compare->lhs);
          }
          lhs->comparison_uses->push_back(&(compare->lhs));

          // Add to the RHS variable's comparison use list.
          if (!rhs->comparison_uses->empty()) {
            rhs->comparison_uses->back()->next = &(compare->rhs);
          }
          rhs->comparison_uses->push_back(&(compare->rhs));

          // Add to the clause's comparison list.
          if (!clause->comparison_uses.empty()) {
            clause->comparison_uses.back()->next = compare;
          }
          clause->comparison_uses.emplace_back(compare);

          state = 8;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected variable name or number/string literal, but got '"
              << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 8:
        pred.reset();
        if (Lexeme::kPuncComma == lexeme) {
          state = 5;
          continue;

        } else if (Lexeme::kPuncPeriod == lexeme) {
          state = 9;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected comma or period, but got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 9: {
        DisplayRange err_range(
            tok.Position(), sub_tokens.back().NextPosition());
        Error err(context->display_manager, SubTokenRange(), err_range);
        err << "Unexpected tokens following clause '" << clause->name << "'";
        context->error_log.Append(std::move(err));
        state = 10;  // Ignore further errors, but add the local in.
        continue;
      }

      // We're just chugging tokens at the end, ignore them.
      case 10:
        continue;

      // We think we're parsing a negated predicate.
      case 11:
        if (Lexeme::kIdentifierAtom == lexeme) {
          pred.reset(new parse::Impl<ParsedPredicate>(module, clause.get()));
          pred->name = tok;
          pred->negation_pos = negation_pos;
          state = 12;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected atom here for negated predicate, but got '"
              << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 12:
        if (Lexeme::kPuncOpenParen == lexeme) {
          state = 13;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected opening parenthesis here to test predicate '"
              << pred->name << "', but got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 13:
        arg = nullptr;

        // Convert literals into variables, just-in-time.
        if (Lexeme::kLiteralString == lexeme ||
            Lexeme::kLiteralNumber == lexeme) {
          arg = CreateLiteralVariable(clause.get(), tok);

        } else if (Lexeme::kIdentifierVariable == lexeme ||
                   Lexeme::kIdentifierUnnamedVariable == lexeme) {
          arg = CreateVariable(clause.get(), tok, false, true);
        }

        if (arg) {
          auto use = new parse::Impl<ParsedUse<ParsedPredicate>>(
              UseKind::kArgument, arg, pred.get());

          // Add to this variable's use list.
          if (!arg->argument_uses->empty()) {
            arg->argument_uses->back()->next = use;
          }
          arg->argument_uses->push_back(use);

          // Link the arguments together.
          if (!pred->argument_uses.empty()) {
            pred->argument_uses.back()->used_var->next_var_in_arg_list = arg;
          }

          pred->argument_uses.emplace_back(use);

          state = 14;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected variable or literal here as argument to predicate '"
              << pred->name << "', but got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 14:
        if (Lexeme::kPuncCloseParen == lexeme) {
          pred->rparen = tok;

          if (!TryMatchPredicateWithDecl(module, pred.get())) {
            return;
          }

          // If it's an aggregating functor then we need to follow-up with
          // the `over` keyword.
          auto pred_decl = ParsedDeclaration::Of(ParsedPredicate(pred.get()));
          if (pred_decl.IsFunctor() &&
              ParsedFunctor::From(pred_decl).IsAggregate()) {

            if (pred->negation_pos.IsValid()) {
              Error err(context->display_manager, SubTokenRange(),
                        ParsedPredicate(pred.get()).SpellingRange());
              err << "Cannot negate aggregating functor '" << pred->name << "'";
              context->error_log.Append(std::move(err));
              return;
            }

            state = 15;  // Go look for an `over`.
            continue;

          } else if (pred->negation_pos.IsValid()) {

            const auto kind = pred->declaration->context->kind;

            // We don't allow negation of functors because a requirement that
            // all argument body_variables be bound.
            //
            // For messages, we don't allow negations because we think of them
            // as ephemeral, i.e. not even part of the database. They come in
            // to trigger some action, and leave.
            if (kind == DeclarationKind::kFunctor ||
                kind == DeclarationKind::kMessage) {
              Error err(context->display_manager, SubTokenRange(),
                        ParsedPredicate(pred.get()).SpellingRange());
              err << "Cannot negate " << pred->declaration->KindName()
                  << " '" << pred->name << "'";
              context->error_log.Append(std::move(err));
              return;
            }

            if (!clause->negated_predicates.empty()) {
              clause->negated_predicates.back()->next = pred.get();
            }
            clause->negated_predicates.emplace_back(std::move(pred));
            state = 8;
            continue;

          } else {
            if (!clause->positive_predicates.empty()) {
              clause->positive_predicates.back()->next = pred.get();
            }
            clause->positive_predicates.emplace_back(std::move(pred));
            state = 8;
            continue;
          }

        } else if (Lexeme::kPuncComma == lexeme) {
          state = 13;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected comma or period, but got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 15:
        if (Lexeme::kKeywordOver == lexeme) {
          if (!ParseAggregatedPredicate(
              module, clause.get(), std::move(pred), tok, next_pos)) {
            return;

          } else {
            state = 8;
            continue;
          }

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected 'over' after usage of aggregate functor '"
              << pred->name << "', but got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }
    }
  }

  if (state != 9 && state != 10) {
    Error err(context->display_manager, SubTokenRange(), next_pos);
    err << "Incomplete clause definition";
    context->error_log.Append(std::move(err));
    return;
  }

  // Go make sure we don't have two messages inside of a given clause. In our
  // bottom-up execution model, the "inputs" to the system are messages, which
  // are ephemeral. If we see that as triggering a clause, then we can't
  // easily account for two messages triggering a given clause, when the
  // ordering in time of those messages can be unbounded.
  parse::Impl<ParsedPredicate> *prev_message = nullptr;
  for (auto &used_pred : clause->positive_predicates) {
    auto kind = used_pred->declaration->context->kind;
    if (kind != DeclarationKind::kMessage) {
      continue;
    }
    if (prev_message) {
      Error err(context->display_manager, SubTokenRange(),
                ParsedPredicate(used_pred.get()).SpellingRange());
      err << "Cannot have direct dependency on more than one messages";

      auto note = err.Note(context->display_manager, SubTokenRange(),
                           ParsedPredicate(prev_message).SpellingRange());
      note << "Previous message use is here";

      context->error_log.Append(std::move(err));
      return;

    } else {
      prev_message = used_pred.get();
    }
  }

  // Link all positive predicate uses into their respective declarations.
  for (auto &used_pred : clause->positive_predicates) {
    auto &pred_decl_context = used_pred->declaration->context;
    auto &positive_uses = pred_decl_context->positive_uses;
    if (!positive_uses.empty()) {
      positive_uses.back()->next_use_in_clause = used_pred.get();
    }
    positive_uses.push_back(used_pred.get());
  }

  // Link all negative predicate uses into their respective declarations.
  for (auto &used_pred : clause->positive_predicates) {
    auto &pred_decl_context = used_pred->declaration->context;
    auto &negated_uses = pred_decl_context->negated_uses;
    if (!negated_uses.empty()) {
      negated_uses.back()->next_use_in_clause = used_pred.get();
    }
    negated_uses.push_back(used_pred.get());
  }

  // Link the clause in to the module.
  if (!module->clauses.empty()) {
    module->clauses.back()->next_in_module = clause.get();
  }
  module->clauses.push_back(clause.get());

  // Link the clause in to its respective declaration.
  auto &clause_decl_context = clause->declaration->context;
  auto &clauses = clause_decl_context->clauses;
  if (!clauses.empty()) {
    clauses.back()->next = clause.get();
  }

  // Add this clause to its decl context.
  clauses.emplace_back(std::move(clause));
}

// Try to parse the predicate application following a use of an aggregating
// functor.
bool ParserImpl::ParseAggregatedPredicate(
    parse::Impl<ParsedModule> *module,
    parse::Impl<ParsedClause> *clause,
    std::unique_ptr<parse::Impl<ParsedPredicate>> functor,
    Token &tok, DisplayPosition &next_pos) {

  auto state = 0;

  std::unique_ptr<parse::Impl<ParsedLocal>> anon_decl;
  std::unique_ptr<parse::Impl<ParsedPredicate>> pred;
  std::unique_ptr<parse::Impl<ParsedParameter>> anon_param;

  parse::Impl<ParsedVariable> *arg = nullptr;

  // Build up a token list representing a synthetic clause definition
  // associated with `anon_decl`.
  std::vector<Token> anon_clause_toks;

  DisplayPosition last_pos;

  int brace_count = 1;

  for (; ReadNextSubToken(tok); next_pos = tok.NextPosition()) {
    const auto lexeme = tok.Lexeme();
    switch (state) {
      case 0:
        // An inline predicate; we'll need to invent a declaration and
        // clause for it.
        if (Lexeme::kPuncOpenParen == lexeme) {
          anon_decl.reset(new parse::Impl<ParsedLocal>(
              module, DeclarationKind::kLocal));
          anon_decl->context->redeclarations.push_back(anon_decl.get());
          anon_decl->directive_pos = tok.Position();
          anon_decl->name = Token::Synthetic(
              Lexeme::kIdentifierUnnamedAtom, tok.SpellingRange());
          assert(anon_decl->name.Lexeme() == Lexeme::kIdentifierUnnamedAtom);
          anon_clause_toks.push_back(anon_decl->name);

          assert(tok.Lexeme() == Lexeme::kPuncOpenParen);
          anon_clause_toks.push_back(tok);
          pred.reset(new parse::Impl<ParsedPredicate>(module, clause));
          pred->declaration = anon_decl.get();
          pred->name = anon_decl->name;
          state = 1;
          continue;

        // Direct application.
        } else if (Lexeme::kIdentifierAtom == lexeme) {
          pred.reset(new parse::Impl<ParsedPredicate>(module, clause));
          pred->name = tok;
          state = 6;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected an opening parenthesis or atom (predicate name) "
              << "here for inline predicate, but got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          return false;
        }

      case 1:
        if (tok.IsType()) {
          anon_param.reset(new parse::Impl<ParsedParameter>);
          anon_param->opt_type = tok;
          state = 2;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected a type name for parameter to inline aggregate "
              << "clause, but got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          return false;
        }

      case 2:
        if (Lexeme::kIdentifierVariable == lexeme) {
          anon_param->name = tok;
          if (!anon_decl->parameters.empty()) {
            anon_decl->parameters.back()->next = anon_param.get();
          }
          anon_decl->parameters.emplace_back(std::move(anon_param));

          assert(tok.Lexeme() == Lexeme::kIdentifierVariable);
          anon_clause_toks.push_back(tok);

          arg = CreateVariable(clause, tok, false, true);
          auto use = new parse::Impl<ParsedUse<ParsedPredicate>>(
              UseKind::kArgument, arg, pred.get());

          // Add to this variable's use list.
          if (!arg->argument_uses->empty()) {
            arg->argument_uses->back()->next = use;
          }
          arg->argument_uses->push_back(use);

          // Link the arguments together.
          if (!pred->argument_uses.empty()) {
            pred->argument_uses.back()->used_var->next_var_in_arg_list = arg;
          }

          pred->argument_uses.emplace_back(use);

          state = 3;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected variable name here  for parameter to inline "
              << "aggregate clause, but got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          return false;
        }

      case 3:
        if (Lexeme::kPuncComma == lexeme) {
          state = 1;
          assert(tok.Lexeme() == Lexeme::kPuncComma);
          anon_clause_toks.push_back(tok);
          continue;

        } else if (Lexeme::kPuncCloseParen == lexeme) {
          state = 4;
          anon_decl->rparen = tok;
          pred->rparen = tok;
          assert(tok.Lexeme() == Lexeme::kPuncCloseParen);
          anon_clause_toks.push_back(tok);
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected comma or closing parenthesis here for parameter list"
              << " to inline aggregate clause, but got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          return false;
        }

      case 4:
        if (Lexeme::kPuncOpenBrace == lexeme) {
          const auto colon = Token::Synthetic(
              Lexeme::kPuncColon, tok.SpellingRange());
          assert(colon.Lexeme() == Lexeme::kPuncColon);
          anon_clause_toks.push_back(colon);
          state = 5;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected opening brace here for body of inline aggregate "
              << "clause, but got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          return false;
        }

      // Collect all tokens in the anonymous block as our clause body up until
      // the next matching closing brace, then move on.
      case 5:
        if (Lexeme::kPuncCloseBrace == lexeme) {
          --brace_count;

          if (!brace_count) {
            last_pos = tok.NextPosition();
            anon_clause_toks.push_back(
                Token::Synthetic(Lexeme::kPuncPeriod, tok.SpellingRange()));

            auto prev_next_sub_tok_index = next_sub_tok_index;
            next_sub_tok_index = 0;
            sub_tokens.swap(anon_clause_toks);
            decltype(prev_named_var) prev_prev_named_var;
            prev_prev_named_var.swap(prev_named_var);

            // Go try to parse the synthetic clause body, telling about our
            // synthetic declaration head.
            ParseClause(module, anon_decl.get());

            next_sub_tok_index = prev_next_sub_tok_index;
            sub_tokens.swap(anon_clause_toks);
            prev_prev_named_var.swap(prev_named_var);

            // Unconditionally add the declaration.
            if (!module->locals.empty()) {
              module->locals.back()->next = anon_decl.get();
            }
            module->locals.emplace_back(std::move(anon_decl));

            // It doesn't matter if we parsed it as a clause or not, we always
            // add the declaration, so we may as well permit further parsing.
            goto done;

          } else {
            anon_clause_toks.push_back(tok);
            continue;
          }

        } else {
          if (Lexeme::kPuncOpenBrace == lexeme) {
            ++brace_count;
          }
          anon_clause_toks.push_back(tok);
          continue;
        }

      case 6:
        if (Lexeme::kPuncOpenParen == lexeme) {
          state = 7;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected opening parenthesis here to test predicate '"
              << pred->name << "' used in aggregation, but got '"
              << tok << "' instead";
          context->error_log.Append(std::move(err));
          return false;
        }

      case 7:
        arg = nullptr;

        // Convert literals into variables, just-in-time.
        if (Lexeme::kLiteralString == lexeme ||
            Lexeme::kLiteralNumber == lexeme) {
          arg = CreateLiteralVariable(clause, tok);

        } else if (Lexeme::kIdentifierVariable == lexeme ||
                   Lexeme::kIdentifierUnnamedVariable == lexeme) {
          arg = CreateVariable(clause, tok, false, true);
        }

        if (arg) {
          auto use = new parse::Impl<ParsedUse<ParsedPredicate>>(
              UseKind::kArgument, arg, pred.get());

          // Add to this variable's use list.
          if (!arg->argument_uses->empty()) {
            arg->argument_uses->back()->next = use;
          }
          arg->argument_uses->push_back(use);

          // Link the arguments together.
          if (!pred->argument_uses.empty()) {
            pred->argument_uses.back()->used_var->next_var_in_arg_list = arg;
          }

          pred->argument_uses.emplace_back(use);

          state = 8;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected variable or literal here as argument to predicate '"
              << pred->name << "' used in aggregation, but got '"
              << tok << "' instead";
          context->error_log.Append(std::move(err));
          return false;
        }

      case 8:
        if (Lexeme::kPuncCloseParen == lexeme) {
          last_pos = tok.NextPosition();
          pred->rparen = tok;

          if (!TryMatchPredicateWithDecl(module, pred.get())) {
            return false;
          }

          // If it's an aggregating functor then we need to follow-up with
          // the `over` keyword.
          auto pred_decl = ParsedDeclaration::Of(ParsedPredicate(pred.get()));
          if (pred_decl.IsFunctor() &&
              ParsedFunctor::From(pred_decl).IsAggregate()) {

            Error err(context->display_manager, SubTokenRange(),
                      ParsedPredicate(pred.get()).SpellingRange());

            err << "Cannot aggregate an aggregating functor '" << pred->name
                << "', try using inline clauses instead";
            context->error_log.Append(std::move(err));
            return false;
          }

          goto done;

        } else if (Lexeme::kPuncComma == lexeme) {
          state = 7;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected comma or period, but got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          return false;
        }
    }
  }

done:

  std::unique_ptr<parse::Impl<ParsedAggregate>> agg(
      new parse::Impl<ParsedAggregate>);
  agg->spelling_range = DisplayRange(functor->name.Position(), last_pos);
  agg->functor = std::move(functor);
  agg->predicate = std::move(pred);

  if (!clause->aggregates.empty()) {
    clause->aggregates.back()->next = agg.get();
  }

  clause->aggregates.emplace_back(std::move(agg));
  return true;
}

// Try to parse all of the tokens.
void ParserImpl::ParseAllTokens(parse::Impl<ParsedModule> *module) {
  next_tok_index = 0;
  Token tok;

  DisplayRange first_non_import;

  while (ReadNextToken(tok)) {
    sub_tokens.clear();
    sub_tokens.push_back(tok);
    next_sub_tok_index = 0;

    switch (tok.Lexeme()) {
      case Lexeme::kHashFunctorDecl:
        ReadLine();
        ParseFunctor(module);
        if (first_non_import.IsInvalid()) {
          first_non_import = SubTokenRange();
        }
        break;

      case Lexeme::kHashMessageDecl:
        ReadLine();
        ParseMessage(module);
        if (first_non_import.IsInvalid()) {
          first_non_import = SubTokenRange();
        }
        break;

      case Lexeme::kHashQueryDecl:
        ReadLine();
        ParseQuery(module);
        if (first_non_import.IsInvalid()) {
          first_non_import = SubTokenRange();
        }
        break;

      case Lexeme::kHashExportDecl:
        ReadLine();
        ParseExport(module);
        if (first_non_import.IsInvalid()) {
          first_non_import = SubTokenRange();
        }
        break;

      case Lexeme::kHashLocalDecl:
        ReadLine();
        ParseLocal(module);
        if (first_non_import.IsInvalid()) {
          first_non_import = SubTokenRange();
        }
        break;

      // Import another module, e.g. `#import "foo/bar"`.
      case Lexeme::kHashImportModuleStmt:
        ReadLine();
        if (first_non_import.IsValid()) {
          Error err(context->display_manager, SubTokenRange());
          err << "Cannot have import following a non-import "
              << "declaration/declaration";

          auto note = err.Note(context->display_manager, first_non_import);
          note << "Import must precede this declaration/declaration";

          context->error_log.Append(std::move(err));

        } else {
          ParseImport(module);
        }
        continue;

      // A clause. For example:
      //
      //    foo(...).
      //    foo(...) : ..., ... .
      case Lexeme::kIdentifierAtom:
        if (!ReadStatement()) {
          Error err(context->display_manager, SubTokenRange(),
                    sub_tokens.back().NextPosition());
          err << "Expected period at end of declaration/clause";
          context->error_log.Append(std::move(err));
        } else {
          ParseClause(module);
        }

        if (first_non_import.IsInvalid()) {
          first_non_import = SubTokenRange();
        }
        break;

      // A deletion clause. For example:
      //
      //    !foo(...) : message_to_delete_foo(...).
      case Lexeme::kPuncExclaim:
        if (!ReadStatement()) {
          Error err(context->display_manager, SubTokenRange(),
                    sub_tokens.back().NextPosition());
          err << "Expected period here at end of declaration/clause";
          context->error_log.Append(std::move(err));
        } else {
          Error err(context->display_manager, SubTokenRange());
          err << "Deletion clauses are not yet supported";
          context->error_log.Append(std::move(err));
        }

        if (first_non_import.IsInvalid()) {
          first_non_import = SubTokenRange();
        }
        break;

      case Lexeme::kComment:
      case Lexeme::kWhitespace:
        continue;

      case Lexeme::kEndOfFile:
        return;

      // Don't warn about this, we've already warned about it.
      case Lexeme::kInvalidUnknown:
        continue;

      // Error, an unexpected top-level token.
      default: {
        Error err(context->display_manager, SubTokenRange());
        err << "Unexpected top-level token; expected either a "
            << "clause definition or a declaration";
        context->error_log.Append(std::move(err));
        break;
      }
    }
  }
}

// Parse a display, returning the parsed module.
//
// NOTE(pag): Due to display caching, this may return a prior parsed module,
//            so as to avoid re-parsing a module.
ParsedModule ParserImpl::ParseDisplay(
    Display display, const DisplayConfiguration &config) {
  auto &module = context->parsed_modules[display.Id()];
  if (!module) {
    module = new parse::Impl<ParsedModule>(config);
    context->modules.emplace_back(module);
    LexAllTokens(display);
    module->first = tokens.front();
    module->last = tokens.back();
    ParseAllTokens(module);

    // Go through and remove the local declarations from the
    // `declarations` so that they are no longer visible.
    std::vector<uint64_t> to_erase;
    for (auto &entry : context->declarations) {
      if (entry.second->context->kind == DeclarationKind::kLocal) {
        to_erase.push_back(entry.first);
      }
    }

    for (auto local_id : to_erase) {
      context->declarations.erase(local_id);
    }

  }
  return ParsedModule(module);
}

Parser::~Parser(void) {}

Parser::Parser(const DisplayManager &display_manager,
               const ErrorLog &error_log)
    : impl(new ParserImpl(
        std::make_shared<SharedParserContext>(display_manager, error_log))) {}

// Parse a buffer.
//
// NOTE(pag): `data` must remain valid for the lifetime of the parser's
//            `display_manager`.
ParsedModule Parser::ParseBuffer(std::string_view data,
                                 const DisplayConfiguration &config) const {
  return impl->ParseDisplay(
      impl->context->display_manager.OpenBuffer(data, config),
      config);
}

// Parse a file, specified by its path.
ParsedModule Parser::ParsePath(
    std::string_view path_, const DisplayConfiguration &config) const {

  auto display = impl->context->display_manager.OpenPath(path_, config);
  Path path(impl->context->file_manager, path_);

  // Special case for path parsing, we need to change the parser's current
  // working directory to the directory containing the file being parsed
  // so that we can do file-relative imports.
  auto prev_path0 = impl->context->search_paths[0];
  impl->context->search_paths[0] = path.DirName();

  auto module = impl->ParseDisplay(display, config);
  impl->context->search_paths[0] = prev_path0;  // Restore back to the CWD.

  return module;
}

// Parse an input stream.
//
// NOTE(pag): `is` must remain a valid reference for the lifetime of the
//            parser's `display_manager`.
ParsedModule Parser::ParseStream(
    std::istream &is, const DisplayConfiguration &config) const {
  return impl->ParseDisplay(
      impl->context->display_manager.OpenStream(is, config),
      config);
}

// Add a directory as a search path for files.
void Parser::AddSearchPath(std::string_view path) const {
  impl->context->search_paths.emplace_back(impl->context->file_manager, path);
}

}  // namespace hyde
