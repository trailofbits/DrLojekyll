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
  std::unordered_map<uint64_t, parse::DeclarationBase *> declarations;
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
  parse::Impl<T> *AddDeclarator(
      parse::Impl<ParsedModule> *module, const char *kind, Token name,
      size_t arity);

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
  void ParseClause(parse::Impl<ParsedModule> *module);

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
parse::Impl<T> *ParserImpl::AddDeclarator(
    parse::Impl<ParsedModule> *module, const char *kind, Token name,
    size_t arity) {

  parse::IdInterpreter interpreter = {};
  interpreter.info.atom_name_id = name.IdentifierId();
  interpreter.info.arity = arity;

  const auto id = interpreter.flat;
  auto first_decl_it = context->declarations.find(id);

  if (first_decl_it != context->declarations.end()) {
    const auto first_decl = first_decl_it->second;
    if (strcmp(first_decl->context->kind, kind)) {
      Error err(context->display_manager, SubTokenRange(),
                name.SpellingRange());
      err << "Cannot re-declare " << first_decl->context->kind
          << " as a " << kind;

      DisplayRange first_decl_range(
          first_decl->directive_pos, first_decl->head.rparen.NextPosition());
      auto note = err.Note(context->display_manager, first_decl_range);
      note << "Original declaration is here";

      context->error_log.Append(std::move(err));
      return nullptr;

    } else {
      auto decl = new parse::Impl<T>(module, first_decl->context);
      decl->context->redeclarations.push_back(decl);
      return decl;
    }
  } else {
    auto decl = new parse::Impl<T>(module, kind);
    decl->context->redeclarations.push_back(decl);
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

// Read until the next new line token. This fill sup `sub_tokens` with all
// read tokens, excluding any whitespace found along the way.
void ParserImpl::ReadLine(void) {
  Token tok;

  while (ReadNextToken(tok)) {
    switch (tok.Lexeme()) {
      case Lexeme::kEndOfFile:
        UnreadToken();
        return;
      case Lexeme::kWhitespace:
        if (tok.Line() < tok.NextPosition().Line()) {
          return;
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
  //                                              |
  //                                              '-> )
  //                                                  6

  int state = 0;
  std::unique_ptr<parse::Impl<ParsedFunctor>> functor;
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
          param.reset(new parse::Impl<ParsedParameter>);
          param->opt_binding = tok;
          state = 3;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected binding specifier ('bound' or 'free') in parameter "
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
          functor.reset(AddDeclarator<ParsedFunctor>(
              module, "functor", name, params.size()));
          if (!functor) {
            return;
          } else {
            functor->directive_pos = sub_tokens.front().Position();
            functor->head.name = name;
            functor->head.rparen = tok;
            functor->head.parameters.swap(params);
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
        Error err(context->display_manager, SubTokenRange(),
                  err_range);
        err << "Unexpected tokens following declaration of the '"
            << name << "' functor";
        context->error_log.Append(std::move(err));
        state = 7;  // Ignore further errors, but add the functor in.
        continue;
      }

      case 7:
        continue;
    }
  }

  if (state < 6) {
    Error err(context->display_manager, SubTokenRange(), next_pos);
    err << "Incomplete functor declaration; the declaration must be "
        << "placed entirely on one line";
    context->error_log.Append(std::move(err));

  } else {
    if (!module->functors.empty()) {
      module->functors.back()->next = functor.get();
    }
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
          query->directive_pos = sub_tokens.front().Position();
          query->head.name = tok;
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
          query.reset(AddDeclarator<ParsedQuery>(
              module, "query", name, params.size()));
          if (!query) {
            return;

          } else {
            query->head.rparen = tok;
            query->head.name = name;
            query->head.parameters.swap(params);
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

  } else {
    if (!module->queries.empty()) {
      module->queries.back()->next = query.get();
    }
    module->queries.emplace_back(std::move(query));
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
          message.reset(AddDeclarator<ParsedMessage>(
              module, "message", name, params.size()));
          if (!message) {
            return;

          } else {
            message->head.rparen = tok;
            message->head.name = name;
            message->head.parameters.swap(params);
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
            << message->head.name << "' message";
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

  } else {
    if (!module->messages.empty()) {
      module->messages.back()->next = message.get();
    }
    module->messages.emplace_back(std::move(message));
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
          exp.reset(AddDeclarator<ParsedExport>(
              module, "export", name, params.size()));
          if (!exp) {
            return;

          } else {
            exp->head.rparen = tok;
            exp->head.name = name;
            exp->head.parameters.swap(params);
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
            << exp->head.name << "' export";
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

  } else {
    if (!module->exports.empty()) {
      module->exports.back()->next = exp.get();
    }
    module->exports.emplace_back(std::move(exp));
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
              << local->head.name << "', but got '" << tok << "' instead";
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
          local.reset(AddDeclarator<ParsedLocal>(
              module, "local", name, params.size()));
          if (!local) {
            return;

          } else {
            local->head.rparen = tok;
            local->head.name = name;
            local->head.parameters.swap(params);
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
            << local->head.name << "' local";
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

  // Add the local to the module.
  } else {
    if (!module->locals.empty()) {
      module->locals.back()->next = local.get();
    }
    module->locals.emplace_back(std::move(local));
  }
}

// Try to parse `sub_range` as an import. We eagerly parse imported modules
// before continuing the parse of our current module. This is so that we
// can make sure all dependencies on exported rules, messages, etc. are
// visible. This is partially enforced by ensuring that imports must precede
// and declarations, and declarations must precede their uses. The result is
// that we can built up a semantically meaningful parse tree in a single pass.
void ParserImpl::ParseImport(hyde::parse::Impl<hyde::ParsedModule> *module) {
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
    hyde::parse::Impl<hyde::ParsedModule> *module,
    hyde::parse::Impl<hyde::ParsedClause> *clause) {

  parse::IdInterpreter interpreter = {};
  interpreter.info.atom_name_id = clause->name.IdentifierId();
  interpreter.info.arity = clause->parameters.size();
  const auto id = interpreter.flat;

  // There are no forward declarations associated with this ID.
  // We'll report an error, then invent one.
  if (!context->declarations.count(id)) {
    Error err(context->display_manager, SubTokenRange());
    err << "Predicate '" << clause->name << "/"
        << clause->parameters.size() << "' has no declaration";
    context->error_log.Append(std::move(err));

    // Recover by adding a local declaration; this will let us keep
    // parsing.
    auto local = new parse::Impl<ParsedLocal>(module, "local");
    local->directive_pos = clause->name.Position();
    local->head.name = clause->name;
    local->head.rparen = clause->rparen;
    parse::Impl<ParsedParameter> *prev_param = nullptr;
    for (const auto param_var : clause->parameters) {
      auto param = new parse::Impl<ParsedParameter>;
      param->name = param_var->name;
      if (prev_param) {
        prev_param->next = param;
      }
      local->head.parameters.emplace_back(param);
      prev_param = param;
    }

    module->locals.emplace_back(local);
    context->declarations.emplace(id, local);
  }

  clause->declaration = context->declarations[id];

  DisplayRange directive_range(
      clause->declaration->directive_pos,
      clause->declaration->head.rparen.NextPosition());

  const auto &decl_context = clause->declaration->context;

  // Don't allow us to define clauses for functors.
  if (!strcmp(decl_context->kind, "functor")) {
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

    // Don't allow us to define clauses for positive_predicates exported by
    // other modules.
  } else if (!strcmp(decl_context->kind, "export") &&
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
    hyde::parse::Impl<hyde::ParsedModule> *module,
    hyde::parse::Impl<hyde::ParsedPredicate> *pred) {

  parse::IdInterpreter interpreter = {};
  interpreter.info.atom_name_id = pred->name.IdentifierId();
  interpreter.info.arity = pred->arguments.size();
  const auto id = interpreter.flat;

  // There are no forward declarations associated with this ID.
  // We'll report an error and invent one.
  if (!context->declarations.count(id)) {
    Error err(context->display_manager, SubTokenRange());
    err << "Predicate '" << pred->name << "/"
        << pred->arguments.size() << "' has no declaration";
    context->error_log.Append(std::move(err));

    // Recover by adding a local declaration; this will let us keep
    // parsing.
    auto local = new parse::Impl<ParsedLocal>(module, "local");
    local->directive_pos = pred->name.Position();
    local->head.name = pred->name;
    local->head.rparen = pred->rparen;

    parse::Impl<ParsedParameter> *prev_param = nullptr;
    for (const auto arg_var : pred->arguments) {
      auto param = new parse::Impl<ParsedParameter>;
      param->name = arg_var->name;
      if (prev_param) {
        prev_param->next = param;
      }
      local->head.parameters.emplace_back(param);
      prev_param = param;
    }

    module->locals.emplace_back(local);
    context->declarations.emplace(id, local);
  }

  pred->declaration = context->declarations[id];
  return true;
}

// Try to parse `sub_range` as a clause.
void ParserImpl::ParseClause(parse::Impl<ParsedModule> *module) {

  auto clause = std::make_unique<parse::Impl<ParsedClause>>(module);

  // Start by opportunistically parsing variables.
  std::unordered_map<unsigned, parse::Impl<ParsedVariable> *> prev_named_var;
  std::vector<parse::Impl<ParsedVariable> *> vars;
  vars.reserve(sub_tokens.size());

  parse::Impl<ParsedVariable> *prev_var = nullptr;
  auto seen_rparen = false;
  for (const auto sub_tok : sub_tokens) {
    parse::Impl<ParsedVariable> *var = nullptr;
    const auto lexeme = sub_tok.Lexeme();

    // Named variabes are grouped together in a usage list.
    if (Lexeme::kIdentifierVariable == lexeme) {
      var = new parse::Impl<ParsedVariable>;
      var->name = sub_tok;
      var->clause = clause.get();
      var->is_argument = !seen_rparen;
      auto &prev = prev_named_var[sub_tok.IdentifierId()];
      if (prev) {
        prev->next_use = var;
        var->first_use = prev->first_use;
      } else {
        var->first_use = var;
      }
      prev = var;

    } else if (Lexeme::kIdentifierUnnamedVariable == lexeme) {
      var = new parse::Impl<ParsedVariable>;
      var->clause = clause.get();
      var->name = sub_tok;

    // Mark us as having passed a closing parenthesis, which should be the end
    // of the argument list.
    } else if (Lexeme::kPuncCloseParen == lexeme) {
      seen_rparen = true;
    }

    // Connect together all variables into a single chain.
    if (var) {
      if (prev_var) {
        prev_var->next = var;
      }
      prev_var = var;
      clause->variables.emplace_back(var);
    }

    // Indexed to correspond 1:1 with the token.
    vars.push_back(var);
  }

  Token tok;
  size_t i = 0;

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
  //                                      | '-> ! -'              '--- , <-' | |
  //                       .------->------'                                  | |
  //                       |                                                 | |
  //                       '-- , <--+-----+------------ ) <------------------' |
  //                                |     '------------------------------------'
  //                           . <--'
  //
  DisplayPosition next_pos;
  DisplayPosition negation_pos;
  parse::Impl<ParsedVariable> *lhs = nullptr;
  Token compare_op;
  std::unique_ptr<parse::Impl<ParsedPredicate>> pred;

  for (next_pos = tok.NextPosition();
       ReadNextSubToken(tok);
       next_pos = tok.NextPosition()) {

    const auto tok_index = i++;

    const auto lexeme = tok.Lexeme();
    switch (state) {
      case 0:
        if (Lexeme::kIdentifierAtom == lexeme) {
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
          clause->parameters.push_back(vars[tok_index]);
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
          if (!TryMatchClauseWithDecl(module, clause.get())) {
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
          lhs = vars[tok_index];
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
            Lexeme::kPuncLessEqual == lexeme ||
            Lexeme::kPuncGreater == lexeme ||
            Lexeme::kPuncGreaterEqual == lexeme) {
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
        // It's a comparison.
        if (Lexeme::kIdentifierVariable == lexeme) {
          auto compare = new parse::Impl<ParsedComparison>;
          compare->lhs = lhs;
          compare->rhs = vars[tok_index];
          compare->compare_op = compare_op;
          if (!clause->comparisons.empty()) {
            clause->comparisons.back()->next = compare;
          }
          clause->comparisons.emplace_back(compare);
          state = 8;
          continue;

        // It's an assignment.
        } else if (Lexeme::kLiteralString == lexeme ||
                   Lexeme::kLiteralNumber == lexeme) {
          auto assign = new parse::Impl<ParsedAssignment>;
          assign->lhs = lhs;
          assign->rhs.literal = tok;
          if (!clause->assignments.empty()) {
            clause->assignments.back()->next = assign;
          }
          clause->assignments.emplace_back(assign);
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
        if (Lexeme::kIdentifierVariable == lexeme) {
          pred->arguments.push_back(vars[tok_index]);
          state = 14;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected variable name here as argument to predicate '"
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

          if (pred->negation_pos.IsValid()) {
            const auto kind = pred->declaration->context->kind;

            // We don't allow negation of functors because a requirement that
            // all argument variables be bound.
            //
            // For messages, we don't allow negations because we think of them
            // as ephemeral, i.e. not even part of the database. They come in
            // to trigger some action, and leave.
            if (!strcmp(kind, "functor") || !strcmp(kind, "message")) {
              Error err(context->display_manager, SubTokenRange(),
                        ParsedPredicate(pred.get()).SpellingRange());
              err << "Cannot negate " << kind << " '" << pred->name << "'";
              context->error_log.Append(std::move(err));
              return;
            }

            if (!clause->negated_predicates.empty()) {
              clause->negated_predicates.back()->next = pred.get();
            }
            clause->negated_predicates.emplace_back(std::move(pred));

          } else {
            if (!clause->positive_predicates.empty()) {
              clause->positive_predicates.back()->next = pred.get();
            }
            clause->positive_predicates.emplace_back(std::move(pred));
          }

          state = 8;
          continue;

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
    }
  }

  if (state != 9 && state != 10) {
    Error err(context->display_manager, SubTokenRange(), next_pos);
    err << "Incomplete clause definition";
    context->error_log.Append(std::move(err));

  // Add the local to the module.
  } else {

    // Go make sure we don't have two messages inside of a given clause.
    parse::Impl<ParsedPredicate> *prev_message = nullptr;
    for (auto &used_pred : clause->positive_predicates) {
      auto kind = used_pred->declaration->context->kind;
      if (strcmp(kind, "message")) {
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
      if (!pred_decl_context->positive_uses.empty()) {
        pred_decl_context->positive_uses.back()->next_use = used_pred.get();
      }
      pred_decl_context->positive_uses.push_back(used_pred.get());
    }

    // Link all negative predicate uses into their respective declarations.
    for (auto &used_pred : clause->positive_predicates) {
      auto &pred_decl_context = used_pred->declaration->context;
      if (!pred_decl_context->negated_uses.empty()) {
        pred_decl_context->negated_uses.back()->next_use = used_pred.get();
      }
      pred_decl_context->positive_uses.push_back(used_pred.get());
    }

    // Link the clause in to its respective declaration.
    auto &clause_decl_context = clause->declaration->context;
    if (!clause_decl_context->clauses.empty()) {
      clause_decl_context->clauses.back()->next = clause.get();
    }

    // Add this clause to its decl context.
    clause_decl_context->clauses.emplace_back(std::move(clause));
  }
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
      if (!strcmp(entry.second->context->kind, "local")) {
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
