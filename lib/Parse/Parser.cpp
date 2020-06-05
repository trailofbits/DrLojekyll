// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include "Parser.h"

// TODO(pag):
//  - Add syntax like:
//
//        foo(A, B)
//          : a(...),
//            b(...)
//          : c(...).
//
//    As a variant form of
//
//        foo(A, B)
//          : a(...),
//            b(...).
//        foo(A, B)
//          : c(...).
//
//  - Add an ordered choice operator :-. This would only be allowed for #local
//    definitions, because otherwise you could define clauses in other modules
//    and not guarantee an order.
//        foo(A, B)
//          :- x(Y, Y, Z),
//             ....
//
//    Independent of cut, the semantics of ordered sequencing doesn't actually
//    matter, and so really, it's all about ordering and the CUT operator.
//    One possibility is that if one case is proven, then all others are somehow
//    double checked. If any fail, the tuple is not sent forward. The mechanics
//    of that double checking isn't super idea.
//
//        not_x(A) :- x(A), !, fail.
//        not_x(A).
//
//    What might that look like as a data flow? On its own, perhaps nothing,
//    but we could require, in the sips visior, that any variable appearing
//    before a CUT is bound, and thus the usage with the argument A is bound.
//
//        foo(A) : bar(A), not_x(A).
//
//        [bar A] -+----------------------------------.
//                 +-> [ANTI-JOIN (A,A)] -> not_x(A) --+--> [JOIN (A,A)] -->
//          [x A] -'
//
//    Maybe an ANTI-JOIN is actually what is needed to support !x(A), and
//    CUTs and ordered choice should be ignored!

namespace hyde {

// Lex all the tokens from a display. This fills up `tokens` with the tokens.
// There should always be at least one token in the display, e.g. for EOF or
// error.
void ParserImpl::LexAllTokens(Display display) {

  DisplayReader reader(display);
  lexer.ReadFromDisplay(reader);
  tokens.clear();

  Token tok;
  DisplayPosition line_start_pos;
  DisplayPosition prev_pos;
  auto ignore_line = false;

  while (lexer.TryGetNextToken(context->string_pool, &tok)) {
    const auto lexeme = tok.Lexeme();

    if (Lexeme::kWhitespace == lexeme &&
        tok.Line() < tok.NextPosition().Line()) {
      line_start_pos = tok.NextPosition();
      assert(line_start_pos.IsValid());
    }

    if (line_start_pos.IsInvalid()) {
      line_start_pos = tok.Position();
    }

    // Report lexing errors and fix up the tokens into non-errors.
    if (tok.IsInvalid()) {
      Error error(context->display_manager,
                  DisplayRange(line_start_pos, tok.NextPosition()),
                  tok.SpellingRange(), tok.ErrorPosition());

      switch (lexeme) {
        case Lexeme::kInvalid: {
          std::stringstream error_ss;
          if (reader.TryGetErrorMessage(&error_ss)) {
            error << error_ss.str();
          } else {
            error << "Unrecognized token in stream";
          }
          ignore_line = true;
          break;
        }

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
          error << "Invalid escape character '" << tok.InvalidChar()
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

        case Lexeme::kInvalidUnterminatedCode:
          error << "Unterminated code literal";
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
  if (tokens.empty() || (tokens.back().Lexeme() != Lexeme::kEndOfFile)) {
    // Ensures that there is always an EOF token at the end of a file if there are no tokens
    tokens.push_back(Token::FakeEndOfFile(tok.Position()));
  }

  // Ensures that there is always an EOF token at the end of a file if there are no tokens
  // or if the file does not end with an EOF token
  if (tokens.empty() || (tokens.back().Lexeme() != Lexeme::kEndOfFile)) {
    tokens.push_back(Token::FakeEndOfFile(tok.Position()));
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
      case Lexeme::kInvalidUnterminatedCode:
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

// Try to parse `sub_range` as an exported rule, adding it to `module`
// if successful.
template <typename NodeType, DeclarationKind kDeclKind,
          Lexeme kIntroducerLexeme>
void ParserImpl::ParseLocalExport(
    Node<ParsedModule> *module,
    std::vector<std::unique_ptr<Node<NodeType>>> &out_vec) {
  Token tok;
  if (!ReadNextSubToken(tok)) {
    assert(false);
  }

  assert(tok.Lexeme() == kIntroducerLexeme);
  auto introducer_tok = tok;

  // State transition diagram for parsing locals.
  //
  //               .--------<--------<-------.
  //     0      1  |     2       3       4   |
  // -- atom -- ( -+-> type-+-> var -+-> , --'
  //               '.______.'        |
  //                                 '-> ) -> inline
  //                                     5      6

  int state = 0;
  std::unique_ptr<Node<NodeType>> local;
  std::unique_ptr<Node<ParsedParameter>> param;
  std::vector<std::unique_ptr<Node<ParsedParameter>>> params;

  DisplayPosition next_pos;
  Token name;

  const auto sub_tok_range = SubTokenRange();

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
          Error err(context->display_manager, sub_tok_range,
                    tok.SpellingRange());
          err << "Expected atom here (lower case identifier) for the name of "
              << "the " << introducer_tok << " being declared, got '" << tok
              << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }
      case 1:
        if (Lexeme::kPuncOpenParen == lexeme) {
          state = 2;
          continue;

        } else {
          Error err(context->display_manager, sub_tok_range,
                    tok.SpellingRange());
          err << "Expected opening parenthesis here to begin parameter list of "
              << introducer_tok << " '" << name << "', but got '"
              << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 2:
        if (!param) {
          param.reset(new Node<ParsedParameter>);
        }
        if (tok.IsType()) {
          param->opt_type = tok;
          param->parsed_opt_type = true;
          state = 3;
          continue;

        } else if (Lexeme::kKeywordMutable == lexeme) {
          param->opt_binding = tok;
          state = 5;
          continue;

        } else if (Lexeme::kIdentifierVariable == lexeme) {
          param->name = tok;
          state = 4;
          continue;

        } else {
          Error err(context->display_manager, sub_tok_range,
                    tok.SpellingRange());
          err << "Expected type name or variable name (capitalized identifier) "
              << "for parameter in " << introducer_tok << " '" << name
              << "', but got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 3:
        if (Lexeme::kIdentifierVariable == lexeme) {
          param->name = tok;
          state = 4;
          continue;

        } else {
          Error err(context->display_manager, sub_tok_range,
                    tok.SpellingRange());
          err << "Expected named variable here (capitalized identifier) as a "
              << "parameter name of " << introducer_tok << " '" << name
              << "', but got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 4:
        // Add the parameter in.
        if (!params.empty()) {
          params.back()->next = param.get();

          if (params.size() == kMaxArity) {
            Error err(context->display_manager, sub_tok_range,
                      ParsedParameter(param.get()).SpellingRange());
            err << "Too many parameters to " << introducer_tok << " '" << name
                << "'; the maximum number of parameters is " << kMaxArity;
            context->error_log.Append(std::move(err));
            return;
          }
        }

        param->index = static_cast<unsigned>(params.size());
        params.push_back(std::move(param));

        if (Lexeme::kPuncComma == lexeme) {
          state = 2;
          continue;

        } else if (Lexeme::kPuncCloseParen == lexeme) {
          local.reset(AddDecl<NodeType>(
              module, kDeclKind, name, params.size()));
          if (!local) {
            return;

          } else {
            local->rparen = tok;
            local->name = name;
            local->parameters.swap(params);
            local->directive_pos = sub_tokens.front().Position();
            state = 8;
            continue;
          }

        } else {
          Error err(context->display_manager, sub_tok_range,
                    tok.SpellingRange());
          err << "Expected either a comma or a closing parenthesis here, "
              << "but got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 5:
        if (Lexeme::kPuncOpenParen == lexeme) {
          state = 6;
          continue;

        } else {
          Error err(context->display_manager, sub_tok_range,
                    tok.SpellingRange());
          err << "Expected an opening parenthesis here, but got '"
              << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 6:
        if (Lexeme::kIdentifierAtom == lexeme) {
          parse::IdInterpreter interpreter = {};
          interpreter.info.atom_name_id = tok.IdentifierId();
          interpreter.info.arity = 3 - 1;  // Old val, proposed val, new val.
          const auto id = interpreter.flat;
          if (!context->declarations.count(id)) {
            Error err(context->display_manager, sub_tok_range,
                      tok.SpellingRange());
            err << "Expected a functor name here, but got '"
                << tok << "' instead; maybe it wasn't declared yet?";
            context->error_log.Append(std::move(err));
            return;
          }

          auto decl = context->declarations[id];
          if (decl->context->kind != DeclarationKind::kFunctor) {
            Error err(context->display_manager, sub_tok_range,
                      tok.SpellingRange());
            err << "Expected a functor name here, but got a "
                << decl->KindName() << " name instead";
            context->error_log.Append(std::move(err));
            return;
          }

          param->opt_merge = reinterpret_cast<Node<ParsedFunctor> *>(decl);
          assert(param->opt_merge->parameters.size() == 3);

          param->opt_type = param->opt_merge->parameters[0]->opt_type;
          // NOTE(pag): We don't mark `param->parsed_opt_type` as `true` because
          //            it's coming from the functor, and thus would result in
          //            an unusual spelling range.

          // Make sure all parameters of the functor being used as a merge
          // operator have matching types.
          for (auto p = 1u; p <= 2u; ++p) {
            if (param->opt_merge->parameters[p]->opt_type.Kind() !=
                param->opt_type.Kind()) {

              Error err(context->display_manager,
                        ParsedFunctor(param->opt_merge).SpellingRange(),
                        param->opt_merge->parameters[p]->opt_type.SpellingRange());
              err << "Mismatch between parameter type '"
                  << param->opt_merge->parameters[p-1]->opt_type.SpellingRange()
                  << "' for parameter '"
                  << param->opt_merge->parameters[p-1]->name
                  << "and parameter type '"
                  << param->opt_merge->parameters[p]->opt_type.SpellingRange()
                  << "' for parameter '"
                  << param->opt_merge->parameters[p]->name
                  << "' of merge functor '" << decl->name << "'";

              auto note = err.Note(context->display_manager, SubTokenRange(),
                                   tok.SpellingRange());
              note << "Functor '" << tok << "' specified as merge operator here";

              context->error_log.Append(std::move(err));
              return;
            }
          }

          // Make sure the first two parameters of the merge functor are bound,
          // and the last is free.
          if (param->opt_merge->parameters[0]->opt_binding.Lexeme() !=
              Lexeme::kKeywordBound) {
            Error err(context->display_manager,
                      ParsedFunctor(param->opt_merge).SpellingRange(),
                      param->opt_merge->parameters[0]->opt_binding.SpellingRange());
            err << "First parameter of merge functor '" << decl->name
                << "' must be bound";

            auto note = err.Note(context->display_manager, sub_tok_range,
                                 tok.SpellingRange());
            note << "Functor '" << tok << "' specified as merge operator here";

            context->error_log.Append(std::move(err));
            return;
          }

          if (param->opt_merge->parameters[1]->opt_binding.Lexeme() !=
              Lexeme::kKeywordBound) {
            Error err(context->display_manager,
                      ParsedFunctor(param->opt_merge).SpellingRange(),
                      param->opt_merge->parameters[0]->opt_binding.SpellingRange());
            err << "Second parameter of merge functor '" << decl->name
                << "' must be bound";

            auto note = err.Note(context->display_manager, sub_tok_range,
                                 tok.SpellingRange());
            note << "Functor '" << tok << "' specified as merge operator here";

            context->error_log.Append(std::move(err));
            return;
          }

          if (param->opt_merge->parameters[2]->opt_binding.Lexeme() !=
              Lexeme::kKeywordFree) {
            Error err(context->display_manager,
                      ParsedFunctor(param->opt_merge).SpellingRange(),
                      param->opt_merge->parameters[0]->opt_binding.SpellingRange());
            err << "Third parameter of merge functor '" << decl->name
                << "' must be free";

            auto note = err.Note(context->display_manager, sub_tok_range,
                                 tok.SpellingRange());
            note << "Functor '" << tok << "' specified as merge operator here";

            context->error_log.Append(std::move(err));
            return;
          }

          // Make sure that the functor isn't impure.
          if (!param->opt_merge->is_pure) {
            Error err(context->display_manager, sub_tok_range,
                      tok.SpellingRange());
            err << "Value merging functor " << tok << "/3 cannot be used in a "
                << "mutable parameter because it's marked as impure";
            context->error_log.Append(std::move(err));
            return;
          }

          state = 7;
          continue;

        } else {
          Error err(context->display_manager, sub_tok_range,
                    tok.SpellingRange());
          err << "Expected a functor name here, but got '"
              << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 7:
        if (Lexeme::kPuncCloseParen == lexeme) {
          state = 3;  // Go parse the variable name; we can infer the type
                      // name from the functor.
          continue;

        } else {
          Error err(context->display_manager, sub_tok_range,
                    tok.SpellingRange());
          err << "Expected a closing parenthesis here, but got '"
              << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }
      case 8:
        if (Lexeme::kKeywordInline == lexeme) {
          if (local->inline_attribute.IsValid()) {
            Error err(context->display_manager, sub_tok_range,
                      tok.SpellingRange());
            err << "Unexpected second 'inline' attribute on "
                << introducer_tok << " '" << local->name << "'";
            context->error_log.Append(std::move(err));
            state = 9;  // Ignore further errors, but add the local in.
            continue;

          } else {
            local->inline_attribute = tok;
            state = 8;
            continue;
          }
        } else {
          DisplayRange err_range(
              tok.Position(), sub_tokens.back().NextPosition());
          Error err(context->display_manager, sub_tok_range, err_range);
          err << "Unexpected tokens following declaration of the '"
              << local->name << "' local";
          context->error_log.Append(std::move(err));
          state = 9;  // Ignore further errors, but add the local in.
          continue;
        }

      case 9:
        continue;
    }
  }

  if (state < 8) {
    Error err(context->display_manager, sub_tok_range, next_pos);
    err << "Incomplete " << introducer_tok
        << " declaration; the declaration must be "
        << "placed entirely on one line";
    context->error_log.Append(std::move(err));
    RemoveDecl<NodeType>(std::move(local));

  // Add the local to the module.
  } else {
    AddDeclAndCheckConsistency<NodeType>(out_vec, std::move(local));
  }
}

// Try to match a clause with a declaration.
bool ParserImpl::TryMatchClauseWithDecl(
    Node<ParsedModule> *module, Node<ParsedClause> *clause) {

  DisplayRange clause_head_range(
      clause->name.Position(), clause->rparen.NextPosition());

  if (clause->head_variables.size() > kMaxArity) {
    Error err(context->display_manager, SubTokenRange(),
              clause_head_range);
    err << "Too many parameters in clause '" << clause->name
        << "; maximum number of parameters is " << kMaxArity;
    context->error_log.Append(std::move(err));
    return false;
  }

  parse::IdInterpreter interpreter = {};
  interpreter.info.atom_name_id = clause->name.IdentifierId();
  interpreter.info.arity = clause->head_variables.size() - 1;
  const auto id = interpreter.flat;

  // There are no forward declarations associated with this ID.
  // We'll report an error, then invent one.
  if (!context->declarations.count(id)) {
    Error err(context->display_manager, SubTokenRange(),
              clause_head_range);
    err << "Missing declaration for '" << clause->name << "/"
        << clause->head_variables.size() << "'";
    context->error_log.Append(std::move(err));

    // Recover by adding a local declaration; this will let us keep
    // parsing.
    auto local = new Node<ParsedLocal>(module, DeclarationKind::kLocal);
    local->directive_pos = clause->name.Position();
    local->name = clause->name;
    local->rparen = clause->rparen;
    Node<ParsedParameter> *prev_param = nullptr;
    for (const auto &param_var : clause->head_variables) {
      auto param = new Node<ParsedParameter>;
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
    Node<ParsedModule> *module, Node<ParsedPredicate> *pred) {

  parse::IdInterpreter interpreter = {};
  interpreter.info.atom_name_id = pred->name.IdentifierId();
  interpreter.info.arity = pred->argument_uses.size() - 1;
  const auto id = interpreter.flat;

  DisplayRange pred_head_range(
      pred->name.Position(), pred->rparen.NextPosition());

  if (pred->argument_uses.size() > kMaxArity) {
    Error err(context->display_manager, SubTokenRange(),
              pred_head_range);
    err << "Too many arguments to predicate '" << pred->name
        << "; maximum number of arguments is " << kMaxArity;
    context->error_log.Append(std::move(err));
    return false;
  }

  // There are no forward declarations associated with this ID.
  // We'll report an error and invent one.
  if (!context->declarations.count(id)) {
    Error err(context->display_manager, SubTokenRange(),
              pred_head_range);
    err << "Missing declaration for '" << pred->name << "/"
        << pred->argument_uses.size() << "'";
    context->error_log.Append(std::move(err));

    // Recover by adding a local declaration; this will let us keep
    // parsing.
    auto local = new Node<ParsedLocal>(module, DeclarationKind::kLocal);
    local->directive_pos = pred->name.Position();
    local->name = pred->name;
    local->rparen = pred->rparen;

    Node<ParsedParameter> *prev_param = nullptr;
    for (const auto &arg_use : pred->argument_uses) {
      auto param = new Node<ParsedParameter>;
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

// Try to parse all of the tokens.
void ParserImpl::ParseAllTokens(Node<ParsedModule> *module) {
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
        ParseLocalExport<ParsedExport, DeclarationKind::kExport,
                         Lexeme::kHashExportDecl>(module, module->exports);
        if (first_non_import.IsInvalid()) {
          first_non_import = SubTokenRange();
        }
        break;

      case Lexeme::kHashLocalDecl:
        ReadLine();
        ParseLocalExport<ParsedLocal, DeclarationKind::kLocal,
                         Lexeme::kHashLocalDecl>(module, module->locals);
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

      // Specify that the generated C++ code should contain a pre-processor
      // include of some file.
      case Lexeme::kHashIncludeStmt:
        ReadLine();
        ParseInclude(module);
        continue;

      // Specify that the generated C++ code should contain a pre-processor
      // include of some file.
      //
      //    #inline <!
      //    ...
      //    !>
      case Lexeme::kHashInlineStmt:
        ReadLine();
        ParseInline(module);
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
        ReadLine();
        Error err(context->display_manager, SubTokenRange(),
                  tok.SpellingRange());
        err << "Unexpected top-level token; expected either a "
            << "clause definition or a declaration";
        context->error_log.Append(std::move(err));
        break;
      }
    }
  }
}

// Perform type checking/assignment. Returns `false` if there was an error.
bool ParserImpl::AssignTypes(Node<ParsedModule> *module) {

  auto var_var_eq_p =
      [=] (Node<ParsedVariable> *a, Node<ParsedVariable> *b) -> bool {
        if (a->type.Kind() == b->type.Kind()) {
          return true;
        }

        ParsedVariable a_var(a);
        ParsedVariable b_var(b);

        Error err(context->display_manager,
                  ParsedClause(a->context->clause).SpellingRange(),
                  a_var.SpellingRange());
        err << "Type mismatch between variable '" << a_var.Name()
            << "' (type '" << a_var.Type().SpellingRange() << "') and '"
            << b_var.Name() << "' (type '" << b_var.Type().SpellingRange()
            << "')";
        context->error_log.Append(std::move(err));

        auto note1 = err.Note(context->display_manager,
                             a_var.Type().SpellingRange());
        note1 << "Variable '" << a_var.Name() << "' with type '"
              << a_var.Type().SpellingRange() << "' is from here";

        auto note2 = err.Note(context->display_manager,
                        b_var.Type().SpellingRange());
        note2 << "Variable '" << b_var.Name() << "' with type '"
              << b_var.Type().SpellingRange() << "' is from here";

        context->error_log.Append(std::move(err));

        return false;
      };

  auto var_param_eq_p =
      [=] (Node<ParsedVariable> *a, Node<ParsedParameter> *b) -> bool {
        if (a->type.Kind() == b->opt_type.Kind()) {
          return true;
        }

        ParsedVariable a_var(a);
        ParsedParameter b_var(b);

        Error err(context->display_manager,
                  ParsedClause(a->context->clause).SpellingRange(),
                  a_var.SpellingRange());
        err << "Type mismatch between variable '" << a_var.Name()
            << "' (type '" << a_var.Type().SpellingRange()
            << "') and parameter '" << b_var.Name() << "' (type '"
            << b_var.Type().SpellingRange() << "')";
        context->error_log.Append(std::move(err));

        auto note1 = err.Note(context->display_manager,
                              a_var.Type().SpellingRange());
        note1 << "Variable '" << a_var.Name() << "' with type '"
              << a_var.Type().SpellingRange() << "' is from here";

        auto note2 = err.Note(context->display_manager,
                              b_var.Type().SpellingRange());
        note2 << "Parameter '" << b_var.Name() << "' with type '"
              << b_var.Type().SpellingRange() << "' is from here";

        context->error_log.Append(std::move(err));

        return false;
      };

  std::vector<Node<ParsedVariable> *> missing;
  auto changed = true;

  auto check_apply_var_types =
      [&] (Node<ParsedVariable> *var) -> bool {
        for (auto next_var = var->context->first_use; next_var != nullptr;
             next_var = next_var->next_use) {
          assert(next_var->name.IdentifierId() == var->name.IdentifierId());
          if (next_var->type.IsInvalid()) {
            next_var->type = var->type;
            changed = true;

          } else if (!var_var_eq_p(var, next_var)) {
            return false;
          }
        }
        return true;
      };

  auto pred_valid =
      [&] (Node<ParsedPredicate> *pred) -> bool {
        auto j = 0u;
        auto pred_decl = pred->declaration;
        for (auto &arg : pred->argument_uses) {
          auto &param = pred_decl->parameters[j++];
          auto &lhs_type = arg->used_var->type;
          auto &rhs_type = param->opt_type;
          auto lhs_is_valid = lhs_type.IsValid();
          auto rhs_is_valid = rhs_type.IsValid();
          if (lhs_is_valid && rhs_is_valid) {
            if (!var_param_eq_p(arg->used_var, param.get())) {
              return false;
            }
          } else if (lhs_is_valid) {
            rhs_type = lhs_type;
            changed = true;

          } else if (rhs_is_valid) {
            lhs_type = rhs_type;
            changed = true;
            check_apply_var_types(arg->used_var);

          } else {
            missing.push_back(arg->used_var);
          }
        }
        return true;
      };

  for (; changed; ) {
    changed = false;
    missing.clear();

    for (auto &clause : module->clauses) {
      auto i = 0u;

      for (const auto &var : clause->head_variables) {
        const auto &decl_param = clause->declaration->parameters[i++];

        // Head variable-based top-down. The head variable has a type, so
        // propagate that type through all uses and check that they all
        // match.
        if (var->type.IsValid()) {
        var_has_type:

          if (decl_param->opt_type.IsInvalid()) {
            decl_param->opt_type = var->type;
            changed = true;

          } else if (!var_param_eq_p(var.get(), decl_param.get())) {
            return false;
          }

          if (!check_apply_var_types(var.get())) {
            return false;
          }

        // Declaration-based top-down: the declaration is typed, so propagate
        // the type from the declaration down to the argument.
        } else if (decl_param->opt_type.IsValid()) {
          var->type = decl_param->opt_type;
          changed = true;
          goto var_has_type;

        // Bottom-up propagation step: find the first typed use of the variable,
        // then assign that type to the parameter variable.
        } else {
          for (auto next_var = var->next_use; next_var != nullptr;
               next_var = next_var->next_use) {
            if (next_var->type.IsValid()) {
              var->type = next_var->type;
              changed = true;
              goto var_has_type;
            }
          }

          // If we reached down here then the parameter variable's type was
          // not inferred from any use.
          missing.push_back(var.get());
        }
      }

      // Go through all assignments and propagate the variable's type to the
      // literals.
      for (const auto &assign : clause->assignments) {
        auto lhs_type = assign->lhs.used_var->type;
        if (lhs_type.IsValid()) {
          assign->rhs.type = lhs_type;
        } else {
          missing.push_back(assign->lhs.used_var);
        }
      }

      // Go through all comparisons and try to match up the types of the
      // compared variables.
      for (const auto &cmp : clause->comparisons) {
        auto &lhs_type = cmp->lhs.used_var->type;
        auto &rhs_type = cmp->rhs.used_var->type;
        auto lhs_is_valid = lhs_type.IsValid();
        auto rhs_is_valid = rhs_type.IsValid();
        if (lhs_is_valid && rhs_is_valid) {
          if (!var_var_eq_p(cmp->lhs.used_var, cmp->rhs.used_var)) {
            return false;
          }
        } else if (lhs_is_valid) {
          rhs_type = lhs_type;
          changed = true;
          check_apply_var_types(cmp->rhs.used_var);

        } else if (rhs_is_valid) {
          lhs_type = rhs_type;
          changed = true;
          check_apply_var_types(cmp->lhs.used_var);

        } else {
          missing.push_back(cmp->lhs.used_var);
          missing.push_back(cmp->rhs.used_var);
        }
      }

      // Go through all positive predicates, and do declaration-based
      // bottom-up type propagation.
      for (const auto &pred : clause->positive_predicates) {
        if (!pred_valid(pred.get())) {
          return false;
        }
      }

      for (const auto &pred : clause->negated_predicates) {
        if (!pred_valid(pred.get())) {
          return false;
        }
      }

      // Go through all aggregates.
      for (const auto &agg : clause->aggregates) {
        if (!pred_valid(agg->functor.get()) ||
            !pred_valid(agg->predicate.get())) {
          return false;
        }
      }
    }
  }

  // Failure to type a variable says that the variable itself is not range-
  // restricted. Successfully typing a program, however, does not imply that
  // all variables are range-restricted. This is because user-annotated types
  // can force a variable to have a type, and thus mask the fact that the
  // variable is not used anywhere. Range restriction issues are generally
  // discoverable at a later stage, by the SIPSVisitor.
  if (!missing.empty()) {
    for (auto var : missing) {
      Error err(context->display_manager,
                ParsedClause(var->context->clause).SpellingRange(),
                ParsedVariable(var).SpellingRange());
      err << "Could not infer type of non-range-restricted variable '"
          << var->name << "'";
      context->error_log.Append(std::move(err));
    }
    return false;
  }

  // Type the redecls. This applies to locals/exports only, as type annotations
  // are required on all parameters of other kinds of declarations.
  auto type_redecls = [&] (const auto &decl_list) {
    for (const auto &first : decl_list) {
      const auto &redecls = first->context->redeclarations;
      for (auto i = 1u; i < redecls.size(); ++i) {
        Node<ParsedDeclaration> *next = redecls[i];
        for (auto j = 0u; j < first->parameters.size(); ++j) {
          auto first_param = first->parameters[j].get();
          auto next_param = next->parameters[j].get();
          if (!next_param->parsed_opt_type) {
            next_param->opt_type = first_param->opt_type;
          }
        }
      }
    }
  };

  type_redecls(module->locals);
  type_redecls(module->exports);

  return true;
}

// Parse a display, returning the parsed module.
//
// NOTE(pag): Due to display caching, this may return a prior parsed module,
//            so as to avoid re-parsing a module.
ParsedModule ParserImpl::ParseDisplay(
    Display display, const DisplayConfiguration &config) {
  auto &weak_module = context->parsed_modules[display.Id()];
  auto module = weak_module.lock();
  if (module) {
    return ParsedModule(module);
  }

  module = std::make_shared<Node<ParsedModule>>(config);
  weak_module = module;

  if (!context->root_module) {
    context->root_module = module.get();
    module->root_module = module.get();
  } else {
    context->root_module->non_root_modules.emplace_back(module);
    module->root_module = context->root_module;
  }

  LexAllTokens(display);
  module->first = tokens.front();
  module->last = tokens.back();
  ParseAllTokens(module.get());

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

  AssignTypes(module.get());

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
  auto prev_path0 = impl->context->import_search_paths[0];
  impl->context->import_search_paths[0] = path.DirName();

  auto module = impl->ParseDisplay(display, config);
  impl->context->import_search_paths[0] = prev_path0;  // Restore back to the CWD.

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
void Parser::AddModuleSearchPath(std::string_view path) const {
  impl->context->import_search_paths.emplace_back(
      impl->context->file_manager, path);
}

// Add a directory as a search path for includes.
void Parser::AddIncludeSearchPath(
    std::string_view path, IncludeSearchPathKind kind) const {
  impl->context->include_search_paths[static_cast<unsigned>(kind)].emplace_back(
      impl->context->file_manager, path);

}

}  // namespace hyde
