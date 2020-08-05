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
      Error err(context->display_manager,
                DisplayRange(line_start_pos, tok.NextPosition()),
                tok.SpellingRange(), tok.ErrorPosition());

      switch (lexeme) {
        case Lexeme::kInvalid: {
          std::stringstream error_ss;
          if (reader.TryGetErrorMessage(&error_ss)) {
            err << error_ss.str();
          } else {
            err << "Unrecognized token in stream";
          }
          ignore_line = true;
          break;
        }

        case Lexeme::kInvalidStreamOrDisplay: {
          std::stringstream error_ss;
          if (reader.TryGetErrorMessage(&error_ss)) {
            err << error_ss.str();
          } else {
            err << "Error reading data from stream";
          }
          tok = Token::FakeEndOfFile(tok.Position());
          break;
        }

        case Lexeme::kInvalidDirective:
          err << "Unrecognized declaration '" << tok << "'";
          ignore_line = true;
          break;

        case Lexeme::kInvalidNumber:
          err << "Invalid number literal '" << tok << "'";
          tok = Token::FakeNumberLiteral(tok.Position(), tok.SpellingWidth());
          break;

        case Lexeme::kInvalidNewLineInString:
          err << "Invalid new line character in string literal";
          tok = Token::FakeStringLiteral(tok.Position(), tok.SpellingWidth());
          break;

        case Lexeme::kInvalidEscapeInString:
          err << "Invalid escape character '" << tok.InvalidChar()
              << "' in string literal";
          tok = Token::FakeStringLiteral(tok.Position(), tok.SpellingWidth());
          break;

        case Lexeme::kInvalidTypeName:
          err << "Invalid type name '" << tok << "'";
          tok = Token::FakeType(tok.Position(), tok.SpellingWidth());
          break;

        case Lexeme::kInvalidUnterminatedString:
          err << "Unterminated string literal";
          ignore_line = true;

          // NOTE(pag): No recovery, i.e. exclude the token.
          break;

        case Lexeme::kInvalidUnterminatedCode:
          err << "Unterminated code literal";
          ignore_line = true;

          // NOTE(pag): No recovery, i.e. exclude the token.
          break;

        case Lexeme::kInvalidUnknown:
          if (ignore_line) {
            err << "Unexpected character sequence '" << tok.InvalidChar()
                << "' in stream";
            break;

          // There's no recovery, but we'll add it into the token list to
          // report a more useful error at a higher level (i.e. in the
          // context of parsing something).
          } else {
            tokens.push_back(tok);
            continue;
          }

        default: break;
      }

      context->error_log.Append(std::move(err));

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
      prev_pos.TryComputeDistanceTo(tok.NextPosition(), nullptr, &num_lines,
                                    nullptr);
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
      case Lexeme::kComment: continue;

      // We pass these through so that we can report more meaningful
      // errors in locations relevant to specific parses.
      case Lexeme::kInvalidUnknown:
      default: tok_out = next_tok; return true;
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
  scope_range = DisplayRange();

  while (ReadNextToken(tok)) {
    switch (tok.Lexeme()) {
      case Lexeme::kEndOfFile:
        scope_range = SubTokenRange();
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
            scope_range = SubTokenRange();
            return;
          }
        } else {
          continue;
        }
      case Lexeme::kComment: continue;
      default: sub_tokens.push_back(tok); continue;
    }
  }

  scope_range = SubTokenRange();

  if (unmatched_paren.IsValid()) {
    context->error_log.Append(scope_range, unmatched_paren)
        << "Unmatched parenthesis";
  }
}

// Read until the next period. This fill sup `sub_tokens` with all
// read tokens (excluding any whitespace found along the way).
// Returns `false` if a period is not found.
bool ParserImpl::ReadStatement(void) {
  Token tok;

  scope_range = DisplayRange();

  while (ReadNextToken(tok)) {
    switch (tok.Lexeme()) {
      case Lexeme::kEndOfFile:
        scope_range = SubTokenRange();
        UnreadToken();
        return false;

      case Lexeme::kPuncPeriod:
        sub_tokens.push_back(tok);
        scope_range = SubTokenRange();
        return true;

      case Lexeme::kWhitespace:
      case Lexeme::kComment: continue;

      default: sub_tokens.push_back(tok); continue;
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

  for (next_pos = tok.NextPosition(); ReadNextSubToken(tok);
       next_pos = tok.NextPosition()) {

    const auto lexeme = tok.Lexeme();
    const auto tok_range = tok.SpellingRange();

    switch (state) {
      case 0:
        if (Lexeme::kIdentifierAtom == lexeme) {
          name = tok;
          state = 1;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected atom here (lower case identifier) for the name of "
              << "the " << introducer_tok << " being declared, got '" << tok
              << "' instead";
          return;
        }
      case 1:
        if (Lexeme::kPuncOpenParen == lexeme) {
          state = 2;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected opening parenthesis here to begin parameter list of "
              << introducer_tok << " '" << name << "', but got '" << tok
              << "' instead";
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
          context->error_log.Append(scope_range, tok_range)
              << "Expected type name (lower case identifier, e.g. u32), "
              << "'mutable' keyword, or variable name (capitalized identifier) "
              << "for parameter in " << introducer_tok << " '" << name
              << "', but got '" << tok << "' instead";
          return;
        }

      case 3:
        if (Lexeme::kIdentifierVariable == lexeme) {
          param->name = tok;
          state = 4;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected named variable here (capitalized identifier) as a "
              << "parameter name of " << introducer_tok << " '" << name
              << "', but got '" << tok << "' instead";
          return;
        }

      case 4:

        // Add the parameter in.
        if (!params.empty()) {
          params.back()->next = param.get();

          if (params.size() == kMaxArity) {
            const auto err_range = ParsedParameter(param.get()).SpellingRange();
            context->error_log.Append(scope_range, err_range)
                << "Too many parameters to " << introducer_tok << " '" << name
                << "'; the maximum number of parameters is " << kMaxArity;
            return;
          }
        }

        param->index = static_cast<unsigned>(params.size());
        params.push_back(std::move(param));

        if (Lexeme::kPuncComma == lexeme) {
          state = 2;
          continue;

        } else if (Lexeme::kPuncCloseParen == lexeme) {
          local.reset(
              AddDecl<NodeType>(module, kDeclKind, name, params.size()));
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
          context->error_log.Append(scope_range, tok_range)
              << "Expected either a comma or a closing parenthesis here, "
              << "but got '" << tok << "' instead";
          return;
        }

      case 5:
        if (Lexeme::kPuncOpenParen == lexeme) {
          state = 6;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected an opening parenthesis here, but got '" << tok
              << "' instead";
          return;
        }

      case 6:
        if (Lexeme::kIdentifierAtom == lexeme) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
          parse::IdInterpreter interpreter = {};
          interpreter.info.atom_name_id = tok.IdentifierId();
          interpreter.info.arity = 3;  // Old val, proposed val, new val.
#pragma GCC diagnostic pop

          const auto id = interpreter.flat;
          if (!context->declarations.count(id)) {
            context->error_log.Append(scope_range, tok_range)
                << "Expected a functor name here, but got '" << tok
                << "' instead; maybe it wasn't declared yet?";
            return;
          }

          auto decl = context->declarations[id];
          if (decl->context->kind != DeclarationKind::kFunctor) {
            context->error_log.Append(scope_range, tok_range)
                << "Expected a functor name here, but got a "
                << decl->KindName() << " name instead";
            return;
          }

          param->opt_merge = reinterpret_cast<Node<ParsedFunctor> *>(decl);
          assert(param->opt_merge->parameters.size() == 3);

          param->opt_type =
              TypeLoc(param->opt_merge->parameters[0]->opt_type.Kind(),
                      param->opt_mutable_range);

          // NOTE(pag): We don't mark `param->parsed_opt_type` as `true` because
          //            it's coming from the functor, and thus would result in
          //            an unusual spelling range.
          //
          // TODO(pag): Does the above setting of `opt_type`, to get it to use
          //            the spelling range of the `mutable(...)` conflict with
          //            the below error reporting?

          // Make sure all parameters of the functor being used as a merge
          // operator have matching types.
          for (auto p = 1u; p <= 2u; ++p) {
            if (param->opt_merge->parameters[p]->opt_type.Kind() !=
                param->opt_type.Kind()) {

              auto err = context->error_log.Append(
                  ParsedFunctor(param->opt_merge).SpellingRange(),
                  param->opt_merge->parameters[p]->opt_type.SpellingRange());

              err << "Mismatch between parameter type '"
                  << param->opt_merge->parameters[p - 1]
                         ->opt_type.SpellingRange()
                  << "' for parameter '"
                  << param->opt_merge->parameters[p - 1]->name
                  << "and parameter type '"
                  << param->opt_merge->parameters[p]->opt_type.SpellingRange()
                  << "' for parameter '"
                  << param->opt_merge->parameters[p]->name
                  << "' of merge functor '" << decl->name << "'";

              auto note = err.Note(scope_range, tok_range);
              note << "Functor '" << tok
                   << "' specified as merge operator here";
              return;
            }
          }

          // Make sure the first two parameters of the merge functor are bound,
          // and the last is free.
          if (param->opt_merge->parameters[0]->opt_binding.Lexeme() !=
              Lexeme::kKeywordBound) {
            auto err = context->error_log.Append(
                ParsedFunctor(param->opt_merge).SpellingRange(),
                param->opt_merge->parameters[0]->opt_binding.SpellingRange());
            err << "First parameter of merge functor '" << decl->name
                << "' must be bound";

            auto note = err.Note(scope_range, tok_range);
            note << "Functor '" << tok << "' specified as merge operator here";
            return;
          }

          if (param->opt_merge->parameters[1]->opt_binding.Lexeme() !=
              Lexeme::kKeywordBound) {
            auto err = context->error_log.Append(
                ParsedFunctor(param->opt_merge).SpellingRange(),
                param->opt_merge->parameters[0]->opt_binding.SpellingRange());
            err << "Second parameter of merge functor '" << decl->name
                << "' must be bound";

            err.Note(scope_range, tok_range)
                << "Functor '" << tok << "' specified as merge operator here";
            return;
          }

          if (param->opt_merge->parameters[2]->opt_binding.Lexeme() !=
              Lexeme::kKeywordFree) {
            auto err = context->error_log.Append(
                ParsedFunctor(param->opt_merge).SpellingRange(),
                param->opt_merge->parameters[0]->opt_binding.SpellingRange());
            err << "Third parameter of merge functor '" << decl->name
                << "' must be free";

            err.Note(scope_range, tok_range)
                << "Functor '" << tok << "' specified as merge operator here";
            return;
          }

          // Make sure that the functor isn't impure.
          if (!param->opt_merge->is_pure) {
            context->error_log.Append(scope_range, tok_range)
                << "Value merging functor " << tok << "/3 cannot be used in a "
                << "mutable parameter because it's marked as impure";
            return;
          }

          state = 7;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected a functor name here, but got '" << tok
              << "' instead";
          return;
        }

      case 7:
        if (Lexeme::kPuncCloseParen == lexeme) {
          param->opt_mutable_range =
              DisplayRange(param->opt_binding.Position(), tok.NextPosition());
          state = 3;  // Go parse the variable name; we can infer the type

          // name from the functor.
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected a closing parenthesis here, but got '" << tok
              << "' instead";
          return;
        }
      case 8:
        if (Lexeme::kKeywordInline == lexeme) {
          if (local->inline_attribute.IsValid()) {
            context->error_log.Append(scope_range, tok_range)
                << "Unexpected second 'inline' attribute on " << introducer_tok
                << " '" << local->name << "'";
            state = 9;  // Ignore further errors, but add the local in.
            continue;

          } else {
            local->inline_attribute = tok;
            state = 8;
            continue;
          }
        } else {
          DisplayRange err_range(tok.Position(),
                                 sub_tokens.back().NextPosition());
          context->error_log.Append(scope_range, err_range)
              << "Unexpected tokens following declaration of the '"
              << local->name << "' local";
          state = 9;  // Ignore further errors, but add the local in.
          continue;
        }

      case 9: continue;
    }
  }

  if (state < 8) {
    context->error_log.Append(scope_range, next_pos)
        << "Incomplete " << introducer_tok
        << " declaration; the declaration must be "
        << "placed entirely on one line";
    RemoveDecl<NodeType>(std::move(local));

  // Add the local to the module.
  } else {
    FinalizeDeclAndCheckConsistency<NodeType>(out_vec, std::move(local));
  }
}

// Try to match a clause with a declaration.
bool ParserImpl::TryMatchClauseWithDecl(Node<ParsedModule> *module,
                                        Node<ParsedClause> *clause) {

  DisplayRange clause_head_range(clause->name.Position(),
                                 clause->rparen.NextPosition());

  if (clause->head_variables.size() > kMaxArity) {
    context->error_log.Append(scope_range, clause_head_range)
        << "Too many parameters in clause '" << clause->name
        << "; maximum number of parameters is " << kMaxArity;
    return false;
  }

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
  parse::IdInterpreter interpreter = {};
  interpreter.info.atom_name_id = clause->name.IdentifierId();
  interpreter.info.arity = clause->head_variables.size();
#pragma GCC diagnostic pop

  const auto id = interpreter.flat;

  DisplayRange directive_range;

  // If it's a zero-arity clause head then it's treated by default as an
  // `#export`.
  if (clause->head_variables.empty()) {
    if (!context->declarations.count(id)) {
      auto export_decl =
          new Node<ParsedExport>(module, DeclarationKind::kExport);
      export_decl->name = clause->name;
      module->exports.emplace_back(export_decl);
      context->declarations.emplace(id, export_decl);
    }

    directive_range = clause->name.SpellingRange();

  // There are no forward declarations associated with this ID.
  // We'll report an error, then invent one.
  } else if (!context->declarations.count(id)) {
    context->error_log.Append(scope_range, clause_head_range)
        << "Missing declaration for '" << clause->name << "/"
        << clause->head_variables.size() << "'";

    // Recover by adding a local_decl declaration; this will let us keep
    // parsing.
    auto local_decl = new Node<ParsedLocal>(module, DeclarationKind::kLocal);
    local_decl->directive_pos = clause->name.Position();
    local_decl->name = clause->name;
    local_decl->rparen = clause->rparen;
    Node<ParsedParameter> *prev_param = nullptr;
    for (const auto &param_var : clause->head_variables) {
      auto param = new Node<ParsedParameter>;
      param->name = param_var->name;
      if (prev_param) {
        prev_param->next = param;
      }
      local_decl->parameters.emplace_back(param);
      prev_param = param;
    }

    module->locals.emplace_back(local_decl);
    context->declarations.emplace(id, local_decl);
  }

  clause->declaration = context->declarations[id];

  directive_range = DisplayRange(clause->declaration->directive_pos,
                                 clause->declaration->rparen.NextPosition());

  const auto &decl_context = clause->declaration->context;

  // Don't allow us to define clauses for functors.
  if (decl_context->kind == DeclarationKind ::kFunctor) {
    auto err = context->error_log.Append(scope_range, clause_head_range);
    err << "Cannot define a clause for the functor '" << clause->name
        << "'; functors are defined by native "
        << "code modules";

    err.Note(directive_range)
        << "Functor '" << clause->name << "' is first declared here";
    return false;

  // Don't allow us to define clauses for predicates exported by
  // other modules. What this says is that we need to have a redeclaration
  // within our current module that matches this declaration. That is, we can't
  // just declare a clause without first declaring
  } else if (decl_context->kind == DeclarationKind ::kExport &&
             module != clause->declaration->module) {
    auto err = context->error_log.Append(scope_range, clause_head_range);
    err << "Cannot define a clause '" << clause->name
        << "' for predicate exported by another module";

    err.Note(directive_range)
        << "Predicate '" << clause->name << "' is declared here";

    return false;
  }

  return true;
}

// Try to match a clause with a declaration.
bool ParserImpl::TryMatchPredicateWithDecl(Node<ParsedModule> *module,
                                           Node<ParsedPredicate> *pred) {

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
  parse::IdInterpreter interpreter = {};
  interpreter.info.atom_name_id = pred->name.IdentifierId();
  interpreter.info.arity = pred->argument_uses.size();
#pragma GCC diagnostic pop

  const auto id = interpreter.flat;

  DisplayRange pred_head_range(pred->name.Position(),
                               pred->rparen.NextPosition());

  if (pred->argument_uses.size() > kMaxArity) {
    context->error_log.Append(scope_range, pred_head_range)
        << "Too many arguments to predicate '" << pred->name
        << "; maximum number of arguments is " << kMaxArity;
    return false;
  }

  // A zero-argument predicate is like a boolean variable / option, and is
  // declared/invented on the spot. Later we'll make sure that there are clauses
  // that prove it.
  if (pred->argument_uses.empty()) {
    if (!context->declarations.count(id)) {
      auto export_decl =
          new Node<ParsedExport>(module, DeclarationKind::kExport);
      export_decl->name = pred->name;
      module->exports.emplace_back(export_decl);
      context->declarations.emplace(id, export_decl);
    }

  // There are no forward declarations associated with this ID.
  // We'll report an error and invent one.
  } else if (!context->declarations.count(id)) {
    context->error_log.Append(scope_range, pred_head_range)
        << "Missing declaration for '" << pred->name << "/"
        << pred->argument_uses.size() << "'";

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

  // Don't let us receive this message if we have any sends of this message.
  if (pred->declaration->context->kind == DeclarationKind::kMessage &&
      !pred->declaration->context->clauses.empty()) {

    auto err = context->error_log.Append(scope_range, pred_head_range);
    err << "Cannot receive input from message " << pred->name << '/'
        << pred->argument_uses.size()
        << "; the message is already used for sending data";

    for (auto &clause_ : pred->declaration->context->clauses) {
      auto clause = ParsedClause(clause_.get());
      err.Note(clause.SpellingRange(), ParsedClauseHead(clause).SpellingRange())
          << "Message send is here";
    }
  }

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
          auto err = context->error_log.Append(SubTokenRange());
          err << "Cannot have import following a non-import "
              << "declaration/declaration";

          err.Note(first_non_import)
              << "Import must precede this declaration/declaration";

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
          context->error_log.Append(SubTokenRange(),
                                    sub_tokens.back().NextPosition())
              << "Expected period at end of declaration/clause";
        } else {
          (void) ParseClause(module);
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
          context->error_log.Append(scope_range,
                                    sub_tokens.back().NextPosition())
              << "Expected period here at end of declaration/clause";

        } else if (2 > sub_tokens.size()) {
          context->error_log.Append(scope_range, tok.NextPosition())
              << "Expected atom here (lower case identifier) after the '!' "
              << "for the name of the negated clause head being declared";

        } else {
          ++next_sub_tok_index;
          ParseClause(module, tok);
        }

        if (first_non_import.IsInvalid()) {
          first_non_import = SubTokenRange();
        }
        break;

      case Lexeme::kComment:
      case Lexeme::kWhitespace: continue;

      case Lexeme::kEndOfFile: return;

      // Don't warn about this, we've already warned about it.
      case Lexeme::kInvalidUnknown: continue;

      // Error, an unexpected top-level token.
      default: {
        ReadLine();
        context->error_log.Append(scope_range, tok.SpellingRange())
            << "Unexpected top-level token; expected either a "
            << "clause definition or a declaration";
        break;
      }
    }
  }
}

// Perform type checking/assignment. Returns `false` if there was an error.
bool ParserImpl::AssignTypes(Node<ParsedModule> *module) {

  auto var_var_eq_p = [=](Node<ParsedVariable> *a,
                          Node<ParsedVariable> *b) -> bool {
    if (a->type.Kind() == b->type.Kind()) {
      return true;
    }

    ParsedVariable a_var(a);
    ParsedVariable b_var(b);

    auto err = context->error_log.Append(
        ParsedClause(a->context->clause).SpellingRange(),
        a_var.SpellingRange());
    err << "Type mismatch between variable '" << a_var.Name() << "' (type '"
        << a_var.Type().SpellingRange() << "') and '" << b_var.Name()
        << "' (type '" << b_var.Type().SpellingRange() << "')";

    err.Note(a_var.Type().SpellingRange())
        << "Variable '" << a_var.Name() << "' with type '"
        << a_var.Type().SpellingRange() << "' is from here";

    err.Note(b_var.Type().SpellingRange())
        << "Variable '" << b_var.Name() << "' with type '"
        << b_var.Type().SpellingRange() << "' is from here";

    return false;
  };

  auto var_param_eq_p = [=](Node<ParsedVariable> *a,
                            Node<ParsedParameter> *b) -> bool {
    if (a->type.Kind() == b->opt_type.Kind()) {
      return true;
    }

    ParsedVariable a_var(a);
    ParsedParameter b_var(b);

    auto err = context->error_log.Append(
        ParsedClause(a->context->clause).SpellingRange(),
        a_var.SpellingRange());
    err << "Type mismatch between variable '" << a_var.Name() << "' (type '"
        << a_var.Type().SpellingRange() << "') and parameter '" << b_var.Name()
        << "' (type '" << b_var.Type().SpellingRange() << "')";

    err.Note(a_var.Type().SpellingRange())
        << "Variable '" << a_var.Name() << "' with type '"
        << a_var.Type().SpellingRange() << "' is from here";

    err.Note(b_var.Type().SpellingRange())
        << "Parameter '" << b_var.Name() << "' with type '"
        << b_var.Type().SpellingRange() << "' is from here";

    return false;
  };

  std::vector<Node<ParsedVariable> *> missing;
  auto changed = true;

  auto check_apply_var_types = [&](Node<ParsedVariable> *var) -> bool {
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

  auto pred_valid = [&](Node<ParsedPredicate> *pred) -> bool {
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

  auto do_clause = [&](Node<ParsedClause> *clause) -> bool {
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

    return true;
  };

  for (; changed;) {
    changed = false;
    missing.clear();

    for (auto clause : module->clauses) {
      if (!do_clause(clause)) {
        return false;
      }
    }

    for (auto clause : module->deletion_clauses) {
      if (!do_clause(clause)) {
        return false;
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
      context->error_log.Append(
          ParsedClause(var->context->clause).SpellingRange(),
          ParsedVariable(var).SpellingRange())
          << "Could not infer type of non-range-restricted variable '"
          << var->name << "'";
    }
    return false;
  }

  // Type the redecls. This applies to locals/exports only, as type annotations
  // are required on all parameters of other kinds of declarations.
  auto type_redecls = [&](const auto &decl_list) {
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

// Checks that all locals and exports are defined.
static bool AllDeclarationsAreDefined(Node<ParsedModule> *root_module,
                                      const ErrorLog &log) {

  auto do_decl = [&](ParsedDeclaration decl) {
    for (ParsedClause clause : decl.Clauses()) {
      if (!clause.IsDeletion()) {
        return;
      }
    }

    auto err = log.Append(decl.SpellingRange());
    err << "Declaration is declared but never defined (by a clause)";

    for (ParsedPredicate pred : decl.PositiveUses()) {
      err.Note(ParsedClause::Containing(pred).SpellingRange(),
               pred.SpellingRange())
          << "Undefined declaration is used here";
    }

    for (ParsedPredicate pred : decl.NegativeUses()) {
      err.Note(ParsedClause::Containing(pred).SpellingRange(),
               pred.SpellingRange())
          << "Use of never-defined declaration is trivially true here";
    }
  };

  const auto prev_num_errors = log.Size();
  for (auto module : root_module->all_modules) {
    for (auto &decl : module->locals) {
      do_decl(ParsedDeclaration(decl.get()));
    }
    for (auto &decl : module->exports) {
      do_decl(ParsedDeclaration(decl.get()));
    }
  }

  return prev_num_errors == log.Size();
}

// Parse a display, returning the parsed module.
//
// NOTE(pag): Due to display caching, this may return a prior parsed module,
//            so as to avoid re-parsing a module.
std::optional<ParsedModule>
ParserImpl::ParseDisplay(Display display, const DisplayConfiguration &config) {
  auto &weak_module = context->parsed_modules[display.Id()];
  auto module = weak_module.lock();
  if (module) {
    return ParsedModule(module);
  }

  const auto prev_num_errors = context->error_log.Size();
  module = std::make_shared<Node<ParsedModule>>(config);

  // Initialize now, even before we know that we have a valid parsed module,
  // just in case we have recursive/cyclic imports.
  weak_module = module;

  if (!context->root_module) {
    context->root_module = module.get();
    module->root_module = module.get();
  } else {
    context->root_module->non_root_modules.emplace_back(module);
    module->root_module = context->root_module;
  }

  module->root_module->all_modules.push_back(module.get());

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

  // Only do usage and type checking when we're done parsing the root module.
  if (module->root_module == module.get()) {
    if (!AllDeclarationsAreDefined(module.get(), context->error_log) ||
        !AssignTypes(module.get())) {
      return std::nullopt;
    }
  }

  if (prev_num_errors == context->error_log.Size()) {
    return ParsedModule(module);

  } else {
    return std::nullopt;
  }
}

Parser::~Parser(void) {}

Parser::Parser(const DisplayManager &display_manager, const ErrorLog &error_log)
    : impl(new ParserImpl(
          std::make_shared<SharedParserContext>(display_manager, error_log))) {}

// Parse a buffer.
//
// NOTE(pag): `data` must remain valid for the lifetime of the parser's
//            `display_manager`.
std::optional<ParsedModule>
Parser::ParseBuffer(std::string_view data,
                    const DisplayConfiguration &config) const {
  return impl->ParseDisplay(
      impl->context->display_manager.OpenBuffer(data, config), config);
}

// Parse a file, specified by its path.
std::optional<ParsedModule>
Parser::ParsePath(std::string_view path_,
                  const DisplayConfiguration &config) const {

  auto display = impl->context->display_manager.OpenPath(path_, config);
  Path path(impl->context->file_manager, path_);

  // Special case for path parsing, we need to change the parser's current
  // working directory to the directory containing the file being parsed
  // so that we can do file-relative imports.
  auto prev_path0 = impl->context->import_search_paths[0];
  impl->context->import_search_paths[0] = path.DirName();

  auto module = impl->ParseDisplay(display, config);
  impl->context->import_search_paths[0] =
      prev_path0;  // Restore back to the CWD.

  return module;
}

// Parse an input stream.
//
// NOTE(pag): `is` must remain a valid reference for the lifetime of the
//            parser's `display_manager`.
std::optional<ParsedModule>
Parser::ParseStream(std::istream &is,
                    const DisplayConfiguration &config) const {
  return impl->ParseDisplay(
      impl->context->display_manager.OpenStream(is, config), config);
}

// Add a directory as a search path for files.
void Parser::AddModuleSearchPath(std::string_view path) const {
  impl->context->import_search_paths.emplace_back(impl->context->file_manager,
                                                  path);
}

// Add a directory as a search path for includes.
void Parser::AddIncludeSearchPath(std::string_view path,
                                  IncludeSearchPathKind kind) const {
  impl->context->include_search_paths[static_cast<unsigned>(kind)].emplace_back(
      impl->context->file_manager, path);
}

}  // namespace hyde
