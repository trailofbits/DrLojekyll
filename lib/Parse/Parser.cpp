// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include "Parser.h"

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
    auto lexeme = tok.Lexeme();

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

        case Lexeme::kInvalidOctalNumber:
          err << "Invalid octal digit in number literal '" << tok << "'";
          tok = Token::FakeNumberLiteral(tok.Position(), tok.SpellingWidth());
          break;

        case Lexeme::kInvalidHexadecimalNumber:
          err << "Invalid hexadecimal digit in number literal '" << tok << "'";
          tok = Token::FakeNumberLiteral(tok.Position(), tok.SpellingWidth());
          break;

        case Lexeme::kInvalidBinaryNumber:
          err << "Invalid binary digit in number literal '" << tok << "'";
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
          err << "Unterminated code literal of unknown language";
          ignore_line = true;

          // NOTE(pag): No recovery, i.e. exclude the token.
          break;

        case Lexeme::kInvalidUnterminatedCxxCode:
          err << "Unterminated C++ code literal";
          ignore_line = true;

          // NOTE(pag): No recovery, i.e. exclude the token.
          break;

        case Lexeme::kInvalidUnterminatedPythonCode:
          err << "Unterminated Python code literal";
          ignore_line = true;

          // NOTE(pag): No recovery, i.e. exclude the token.
          break;

        case Lexeme::kInvalidPragma:
          if (ignore_line) {
            err << "Unexpected pragma '" << tok << "'";
            break;

          // Recovery is to drop the token.
          } else {
            continue;
          }

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

      int64_t num_lines = 0;
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
    auto &next_tok = tokens[next_tok_index];
    ++next_tok_index;

    switch (next_tok.Lexeme()) {
      case Lexeme::kInvalid:
      case Lexeme::kInvalidDirective:
      case Lexeme::kInvalidNumber:
      case Lexeme::kInvalidOctalNumber:
      case Lexeme::kInvalidHexadecimalNumber:
      case Lexeme::kInvalidBinaryNumber:
      case Lexeme::kInvalidNewLineInString:
      case Lexeme::kInvalidEscapeInString:
      case Lexeme::kInvalidUnterminatedString:
      case Lexeme::kInvalidUnterminatedCxxCode:
      case Lexeme::kInvalidUnterminatedPythonCode:
      case Lexeme::kInvalidPragma:
      case Lexeme::kComment: continue;

      // Adjust for foreign types.
      case Lexeme::kIdentifierAtom:
      case Lexeme::kIdentifierVariable: {
        const auto id = next_tok.IdentifierId();
        if (context->foreign_types.count(id)) {
          assert(context->foreign_types[id] != nullptr);
          next_tok = next_tok.AsForeignType();
          assert(next_tok.IdentifierId() == id);

        } else if (context->foreign_constants.count(id)) {
          assert(context->foreign_constants[id] != nullptr);
          next_tok = next_tok.AsForeignConstant(
              context->foreign_constants[id]->type.Kind());
          assert(next_tok.IdentifierId() == id);
        }
      }
        [[clang::fallthrough]];

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

// Read until the next period. This fills up `sub_tokens` with all
// read tokens (excluding any whitespace found along the way).
// Returns `false` if a period is not found.
bool ParserImpl::ReadStatement(void) {
  Token tok;

  std::vector<Token> opening_parens;
  scope_range = DisplayRange();

  while (ReadNextToken(tok)) {
    switch (tok.Lexeme()) {
      case Lexeme::kEndOfFile:
        scope_range = SubTokenRange();
        UnreadToken();
        return false;
      case Lexeme::kPuncPeriod:
        sub_tokens.push_back(tok);
        if (opening_parens.empty()) {
          scope_range = SubTokenRange();
          return true;
        }
        continue;
      case Lexeme::kPuncOpenParen:
        sub_tokens.push_back(tok);
        opening_parens.push_back(tok);
        continue;
      case Lexeme::kPuncCloseParen:
        sub_tokens.push_back(tok);
        if (opening_parens.empty() ||
            opening_parens.back().Lexeme() != Lexeme::kPuncOpenParen) {
          context->error_log.Append(scope_range, tok.SpellingRange())
              << "Unmatched closing parenthesis";
        } else {
          opening_parens.pop_back();
        }
        continue;
      case Lexeme::kPuncOpenBrace:
        sub_tokens.push_back(tok);
        opening_parens.push_back(tok);
        continue;
      case Lexeme::kPuncCloseBrace:
        sub_tokens.push_back(tok);
        if (opening_parens.empty() ||
            opening_parens.back().Lexeme() != Lexeme::kPuncOpenBrace) {
          context->error_log.Append(scope_range, tok.SpellingRange())
              << "Unmatched closing brace";
        } else {
          opening_parens.pop_back();
        }
        continue;
      case Lexeme::kWhitespace: continue;
      case Lexeme::kComment: continue;
      default: sub_tokens.push_back(tok); continue;
    }
  }

  scope_range = SubTokenRange();

  for (auto opening_paren : opening_parens) {
    context->error_log.Append(scope_range, opening_paren.SpellingRange())
        << "Unmatched opening parenthesis/brace";
  }
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
template <typename NodeTypeImpl, DeclarationKind kDeclKind,
          Lexeme kIntroducerLexeme>
void ParserImpl::ParseLocalExport(
    ParsedModuleImpl *module,
    UseList<NodeTypeImpl, ParsedDeclarationImpl> &out_vec) {
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
  NodeTypeImpl *local = nullptr;

  TypeLoc param_type;
  Token param_binding;
  Token param_merge_functor_name;
  DisplayRange prev_param_mutable_range;
  DisplayRange param_mutable_range;
  ParsedFunctorImpl *param_merge_functor = nullptr;
  Token param_name;
  std::vector<std::tuple<TypeLoc, Token, DisplayRange, ParsedFunctorImpl *, Token>> params;

  // Interpretation of this local/export as a clause.
  std::vector<Token> clause_toks;
  bool has_embedded_clauses = false;

  DisplayPosition next_pos;
  Token name;
  Token highlight;

  bool has_mutable_parameter = false;

  for (next_pos = tok.NextPosition(); ReadNextSubToken(tok);
       next_pos = tok.NextPosition()) {

    if (local) {
      local->last_tok = tok;
    }

    const auto lexeme = tok.Lexeme();
    const auto tok_range = tok.SpellingRange();

    switch (state) {
      case 0:
        if (Lexeme::kIdentifierAtom == lexeme) {
          name = tok;
          state = 1;
          clause_toks.push_back(tok);
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
          clause_toks.push_back(tok);
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected opening parenthesis here to begin parameter list of "
              << introducer_tok << " '" << name << "', but got '" << tok
              << "' instead";
          return;
        }

      case 2:

        if (tok.IsType()) {
          param_type = tok;
          state = 3;
          continue;

        } else if (Lexeme::kKeywordMutable == lexeme) {
          param_binding = tok;
          state = 5;
          continue;

        } else if (Lexeme::kIdentifierVariable == lexeme) {
          param_name = tok;
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
          param_name = tok;
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
        clause_toks.push_back(param_name);

        if (params.size() == kMaxArity) {
          context->error_log.Append(scope_range, param_name.SpellingRange())
              << "Too many parameters to " << introducer_tok << " '" << name
              << "'; the maximum number of parameters is " << kMaxArity;
          return;
        }

        params.emplace_back(param_type, param_binding, param_mutable_range,
                            param_merge_functor, param_name);
        param_type = TypeLoc();
        param_binding = Token();
        param_merge_functor_name = Token();
        param_mutable_range = DisplayRange();
        param_merge_functor = nullptr;
        param_name = Token();

        if (Lexeme::kPuncComma == lexeme) {
          clause_toks.push_back(tok);
          state = 2;
          continue;

        } else if (Lexeme::kPuncCloseParen == lexeme) {
          clause_toks.push_back(tok);

          local = AddDecl<NodeTypeImpl>(module, kDeclKind, name, params.size());
          if (!local) {
            return;
          }

          out_vec.AddUse(local);

          local->rparen = tok;
          local->name = name;
          context->display_manager.TryReadData(name.SpellingRange(),
                                               &(local->name_view));
          local->directive_pos = sub_tokens.front().Position();
          local->has_mutable_parameter = has_mutable_parameter;

          // Add in the parameters.
          for (auto [p_type, p_binding, p_binding_range,
                     p_merge_functor, p_name] : params) {
            ParsedParameterImpl * const param = local->parameters.Create(local);
            param->opt_type = p_type;
            param->name = p_name;
            context->display_manager.TryReadData(p_name.SpellingRange(),
                                                 &(param->name_view));
            param->parsed_opt_type = p_type.IsValid();
            param->opt_binding = p_binding;
            param->index = local->parameters.Size() - 1u;
            param->opt_merge = p_merge_functor;
            param->opt_mutable_range = p_binding_range;
          }

          state = 8;
          continue;

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
          param_merge_functor_name = tok;
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
          param_mutable_range =
              DisplayRange(param_binding.Position(), tok.NextPosition());


#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Wconversion"
#endif
          parse::IdInterpreter interpreter = {};
          interpreter.info.atom_name_id =
              param_merge_functor_name.IdentifierId();
          interpreter.info.arity = 3;  // Old val, proposed val, new val.

#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic pop
#endif

          // Try to find any declaration that should match the 3-argument
          // prototype of merge functors: input old val, input proposed val,
          // output merged val.
          const auto id = interpreter.flat;
          if (!context->declarations.count(id)) {
            context->error_log.Append(scope_range, param_mutable_range,
                                      param_merge_functor_name.Position())
                << "Expected a functor name here, but got '"
                << param_merge_functor_name
                << "' instead; maybe it wasn't declared yet?";
            return;
          }

          // Make sure the found declaration is actually a functor.
          ParsedDeclarationImpl *decl = context->declarations[id];
          if (decl->context->kind != DeclarationKind::kFunctor) {
            context->error_log.Append(scope_range,
                                      param_merge_functor_name.SpellingRange())
                << "Expected a functor name here, but got a "
                << decl->KindName() << " name instead";
            return;
          }

          // We don't permit more than one mutable parameters, because that gets
          // really tricky and annoying from a codegen perspective. This is a
          // recoverable error.
          if (has_mutable_parameter) {
            auto err = context->error_log.Append(
                scope_range, prev_param_mutable_range);
            err << "The " << local->KindName() << " " << name
                << " cannot be declared  with more than one mutable parameters";

            err.Note(scope_range, prev_param_mutable_range)
                << "Previous mutable parameter is declared here";
          }

          // Save for later for reporting the abover error.
          prev_param_mutable_range = param_mutable_range;

          has_mutable_parameter = true;
          param_merge_functor = dynamic_cast<ParsedFunctorImpl *>(decl);
          assert(param_merge_functor != nullptr);
          assert(param_merge_functor->parameters.Size() == 3u);

          DisplayRange functor_decl_range =
              ParsedFunctor(param_merge_functor).SpellingRange();

          // Make sure the `range` specification of the merge functor is sane.
          if (decl->range_begin_opt.IsValid()) {
            if (decl->range != FunctorRange::kOneToOne) {
              DisplayRange range_spec(decl->range_begin_opt.Position(),
                                      decl->range_end_opt.NextPosition());

              auto err = context->error_log.Append(
                  scope_range, param_mutable_range,
                  param_merge_functor_name.Position());

              err << "Merge functor '" << decl->name << "/3' declared with "
                  << "explicit, non-one-to-one range specification, but must "
                  << "be one-to-one, i.e. 'range(.)'";

              err.Note(functor_decl_range, range_spec)
                  << "Incorrect range specification specification is here";
              return;
            }
          } else {
            decl->range = FunctorRange::kOneToOne;
          }

          // Infer the type from the mutable range.
          param_type =
              TypeLoc(param_merge_functor->parameters[0]->opt_type.Kind(),
                      param_mutable_range);

          // Make sure all parameters of the functor being used as a merge
          // operator have matching types.
          if (!param_merge_functor->is_merge) {
            for (auto p = 1u; p <= 2u; ++p) {
              ParsedParameterImpl * const merge_param =
                  param_merge_functor->parameters[p];
              ParsedParameterImpl * const prev_merge_param =
                  param_merge_functor->parameters[p - 1u];
              if (merge_param->opt_type.Kind() !=
                  prev_merge_param->opt_type.Kind()) {

                auto err = context->error_log.Append(
                    functor_decl_range, merge_param->opt_type.SpellingRange());

                err << "Mismatch between parameter type '"
                    << prev_merge_param->opt_type.SpellingRange()
                    << "' for parameter '"
                    << prev_merge_param->name
                    << "and parameter type '"
                    << merge_param->opt_type.SpellingRange()
                    << "' for parameter '"
                    << merge_param->name
                    << "' of merge functor '" << decl->name << "/3'";

                err.Note(scope_range, param_mutable_range,
                         param_merge_functor_name.Position())
                     << "Functor '" << param_merge_functor_name
                     << "/3' specified as merge operator here";
                return;
              }
            }

            // Make sure the first two parameters of the merge functor are bound,
            // and the last is free.
            ParsedParameterImpl * const p0 = param_merge_functor->parameters[0u];
            ParsedParameterImpl * const p1 = param_merge_functor->parameters[1u];
            ParsedParameterImpl * const p2 = param_merge_functor->parameters[2u];
            if (param_merge_functor->parameters[0]->opt_binding.Lexeme() !=
                Lexeme::kKeywordBound) {
              auto err = context->error_log.Append(
                  functor_decl_range, p0->opt_binding.SpellingRange());
              err << "First parameter of merge functor '" << decl->name
                  << "' must be bound";

              err.Note(scope_range, param_mutable_range,
                                   param_merge_functor_name.Position())
                  << "Functor '" << param_merge_functor_name
                  << "/3' specified as merge operator here";
              return;
            }

            if (p1->opt_binding.Lexeme() !=
                Lexeme::kKeywordBound) {
              auto err = context->error_log.Append(
                  functor_decl_range, p1->opt_binding.SpellingRange());
              err << "Second parameter of merge functor '" << decl->name
                  << "' must be bound";

              err.Note(scope_range, param_mutable_range,
                       param_merge_functor_name.Position())
                  << "Functor '" << param_merge_functor_name
                  << "/3' specified as merge operator here";
              return;
            }

            if (p2->opt_binding.Lexeme() !=
                Lexeme::kKeywordFree) {
              auto err = context->error_log.Append(
                  functor_decl_range, p2->opt_binding.SpellingRange());
              err << "Third parameter of merge functor '" << decl->name
                  << "' must be free";

              err.Note(scope_range, param_mutable_range,
                       param_merge_functor_name.Position())
                  << "Functor '" << param_merge_functor_name
                  << "/3' specified as merge operator here";
              return;
            }

            // Make sure that the functor isn't impure.
            if (!param_merge_functor->is_pure) {
              context->error_log.Append(scope_range, param_mutable_range,
                                        param_merge_functor_name.Position())
                  << "Value merging functor '" << param_merge_functor_name
                  << "/3' cannot be used in a mutable parameter because it's "
                  << "marked as impure";
              return;
            }

            // Make sure to mark the functor as being a merge functor so that
            // we don't need to repeat the above checks on each use in a
            // declaration.
            param_merge_functor->is_merge = true;
          }

          // Go parse the variable name; we've already inferred the type from
          // the functor.
          state = 3;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected a closing parenthesis here, but got '" << tok
              << "' instead";
          return;
        }

      case 8:
        if (Lexeme::kPragmaPerfInline == lexeme) {

          // Found more than one `@inline` attributes
          if (local->inline_attribute.IsValid()) {
            context->error_log.Append(scope_range, tok_range)
                << "Unexpected second '" << tok << "' pragma on "
                << local->KindName() << " '" << local->name << "/"
                << local->parameters.Size() << "'";
            state = 10;  // Ignore further errors, but add the local in.
            continue;

          // Found an `@inline` attribute.
          } else {
            local->inline_attribute = tok;
            state = 8;
            continue;
          }

        // Done with our declaration.
        } else if (Lexeme::kPuncPeriod == lexeme) {
          if (highlight.IsValid()) {
            context->error_log.Append(scope_range, highlight.SpellingRange())
                << "Declaration is marked with clause '" << highlight
                << "' pragma but ends instead of containing an embedded clause";
          }
          local->last_tok = tok;
          state = 9;
          continue;

        } else if (Lexeme::kPragmaDebugHighlight == lexeme) {
          clause_toks.push_back(tok);
          highlight = tok;
          state = 8;
          continue;

        // We've declared a local or export, and instead of the declaration
        // immediatel ending in a period, instead in ends in a colon, marking
        // it as containing trailing/embedded clauses.
        } else if (Lexeme::kPuncColon == lexeme) {
          has_embedded_clauses = true;
          clause_toks.push_back(tok);
          for (; ReadNextSubToken(tok); next_pos = tok.NextPosition()) {
            clause_toks.push_back(tok);
          }

          // Look at the last token.
          if (Lexeme::kPuncPeriod == tok.Lexeme()) {
            local->last_tok = tok;
            state = 9;
            continue;

          } else {
            context->error_log.Append(scope_range, next_pos)
                << "Declaration of '" << local->name
                << "/" << local->parameters.Size()
                << "' containing an embedded clause does not end with a period";
            state = 10;
            continue;
          }

        } else {
          DisplayRange err_range(tok.Position(),
                                 sub_tokens.back().NextPosition());
          context->error_log.Append(scope_range, err_range)
              << "Unexpected tokens before the terminating period in the"
              << " declaration of the " << local->KindName()
              << " '" << local->name << "/" << local->parameters.Size() << "'";
          state = 10;
          continue;
        }

      case 9: {
        DisplayRange err_range(tok.Position(),
                               sub_tokens.back().NextPosition());
        context->error_log.Append(scope_range, err_range)
            << "Unexpected tokens following declaration of the "
            << local->KindName() << " '"
            << local->name << "/" << local->parameters.Size()
            << "'";
        state = 10;  // Ignore further errors, but add the local in.
        continue;
      }

      case 10: continue;
    }
  }

  if (state != 9) {
    context->error_log.Append(scope_range, next_pos)
        << "The " << local->KindName()
        << " declaration for '" << local->name << "/"
        << local->parameters.Size()
        << "' must end with a period";
    RemoveDecl(local);

  // Add the local/export to the module.
  } else {
    const auto decl_for_clause = local;
    FinalizeDeclAndCheckConsistency(local);

    // If we parsed a `:` after the head of the `#local` or `#export` then
    // go parse the attached bodies recursively.
    if (has_embedded_clauses) {
      sub_tokens.swap(clause_toks);
      const auto prev_next_sub_tok_index = next_sub_tok_index;
      next_sub_tok_index = 0;
      ParseClause(module, decl_for_clause);
      next_sub_tok_index = prev_next_sub_tok_index;
      sub_tokens.swap(clause_toks);
    }
  }
}

// Try to match a clause with a declaration.
bool ParserImpl::TryMatchClauseWithDecl(ParsedModuleImpl *module,
                                        ParsedClauseImpl *clause) {

  DisplayRange clause_head_range(clause->name.Position(),
                                 clause->rparen.NextPosition());

  if (clause->head_variables.Size() > kMaxArity) {
    context->error_log.Append(scope_range, clause_head_range)
        << "Too many parameters in clause '" << clause->name
        << "; maximum number of parameters is " << kMaxArity;
    return false;
  }

#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Wconversion"
#endif

  parse::IdInterpreter interpreter = {};
  interpreter.info.atom_name_id = clause->name.IdentifierId();
  interpreter.info.arity = clause->head_variables.Size();

#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic pop
#endif

  const auto id = interpreter.flat;
  assert(!!id);

  DisplayRange directive_range;
  ParsedDeclarationImpl *decl = nullptr;

  // If it's a zero-arity clause head then it's treated by default as an
  // `#export`.
  if (clause->head_variables.Empty()) {
    if (auto decl_it = context->declarations.find(id);
        decl_it != context->declarations.end()) {
      decl = decl_it->second;
      assert(decl->context->kind == DeclarationKind::kExport);

    } else {
      auto export_decl = module->declarations.CreateDerived<ParsedExportImpl>(
          module, DeclarationKind::kExport);
      module->exports.AddUse(export_decl);
      context->declarations.emplace(id, export_decl);
      decl = export_decl;
    }

    directive_range = clause->name.SpellingRange();

  // There is a forward declaration associated with this clause head; use it.
  } else if (auto decl_it = context->declarations.find(id);
             decl_it != context->declarations.end()) {
    decl = decl_it->second;
    directive_range =
        DisplayRange(decl->directive_pos, decl->rparen.NextPosition());

  // There are no forward declarations associated with this ID.
  // We'll report an error, then invent one.
  } else {
    context->error_log.Append(scope_range, clause_head_range)
        << "Missing declaration for clause head '" << clause->name << "/"
        << clause->head_variables.Size() << "'";

    // Recover by adding a local_decl declaration; this will let us keep
    // parsing.
    auto local_decl = module->declarations.CreateDerived<ParsedLocalImpl>(
        module, DeclarationKind::kLocal);

    local_decl->directive_pos = clause->name.Position();
    local_decl->name = clause->name;
    local_decl->name_view = clause->name_view;
    local_decl->rparen = clause->rparen;

    for (ParsedVariableImpl *param_var : clause->head_variables) {
      ParsedParameterImpl * const param =
          local_decl->parameters.Create(local_decl);
      param->name = param_var->name;
      param->name_view = param_var->name_view;
      param->index = local_decl->parameters.Size() - 1u;
    }

    module->locals.AddUse(local_decl);
    context->declarations.emplace(id, local_decl);
    decl = local_decl;

    directive_range =
        DisplayRange(decl->directive_pos, decl->rparen.NextPosition());
  }

  assert(decl != nullptr);
  assert(!clause->declaration);
  clause->declaration = decl;

  const auto &decl_context = decl->context;

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
             module != decl->module) {
    auto found_redecl_in_module = false;
    for (auto redecl : decl_context->redeclarations) {
      if (redecl->module == decl->module) {
        found_redecl_in_module = true;
        break;
      }
    }

    if (!found_redecl_in_module) {
      auto err = context->error_log.Append(scope_range, clause_head_range);
      err << "Cannot define a clause '" << clause->name
          << "' for predicate exported by another module";

      err.Note(directive_range)
          << "Predicate '" << clause->name << "' is declared here";

      return false;
    }
  }

  return true;
}

// Try to match a clause with a declaration.
ParsedDeclarationImpl *ParserImpl::TryMatchPredicateWithDecl(
    ParsedModuleImpl *module, Token pred_name,
    const std::vector<ParsedVariableImpl *> &pred_vars,
    Token pred_end_tok) {

  const DisplayRange pred_head_range(pred_name.Position(),
                                     pred_end_tok.NextPosition());
  const auto arity = static_cast<unsigned>(pred_vars.size());
  if (arity > kMaxArity) {
    context->error_log.Append(scope_range, pred_head_range)
        << "Too many arguments to predicate '" << pred_name
        << "; maximum number of arguments is " << kMaxArity;
    return nullptr;
  }

#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Wconversion"
#endif

  parse::IdInterpreter interpreter = {};
  interpreter.info.atom_name_id = pred_name.IdentifierId();
  interpreter.info.arity = arity;

#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic pop
#endif

  const auto id = interpreter.flat;
  auto &decl = context->declarations[id];

  // A zero-argument predicate is like a boolean variable / option, and is
  // declared/invented on the spot. Later we'll make sure that there are clauses
  // that prove it.
  if (!arity) {
    if (!decl) {
      ParsedExportImpl * const export_decl =
          module->declarations.CreateDerived<ParsedExportImpl>(
              module, DeclarationKind::kExport);
      export_decl->name = pred_name;
      context->display_manager.TryReadData(pred_name.SpellingRange(),
                                           &(export_decl->name_view));

      module->exports.AddUse(export_decl);
      context->declarations.emplace(id, export_decl);

      decl = export_decl;
    }

  // There are no forward declarations associated with this ID.
  // We'll report an error and invent one.
  } else if (!decl) {
    context->error_log.Append(scope_range, pred_head_range)
        << "Missing declaration for predicate '" << pred_name << "/"
        << arity << "'";

    // Recover by adding a local declaration; this will let us keep
    // parsing.
    ParsedLocalImpl * const local_decl =
        module->declarations.CreateDerived<ParsedLocalImpl>(
            module, DeclarationKind::kLocal);
    local_decl->directive_pos = pred_name.Position();
    local_decl->name = pred_name;
    context->display_manager.TryReadData(pred_name.SpellingRange(),
                                         &(local_decl->name_view));
    local_decl->rparen = pred_end_tok;

    for (ParsedVariableImpl *used_var : pred_vars) {
      ParsedParameterImpl *param = local_decl->parameters.Create(local_decl);
      param->name = used_var->name;
      param->name_view = used_var->name_view;
      param->opt_type = used_var->type;
      param->index = local_decl->parameters.Size() - 1u;
    }

    module->locals.AddUse(local_decl);
    context->declarations.emplace(id, local_decl);

    decl = local_decl;
  }

  // Don't let us receive this message if we have any sends of this message.
  parse::DeclarationContext *const decl_context = decl->context.get();
  if (decl_context->kind == DeclarationKind::kMessage &&
      !decl_context->clauses.Empty()) {

    auto err = context->error_log.Append(scope_range, pred_head_range);
    err << "Cannot receive input from message '" << pred_name << '/'
        << arity << "'; the message is already used for sending data";

    for (ParsedClauseImpl *clause_ : decl_context->clauses) {
      auto clause = ParsedClause(clause_);
      err.Note(clause.SpellingRange(), ParsedClauseHead(clause).SpellingRange())
          << "Message send is here";
    }
  }

  return decl;
}

// Try to parse all of the tokens.
void ParserImpl::ParseAllTokens(ParsedModuleImpl *module) {
  next_tok_index = 0;
  Token tok;

  DisplayRange first_non_import;

  while (ReadNextToken(tok)) {
    sub_tokens.clear();
    sub_tokens.push_back(tok);
    next_sub_tok_index = 0;

    switch (tok.Lexeme()) {
      case Lexeme::kHashEnum:
        ReadStatement();
        ParseEnum(module);
        if (first_non_import.IsInvalid()) {
          first_non_import = SubTokenRange();
        }
        break;

      case Lexeme::kHashFunctorDecl:
        ReadStatement();
        ParseFunctor(module);
        if (first_non_import.IsInvalid()) {
          first_non_import = SubTokenRange();
        }
        break;

      case Lexeme::kHashMessageDecl:
        ReadStatement();
        ParseMessage(module);
        if (first_non_import.IsInvalid()) {
          first_non_import = SubTokenRange();
        }
        break;

      case Lexeme::kHashQueryDecl:
        ReadStatement();
        ParseQuery(module);
        if (first_non_import.IsInvalid()) {
          first_non_import = SubTokenRange();
        }
        break;

      case Lexeme::kHashExportDecl:
        ReadStatement();
        ParseLocalExport<ParsedExportImpl, DeclarationKind::kExport,
                         Lexeme::kHashExportDecl>(module, module->exports);
        if (first_non_import.IsInvalid()) {
          first_non_import = SubTokenRange();
        }
        break;

      case Lexeme::kHashLocalDecl:
        ReadStatement();
        ParseLocalExport<ParsedLocalImpl, DeclarationKind::kLocal,
                         Lexeme::kHashLocalDecl>(module, module->locals);
        if (first_non_import.IsInvalid()) {
          first_non_import = SubTokenRange();
        }
        break;

      case Lexeme::kHashForeignTypeDecl:
        ReadStatement();
        ParseForeignTypeDecl(module);
        if (first_non_import.IsInvalid()) {
          first_non_import = SubTokenRange();
        }
        break;

      case Lexeme::kHashForeignConstantDecl:
        ReadStatement();
        ParseForeignConstantDecl(module);
        if (first_non_import.IsInvalid()) {
          first_non_import = SubTokenRange();
        }
        break;

      // Import another module, e.g. `#import "foo/bar".`.
      case Lexeme::kHashImportModuleStmt:
        ReadStatement();
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

      // Specify that the generated code should be emitted into the target code
      // by the code generator at a specific stage of code generation.
      //
      //    #inline(stage-name) ```
      //    ...
      //    ```.
      case Lexeme::kHashInlineStmt:
        ReadStatement();
        ParseInlineCode(module);
        continue;

      // Declare the name of this datbase.
      //
      //    #database atom_name.
      //
      // This affects later code generation. E.g. the
      case Lexeme::kHashDatabase:
        ReadStatement();
        ParseDatabase(module);
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

      case Lexeme::kComment:
      case Lexeme::kWhitespace: continue;

      case Lexeme::kEndOfFile: return;

      // Don't warn about this, we've already warned about it.
      case Lexeme::kInvalidUnknown: continue;

      // Error, an unexpected top-level token.
      default: {
        ReadStatement();
        context->error_log.Append(scope_range, tok.SpellingRange())
            << "Unexpected top-level token; expected either a "
            << "clause definition or a declaration";
        break;
      }
    }
  }
}

// Remove a declaration.
void ParserImpl::RemoveDecl(ParsedDeclarationImpl *decl) {
  if (!decl) {
    return;
  }

#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Wconversion"
#endif

  parse::IdInterpreter interpreter = {};
  interpreter.info.atom_name_id = decl->name.IdentifierId();
  interpreter.info.arity = decl->parameters.Size();
  const auto id = interpreter.flat;

#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic pop
#endif

  decl->context->redeclarations.RemoveIf([=] (ParsedDeclarationImpl *that) {
    return that == decl;
  });

  context->declarations.erase(id);
}

// Add `decl` to the end of `decl_list`, and make sure `decl` is consistent
// with any prior declarations of the same name.
bool ParserImpl::FinalizeDeclAndCheckConsistency(ParsedDeclarationImpl *decl) {

  const auto scope_range = SubTokenRange();
  const auto num_params = decl->parameters.Size();

  const ParsedDeclaration decl_pub(decl);
  const parse::DeclarationContext *decl_context = decl->context.get();
  auto &redecls = decl_context->redeclarations;

  // Link this declaration in.
  switch (auto &unique_redeclarations = decl->context->unique_redeclarations;
          decl->context->kind) {
    case DeclarationKind::kFunctor:
    case DeclarationKind::kQuery:
      for (auto redecl : unique_redeclarations) {
        const auto arity = decl->parameters.Size();
        auto all_same = true;
        for (auto i = 0u; i < arity; ++i) {
          ParsedParameterImpl *const old_p = redecl->parameters[i];
          ParsedParameterImpl *const new_p = decl->parameters[i];
          if (old_p->opt_binding.Lexeme() != new_p->opt_binding.Lexeme()) {
            all_same = false;
            break;
          }
        }
        if (all_same) {
          return true;  // Not a unique redecl.
        }
      }

      // If we made it down here, then it's not unique.
      unique_redeclarations.AddUse(decl);
      break;

    default:
      if (unique_redeclarations.Empty()) {
        unique_redeclarations.AddUse(decl);
      }
      break;
  }

  auto num_redecls = redecls.Size();
  if (1u >= num_redecls) {
    return true;
  }

  ParsedDeclarationImpl *const prev_decl = redecls[num_redecls - 1u];
  const ParsedDeclaration prev_decl_pub(prev_decl);
  const DisplayRange prev_decl_range = prev_decl_pub.SpellingRange();
  assert(prev_decl->parameters.Size() == num_params);

  // The first usage of a functor in a `mutable` attribute marks it as
  // being a merge functor and forces a `1:1` range.
  if (prev_decl->range != decl->range &&
      prev_decl->range == FunctorRange::kOneToOne && prev_decl->is_merge) {
    assert(!decl->is_merge);
    decl->is_merge = true;
    decl->range = FunctorRange::kOneToOne;
  }

  // Different `@first` attributes.
  if (prev_decl->first_attribute.IsValid() !=
      decl->first_attribute.IsValid()) {
    if (decl->first_attribute.IsValid()) {
      auto err = context->error_log.Append(
          scope_range, decl->differential_attribute.SpellingRange());
      err << "Message is marked as returning at most one output, "
          << "but prior declaration isn't";

      auto note = err.Note(prev_decl_range);
      note << "Previous declaration is here";

    } else {
      auto err = context->error_log.Append(
          prev_decl_range,
          prev_decl->first_attribute.SpellingRange());
      err << "Message is marked as returning at most one output, "
          << "but redeclaration isn't";

      auto note = err.Note(scope_range);
      note << "Redeclaration is here";
    }
  }

  // Different `@differential` attributes.
  if (prev_decl->differential_attribute.IsValid() !=
      decl->differential_attribute.IsValid()) {

    if (decl->differential_attribute.IsValid()) {
      auto err = context->error_log.Append(
          scope_range, decl->differential_attribute.SpellingRange());
      err << "Message is marked as differential, but prior declaration isn't";

      auto note = err.Note(prev_decl_range);
      note << "Previous declaration is here";

    } else {
      auto err = context->error_log.Append(
          prev_decl_range,
          prev_decl->differential_attribute.SpellingRange());
      err << "Message is marked as differential, but redeclaration isn't";

      auto note = err.Note(scope_range);
      note << "Redeclaration is here";
    }
  }

  // The inferred range specifications don't match.
  if (prev_decl->range != decl->range &&
      (prev_decl_pub.BindingPattern() == decl_pub.BindingPattern())) {
    DisplayRange prev_range_spec(prev_decl->range_begin_opt.Position(),
                                 prev_decl->range_end_opt.NextPosition());

    DisplayRange curr_range_spec(decl->range_begin_opt.Position(),
                                 decl->range_end_opt.NextPosition());

    // Examine the concrete syntax to produce a meaningful error message.
    if (prev_decl->range_begin_opt.IsValid() &&
        decl->range_begin_opt.IsValid()) {
      auto err = context->error_log.Append(scope_range, curr_range_spec);
      err << "Functor range specifier differs from prior range specifier";

      auto note = err.Note(prev_decl_range, prev_range_spec);
      note << "Previous range specifier is here";

    } else if (prev_decl->range_begin_opt.IsValid()) {
      auto err = context->error_log.Append(scope_range);
      err << "Functor uses default zero-or-more range specifier, but prior "
          << "declaration explicitly changes the range";

      auto note = err.Note(prev_decl_range, prev_range_spec);
      note << "Previous range specifier is here";

    } else if (decl->range_begin_opt.IsValid()) {
      auto err = context->error_log.Append(scope_range, curr_range_spec);
      err << "Functor explicitly specifies a non-default range specifier "
          << "that is different than the implicit zero-or-more specification";

      auto note = err.Note(prev_decl_range);
      note << "Previous declaration uses the implicit zero-or-more range "
           << "specification";

    // Neither functor has explicit `range` syntax, and they disagree.
    } else {
      auto err = context->error_log.Append(scope_range);
      err << "Inferred functor range differs from prior inferred range";

      auto note = err.Note(prev_decl_range);
      note << "Previous declaration is here";
    }

    RemoveDecl(decl);
    return false;
  }

  const DeclarationKind prev_decl_kind = prev_decl->context->kind;

  // Make sure all parameters bindings, types, merge declarations, etc. match
  // across all re-declarations.
  for (size_t i = 0; i < num_params; ++i) {
    ParsedParameterImpl * const prev_param = prev_decl->parameters[i];
    ParsedParameterImpl * const curr_param = decl->parameters[i];
    if ((prev_param->opt_binding.Lexeme() !=
         curr_param->opt_binding.Lexeme()) &&
        prev_decl_kind != DeclarationKind::kFunctor &&
        prev_decl_kind != DeclarationKind::kQuery) {
      auto err = context->error_log.Append(
          scope_range, curr_param->opt_binding.SpellingRange());
      err << "Parameter binding attribute differs";

      auto note = err.Note(prev_decl_range,
                           prev_param->opt_binding.SpellingRange());
      note << "Previous parameter binding attribute is here";

      RemoveDecl(decl);
      return false;
    }

    // Make sure the names of the parameters match, as these are all "exported"
    // symbols to codegen, and we want the names of structure fields and such to
    // be consistently named.
    if ((prev_decl_kind == DeclarationKind::kFunctor ||
         prev_decl_kind == DeclarationKind::kQuery ||
         prev_decl_kind == DeclarationKind::kMessage) &&
        (prev_param->name.IdentifierId() !=
         curr_param->name.IdentifierId())) {
      auto err = context->error_log.Append(
          scope_range, curr_param->name.SpellingRange());
      err << "Parameter name on externally visible declaration (query, "
          << "message, functor) differs from prior declaration";

      auto note = err.Note(prev_decl_range,
                           prev_param->name.SpellingRange());
      note << "Previous parameter name here";

      RemoveDecl(decl);
      return false;
    }

    if (prev_param->opt_merge != curr_param->opt_merge) {
      auto err = context->error_log.Append(
          scope_range, curr_param->opt_binding.SpellingRange());
      err << "Mutable parameter's merge operator differs";

      auto note = err.Note(ParsedDeclaration(prev_decl).SpellingRange(),
                           prev_param->opt_binding.SpellingRange());
      note << "Previous mutable attribute declaration is here";

      RemoveDecl(decl);
      return false;
    }

    if (prev_param->opt_type.Kind() != curr_param->opt_type.Kind()) {
      auto err = context->error_log.Append(
          scope_range, curr_param->opt_type.SpellingRange());
      err << "Parameter type specification differs";

      auto note = err.Note(prev_decl_range,
                           prev_param->opt_type.SpellingRange());
      note << "Previous type specification is here";

      RemoveDecl(decl);
      return false;
    }
  }

  // Make sure this inline attribute matches the prior one.
  if (prev_decl->inline_attribute.Lexeme() !=
      decl->inline_attribute.Lexeme()) {
    auto err = context->error_log.Append(
        scope_range, decl->inline_attribute.SpellingRange());
    err << "Inline attribute differs";

    auto note = err.Note(prev_decl_range,
                         prev_decl->inline_attribute.SpellingRange());
    note << "Previous inline attribute is here";
    RemoveDecl(decl);
    return false;
  }

  return true;
}

// Try to parse `sub_range` as a database name declaration.
void ParserImpl::ParseDatabase(ParsedModuleImpl *module) {
  Token tok;
  if (!ReadNextSubToken(tok)) {
    assert(false);
  }

  assert(tok.Lexeme() == Lexeme::kHashDatabase);

  const Token introducer_tok = tok;

  if (!ReadNextSubToken(tok)) {
    context->error_log.Append(scope_range, introducer_tok.NextPosition())
        << "Unexpected end-of-stream here; expected an atom for the database "
        << "name here";
    return;
  }

  bool try_split = false;
  std::string_view data_view;
  std::function<bool(void)> read_data = [] (void) { return false; };

  const Token db_name = tok;
  switch (db_name.Lexeme()) {
    case Lexeme::kIdentifierAtom:
    case Lexeme::kIdentifierVariable:
    case Lexeme::kIdentifierConstant:
    case Lexeme::kIdentifierType:
      read_data = [&] (void) {
        return context->display_manager.TryReadData(db_name.SpellingRange(),
                                                    &data_view);
      };
      break;
    case Lexeme::kLiteralString:
      try_split = true;
      read_data = [&] (void) {
        return context->string_pool.TryReadString(db_name.StringId(),
                                                  db_name.StringLength(),
                                                  &data_view);
      };
      break;
    case Lexeme::kLiteralCode:
      try_split = true;
      read_data = [&] (void) {
        return context->string_pool.TryReadCode(db_name.CodeId(), &data_view);
      };
      break;

    default:
      context->error_log.Append(scope_range, introducer_tok.NextPosition())
          << "Expected an atom or variable identifier, but got '"
          << db_name << "' instead";
      return;
  }

  if (!ReadNextSubToken(tok)) {
    context->error_log.Append(scope_range, db_name.NextPosition())
        << "Unexpected end-of-stream here; expected a period to end the "
        << "database name declaration here";
    return;
  }

  if (!read_data() || data_view.empty()) {
    context->error_log.Append(scope_range, db_name.SpellingRange())
        << "Unable to read database name, or database name is empty";
    return;
  }

  const Token dot = tok;
  std::string name_str(data_view.data(), data_view.size());

  std::vector<std::string> parts;
  if (try_split) {
    std::string part;
    for (auto ch : name_str) {
      if (std::isalpha(ch) || ch == '_') {
        part.push_back(ch);
      } else if (std::isdigit(ch)) {
        if (part.empty()) {
          continue;
        } else {
          part.push_back(ch);
        }
      } else if (!part.empty()) {
        parts.emplace_back(std::move(part));
      }
    }
    if (!part.empty()) {
      parts.emplace_back(std::move(part));
    }

    if (parts.empty()) {
      context->error_log.Append(scope_range, db_name.SpellingRange())
          << "Database name could not be split into parts matching the "
          << "pattern '[a-zA-Z_][a-zA-Z0-9_]*'";
      return;
    }
  } else {
    parts.emplace_back(std::move(name_str));
  }

  const auto name = module->root_module->names.Create(
      introducer_tok, db_name, dot, std::move(parts));
  const auto first_name = module->root_module->names[0];

  if (name->name_parts != first_name->name_parts) {
    auto err = context->error_log.Append(scope_range, db_name.SpellingRange());

    err
        << "Unexpected change in database name here; previous name was '"
        << first_name->name_tok << "'";

    err.Note(DisplayRange(first_name->introducer_tok.Position(),
                          first_name->dot_tok.NextPosition()),
             first_name->name_tok.SpellingRange())
        << "Previous name declaration was here";
    return;
  }
}

// Perform type checking/assignment. Returns `false` if there was an error.
bool ParserImpl::AssignTypes(ParsedModuleImpl *root_module) {

  auto var_var_eq_p = [=](ParsedVariableImpl *a,
                          ParsedVariableImpl *b) -> bool {
    if (a->type.Kind() == b->type.Kind()) {
      return true;
    }

    ParsedVariable a_var(a);
    ParsedVariable b_var(b);

    auto err = context->error_log.Append(
        ParsedClause(a->clause).SpellingRange(),
        a_var.SpellingRange());
    err << "Type mismatch between variable '" << a_var.Name() << "' (type '"
        << a_var.Type().SpellingRange() << "') and '" << b_var.Name()
        << "' (type '" << b_var.Type().SpellingRange() << "')";

    err.Note(a_var.Type().SpellingRange(), a_var.Type().SpellingRange())
        << "Variable '" << a_var.Name() << "' with type '"
        << a_var.Type().SpellingRange() << "' is from here";

    err.Note(b_var.Type().SpellingRange(), b_var.Type().SpellingRange())
        << "Variable '" << b_var.Name() << "' with type '"
        << b_var.Type().SpellingRange() << "' is from here";

    return false;
  };

  auto var_param_eq_p = [=](ParsedVariableImpl *a,
                            ParsedParameterImpl *b) -> bool {
    if (a->type.Kind() == b->opt_type.Kind()) {
      return true;
    }

    ParsedVariable a_var(a);
    ParsedParameter b_var(b);

    auto err = context->error_log.Append(
        ParsedClause(a->clause).SpellingRange(),
        a_var.SpellingRange());
    err << "Type mismatch between variable '" << a_var.Name() << "' (type '"
        << a_var.Type().SpellingRange() << "') and parameter '" << b_var.Name()
        << "' (type '" << b_var.Type().SpellingRange() << "')";

    err.Note(a_var.Type().SpellingRange(), a_var.Type().SpellingRange())
        << "Variable '" << a_var.Name() << "' with type '"
        << a_var.Type().SpellingRange() << "' is from here";

    err.Note(b_var.Type().SpellingRange(), b_var.Type().SpellingRange())
        << "Parameter '" << b_var.Name() << "' with type '"
        << b_var.Type().SpellingRange() << "' is from here";

    return false;
  };

  std::vector<ParsedVariableImpl *> missing;
  auto changed = true;

  auto check_apply_var_types = [&](ParsedVariableImpl *var) -> bool {
    for (ParsedVariableImpl *next_var = var->first_appearance;
         next_var != nullptr; next_var = next_var->next_appearance) {

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

  auto pred_valid = [&](ParsedPredicateImpl *pred) -> bool {
    auto j = 0u;
    auto pred_decl = pred->declaration;
    for (ParsedVariableImpl * const arg : pred->argument_uses) {
      ParsedParameterImpl * const param = pred_decl->parameters[j++];
      auto &lhs_type = arg->type;
      auto &rhs_type = param->opt_type;
      auto lhs_is_valid = lhs_type.IsValid();
      auto rhs_is_valid = rhs_type.IsValid();
      if (lhs_is_valid && rhs_is_valid) {
        if (!var_param_eq_p(arg, param)) {
          return false;
        }
      } else if (lhs_is_valid) {
        rhs_type = lhs_type;
        changed = true;

      } else if (rhs_is_valid) {
        lhs_type = rhs_type;
        changed = true;
        check_apply_var_types(arg);

      } else {
        missing.push_back(arg);
      }
    }
    return true;
  };

  auto do_clause = [&](ParsedClauseImpl *clause) -> bool {
    auto i = 0u;

    for (ParsedVariableImpl * const var : clause->head_variables) {
      ParsedParameterImpl * const decl_param =
          clause->declaration->parameters[i++];

      // Head variable-based top-down. The head variable has a type, so
      // propagate that type through all uses and check that they all
      // match.
      if (var->type.IsValid()) {
      var_has_type:

        if (decl_param->opt_type.IsInvalid()) {
          decl_param->opt_type = var->type;
          changed = true;

        } else if (!var_param_eq_p(var, decl_param)) {
          return false;
        }

        if (!check_apply_var_types(var)) {
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
        for (auto next_var = var->next_appearance; next_var != nullptr;
             next_var = next_var->next_appearance) {
          if (next_var->type.IsValid()) {
            var->type = next_var->type;
            changed = true;
            goto var_has_type;
          }
        }

        // If we reached down here then the parameter variable's type was
        // not inferred from any use.
        missing.push_back(var);
      }
    }

    // Go through all positive predicates, and do declaration-based
    // bottom-up type propagation.
    for (ParsedPredicateImpl *pred : clause->forcing_predicates) {
      if (!pred_valid(pred)) {
        return false;
      }
    }

    // Go through all assignments and propagate the variable's type to the
    // literals.
    for (const auto &group : clause->groups) {
      for (ParsedAssignmentImpl *assign : group->assignments) {
        TypeLoc &lhs_type = assign->lhs->type;
        if (lhs_type.IsValid()) {
          if (!assign->rhs.type.IsValid()) {
            assign->rhs.type = lhs_type;

          // E.g. assigning a type-inferred variable to a named constant.
          } else if (assign->rhs.type.Kind() != lhs_type.Kind()) {
            auto lhs_var = ParsedVariable(assign->lhs.get());
            auto rhs_const = ParsedLiteral(&(assign->rhs));
            auto err = context->error_log.Append(
                ParsedClause(clause).SpellingRange(), lhs_var.SpellingRange());
            err << "Type mismatch between variable '" << lhs_var.Name()
                << "' (type '" << lhs_var.Type().SpellingRange()
                << "') and constant '" << rhs_const.Literal() << "' (type '"
                << rhs_const.Type().SpellingRange() << "')";

            err.Note(lhs_var.Type().SpellingRange(),
                     lhs_var.Type().SpellingRange())
                << "Variable '" << lhs_var.Name() << "' with type '"
                << lhs_var.Type().SpellingRange() << "' is from here";

            err.Note(rhs_const.Type().SpellingRange(),
                     rhs_const.Type().SpellingRange())
                << "Constant '" << rhs_const.Literal() << "' with type '"
                << rhs_const.Type().SpellingRange() << "' is from here";
            return false;
          }

        // E.g. assigning a variable to a named constant.
        } else if (assign->rhs.type.IsValid()) {
          lhs_type = assign->rhs.type;

        } else {
          missing.push_back(assign->lhs.get());
        }
      }

      // Go through all comparisons and try to match up the types of the
      // compared variables.
      for (ParsedComparisonImpl * const cmp : group->comparisons) {
        ParsedVariableImpl * const lhs = cmp->lhs.get();
        ParsedVariableImpl * const rhs = cmp->rhs.get();
        TypeLoc &lhs_type = lhs->type;
        TypeLoc &rhs_type = rhs->type;
        auto lhs_is_valid = lhs_type.IsValid();
        auto rhs_is_valid = rhs_type.IsValid();
        if (lhs_is_valid && rhs_is_valid) {
          if (!var_var_eq_p(lhs, rhs)) {
            return false;
          }
        } else if (lhs_is_valid) {
          rhs_type = lhs_type;
          changed = true;
          check_apply_var_types(rhs);

        } else if (rhs_is_valid) {
          lhs_type = rhs_type;
          changed = true;
          check_apply_var_types(lhs);

        } else {
          missing.push_back(lhs);
          missing.push_back(rhs);
        }
      }

      for (ParsedPredicateImpl *pred : group->positive_predicates) {
        if (!pred_valid(pred)) {
          return false;
        }
      }

      for (ParsedPredicateImpl *pred : group->negated_predicates) {
        if (!pred_valid(pred)) {
          return false;
        }
      }

      // Go through all aggregates.
      for (ParsedAggregateImpl *agg : group->aggregates) {
        if (!pred_valid(&(agg->functor)) || !pred_valid(&(agg->predicate))) {
          return false;
        }
      }
    }

    return true;
  };

  for (; changed;) {
    changed = false;
    missing.clear();

    for (ParsedModuleImpl *module : root_module->all_modules) {
      for (ParsedClauseImpl *clause : module->clauses) {
        if (!do_clause(clause)) {
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
    for (ParsedVariableImpl *var : missing) {
      context->error_log.Append(
          ParsedClause(var->clause).SpellingRange(),
          ParsedVariable(var).SpellingRange())
          << "Could not infer type of non-range-restricted variable '"
          << var->name << "'";
    }
    return false;
  }

  // Type the redecls. This applies to locals/exports only, as type annotations
  // are required on all parameters of other kinds of declarations.
  auto type_redecls = [&](const auto &decl_list) {
    for (ParsedDeclarationImpl *first : decl_list) {
      const auto num_params = first->parameters.Size();

      for (ParsedDeclarationImpl *next : first->context->redeclarations) {

        for (auto j = 0u; j < num_params; ++j) {
          ParsedParameterImpl * const first_param = first->parameters[j];
          ParsedParameterImpl * const next_param = next->parameters[j];
          if (!next_param->parsed_opt_type) {
            next_param->opt_type = first_param->opt_type;
          }
        }
      }
    }
  };

  for (ParsedModuleImpl *module : root_module->all_modules) {
    type_redecls(module->locals);
    type_redecls(module->exports);
  }

  return true;
}

// Checks that all locals and exports are defined.
static bool AllDeclarationsAreDefined(ParsedModuleImpl *root_module,
                                      const ErrorLog &log) {

  auto do_decl = [&](ParsedDeclaration decl) {
    if (!decl.Clauses().empty()) {
      return;
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
  for (ParsedModuleImpl *module : root_module->all_modules) {
    for (ParsedLocalImpl *decl : module->locals) {
      do_decl(ParsedDeclaration(decl));
    }
    for (ParsedExportImpl *decl : module->exports) {
      do_decl(ParsedDeclaration(decl));
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
  module = std::make_shared<ParsedModuleImpl>(config);

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
  std::error_code ec;
  auto path = std::filesystem::canonical(std::filesystem::path(path_), ec);
  if (ec) {
    impl->context->error_log.Append()
        << "Could not find file '" << path_ << "': " << ec.message();
    return std::nullopt;
  }

  auto display = impl->context->display_manager.OpenPath(
      path.generic_string(), config);

  // Special case for path parsing, we need to change the parser's current
  // working directory to the directory containing the file being parsed
  // so that we can do file-relative imports.
  auto prev_path0 = impl->context->import_search_paths[0];
  impl->context->import_search_paths[0] = path.parent_path();
  auto module = impl->ParseDisplay(display, config);
  impl->context->import_search_paths[0] = prev_path0;  // Restore cwd

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
void Parser::AddModuleSearchPath(std::filesystem::path path) const {
  impl->context->import_search_paths.emplace_back(path);
}

// Return a copy of the list of search paths used by the parser.
std::vector<std::filesystem::path> Parser::SearchPaths(void) const noexcept {
  return impl->context->import_search_paths;
}

}  // namespace hyde
