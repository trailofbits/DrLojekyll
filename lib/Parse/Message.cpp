// Copyright 2020, Trail of Bits. All rights reserved.

#include "Parser.h"

namespace hyde {

// Try to parse `sub_range` as a message, adding it to `module` if successful.
void ParserImpl::ParseMessage(ParsedModuleImpl *module) {
  Token tok;
  if (!ReadNextSubToken(tok)) {
    assert(false);
  }

  const Token directive = tok;
  assert(directive.Lexeme() == Lexeme::kHashMessageDecl);

  // State transition diagram for parsing messages.
  //
  //               .---------<-------<-------.
  //     0      1  |    2        4       5   |
  // -- atom -- ( -+-> type --> var -.-> , --'
  //                                 |
  //                                 '-> )
  //                                     6

  int state = 0;
  ParsedMessageImpl *message = nullptr;
  ParsedParameterImpl *param = nullptr;

  Token param_type;
  Token param_name;
  std::vector<std::pair<Token, Token>> params;

  DisplayPosition next_pos;
  Token name;

  // Interpretation of this query as a clause.
  std::vector<Token> clause_toks;
  bool has_embedded_clauses = false;

  Token product;

  for (next_pos = tok.NextPosition(); ReadNextSubToken(tok);
       next_pos = tok.NextPosition()) {

    if (message) {
      message->last_tok = tok;
    }

    const auto lexeme = tok.Lexeme();
    const auto tok_range = tok.SpellingRange();

    switch (state) {
      case 0:
        if (Lexeme::kIdentifierAtom == lexeme) {
          clause_toks.push_back(tok);
          name = tok;
          state = 1;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected atom here (lower case identifier) for the name of "
              << "the " << directive << " being declared, got '" << tok
              << "' instead";
          return;
        }

      case 1:
        if (Lexeme::kPuncOpenParen == lexeme) {
          clause_toks.push_back(tok);
          state = 2;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected opening parenthesis here to begin parameter list of "
              << directive << " '" << name << "', but got '" << tok
              << "' instead";
          return;
        }

      case 2:
        if (tok.IsType()) {
          param_type = tok;
          state = 3;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected type name here for parameter in " << directive
              << " '" << name << "', but got '" << tok << "' instead";
          return;
        }

      case 3:
        if (Lexeme::kIdentifierVariable == lexeme) {
          param_name = tok;
          clause_toks.push_back(tok);
          state = 4;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected named variable here (capitalized identifier) as a "
              << "parameter name of " << directive << " '" << name
              << "', but got '" << tok << "' instead";
          return;
        }

      case 4:

        if (params.size() == kMaxArity) {
          DisplayRange err_range(param_type.Position(),
                                   param_name.NextPosition());
          context->error_log.Append(scope_range, err_range)
              << "Too many parameters to message '" << name
              << "'; the maximum number of parameters is " << kMaxArity;
          return;
        }

        // Add the parameter in.
        params.emplace_back(param_type, param_name);
        param_type = Token();
        param_name = Token();

        if (Lexeme::kPuncComma == lexeme) {
          clause_toks.push_back(tok);
          state = 2;
          continue;

        } else if (Lexeme::kPuncCloseParen == lexeme) {
          message = AddDecl<ParsedMessageImpl>(
              module, DeclarationKind::kMessage, name, params.size());

          if (!message) {
            return;
          }

          clause_toks.push_back(tok);

          module->messages.AddUse(message);

          for (auto [p_type, p_name] : params) {
            param = message->parameters.Create(message);
            param->opt_type = p_type;
            param->parsed_opt_type = param->opt_type.IsValid();
            param->name = p_name;
            param->index = message->parameters.Size() - 1u;
          }

          message->rparen = tok;
          message->name = name;
          message->directive_pos = directive.Position();
          state = 5;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected either a comma or a closing parenthesis here, "
              << "but got '" << tok << "' instead";
          return;
        }

      case 5:
        if (Lexeme::kPuncPeriod == lexeme) {
          message->last_tok = tok;
          state = 6;
          continue;

        } else if (Lexeme::kPragmaDifferential == lexeme) {
          if (message->differential_attribute.IsValid()) {
            auto err = context->error_log.Append(scope_range, tok_range);
            err << "Unexpected repeat of the '" << tok << "' pragma here";

            err.Note(scope_range,
                     message->differential_attribute.SpellingRange())
                << "Previous use was here";

          } else {
            message->differential_attribute = tok;
          }
          state = 5;
          continue;

        } else if (Lexeme::kPragmaPerfProduct == lexeme) {
          if (product.IsValid()) {
            auto err = context->error_log.Append(scope_range, tok_range);
            err << "Cannot repeat pragma '" << tok << "'";

            err.Note(scope_range, product.SpellingRange())
                << "Previous use of the '" << tok << "' pragma was here";
            return;

          } else {
            clause_toks.push_back(tok);
            product = tok;
            state = 5;
            continue;
          }

        } else if (Lexeme::kPuncColon == lexeme) {
          clause_toks.push_back(tok);
          has_embedded_clauses = true;
          message->last_tok = tok;

          for (; ReadNextSubToken(tok); next_pos = tok.NextPosition()) {
            clause_toks.push_back(tok);
          }

          // Look at the last token.
          if (Lexeme::kPuncPeriod == clause_toks.back().Lexeme()) {
            message->last_tok = clause_toks.back();
            state = 6;
            continue;

          } else {
            context->error_log.Append(scope_range,
                                      clause_toks.back().NextPosition())
                << "Declaration of message '" << message->name
                << "/" << message->parameters.Size()
                << "' containing an embedded clause does not end with a period";
            state = 7;
            continue;
          }
        }
        [[clang::fallthrough]];

      case 6: {
        DisplayRange err_range(tok.Position(),
                               sub_tokens.back().NextPosition());
        context->error_log.Append(scope_range, err_range)
            << "Unexpected tokens following declaration of message '"
            << message->name << "/" << message->parameters.Size() << "'";
        state = 7;  // Ignore further errors, but add the message in.
        continue;
      }
      case 7: continue;
    }
  }

  if (state != 6) {
    context->error_log.Append(scope_range, next_pos)
        << "Incomplete message declaration; the declaration '" << name
        << "/" << message->parameters.Size() << "' must end with a period";

    RemoveDecl(message);

  } else {
    const auto decl_for_clause = message;
    FinalizeDeclAndCheckConsistency(message);

    // If we parsed a `:` after the head of the `#message` then
    // go parse the attached bodies recursively.
    if (has_embedded_clauses) {
      sub_tokens.swap(clause_toks);
      const auto prev_next_sub_tok_index = next_sub_tok_index;
      next_sub_tok_index = 0;
      ParseClause(module, decl_for_clause);
      next_sub_tok_index = prev_next_sub_tok_index;
      sub_tokens.swap(clause_toks);

    } else if (product.IsValid()) {
      context->error_log.Append(scope_range, product.SpellingRange())
          << "Superfluous '" << product
          << "' specified without any accompanying clause";
    }
  }
}

}  // namespace hyde
