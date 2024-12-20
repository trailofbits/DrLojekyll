// Copyright 2020, Trail of Bits. All rights reserved.

#include "Parser.h"

namespace hyde {

// Try to parse `sub_range` as a query, adding it to `module` if successful.
void ParserImpl::ParseQuery(ParsedModuleImpl *module) {
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
  ParsedQueryImpl *query = nullptr;

  Token param_binding;
  Token param_type;
  Token param_name;
  std::vector<std::tuple<Token, Token, Token>> params;

  DisplayPosition next_pos;
  Token name;

  // Interpretation of this query as a clause.
  std::vector<Token> clause_toks;
  bool has_embedded_clauses = false;

  for (next_pos = tok.NextPosition(); ReadNextSubToken(tok);
       next_pos = tok.NextPosition()) {

    if (query) {
      query->last_tok = tok;
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
              << "the query being declared, got '" << tok << "' instead";
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
              << "query '" << name << "', but got '" << tok << "' instead";
          return;
        }

      case 2:
        if (Lexeme::kKeywordBound == lexeme ||
            Lexeme::kKeywordFree == lexeme) {
          param_binding = tok;
          state = 3;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected binding specifier ('bound' or 'free') in parameter "
              << "declaration of query '" << name << "', "
              << "but got '" << tok << "' instead";
          return;
        }

      case 3:
        if (tok.IsType()) {
          param_type = tok;
          state = 4;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected type name here for parameter in query '" << name
              << "', but got '" << tok << "' instead";
          return;
        }

      case 4:
        if (Lexeme::kIdentifierVariable == lexeme) {
          param_name = tok;
          state = 5;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected named variable here (capitalized identifier) as a "
              << "parameter name of query '" << name << "', but got '" << tok
              << "' instead";
          return;
        }

      case 5:
        clause_toks.push_back(param_name);

        if (params.size() == kMaxArity) {
          DisplayRange err_range(param_binding.Position(),
                                 param_name.NextPosition());
          context->error_log.Append(scope_range, err_range)
              << "Too many parameters to query '" << name
              << "'; the maximum number of parameters is " << kMaxArity;
          return;
        }

        params.emplace_back(param_binding, param_type, param_name);
        param_binding = Token();
        param_type = Token();
        param_name = Token();

        if (Lexeme::kPuncComma == lexeme) {
          clause_toks.push_back(tok);
          state = 2;
          continue;

        } else if (Lexeme::kPuncCloseParen == lexeme) {
          clause_toks.push_back(tok);
          query = AddDecl<ParsedQueryImpl>(module, DeclarationKind::kQuery,
                                           name, params.size());
          if (!query) {
            return;
          }

          module->queries.AddUse(query);

          // Add the parameters in.
          for (auto [p_binding, p_type, p_name] : params) {
            ParsedParameterImpl * const param = query->parameters.Create(query);
            param->opt_binding = p_binding;
            param->opt_type = p_type;
            param->parsed_opt_type = param->opt_type.IsValid();
            param->name = p_name;
            context->display_manager.TryReadData(p_name.SpellingRange(),
                                                 &(param->name_view));
            param->index = query->parameters.Size() - 1u;
          }

          params.clear();

          query->rparen = tok;
          query->name = name;
          context->display_manager.TryReadData(name.SpellingRange(),
                                               &(query->name_view));
          query->directive_pos = sub_tokens.front().Position();
          state = 6;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected either a comma or a closing parenthesis here, "
              << "but got '" << tok << "' instead";
          return;
        }

      case 6:
        if (Lexeme::kPragmaCodeGenFirst == lexeme) {
          if (query->first_attribute.IsValid()) {
            auto err = context->error_log.Append(scope_range, tok_range);
            err << "Unexpected '" << tok << "' pragma here; query " << name
                << " was already specified with a return stream limiter";

            DisplayRange prev_range(query->first_attribute.Position(),
                                    query->first_attribute.NextPosition());
            err.Note(scope_range, prev_range)
                << "Previous '" << tok << "' pragma was here";

            RemoveDecl(query);
            return;

          } else {
            bool has_free = false;
            for (auto param : query->parameters) {
              if (param->opt_binding.Lexeme() == Lexeme::kKeywordFree) {
                has_free = true;
                break;
              }
            }

            if (!has_free) {
              context->error_log.Append(scope_range, tok_range)
                  << "Cannot use  '" << tok << "' return stream limiter "
                  << "pragma without specifying any `free`-attributed "
                  << "parameters to the query " << name;

              RemoveDecl(query);
              return;

            } else {
              query->last_tok = tok;
              query->first_attribute = tok;
              state = 6;
              continue;
            }
          }

        } else if (Lexeme::kPuncPeriod == lexeme) {
          query->last_tok = tok;
          state = 7;
          continue;

        } else if (Lexeme::kPuncColon == lexeme) {
          has_embedded_clauses = true;
          clause_toks.push_back(tok);
          for (; ReadNextSubToken(tok); next_pos = tok.NextPosition()) {
            clause_toks.push_back(tok);
          }

          // Look at the last token.
          if (Lexeme::kPuncPeriod == clause_toks.back().Lexeme()) {
            query->last_tok = clause_toks.back();
            state = 7;
            continue;

          } else {
            context->error_log.Append(scope_range,
                                      clause_toks.back().NextPosition())
                << "Declaration of query '" << query->name
                << "/" << query->parameters.Size()
                << "' containing an embedded clause does not end with a period";
            state = 8;
            continue;
          }
        }
        [[clang::fallthrough]];

      case 7: {
        DisplayRange err_range(tok.Position(),
                               sub_tokens.back().NextPosition());
        context->error_log.Append(scope_range, err_range)
            << "Unexpected tokens following declaration of query '" << name
            << "/" << query->parameters.Size() << "'";
        state = 8;  // Ignore further errors, but add the query in.
        continue;
      }

      case 8: continue;  // absorb excess tokens
    }
  }

  if (state != 7) {
    context->error_log.Append(scope_range, next_pos)
        << "Incomplete query declaration; the declaration must end with a "
        << "period; last token was " << tok;

    RemoveDecl(query);

  } else {
    const auto decl_for_clause = query;
    FinalizeDeclAndCheckConsistency(query);

    // If we parsed a `:` after the head of the `#query` then
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

}  // namespace hyde
