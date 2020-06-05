// Copyright 2020, Trail of Bits. All rights reserved.

#include "Parser.h"

namespace hyde {

// Try to parse `sub_range` as a query, adding it to `module` if successful.
void ParserImpl::ParseQuery(Node<ParsedModule> *module) {
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
  std::unique_ptr<Node<ParsedQuery>> query;
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
              << "the #query being declared, got '" << tok << "' instead";
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
              << "#query '" << name << "', but got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 2:
        if (Lexeme::kKeywordBound == lexeme) {
          param.reset(new Node<ParsedParameter>);
          param->opt_binding = tok;
          state = 3;
          continue;

        } else if (Lexeme::kKeywordFree == lexeme) {
          param.reset(new Node<ParsedParameter>);
          param->opt_binding = tok;
          state = 3;
          continue;

        } else {
          Error err(context->display_manager, sub_tok_range,
                    tok.SpellingRange());
          err << "Expected binding specifier ('bound' or 'free') in parameter "
              << "declaration of #query '" << name << "', " << "but got '"
              << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 3:
        if (tok.IsType()) {
          param->opt_type = tok;
          param->parsed_opt_type = true;
          state = 4;
          continue;

        } else {
          Error err(context->display_manager, sub_tok_range,
                    tok.SpellingRange());
          err << "Expected type name here for parameter in #query '"
              << name << "', but got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 4:
        if (Lexeme::kIdentifierVariable == lexeme) {
          param->name = tok;
          state = 5;
          continue;

        } else {
          Error err(context->display_manager, sub_tok_range,
                    tok.SpellingRange());
          err << "Expected named variable here (capitalized identifier) as a "
              << "parameter name of #query '" << name << "', but got '"
              << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 5:
        // Add the parameter in.
        if (!params.empty()) {
          params.back()->next = param.get();

          if (params.size() == kMaxArity) {
            Error err(context->display_manager, sub_tok_range,
                      ParsedParameter(param.get()).SpellingRange());
            err << "Too many parameters to #query '" << name
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
          Error err(context->display_manager, sub_tok_range,
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
            << name << "' #query";
        context->error_log.Append(std::move(err));
        state = 7;  // Ignore further errors, but add the query in.
        continue;
      }

      case 7:
        continue;
    }
  }

  if (state < 6) {
    Error err(context->display_manager, sub_tok_range, next_pos);
    err << "Incomplete query declaration; the declaration must be "
        << "placed entirely on one line";
    context->error_log.Append(std::move(err));

    RemoveDecl<ParsedQuery>(std::move(query));
  } else {
    AddDeclAndCheckConsistency<ParsedQuery>(
        module->queries, std::move(query));
  }
}

}  // namespace hyde
