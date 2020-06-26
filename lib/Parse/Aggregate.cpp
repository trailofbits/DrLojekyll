// Copyright 2020, Trail of Bits. All rights reserved.

#include "Parser.h"

namespace hyde {

// Try to parse the predicate application following a use of an aggregating
// functor.
bool ParserImpl::ParseAggregatedPredicate(
    Node<ParsedModule> *module,
    Node<ParsedClause> *clause,
    std::unique_ptr<Node<ParsedPredicate>> functor,
    Token &tok, DisplayPosition &next_pos) {

  auto state = 0;

  std::unique_ptr<Node<ParsedLocal>> anon_decl;
  std::unique_ptr<Node<ParsedPredicate>> pred;
  std::unique_ptr<Node<ParsedParameter>> anon_param;

  Node<ParsedVariable> *arg = nullptr;

  // Build up a token list representing a synthetic clause definition
  // associated with `anon_decl`.
  std::vector<Token> anon_clause_toks;

  DisplayPosition last_pos;

  int brace_count = 1;

  for (; ReadNextSubToken(tok); next_pos = tok.NextPosition()) {
    const auto lexeme = tok.Lexeme();
    const auto tok_range = tok.SpellingRange();

    switch (state) {
      case 0:
        // An inline predicate; we'll need to invent a declaration and
        // clause for it.
        if (Lexeme::kPuncOpenParen == lexeme) {
          anon_decl.reset(new Node<ParsedLocal>(
              module, DeclarationKind::kLocal));
          anon_decl->directive_pos = tok.Position();
          anon_decl->name = Token::Synthetic(
              Lexeme::kIdentifierUnnamedAtom, tok_range);
          anon_decl->inline_attribute = Token::Synthetic(
              Lexeme::kKeywordInline, DisplayRange());
          assert(anon_decl->name.Lexeme() == Lexeme::kIdentifierUnnamedAtom);
          anon_clause_toks.push_back(anon_decl->name);

          assert(tok.Lexeme() == Lexeme::kPuncOpenParen);
          anon_clause_toks.push_back(tok);
          pred.reset(new Node<ParsedPredicate>(module, clause));
          pred->declaration = anon_decl.get();
          pred->name = anon_decl->name;
          state = 1;
          continue;

        // Direct application.
        } else if (Lexeme::kIdentifierAtom == lexeme) {
          pred.reset(new Node<ParsedPredicate>(module, clause));
          pred->name = tok;
          state = 6;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected an opening parenthesis or atom (predicate name) "
              << "here for inline predicate, but got '" << tok << "' instead";
          return false;
        }

      case 1:
        if (tok.IsType()) {
          anon_param.reset(new Node<ParsedParameter>);
          anon_param->opt_type = tok;
          anon_param->parsed_opt_type = true;
          state = 2;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected a type name for parameter to inline aggregate "
              << "clause, but got '" << tok << "' instead";
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
          auto use = new Node<ParsedUse<ParsedPredicate>>(
              UseKind::kArgument, arg, pred.get());

          // Add to this variable's use list.
          auto &argument_uses = arg->context->argument_uses;
          if (!argument_uses.empty()) {
            argument_uses.back()->next = use;
          }
          argument_uses.push_back(use);

          // Link the arguments together.
          if (!pred->argument_uses.empty()) {
            pred->argument_uses.back()->used_var->next_var_in_arg_list = arg;
          }

          pred->argument_uses.emplace_back(use);

          state = 3;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected variable name here  for parameter to inline "
              << "aggregate clause, but got '" << tok << "' instead";
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
          context->error_log.Append(scope_range, tok_range)
              << "Expected comma or closing parenthesis here for parameter list"
              << " to inline aggregate clause, but got '" << tok << "' instead";
          return false;
        }

      case 4:
        if (Lexeme::kPuncOpenBrace == lexeme) {
          const auto colon = Token::Synthetic(
              Lexeme::kPuncColon, tok_range);
          assert(colon.Lexeme() == Lexeme::kPuncColon);
          anon_clause_toks.push_back(colon);
          state = 5;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected opening brace here for body of inline aggregate "
              << "clause, but got '" << tok << "' instead";
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
                Token::Synthetic(Lexeme::kPuncPeriod, tok_range));

            auto prev_next_sub_tok_index = next_sub_tok_index;
            next_sub_tok_index = 0;
            sub_tokens.swap(anon_clause_toks);
            decltype(prev_named_var) prev_prev_named_var;
            prev_prev_named_var.swap(prev_named_var);

            // Go try to parse the synthetic clause body, telling about our
            // synthetic declaration head.
            ParseClause(module, Token(), anon_decl.get());

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
          context->error_log.Append(scope_range, tok_range)
              << "Expected opening parenthesis here to test predicate '"
              << pred->name << "' used in aggregation, but got '"
              << tok << "' instead";
          return false;
        }

      case 7:
        arg = nullptr;

        // Convert literals into variables, just-in-time.
        if (Lexeme::kLiteralString == lexeme ||
            Lexeme::kLiteralNumber == lexeme) {
          arg = CreateLiteralVariable(clause, tok, false, true);

        } else if (Lexeme::kIdentifierVariable == lexeme ||
                   Lexeme::kIdentifierUnnamedVariable == lexeme) {
          arg = CreateVariable(clause, tok, false, true);
        }

        if (arg) {
          auto use = new Node<ParsedUse<ParsedPredicate>>(
              UseKind::kArgument, arg, pred.get());

          // Add to this variable's use list.
          auto &argument_uses = arg->context->argument_uses;
          if (!argument_uses.empty()) {
            argument_uses.back()->next = use;
          }
          argument_uses.push_back(use);

          // Link the arguments together.
          if (!pred->argument_uses.empty()) {
            pred->argument_uses.back()->used_var->next_var_in_arg_list = arg;
          }

          pred->argument_uses.emplace_back(use);

          state = 8;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected variable or literal here as argument to predicate '"
              << pred->name << "' used in aggregation, but got '"
              << tok << "' instead";
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

            const auto err_range = ParsedPredicate(pred.get()).SpellingRange();
            context->error_log.Append(scope_range, err_range)
                << "Cannot aggregate an aggregating functor '" << pred->name
                << "', try using inline clauses instead";
            return false;
          }

          goto done;

        } else if (Lexeme::kPuncComma == lexeme) {
          state = 7;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected comma or period, but got '" << tok << "' instead";
          return false;
        }
    }
  }

done:

  std::unique_ptr<Node<ParsedAggregate>> agg(
      new Node<ParsedAggregate>);
  agg->spelling_range = DisplayRange(functor->name.Position(), last_pos);
  agg->functor = std::move(functor);
  agg->predicate = std::move(pred);

  if (!clause->aggregates.empty()) {
    clause->aggregates.back()->next = agg.get();
  }

  clause->aggregates.emplace_back(std::move(agg));
  return true;
}

}  // namespace hyde
