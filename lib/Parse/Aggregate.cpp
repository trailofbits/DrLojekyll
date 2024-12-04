// Copyright 2020, Trail of Bits. All rights reserved.

#include "Parser.h"

namespace hyde {
namespace {

static void AnalyzeAggregateVars(ParsedAggregateImpl *impl,
                                 const ErrorLog &log) {
  ParsedDeclarationImpl * const decl = impl->functor->declaration;
  if (decl->context->kind != DeclarationKind::kFunctor) {
    assert(0u < log.Size());
    return;
  }

  for (ParsedVariableImpl *param_var : impl->predicate->argument_uses) {
    ParsedVariableImpl *found_as = nullptr;

    auto arg_num = 0u;
    for (ParsedVariableImpl *arg_var : impl->functor->argument_uses) {
      if (param_var->first_appearance == arg_var->first_appearance) {
        found_as = arg_var;
        break;
      }
      ++arg_num;
    }

    if (!found_as) {
      impl->group_vars.AddUse(param_var);

    } else {
      ParsedParameterImpl * const param = decl->parameters[arg_num];
      switch (param->opt_binding.Lexeme()) {
        case Lexeme::kKeywordBound:
          impl->config_vars.AddUse(param_var);
          break;

        case Lexeme::kKeywordAggregate:
          impl->aggregate_vars.AddUse(param_var);
          break;

        case Lexeme::kKeywordSummary: {
          DisplayRange decl_range = ParsedDeclaration(decl).SpellingRange();
          DisplayRange param_range = ParsedParameter(param).SpellingRange();

          DisplayRange agg_range = ParsedAggregate(impl).SpellingRange();
          DisplayRange found_as_range = ParsedVariable(found_as).SpellingRange();
          auto err = log.Append(agg_range, param_range);

          err << "Parameter variable '" << param_var->name
              << "' to predicate being aggregated shares the same name "
              << "as a summary variable";

          err.Note(decl_range, param_range)
              << "Parameter '" << param->name << "' declared as summary here";

          err.Note(agg_range, found_as_range)
              << "Variable '" << param_var->name
              << "' used here as as a summary argument to the aggregating functor '"
              << impl->functor->name << "/"
              << impl->functor->argument_uses.Size() << "'";

          break;
        }
        default: assert(false); break;
      }
    }
  }
}

}  // namespace

// Try to parse the predicate application following a use of an aggregating
// functor.
bool ParserImpl::ParseAggregatedPredicate(
    ParsedModuleImpl *module, ParsedClauseImpl *clause,
    ParsedAggregateImpl *agg, Token &tok,
    DisplayPosition &next_pos) {

  auto state = 0;

  ParsedLocalImpl *anon_decl = nullptr;
  ParsedPredicateImpl * const functor = &(agg->functor);
  ParsedPredicateImpl * const pred = &(agg->predicate);

  Token param_type;
  Token param_name;
  std::vector<std::pair<Token, Token>> anon_params;

  ParsedVariableImpl *arg = nullptr;
  std::vector<ParsedVariableImpl *> pred_args;

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
          anon_decl = module->declarations.CreateDerived<ParsedLocalImpl>(
              module, DeclarationKind::kLocal);

          // Unconditionally add the declaration. We'll fill in the parameters
          // as we parse.
          module->locals.AddUse(anon_decl);

          anon_decl->directive_pos = tok.Position();
          anon_decl->name =
              Token::Synthetic(Lexeme::kIdentifierUnnamedAtom, tok_range);
          context->display_manager.TryReadData(tok_range, &(anon_decl->name_view));
          anon_decl->inline_attribute =
              Token::Synthetic(Lexeme::kPragmaPerfInline, DisplayRange());
          assert(anon_decl->name.Lexeme() == Lexeme::kIdentifierUnnamedAtom);
          anon_clause_toks.push_back(anon_decl->name);

          assert(tok.Lexeme() == Lexeme::kPuncOpenParen);
          anon_clause_toks.push_back(tok);
          pred->declaration = anon_decl;
          pred->name = anon_decl->name;
          pred->name_view = anon_decl->name_view;
          anon_decl->context->positive_uses.AddUse(pred);
          state = 1;
          continue;

        // Direct application of another predicate.
        } else if (Lexeme::kIdentifierAtom == lexeme) {
          pred->name = tok;
          context->display_manager.TryReadData(tok.SpellingRange(), &(pred->name_view));
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
          param_type = tok;
          state = 2;
          continue;

        } else if (Lexeme::kIdentifierVariable == lexeme) {
          anon_clause_toks.push_back(tok);

          param_name = tok;
          state = 3;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected a type name or variable name for parameter to "
              << "inline aggregate clause, but got '" << tok << "' instead";
          return false;
        }

      case 2:
        if (Lexeme::kIdentifierVariable == lexeme) {
          anon_clause_toks.push_back(tok);

          param_name = tok;
          state = 3;
          continue;

          assert(tok.Lexeme() == Lexeme::kIdentifierVariable);

          state = 3;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected variable name here for parameter to inline "
              << "aggregate clause, but got '" << tok << "' instead";
          return false;
        }

      case 3:
        if (Lexeme::kPuncComma == lexeme) {
          anon_params.emplace_back(param_type, param_name);
          param_type = Token();
          param_name = Token();

          assert(tok.Lexeme() == Lexeme::kPuncComma);
          anon_clause_toks.push_back(tok);

          state = 1;
          continue;

        } else if (Lexeme::kPuncCloseParen == lexeme) {
          anon_params.emplace_back(param_type, param_name);

          // Add in the parameters to the anonymous declaration, and match them
          // up as arguments to the predicate.
          for (auto [p_type, p_name] : anon_params) {
            ParsedParameterImpl *p = anon_decl->parameters.Create(anon_decl);
            p->name = p_name;
            context->display_manager.TryReadData(p_name.SpellingRange(), &(p->name_view));
            p->opt_type = p_type;
            p->parsed_opt_type = p->opt_type.IsValid();

            arg = CreateVariable(clause, p_name, false, true);
            arg->type = p->opt_type;
            pred->argument_uses.AddUse(arg);
          }

          param_type = Token();
          param_name = Token();
          anon_params.clear();

          assert(tok.Lexeme() == Lexeme::kPuncCloseParen);
          anon_decl->rparen = tok;
          pred->rparen = tok;
          anon_clause_toks.push_back(tok);

          FinalizeDeclAndCheckConsistency(anon_decl);

          state = 4;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected comma or closing parenthesis here for parameter list"
              << " to inline aggregate clause, but got '" << tok << "' instead";
          return false;
        }

      case 4:
        if (Lexeme::kPuncOpenBrace == lexeme) {
          const auto colon = Token::Synthetic(Lexeme::kPuncColon, tok_range);
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
            ParseClause(module, anon_decl);

            next_sub_tok_index = prev_next_sub_tok_index;
            sub_tokens.swap(anon_clause_toks);
            prev_prev_named_var.swap(prev_named_var);

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

      // We want to parse a predicate application.
      case 6:
        if (Lexeme::kPuncOpenParen == lexeme) {
          state = 7;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected opening parenthesis here to test predicate '"
              << pred->name << "' used in aggregation, but got '" << tok
              << "' instead";
          return false;
        }

      case 7:
        arg = nullptr;

        // Convert literals into variables, just-in-time.
        if (Lexeme::kLiteralString == lexeme ||
            Lexeme::kLiteralNumber == lexeme ||
            Lexeme::kLiteralTrue == lexeme || Lexeme::kLiteralFalse == lexeme ||
            Lexeme::kIdentifierConstant == lexeme) {
          arg = CreateLiteralVariable(clause, tok, false, true);

        } else if (Lexeme::kIdentifierVariable == lexeme ||
                   Lexeme::kIdentifierUnnamedVariable == lexeme) {
          arg = CreateVariable(clause, tok, false, true);
        }

        if (arg) {
          pred->argument_uses.AddUse(arg);
          pred_args.push_back(arg);
          state = 8;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected variable or literal here as argument to predicate '"
              << pred->name << "' used in aggregation, but got '" << tok
              << "' instead";
          return false;
        }

      case 8:
        if (Lexeme::kPuncCloseParen == lexeme) {
          last_pos = tok.NextPosition();
          pred->rparen = tok;
          pred->declaration = TryMatchPredicateWithDecl(
              module, pred->name, pred_args, tok);

          // Failed to match it :-( Recover by adding a local declaration;
          // this will let us keep parsing.
          if (!pred->declaration) {
#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Wconversion"
#endif

          parse::IdInterpreter interpreter = {};
          interpreter.info.atom_name_id = pred->name.IdentifierId();
          interpreter.info.arity = std::min(
              pred->argument_uses.Size(), kMaxArity);

#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic pop
#endif

            const auto id = interpreter.flat;

            ParsedLocalImpl * const local_decl =
                module->declarations.CreateDerived<ParsedLocalImpl>(
                    module, DeclarationKind::kLocal);
            local_decl->directive_pos = pred->name.Position();
            local_decl->name = pred->name;
            local_decl->name_view = pred->name_view;
            local_decl->rparen = tok;

            for (ParsedVariableImpl *used_var : pred_args) {
              ParsedParameterImpl *param = local_decl->parameters.Create(local_decl);
              param->name = used_var->name;
              param->name_view = used_var->name_view;
              param->opt_type = used_var->type;
              param->index = local_decl->parameters.Size() - 1u;
            }

            module->locals.AddUse(local_decl);
            context->declarations.emplace(id, local_decl);

            // Use the fake declaration.
            pred->declaration = local_decl;
          }

          // Attach this use in.
          pred->declaration->context->positive_uses.AddUse(pred);

          // This is basically to keep the data flow builder simpler; we want
          // to source of data feeding an aggregate into a basic view on which
          // we can build, and thus treat as unconditionally and immediately
          // available.
          if (pred->declaration->context->kind == DeclarationKind::kFunctor) {
            DisplayRange pred_range = ParsedPredicate(pred).SpellingRange();
            context->error_log.Append(scope_range, pred_range)
                << "Cannot aggregate over functor '" << pred->name
                << "/" << pred->argument_uses.Size()
                << "'; aggregates must operate over relations";

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

  agg->spelling_range = DisplayRange(functor->name.Position(), last_pos);

  // Make sure the usage of variables is reasonable.
  AnalyzeAggregateVars(agg, context->error_log);
  return true;
}

}  // namespace hyde
