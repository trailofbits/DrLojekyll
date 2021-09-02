// Copyright 2020, Trail of Bits. All rights reserved.

#include "Parser.h"

namespace hyde {

// Try to parse `sub_range` as a functor, adding it to `module` if successful.
void ParserImpl::ParseFunctor(ParsedModuleImpl *module) {
  std::vector<Token> parsed_tokens;

  Token tok;
  if (!ReadNextSubToken(tok)) {
    assert(false);
  }

  assert(tok.Lexeme() == Lexeme::kHashFunctorDecl);
  parsed_tokens.push_back(tok);

  // State transition diagram for parsing functors.
  //
  //               .---------------<-------<------<-------.
  //     0      1  |        2         3       4       5   |
  // -- atom -- ( -+-> bound/free -> type -> var -+-> , --'  .------<------.
  //               aggregate/summary              |         |              |
  //                                              '-> ) -+--+---> impure --'
  //                                                  6  |   |
  //                                    9         .------'   |
  //                               .--> ) --->----'      unordered
  //                               |       8                 |
  //                               +--<-- var --+-- ( <------'
  //                               |            |   7
  //                               '-->-- , -->-'
  //                                      9
  int state = 0;
  ParsedFunctorImpl *functor{nullptr};

  DisplayPosition next_pos;
  Token last_aggregate;
  Token last_summary;
  Token last_free;
  Token name;
  Token impure;

  Token param_binding;
  Token param_type;
  Token param_name;

  std::vector<std::tuple<Token, Token, Token>> params;

  unsigned num_bound_params = 0;
  unsigned num_free_params = 0;

  for (next_pos = tok.NextPosition(); ReadNextSubToken(tok);
       next_pos = tok.NextPosition()) {

    parsed_tokens.push_back(tok);

    const auto lexeme = tok.Lexeme();
    const auto tok_range = tok.SpellingRange();
    if (functor) {
      functor->last_tok = tok;
    }
    switch (state) {
      case 0:
        if (Lexeme::kIdentifierAtom == lexeme) {
          name = tok;
          state = 1;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected atom here (lower case identifier) for the name of "
              << "the functor being declared, got '" << tok << "' instead";
          return;
        }
      case 1:
        if (Lexeme::kPuncOpenParen == lexeme) {
          state = 2;
          continue;
        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected opening parenthesis here to begin parameter list of "
              << "functor '" << name << "', but got '" << tok << "' instead";
          return;
        }

      case 2:
        if (Lexeme::kKeywordBound == lexeme) {
          ++num_bound_params;
          param_binding = tok;
          state = 3;
          continue;

        } else if (Lexeme::kKeywordFree == lexeme) {
          ++num_free_params;
          param_binding = tok;
          last_free = tok;
          state = 3;
          continue;

        } else if (Lexeme::kKeywordAggregate == lexeme) {
          last_aggregate = tok;
          param_binding = tok;
          state = 3;
          continue;

        } else if (Lexeme::kKeywordSummary == lexeme) {
          last_summary = tok;
          param_binding = tok;
          state = 3;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected binding specifier ('bound', 'free', 'aggregate', "
              << "or 'summary') in parameter "
              << "declaration of functor '" << name << "', "
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
              << "Expected type name here for parameter in functor '" << name
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
              << "parameter name of functor '" << name << "', but got '" << tok
              << "' instead";
          return;
        }

      case 5:
        params.emplace_back(param_binding, param_type, param_name);
        if (params.size() == kMaxArity) {
          auto err = context->error_log.Append(
              scope_range, DisplayRange(param_binding.Position(),
                                        param_name.NextPosition()));
          err << "Too many parameters to #functor '" << name
              << "'; the maximum number of parameters is " << kMaxArity;
          return;
        }

        if (Lexeme::kPuncComma == lexeme) {

          param_binding = Token();
          param_type = Token();
          param_name = Token();

          state = 2;
          continue;

        } else if (Lexeme::kPuncCloseParen == lexeme) {
          functor = AddDecl<ParsedFunctorImpl>(
              module, DeclarationKind::kFunctor, name, params.size());

          if (!functor) {
            return;
          }

          functor->is_aggregate =
              last_aggregate.IsValid() || last_summary.IsValid();
          functor->rparen = tok;
          functor->directive_pos = sub_tokens.front().Position();
          functor->name = name;

          for (auto [binding, type, name] : params) {
            ParsedParameterImpl * const param = functor->parameters.Create();
            param->opt_binding = binding;
            param->opt_type = type;
            param->name = name;
            param->parsed_opt_type = type.IsValid();
          }

          state = 6;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected either a comma or a closing parenthesis here, "
              << "but got '" << tok << "' instead";
          return;
        }

      case 6:
        if (Lexeme::kPragmaPerfRange == lexeme) {
          if (functor->range_begin_opt.IsValid()) {
            auto err = context->error_log.Append(scope_range, tok_range);
            err << "Unexpected '@range' pragma here; functor " << name
                << " was already specified with a range";

            DisplayRange prev_range(functor->range_begin_opt.Position(),
                                    functor->range_end_opt.NextPosition());
            err.Note(scope_range, prev_range)
                << "Previous '@range' pragma was here";

            RemoveDecl(functor);
            return;

          } else {
            functor->range_begin_opt = tok;
            state = 7;
            continue;
          }

        } else if (Lexeme::kPragmaHintImpure == lexeme) {
          if (functor->is_pure) {
            impure = tok;
            functor->is_pure = false;
            state = 6;
            continue;

          } else {
            auto err = context->error_log.Append(scope_range, tok_range);
            err << "Unexpected '@impure' pragma here; functor " << name
                << " was already marked as impure";

            err.Note(scope_range, impure.SpellingRange())
                << "Previous '@impure' pragma was here";

            RemoveDecl(functor);
            return;
          }

        } else if (Lexeme::kPuncPeriod == lexeme) {
          functor->last_tok = tok;
          state = 10;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected either a terminating period or an "
              << "'@range' pragma or '@impure' pragma here, "
              << "but got '" << tok << "' instead";
          RemoveDecl(functor);
          return;
        }

      case 7:
        if (Lexeme::kPuncOpenParen == lexeme) {
          state = 8;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected an opening parenthesis here to begin 'range' "
              << "specification '" << tok << "' instead";
          RemoveDecl(functor);
          return;
        }

      case 8:

        // Infer functor range based on what was explicitly provided in the
        // syntax.
        if (Lexeme::kPuncPeriod == lexeme) {
          functor->range = FunctorRange::kOneToOne;

        } else if (Lexeme::kPuncStar == lexeme) {
          functor->range = FunctorRange::kZeroOrMore;

        } else if (Lexeme::kPuncQuestion == lexeme) {
          functor->range = FunctorRange::kZeroOrOne;

        } else if (Lexeme::kPuncPlus == lexeme) {
          functor->range = FunctorRange::kOneOrMore;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected a parameter variable name here but got '" << tok
              << "' instead";
          RemoveDecl(functor);
          return;
        }
        state = 9;
        continue;

      case 9:
        if (Lexeme::kPuncCloseParen == lexeme) {
          functor->range_end_opt = tok;
          state = 6;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected a closing parenthesis (to end range specifier) here,"
              << " but got '" << tok << "' instead";
          RemoveDecl(functor);
          return;
        }
      case 10: state = 11; break;
    }
  }

  functor->parsed_tokens.swap(parsed_tokens);

  if (state != 10) {
    context->error_log.Append(scope_range, next_pos)
        << "Incomplete functor declaration; the declaration must end "
        << "with a period";
    RemoveDecl(functor);
    return;
  }

  if (impure.IsValid()) {
    context->error_log.Append(scope_range, impure.SpellingRange())
        << "Impure functors are not yet supported.";
    RemoveDecl(functor);
    return;
  }

  const auto is_aggregate = last_summary.IsValid() || last_aggregate.IsValid();

  // If no explicit range syntax was provided, and this is a filter functor,
  // then change the default range behavior.
  if (functor->range_begin_opt.IsInvalid() && !num_free_params &&
      !is_aggregate) {
    functor->range = FunctorRange::kZeroOrOne;
  }

  DisplayRange range_spec(functor->range_begin_opt.Position(),
                          functor->range_end_opt.NextPosition());

  // Aggregating functors can't have range specifiers.
  if (is_aggregate && functor->range_begin_opt.IsValid()) {
    context->error_log.Append(scope_range, range_spec)
        << "Aggregating functors are not allowed to have range specifiers";
    RemoveDecl(functor);

  // Filter functors, i.e. functors taking in only bound parameters, must have
  // a zero-or-one range.
  } else if (!is_aggregate && !num_free_params &&
             functor->range != FunctorRange::kZeroOrOne) {
    context->error_log.Append(scope_range, range_spec)
        << "Invalid range specified on filter functor (having only bound "
        << "parameters); range must be 'range(?)`, i.e. zero-or-one";
    RemoveDecl(functor);

  // If we have a summary argument, then require us to have an aggregate
  // argument.
  } else if (last_summary.IsValid() && !last_aggregate.IsValid()) {
    context->error_log.Append(scope_range, last_summary.SpellingRange())
        << "Functor '" << functor->name << "' produces a summary value without "
        << "any corresponding aggregate inputs";
    RemoveDecl(functor);

  } else if (last_aggregate.IsValid() && !last_summary.IsValid()) {
    context->error_log.Append(scope_range, last_aggregate.SpellingRange())
        << "Functor '" << functor->name << "' aggregates values without "
        << "producing any corresponding summary outputs";
    RemoveDecl(functor);

  // Don't let us have both summary and free variables.
  //
  // NOTE(pag): We permit `bound` arguments to be used along with aggregates.
  } else if (last_summary.IsValid() && last_free.IsValid()) {
    auto err =
        context->error_log.Append(scope_range, last_summary.SpellingRange());
    err << "Functor cannot bind both summary and free variables";
    err.Note(last_free.SpellingRange()) << "Free variable is here";
    RemoveDecl(functor);

  // Aggregating functors aren't meant to be marked as impure. It's more that
  // they are implicitly impure so it's redundant.
  } else if (!functor->is_pure && is_aggregate) {

    context->error_log.Append(scope_range, impure.SpellingRange())
        << "Marking an aggregating functor as impure is redundant";
    RemoveDecl(functor);

  // A functor with no bound parameters cannot reasonably be supported.
  //
  // NOTE(pag): I had considered supporting it before as the concept of a
  //            "generator", e.g. for producing random values, but fitting
  //            it into a differential dataflow made no sense after all.
  } else if (!num_bound_params && !is_aggregate) {
    assert(0 < num_free_params);

    context->error_log.Append(scope_range)
        << "Functors that only have free-attributed parameters are not allowed";
    RemoveDecl(functor);

  // If this is a redeclaration, check it for consistency against prior
  // declarations. Functors require special handling for things like aggregate/
  // summary parameters.
  } else if (1 < functor->context->redeclarations.Size()) {

    const auto redecl = functor->context->redeclarations[0];
    DisplayRange redecl_range(redecl->parsed_tokens.front().Position(),
                              redecl->parsed_tokens.back().NextPosition());
    auto i = 0u;

    const auto arity = functor->parameters.Size();

    // Didn't match the purity.
    if (functor->is_pure && !redecl->is_pure) {
      auto err = context->error_log.Append(scope_range, tok.NextPosition());
      err << "Missing 'impure' attribute here to match with prior declaration "
          << "of functor '" << name << "/" << arity << "'";

      err.Note(redecl_range)
          << "Prior declaration of functor was here";
      RemoveDecl(functor);
      return;

    // Didn't match the purity.
    } else if (!functor->is_pure && redecl->is_pure) {
      auto err = context->error_log.Append(scope_range, impure.SpellingRange());
      err << "Unexpected 'impure' attribute here doesn't match with prior "
          << "declaration of functor '" << name << "/" << arity << "'";

      err.Note(redecl_range)
          << "Prior declaration of functor was here";
      RemoveDecl(functor);
      return;
    }

    // Make sure the binding specifiers all agree.
    for (ParsedParameterImpl *redecl_param : redecl->parameters) {
      const auto &orig_param = functor->parameters[i++];
      const auto lexeme = orig_param->opt_binding.Lexeme();
      const auto redecl_lexeme = redecl_param->opt_binding.Lexeme();

      // We can redeclare bound/free parameters with other variations of
      // bound/free, but the aggregation binding types must be equivalent.
      if (lexeme != redecl_lexeme && is_aggregate) {

        auto err = context->error_log.Append(
            scope_range, ParsedParameter(orig_param).SpellingRange());
        err << "Aggregation functor '" << functor->name << "/" << arity
            << "' cannot be re-declared with different parameter attributes";

        auto note =
            err.Note(ParsedDeclaration(redecl).SpellingRange(),
                     ParsedParameter(redecl_param).SpellingRange());
        note << "Conflicting parameter is declared here";

        RemoveDecl(functor);
        return;
      }
    }

    // Do generic consistency checking.
    FinalizeDeclAndCheckConsistency(functor);

  // This is the first time we're adding the functor.
  } else {
    module->functors.AddUse(functor);
  }
}


}  // namespace hyde
