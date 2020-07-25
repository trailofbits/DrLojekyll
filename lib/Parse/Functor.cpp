// Copyright 2020, Trail of Bits. All rights reserved.

#include "Parser.h"

namespace hyde {

// Try to parse `sub_range` as a functor, adding it to `module` if successful.
void ParserImpl::ParseFunctor(Node<ParsedModule> *module) {
  Token tok;
  if (!ReadNextSubToken(tok)) {
    assert(false);
  }

  assert(tok.Lexeme() == Lexeme::kHashFunctorDecl);

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
  std::unique_ptr<Node<ParsedFunctor>> functor;
  std::unique_ptr<Node<ParsedParameter>> param;
  std::vector<std::unique_ptr<Node<ParsedParameter>>> params;

  DisplayPosition next_pos;
  Token last_aggregate;
  Token last_summary;
  Token last_free;
  Token name;
  Token impure;

  unsigned num_bound_params = 0;
  unsigned num_free_params = 0;

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
          param.reset(new Node<ParsedParameter>);
          param->opt_binding = tok;
          state = 3;
          continue;

        } else if (Lexeme::kKeywordFree == lexeme) {
          ++num_free_params;
          last_free = tok;
          param.reset(new Node<ParsedParameter>);
          param->opt_binding = tok;
          state = 3;
          continue;

        } else if (Lexeme::kKeywordAggregate == lexeme) {
          last_aggregate = tok;
          param.reset(new Node<ParsedParameter>);
          param->opt_binding = tok;
          state = 3;
          continue;

        } else if (Lexeme::kKeywordSummary == lexeme) {
          last_summary = tok;
          param.reset(new Node<ParsedParameter>);
          param->opt_binding = tok;
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
          param->opt_type = tok;
          param->parsed_opt_type = true;
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
          param->name = tok;
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

        // Add the parameter in.
        if (!params.empty()) {
          params.back()->next = param.get();

          if (params.size() == kMaxArity) {
            auto err = context->error_log.Append(
                scope_range, ParsedParameter(param.get()).SpellingRange());
            err << "Too many parameters to #functor '" << name
                << "'; the maximum number of parameters is " << kMaxArity;
            return;
          }
        }

        param->index = static_cast<unsigned>(params.size());
        params.emplace_back(std::move(param));

        if (Lexeme::kPuncComma == lexeme) {
          state = 2;
          continue;

        } else if (Lexeme::kPuncCloseParen == lexeme) {
          functor.reset(AddDecl<ParsedFunctor>(
              module, DeclarationKind::kFunctor, name, params.size()));

          if (!functor) {
            return;
          } else {
            functor->is_aggregate =
                last_aggregate.IsValid() || last_summary.IsValid();
            functor->rparen = tok;
            functor->directive_pos = sub_tokens.front().Position();
            functor->name = name;
            functor->parameters.swap(params);
            state = 6;
            continue;
          }

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected either a comma or a closing parenthesis here, "
              << "but got '" << tok << "' instead";
          return;
        }

      case 6:
        if (Lexeme::kKeywordUnordered == lexeme) {
          functor->unordered_sets.emplace_back();
          auto &uset = functor->unordered_sets.back();
          uset.begin = tok;
          uset.end = tok;
          state = 7;
          continue;

        } else if (Lexeme::kKeywordImpure == lexeme) {
          if (functor->is_pure) {
            impure = tok;
            functor->is_pure = false;
            state = 6;
            continue;

          } else {
            auto err = context->error_log.Append(scope_range, tok_range);
            err << "Unexpected 'impure' attribute here; functor " << name
                << " was already marked as impure";

            err.Note(scope_range, impure.SpellingRange())
                << "Previous 'impure' attribute was here";

            RemoveDecl<ParsedFunctor>(std::move(functor));
            return;
          }

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected 'unordered' attribute here, "
              << "but got '" << tok << "' instead";
          RemoveDecl<ParsedFunctor>(std::move(functor));
          return;
        }

      case 7:
        if (Lexeme::kPuncOpenParen == lexeme) {
          assert(!functor->unordered_sets.empty());
          auto &uset = functor->unordered_sets.back();
          uset.end = tok;

          state = 8;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected an opening parenthesis here to begin 'unordered' set"
              << " but got '" << tok << "' instead";
          RemoveDecl<ParsedFunctor>(std::move(functor));
          return;
        }

      case 8:
        if (Lexeme::kIdentifierVariable == lexeme) {
          assert(!functor->unordered_sets.empty());
          auto &uset = functor->unordered_sets.back();
          uset.end = tok;
          auto found = false;
          for (const auto &func_param : functor->parameters) {
            if (func_param->name.IdentifierId() != tok.IdentifierId()) {
              continue;
            }

            if (func_param->opt_binding.Lexeme() != Lexeme::kKeywordBound) {
              context->error_log.Append(scope_range, tok_range)
                  << "Variable '" << tok
                  << "' specified in unordered set is not a 'bound'-attributed "
                  << "parameter of this #functor";
              RemoveDecl<ParsedFunctor>(std::move(functor));
              return;
            }

            if (func_param->opt_unordered_name.IsValid()) {
              auto err = context->error_log.Append(scope_range, tok_range);
              err << "Parameter variable '" << tok
                  << "' cannot belong to more than one unordered sets";

              err.Note(scope_range,
                       func_param->opt_unordered_name.SpellingRange())
                  << "Previous use in an unordered set was here";

              RemoveDecl<ParsedFunctor>(std::move(functor));
              return;
            }

            func_param->opt_unordered_name = tok;
            if (!uset.params.empty()) {
              uset.params.back()->next_unordered = func_param.get();
            }
            uset.params.push_back(func_param.get());
            uset.mask |= 1ull << func_param->index;
            found = true;
            break;
          }

          if (!found) {
            context->error_log.Append(scope_range, tok_range)
                << "Variable '" << tok
                << "' specified in unordered set is not a parameter "
                << "variable of this #functor";
            RemoveDecl<ParsedFunctor>(std::move(functor));
            return;

          } else {
            state = 9;
            continue;
          }

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected a parameter variable name here but got '" << tok
              << "' instead";
          RemoveDecl<ParsedFunctor>(std::move(functor));
          return;
        }

      case 9:
        if (Lexeme::kPuncComma == lexeme) {
          assert(!functor->unordered_sets.empty());
          auto &uset = functor->unordered_sets.back();
          uset.end = tok;
          state = 8;
          continue;

        } else if (Lexeme::kPuncCloseParen == lexeme) {
          assert(!functor->unordered_sets.empty());
          auto &uset = functor->unordered_sets.back();
          uset.end = tok;

          if (2 > uset.params.size()) {
            context->error_log.Append(scope_range, uset.SpellingRange())
                << "Unordered set specification must list at least two "
                << "parameter variables";
            RemoveDecl<ParsedFunctor>(std::move(functor));
            return;
          }

          functor->rparen = tok;
          state = 6;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected either a comma (to continue unordered set) or a "
              << "closing parenthesis (to end undordered set) here, "
              << "but got '" << tok << "' instead";
          RemoveDecl<ParsedFunctor>(std::move(functor));
          return;
        }
    }
  }

  const auto is_aggregate = last_summary.IsValid() || last_aggregate.IsValid();

  if (state != 6 && state != 13) {
    context->error_log.Append(scope_range, next_pos)
        << "Incomplete functor declaration; the declaration must be "
        << "placed entirely on one line";
    RemoveDecl<ParsedFunctor>(std::move(functor));

  // If we have a summary argument, then require us to have an aggregate
  // argument.
  } else if (last_summary.IsValid() && !last_aggregate.IsValid()) {
    context->error_log.Append(scope_range, last_summary.SpellingRange())
        << "Functor '" << functor->name << "' produces a summary value without "
        << "any corresponding aggregate inputs";
    RemoveDecl<ParsedFunctor>(std::move(functor));

  } else if (last_aggregate.IsValid() && !last_summary.IsValid()) {
    context->error_log.Append(scope_range, last_aggregate.SpellingRange())
        << "Functor '" << functor->name << "' aggregates values without "
        << "producing any corresponding summary outputs";
    RemoveDecl<ParsedFunctor>(std::move(functor));

  // Don't let us have both summary and free variables.
  //
  // NOTE(pag): We permit `bound` arguments to be used along with aggregates.
  } else if (last_summary.IsValid() && last_free.IsValid()) {
    auto err =
        context->error_log.Append(scope_range, last_summary.SpellingRange());
    err << "Functor cannot bind both summary and free variables";
    err.Note(last_free.SpellingRange()) << "Free variable is here";
    RemoveDecl<ParsedFunctor>(std::move(functor));

  // Aggregating functors aren't meant to be marked as impure. It's more that
  // they are implicitly impure so it's redundant.
  } else if (!functor->is_pure && is_aggregate) {

    context->error_log.Append(scope_range, impure.SpellingRange())
        << "Marking an aggregating functor as impure is redundant";
    RemoveDecl<ParsedFunctor>(std::move(functor));

  // A functor with no bound parameters cannot reasonably be supported.
  //
  // NOTE(pag): I had considered supporting it before as the concept of a
  //            "generator", e.g. for producing random values, but fitting
  //            it into a differential dataflow made no sense after all.
  } else if (!num_bound_params && !is_aggregate) {
    assert(0 < num_free_params);

    context->error_log.Append(scope_range)
        << "Functors that only have free-attributed parameters are not allowed";
    RemoveDecl<ParsedFunctor>(std::move(functor));

  // If this is a redeclaration, check it for consistency against prior
  // declarations. Functors require special handling for things like aggregate/
  // summary parameters.
  } else if (1 < functor->context->redeclarations.size()) {

    const auto redecl = functor->context->redeclarations[0];
    auto i = 0u;

    const auto arity = functor->parameters.size();

    // Didn't match the purity.
    if (functor->is_pure && !redecl->is_pure) {
      auto err = context->error_log.Append(scope_range, tok.NextPosition());
      err << "Missing 'impure' attribute here to match with prior declaration "
          << "of functor '" << name << "/" << arity << "'";

      err.Note(ParsedDeclaration(redecl).SpellingRange())
          << "Prior declaration of functor was here";
      RemoveDecl<ParsedFunctor>(std::move(functor));
      return;

    // Didn't match the purity.
    } else if (!functor->is_pure && redecl->is_pure) {
      auto err = context->error_log.Append(scope_range, impure.SpellingRange());
      err << "Unexpected 'impure' attribute here doesn't match with prior "
          << "declaration of functor '" << name << "/" << arity << "'";

      err.Note(ParsedDeclaration(redecl).SpellingRange())
          << "Prior declaration of functor was here";
      RemoveDecl<ParsedFunctor>(std::move(functor));
      return;
    }

    // Make sure the binding specifiers all agree.
    for (auto &redecl_param : redecl->parameters) {
      const auto &orig_param = functor->parameters[i++];
      const auto lexeme = orig_param->opt_binding.Lexeme();
      const auto redecl_lexeme = redecl_param->opt_binding.Lexeme();

      // We can redeclare bound/free parameters with other variations of
      // bound/free, but the aggregation binding types must be equivalent.
      if (lexeme != redecl_lexeme && is_aggregate) {

        auto err = context->error_log.Append(
            scope_range, ParsedParameter(orig_param.get()).SpellingRange());
        err << "Aggregation functor '" << functor->name << "/" << arity
            << "' cannot be re-declared with different parameter attributes";

        auto note =
            err.Note(ParsedDeclaration(redecl).SpellingRange(),
                     ParsedParameter(redecl_param.get()).SpellingRange());
        note << "Conflicting parameter is declared here";

        RemoveDecl<ParsedFunctor>(std::move(functor));
        return;
      }
    }

    // Make sure both have the same number of unordered sets.
    const auto num_sets = redecl->unordered_sets.size();
    if (num_sets != functor->unordered_sets.size()) {
      auto err = context->error_log.Append(scope_range);
      err << "Mismatch between the number of unordered parameter sets "
          << "specified for functor '" << name << "/" << arity
          << "' has different number of unordered parameter sets specified";

      err.Note(ParsedDeclaration(redecl).SpellingRange())
          << "Conflicting functor declaration is here";

      RemoveDecl<ParsedFunctor>(std::move(functor));
      return;
    }

    // Check the unordered sets.
    for (auto u = 0u; u < num_sets; ++u) {
      auto &uset = functor->unordered_sets[u];
      auto &redecl_uset = redecl->unordered_sets[u];
      if (uset.mask == redecl_uset.mask) {
        continue;
      }

      auto err = context->error_log.Append(scope_range, uset.SpellingRange());
      err << "Parameter variables covered by this unordered set doesn't "
          << "match the corresponding unordered set specification in the "
          << "first declaration of this #functor";

      err.Note(ParsedDeclaration(redecl).SpellingRange(),
               redecl_uset.SpellingRange())
          << "Corresponding unordered set specification is here";

      RemoveDecl<ParsedFunctor>(std::move(functor));
      return;
    }

    // Do generic consistency checking.
    FinalizeDeclAndCheckConsistency<ParsedFunctor>(module->functors,
                                                   std::move(functor));

  } else {
    if (!module->functors.empty()) {
      module->functors.back()->next = functor.get();
    }
    module->functors.emplace_back(std::move(functor));
  }
}


}  // namespace hyde
