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
              << "the functor being declared, got '" << tok << "' instead";
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
              << "functor '" << name << "', but got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
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
          Error err(context->display_manager, sub_tok_range,
                    tok.SpellingRange());
          err << "Expected binding specifier ('bound', 'free', 'aggregate', "
              << "or 'summary') in parameter "
              << "declaration of functor '" << name << "', " << "but got '"
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
          err << "Expected type name here for parameter in functor '"
              << name << "', but got '"
              << tok << "' instead";
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
              << "parameter name of functor '" << name << "', but got '"
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
            err << "Too many parameters to #functor '" << name
                << "'; the maximum number of parameters is " << kMaxArity;
            context->error_log.Append(std::move(err));
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
            functor->is_aggregate = last_aggregate.IsValid() ||
                                    last_summary.IsValid();
            functor->rparen = tok;
            functor->directive_pos = sub_tokens.front().Position();
            functor->name = name;
            functor->parameters.swap(params);
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
            Error err(context->display_manager, sub_tok_range,
                      tok.SpellingRange());
            err << "Unexpected 'impure' attribute here; functor "
                << name << " was already marked as impure";

            auto note = err.Note(context->display_manager,
                                 sub_tok_range, impure.SpellingRange());
            note << "Previous 'impure' attribute was here";

            context->error_log.Append(std::move(err));
            RemoveDecl<ParsedFunctor>(std::move(functor));
            return;
          }

        } else {
          Error err(context->display_manager, sub_tok_range,
                    tok.SpellingRange());
          err << "Expected 'unordered' attribute here, "
              << "but got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
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
          Error err(context->display_manager, sub_tok_range,
                    tok.SpellingRange());
          err << "Expected an opening parenthesis here to begin 'unordered' set"
              << " but got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
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
              Error err(context->display_manager, sub_tok_range,
                        tok.SpellingRange());
              err << "Variable '" << tok
                  << "' specified in unordered set is not a 'bound'-attributed "
                  << "parameter of this #functor";
              context->error_log.Append(std::move(err));
              RemoveDecl<ParsedFunctor>(std::move(functor));
              return;
            }

            if (func_param->opt_unordered_name.IsValid()) {
              Error err(context->display_manager, sub_tok_range,
                        tok.SpellingRange());
              err << "Parameter variable '" << tok
                  << "' cannot belong to more than one unordered sets";

              auto note = err.Note(context->display_manager, sub_tok_range,
                                   func_param->opt_unordered_name.SpellingRange());
              note << "Previous use in an unordered set was here";

              context->error_log.Append(std::move(err));
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
            Error err(context->display_manager, sub_tok_range,
                      tok.SpellingRange());
            err << "Variable '" << tok
                << "' specified in unordered set is not a parameter "
                << "variable of this #functor";
            context->error_log.Append(std::move(err));
            RemoveDecl<ParsedFunctor>(std::move(functor));
            return;

          } else {
            state = 9;
            continue;
          }

        } else {
          Error err(context->display_manager, sub_tok_range,
                    tok.SpellingRange());
          err << "Expected a parameter variable name here but got '"
              << tok << "' instead";
          context->error_log.Append(std::move(err));
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
            Error err(context->display_manager, sub_tok_range,
                      uset.SpellingRange());
            err << "Unordered set specification must list at least two "
                << "parameter variables";
            context->error_log.Append(std::move(err));
            RemoveDecl<ParsedFunctor>(std::move(functor));
            return;
          }

          functor->rparen = tok;
          state = 6;
          continue;

        } else {
          Error err(context->display_manager, sub_tok_range,
                    tok.SpellingRange());
          err << "Expected either a comma (to continue unordered set) or a "
              << "closing parenthesis (to end undordered set) here, "
              << "but got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          RemoveDecl<ParsedFunctor>(std::move(functor));
          return;
        }
    }
  }

  if (state != 6 && state != 13) {
    Error err(context->display_manager, sub_tok_range, next_pos);
    err << "Incomplete functor declaration; the declaration must be "
        << "placed entirely on one line";
    context->error_log.Append(std::move(err));
    RemoveDecl<ParsedFunctor>(std::move(functor));

  // If we have a summary argument, then require us to have an aggregate
  // argument.
  } else if (last_summary.IsValid() && !last_aggregate.IsValid()) {
    Error err(context->display_manager, sub_tok_range,
              last_summary.SpellingRange());
    err << "Functor '" << functor->name << "' produces a summary value without "
        << "any corresponding aggregate inputs";
    context->error_log.Append(std::move(err));
    RemoveDecl<ParsedFunctor>(std::move(functor));

  } else if (last_aggregate.IsValid() && !last_summary.IsValid()) {
    Error err(context->display_manager, sub_tok_range,
              last_aggregate.SpellingRange());
    err << "Functor '" << functor->name << "' aggregates values without "
        << "producing any corresponding summary outputs";
    context->error_log.Append(std::move(err));
    RemoveDecl<ParsedFunctor>(std::move(functor));

  // Don't let us have both summary and free variables.
  //
  // NOTE(pag): We permit `bound` arguments to be used along with aggregates.
  } else if (last_summary.IsValid() && last_free.IsValid()) {
    Error err(context->display_manager, sub_tok_range,
              last_summary.SpellingRange());
    err << "Functor cannot bind both summary and free variables";
    auto note = err.Note(context->display_manager, sub_tok_range,
                         last_free.SpellingRange());
    note << "Free variable is here";
    context->error_log.Append(std::move(err));
    RemoveDecl<ParsedFunctor>(std::move(functor));

  // Aggregating functors aren't meant to be marked as impure. It's more that
  // they are implicitly impure so it's redundant.
  } else if (!functor->is_pure &&
             (last_summary.IsValid() || last_aggregate.IsValid())) {

    Error err(context->display_manager, sub_tok_range,
              impure.SpellingRange());
    err << "Marking an aggregating functor as impure is redundant";
    context->error_log.Append(std::move(err));
    RemoveDecl<ParsedFunctor>(std::move(functor));

  // A functor with no bound parameters is just a generator. They are by default
  // treated as impure.
  //
  // TODO(pag): Consider making marking generating functors as impure a
  //            requirement, rather than an error.
  } else if (!functor->is_pure && !num_bound_params) {

    Error err(context->display_manager, sub_tok_range,
              impure.SpellingRange());
    err << "Marking a functor with no bound parameters as impure is redundant";
    context->error_log.Append(std::move(err));
    RemoveDecl<ParsedFunctor>(std::move(functor));

  // If this is a redeclaration, check it for consistency against prior
  // declarations. Functors require special handling for things like aggregate/
  // summary parameters.
  } else if (1 < functor->context->redeclarations.size()) {

    const auto redecl = functor->context->redeclarations[0];
    auto i = 0u;

    // Didn't match the purity.
    if (functor->is_pure && !redecl->is_pure) {
      Error err(context->display_manager, sub_tok_range,
                tok.NextPosition());
      err << "Missing 'impure' attribute here to match with prior declaration "
          << "of functor " << name;

      auto note = err.Note(context->display_manager,
                           ParsedDeclaration(redecl).SpellingRange());
      note << "Prior declaration of functor was here";
      context->error_log.Append(std::move(err));
      RemoveDecl<ParsedFunctor>(std::move(functor));
      return;

    // Didn't match the purity.
    } else {
      Error err(context->display_manager, sub_tok_range,
                impure.SpellingRange());
      err << "Unexpected 'impure' attribute here doesn't match with prior "
          << "declaration of functor " << name;

      auto note = err.Note(context->display_manager,
                           ParsedDeclaration(redecl).SpellingRange());
      note << "Prior declaration of functor was here";
      context->error_log.Append(std::move(err));
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
      if (lexeme != redecl_lexeme &&
          ((lexeme == Lexeme::kKeywordAggregate ||
            lexeme == Lexeme::kKeywordSummary) ||
           (redecl_lexeme == Lexeme::kKeywordAggregate ||
            redecl_lexeme == Lexeme::kKeywordSummary))) {

        Error err(context->display_manager, sub_tok_range,
                  ParsedParameter(orig_param.get()).SpellingRange());
        err << "Aggregation functor '" << functor->name
            << "' cannot be re-declared with different aggregation semantics";

        auto note = err.Note(
            context->display_manager,
            ParsedDeclaration(redecl).SpellingRange(),
            ParsedParameter(redecl_param.get()).SpellingRange());
        note << "Conflicting aggregation parameter is specified here";

        context->error_log.Append(std::move(err));
        RemoveDecl<ParsedFunctor>(std::move(functor));
        return;
      }
    }

    // Make sure both have the same number of unordered sets.
    const auto num_sets = redecl->unordered_sets.size();
    if (num_sets != functor->unordered_sets.size()) {
      Error err(context->display_manager, sub_tok_range);
      err << "Mismatch between the number of unordered parameter sets "
          << "specified for #functor '" << functor->name
          << "' has different number of unordered parameter sets specified";

      auto note = err.Note(
          context->display_manager,
          ParsedDeclaration(redecl).SpellingRange());
      note << "Conflicting functor declaration is here";

      context->error_log.Append(std::move(err));
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

      Error err(context->display_manager, sub_tok_range,
                uset.SpellingRange());
      err << "Parameter variables covered by this unordered set doesn't "
          << "match the corresponding unordered set specification in the "
          << "first declaration of this #functor";

      auto note1 = err.Note(
          context->display_manager,
          ParsedDeclaration(redecl).SpellingRange(),
          redecl_uset.SpellingRange());
      note1 << "Corresponding unordered set specification is here";

      context->error_log.Append(std::move(err));
      RemoveDecl<ParsedFunctor>(std::move(functor));
      return;
    }

    // Do generic consistency checking.
    AddDeclAndCheckConsistency<ParsedFunctor>(
        module->functors, std::move(functor));

  } else {
    if (!module->functors.empty()) {
      module->functors.back()->next = functor.get();
    }
    module->functors.emplace_back(std::move(functor));
  }
}


}  // namespace hyde
