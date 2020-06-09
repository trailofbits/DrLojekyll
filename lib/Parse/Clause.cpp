// Copyright 2020, Trail of Bits. All rights reserved.

#include "Parser.h"

namespace hyde {

// Try to parse `sub_range` as a clause.
void ParserImpl::ParseClause(Node<ParsedModule> *module, Token negation_tok,
                             Node<ParsedDeclaration> *decl) {

  auto clause = std::make_unique<Node<ParsedClause>>(module);
  prev_named_var.clear();

  Token tok;
  int state = 0;

  // Approximate state transition diagram for parsing clauses.
  //
  //               .--------<-------.
  //               |                |                      .-> var -->--.
  // -- atom -> ( -+-> var -+-> , --'       .-> var --> = -+           +-->---.
  //                        |               |              '-> literal -'      |
  //                        '-> ) ---> : -+-+                                  |
  //                                      | |                                  |
  //                                      | +------+-> atom -> ( -+-> var -+-. |
  //                                      | '-> ! -'-<-------.    '--- , <-' | |
  //                       .------->------'        .-> over -'               | |
  //                       |                       |                         | |
  //                       '-- , <--+-----+--------+--- ) <------------------' |
  //                                |     '------------------------------------'
  //                           . <--'
  //
  DisplayPosition next_pos;
  DisplayPosition negation_pos;
  Node<ParsedVariable> *arg = nullptr;
  Node<ParsedVariable> *lhs = nullptr;
  Node<ParsedVariable> *rhs = nullptr;
  Token compare_op;
  std::unique_ptr<Node<ParsedPredicate>> pred;
  bool equates_parameters = false;

  // Link `pred` into `clause`.
  auto link_pred = [&] (void) {
    if (negation_pos.IsValid()) {
      if (!clause->negated_predicates.empty()) {
        clause->negated_predicates.back()->next = pred.get();
      }
      clause->negated_predicates.emplace_back(std::move(pred));
    } else {
      if (!clause->positive_predicates.empty()) {
        clause->positive_predicates.back()->next = pred.get();
      }
      clause->positive_predicates.emplace_back(std::move(pred));
    }
  };

  for (next_pos = tok.NextPosition();
       ReadNextSubToken(tok);
       next_pos = tok.NextPosition()) {

    const auto lexeme = tok.Lexeme();
    switch (state) {
      case 0:
        if (Lexeme::kIdentifierAtom == lexeme ||
            Lexeme::kIdentifierUnnamedAtom == lexeme) {
          clause->name = tok;
          state = 1;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected atom here (lower case identifier) for the name of "
              << "the clause head being declared, got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 1:
        if (Lexeme::kPuncOpenParen == lexeme) {
          state = 2;
          continue;

        } else if (Lexeme::kPuncColon == lexeme) {
          if (!TryMatchClauseWithDecl(module, clause.get())) {
            return;

          } else {
            decl = clause->declaration;
            state = 5;
            continue;
          }

          state = 5;
          continue;

        // TODO(pag): Support `foo.` syntax? Could be an intersting way to
        //            turn on/off options.

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected opening parenthesis here to begin parameter list of "
              << "clause head '" << clause->name << "', or a colon for a zero-"
              << "arity predicate, but got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 2:
        if (Lexeme::kIdentifierVariable == lexeme) {

          // Check to see if the same variable comes up as another parameter.
          Node<ParsedVariable> *prev_var = nullptr;
          for (const auto &head_var : clause->head_variables) {
            if (head_var->name.IdentifierId() == tok.IdentifierId()) {
              prev_var = head_var.get();
              break;
            }
          }

          if (!prev_var) {
            (void) CreateVariable(clause.get(), tok, true, false);

          // Something like `foo(A, A)`, which we want to translate into
          // `foo(A, V123) : A=V123, ...`.
          } else {
            equates_parameters = true;

            const auto new_tok = Token::Synthetic(
                Lexeme::kIdentifierUnnamedVariable, tok.SpellingRange());
            const auto dup_var = CreateVariable(
                clause.get(), new_tok, true, false);

            const auto compare = new Node<ParsedComparison>(
                prev_var, dup_var,
                Token::Synthetic(Lexeme::kPuncEqual, tok.SpellingRange()));

            // Add to the LHS variable's comparison use list.
            auto &lhs_comparison_uses = prev_var->context->comparison_uses;
            if (!lhs_comparison_uses.empty()) {
              lhs_comparison_uses.back()->next = &(compare->lhs);
            }
            lhs_comparison_uses.push_back(&(compare->lhs));

            // Add to the RHS variable's comparison use list.
            auto &rhs_comparison_uses = dup_var->context->comparison_uses;
            if (!rhs_comparison_uses.empty()) {
              rhs_comparison_uses.back()->next = &(compare->rhs);
            }
            rhs_comparison_uses.push_back(&(compare->rhs));

            // Add to the clause's comparison list.
            if (!clause->comparisons.empty()) {
              clause->comparisons.back()->next = compare;
            }
            clause->comparisons.emplace_back(compare);
          }
          state = 3;
          continue;

        // Support something like `foo(1, ...) : ...`, converting it into
        // `foo(V, ...) : V=1, ...`.
        } else if (Lexeme::kLiteralString == lexeme ||
                   Lexeme::kLiteralNumber == lexeme) {
          (void) CreateLiteralVariable(
              clause.get(), tok, true, false);
          state = 3;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected variable name (capitalized identifier) for "
              << "parameter in clause '" << clause->name << "', but got '"
              << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 3:
        if (Lexeme::kPuncComma == lexeme) {
          state = 2;
          continue;

        } else if (Lexeme::kPuncCloseParen == lexeme) {
          clause->rparen = tok;

          if (decl) {
            clause->declaration = decl;
            state = 4;
            continue;

          } else if (!TryMatchClauseWithDecl(module, clause.get())) {
            return;

          } else {
            decl = clause->declaration;
            state = 4;
            continue;
          }

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected comma (to continue parameter list) or closing "
              << "parenthesis (to end paramater list) for clause head '"
              << clause->name << "', but got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 4:
        if (Lexeme::kPuncColon == lexeme) {
          state = 5;
          continue;

        } else if (Lexeme::kPuncPeriod == lexeme) {
          clause->dot = tok;
          state = 9;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected colon to denote the beginning of the body "
              << "of the clause '" << clause->name << "', but got '"
              << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 5:
        if (Lexeme::kIdentifierVariable == lexeme) {
          lhs = CreateVariable(clause.get(), tok, false, false);
          state = 6;
          continue;

        } else if (Lexeme::kLiteralString == lexeme ||
                   Lexeme::kLiteralNumber == lexeme) {
          lhs = CreateLiteralVariable(clause.get(), tok, false, false);
          state = 6;
          continue;

        } else if (Lexeme::kPuncExclaim == lexeme) {
          negation_pos = tok.Position();
          state = 11;
          continue;

        } else if (Lexeme::kIdentifierAtom == lexeme) {
          pred.reset(new Node<ParsedPredicate>(module, clause.get()));
          pred->name = tok;
          state = 12;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected variable name, atom, or exclamation point, but got '"
              << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 6:
        if (Lexeme::kPuncEqual == lexeme ||
            Lexeme::kPuncNotEqual == lexeme ||
            Lexeme::kPuncLess == lexeme ||
            Lexeme::kPuncGreater == lexeme) {
          compare_op = tok;
          state = 7;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected comparison operator, but got '" << tok
              << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 7:
        rhs = nullptr;

        // Allow comparisons with literals by converting the literals into
        // variables and assigning values to those variables.
        if (Lexeme::kLiteralString == lexeme ||
            Lexeme::kLiteralNumber == lexeme) {

          // If we're doing `<var> = <literal>` then we don't want to explode
          // it into `<temp> = literal, <var> = <temp>`.
          if (Lexeme::kPuncEqual == compare_op.Lexeme()) {
            auto assign = new Node<ParsedAssignment>(lhs);
            assign->rhs.literal = tok;
            assign->rhs.assigned_to = lhs;
            std::string_view data;
            if (context->display_manager.TryReadData(tok.SpellingRange(), &data)) {
              assert(!data.empty());
              assign->rhs.data = data;
            } else {
              assert(false);
            }

            // Add to the clause's assignment list.
            if (!clause->assignments.empty()) {
              clause->assignments.back()->next = assign;
            }
            clause->assignments.emplace_back(assign);

            // Add to the variable's assignment list. We support the list, but for
            // these auto-created variables, there can be only one use.
            lhs->context->assignment_uses.push_back(&(assign->lhs));
            state = 8;
            continue;

          } else {
            rhs = CreateLiteralVariable(clause.get(), tok, false, false);
          }

        } else if (Lexeme::kIdentifierVariable == lexeme) {
          rhs = CreateVariable(clause.get(), tok, false, false);
        }

        if (rhs) {
          // Don't allow comparisons against the same named variable. This
          // simplifies later checks, and makes sure that iteration over the
          // comparisons containing a given variable are well-founded.
          if (ParsedVariable(lhs).Id() == ParsedVariable(rhs).Id()) {
            Error err(context->display_manager, SubTokenRange(),
                      DisplayRange(lhs->name.Position(),
                                   rhs->name.NextPosition()));
            err << "Variable '" << lhs->name
                << "' cannot appear on both sides of a comparison";
            context->error_log.Append(std::move(err));
            return;
          }

          const auto compare = new Node<ParsedComparison>(
              lhs, rhs, compare_op);

          // Add to the LHS variable's comparison use list.
          auto &lhs_comparison_uses = lhs->context->comparison_uses;
          if (!lhs_comparison_uses.empty()) {
            lhs_comparison_uses.back()->next = &(compare->lhs);
          }
          lhs_comparison_uses.push_back(&(compare->lhs));

          // Add to the RHS variable's comparison use list.
          auto &rhs_comparison_uses = rhs->context->comparison_uses;
          if (!rhs_comparison_uses.empty()) {
            rhs_comparison_uses.back()->next = &(compare->rhs);
          }
          rhs_comparison_uses.push_back(&(compare->rhs));

          // Add to the clause's comparison list.
          if (!clause->comparisons.empty()) {
            clause->comparisons.back()->next = compare;
          }
          clause->comparisons.emplace_back(compare);

          state = 8;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected variable name or number/string literal, but got '"
              << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 8:
        pred.reset();
        if (Lexeme::kPuncComma == lexeme) {
          state = 5;
          continue;

        } else if (Lexeme::kPuncPeriod == lexeme) {
          clause->dot = tok;
          state = 9;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected comma or period, but got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 9: {
        DisplayRange err_range(
            tok.Position(), sub_tokens.back().NextPosition());
        Error err(context->display_manager, SubTokenRange(), err_range);
        err << "Unexpected tokens following clause '" << clause->name << "'";
        context->error_log.Append(std::move(err));
        state = 10;  // Ignore further errors, but add the local in.
        continue;
      }

      // We're just chugging tokens at the end, ignore them.
      case 10:
        continue;

      // We think we're parsing a negated predicate.
      case 11:
        if (Lexeme::kIdentifierAtom == lexeme) {
          pred.reset(new Node<ParsedPredicate>(module, clause.get()));
          pred->name = tok;
          pred->negation_pos = negation_pos;
          state = 12;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected atom here for negated predicate, but got '"
              << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 12:
        if (Lexeme::kPuncOpenParen == lexeme) {
          state = 13;
          continue;

        } else if (Lexeme::kPuncPeriod == lexeme) {
          if (!TryMatchPredicateWithDecl(module, pred.get())) {
            return;
          }
          state = 9;
          clause->dot = tok;
          link_pred();

          continue;

        } else if (Lexeme::kPuncComma == lexeme) {
          if (!TryMatchPredicateWithDecl(module, pred.get())) {
            return;
          }
          state = 5;
          link_pred();
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected an opening parenthesis, comma, or period here to"
              << "test predicate '" << pred->name << "', but got '" << tok
              << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 13:
        arg = nullptr;

        // Convert literals into variables, just-in-time.
        if (Lexeme::kLiteralString == lexeme ||
            Lexeme::kLiteralNumber == lexeme) {
          arg = CreateLiteralVariable(clause.get(), tok, false, true);

        } else if (Lexeme::kIdentifierVariable == lexeme ||
                   Lexeme::kIdentifierUnnamedVariable == lexeme) {
          arg = CreateVariable(clause.get(), tok, false, true);
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

          state = 14;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected variable or literal here as argument to predicate '"
              << pred->name << "', but got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 14:
        if (Lexeme::kPuncCloseParen == lexeme) {
          pred->rparen = tok;

          if (!TryMatchPredicateWithDecl(module, pred.get())) {
            return;
          }

          // Not allowed to negate inline declarations, as they might not be
          // backed by actual relations.
          if (pred->negation_pos.IsValid() &&
              pred->declaration->inline_attribute.IsValid()) {
            Error err(context->display_manager, SubTokenRange(),
                      ParsedPredicate(pred.get()).SpellingRange());
            err << "Cannot negate " << pred->declaration->KindName()
                << " '" << pred->name
                << "' because it has been marked as inline";

            auto note = err.Note(
                context->display_manager,
                ParsedDeclaration(pred->declaration).SpellingRange(),
                pred->declaration->inline_attribute.SpellingRange());
            note << "Marked as inline here";

            context->error_log.Append(std::move(err));
            return;
          }

          // If it's an aggregating functor then we need to follow-up with
          // the `over` keyword.
          auto pred_decl = ParsedDeclaration::Of(ParsedPredicate(pred.get()));
          if (pred_decl.IsFunctor() &&
              ParsedFunctor::From(pred_decl).IsAggregate()) {

            if (pred->negation_pos.IsValid()) {
              Error err(context->display_manager, SubTokenRange(),
                        ParsedPredicate(pred.get()).SpellingRange());
              err << "Cannot negate aggregating functor '" << pred->name << "'";
              context->error_log.Append(std::move(err));
              return;
            }

            state = 15;  // Go look for an `over`.
            continue;

          } else if (pred->negation_pos.IsValid()) {

            const auto kind = pred->declaration->context->kind;

            // We don't allow negation of functors because a requirement that
            // all argument body_variables be bound.
            //
            // For messages, we don't allow negations because we think of them
            // as ephemeral, i.e. not even part of the database. They come in
            // to trigger some action, and leave.
            //
            // We *do* allow negation of queries because we proxy them
            // externally via later source-to-source transforms.
            if (kind == DeclarationKind::kFunctor ||
                kind == DeclarationKind::kMessage) {
              Error err(context->display_manager, SubTokenRange(),
                        ParsedPredicate(pred.get()).SpellingRange());
              err << "Cannot negate " << pred->declaration->KindName()
                  << " '" << pred->name << "'";
              context->error_log.Append(std::move(err));
              return;
            }
          }

          link_pred();
          state = 8;
          continue;

        } else if (Lexeme::kPuncComma == lexeme) {
          state = 13;
          continue;

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected comma or period, but got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }

      case 15:
        if (Lexeme::kKeywordOver == lexeme) {
          if (!ParseAggregatedPredicate(
              module, clause.get(), std::move(pred), tok, next_pos)) {
            return;

          } else {
            state = 8;
            continue;
          }

        } else {
          Error err(context->display_manager, SubTokenRange(),
                    tok.SpellingRange());
          err << "Expected 'over' after usage of aggregate functor '"
              << pred->name << "', but got '" << tok << "' instead";
          context->error_log.Append(std::move(err));
          return;
        }
    }
  }

  if (state != 9 && state != 10) {
    Error err(context->display_manager, SubTokenRange(), next_pos);
    err << "Incomplete clause definition";
    context->error_log.Append(std::move(err));
    return;
  }

  const auto is_query_clause = DeclarationKind::kQuery ==
                               clause->declaration->context->kind;
  const auto is_message_clause = DeclarationKind::kMessage ==
                                 clause->declaration->context->kind;

  // TODO(pag): Consider doing some kind of basic range restriction check and
  //            transformation. For example, if we have `foo(A,A).` then is
  //            it legal to translate that to `foo(A0, A0) : foo(A0, _).` and
  //            `foo(A1, A1) : foo(_, A1).`?
  if (equates_parameters) {

  }

  // Go make sure we don't have two messages inside of a given clause. In our
  // bottom-up execution model, the "inputs" to the system are messages, which
  // are ephemeral. If we see that as triggering a clause, then we can't
  // easily account for two messages triggering a given clause, when the
  // ordering in time of those messages can be unbounded.
  //
  // TODO(pag): This restriction can be eliminated by rewriting the module to
  //            proxy messages with locals/exports. Do that then remove this
  //            issue.
  Node<ParsedPredicate> *prev_message = nullptr;
  for (auto &used_pred : clause->positive_predicates) {
    auto kind = used_pred->declaration->context->kind;
    if (kind != DeclarationKind::kMessage) {
      continue;
    }
    if (prev_message) {
      Error err(context->display_manager, SubTokenRange(),
                ParsedPredicate(used_pred.get()).SpellingRange());
      err << "Cannot have direct dependency on more than one messages";

      auto note = err.Note(context->display_manager, SubTokenRange(),
                           ParsedPredicate(prev_message).SpellingRange());
      note << "Previous message use is here";

      context->error_log.Append(std::move(err));
      return;

    } else {
      prev_message = used_pred.get();
    }

    // We might rewrite queries into a kind of request/response message pattern,
    // and so to make our lives easier later on, we restrict query clause bodies
    // to not be allowed to contain messages.
    //
    // TODO(pag): This restriction can be eliminated by rewriting messages used
    //            by queries to be proxied by locals. Do that then remove this
    //            issue.
    if (is_query_clause) {
      Error err(context->display_manager, SubTokenRange(),
                ParsedPredicate(used_pred.get()).SpellingRange());
      err << "Queries cannot depend directly on messages";
      context->error_log.Append(std::move(err));
      return;
    }
  }

  if (negation_tok.IsValid()) {
    // We don't let deletion clauses be specified on queries because a query
    // gives us point-in-time results according to some request.
    if (is_query_clause) {
      Error err(context->display_manager, SubTokenRange(),
                negation_tok.SpellingRange());
      err << "Deletion clauses cannot be specified on queries";
      context->error_log.Append(std::move(err));
      return;

    // We also don't support negations of messages, as it's a message isn't
    // something that "exists" in the database. That is, we can publish the
    // fact that something was deleted/changed, but we can't publish the
    // deletion of a message because they are ephemeral, and even if we had
    // received a corresponding "equivalent" message, then we never really
    // stored it to begin with.
    } else if (is_message_clause) {
      Error err(context->display_manager, SubTokenRange(),
                negation_tok.SpellingRange());
      err << "Deletion clauses cannot be specified on messages";
      context->error_log.Append(std::move(err));
      return;

    // Negation (i.e. removal) clauses must have a direct dependency on a
    // message. This keeps removal in the control of external users, and means
    // that, absent external messages, the system won't get into trivial cycles
    // that preven fixpoints.
    } else if (!prev_message) {
      Error err(context->display_manager, SubTokenRange(),
                negation_tok.SpellingRange().From());
      err << "The explicit deletion clause for " << decl->name << '/'
          << decl->parameters.size() << " must directly depend on a message";
      context->error_log.Append(std::move(err));
      return;
    }

    // Check that all other insertions depend on messages? The key here is to
    // not permit a situation where you ask to remove a tuple, but where that
    // tuple is independently provable via multiple "paths" (that don't use
    // messages). Because a message is ultimately ephemeral, there is no prior
    // record of its receipt per se, and so there is no prior evidence to re-
    // prove a clause head that we're asking to remove.
    auto has_errors = false;
    for (const auto &clause : decl->context->clauses) {
      if (!clause->depends_on_messages) {
        Error err(context->display_manager, SubTokenRange(),
                  negation_tok.SpellingRange().From());
        err << "All positive clauses of " << decl->name << '/'
            << decl->parameters.size() << " must directly depend on a message "
            << "because of the presence of a deletion clause";

        auto note = err.Note(context->display_manager,
                             ParsedClause(clause.get()).SpellingRange());
        note << "Clause without a direct message dependency is here";
        context->error_log.Append(std::move(err));
      }
    }
    if (has_errors) {
      return;
    }

  } else if (!prev_message && !decl->context->deletion_clauses.empty()) {
    Error err(context->display_manager, SubTokenRange(),
              negation_tok.SpellingRange().From());
    err << "All positive clauses of " << decl->name << '/'
        << decl->parameters.size() << " must directly depend on a message "
        << "because of the presence of a deletion clause";

    auto del_clause = decl->context->deletion_clauses.front().get();
    auto note = err.Note(context->display_manager,
                         ParsedClause(del_clause).SpellingRange());
    note << "First deletion clause is here";
    context->error_log.Append(std::move(err));
  }

  // Keep track of whether or not any clause for this decl uses messages.
  clause->depends_on_messages = true;

  // Link all positive predicate uses into their respective declarations.
  for (auto &used_pred : clause->positive_predicates) {
    auto &pred_decl_context = used_pred->declaration->context;
    auto &positive_uses = pred_decl_context->positive_uses;
    if (!positive_uses.empty()) {
      positive_uses.back()->next_use = used_pred.get();
    }
    positive_uses.push_back(used_pred.get());
  }

  // Link all negative predicate uses into their respective declarations.
  for (auto &used_pred : clause->negated_predicates) {
    auto &pred_decl_context = used_pred->declaration->context;
    auto &negated_uses = pred_decl_context->negated_uses;
    if (!negated_uses.empty()) {
      negated_uses.back()->next_use = used_pred.get();
    }
    negated_uses.push_back(used_pred.get());
  }

  auto &clause_decl_context = clause->declaration->context;

  std::vector<Node<ParsedClause> *> *module_clause_list = nullptr;
  std::vector<std::unique_ptr<Node<ParsedClause>>> *decl_clause_list = nullptr;

  if (negation_tok.IsValid()) {
    clause->negation = negation_tok;
    module_clause_list = &(module->deletion_clauses);
    decl_clause_list = &(clause_decl_context->deletion_clauses);

  } else {
    module_clause_list = &(module->clauses);
    decl_clause_list = &(clause_decl_context->clauses);
  }

  // Link the clause in to the module.
  if (!module_clause_list->empty()) {
    module_clause_list->back()->next_in_module = clause.get();
  }
  module_clause_list->push_back(clause.get());

  // Link the clause in to its respective declaration.
  if (!decl_clause_list->empty()) {
    decl_clause_list->back()->next = clause.get();
  }

  // Add this clause to its decl context.
  decl_clause_list->emplace_back(std::move(clause));
}

}  // namespace hyde
