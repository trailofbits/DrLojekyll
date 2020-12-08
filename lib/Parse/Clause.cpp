// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/Util/DisjointSet.h>

#include "Parser.h"

namespace hyde {
namespace {

// Go through the variables used in the clause and try to identify groups of
// predicates that should be extracted into boolean conditions (zero-argument
// clauses). It's simpler to demand that the writer of the code factor out these
// conditions instead of having us do it automatically.
static void FindUnrelatedConditions(Node<ParsedClause> *clause,
                                    const ErrorLog &log) {

  std::vector<std::unique_ptr<DisjointSet>> sets;
  std::unordered_map<uint64_t, DisjointSet *> var_to_set;
  std::unordered_map<Node<ParsedPredicate> *, DisjointSet *> pred_to_set;

  auto next_id = 0u;
  const auto clause_head_id = next_id;
  const auto clause_set = new DisjointSet(next_id++);
  sets.emplace_back(clause_set);
  for (auto &var : clause->head_variables) {
    var_to_set.emplace(var->Id(), clause_set);
  }

  auto do_pred = [&](Node<ParsedPredicate> *pred, DisjointSet *set) {
    // This predicate has zero arguments, i.e. it is a condition already.
    // Associated it with the clause head set.
    if (pred->argument_uses.empty()) {
      if (set) {
        set = DisjointSet::Union(set, clause_set);
      } else {
        set = clause_set;
      }
    }

    for (auto &var_use : pred->argument_uses) {
      Node<ParsedVariable> *var = var_use->used_var;
      auto &found_set = var_to_set[var->Id()];
      if (found_set) {
        if (set) {
          set = DisjointSet::Union(set, found_set);
        } else {
          set = found_set;
        }
      } else if (set) {
        found_set = set;

      } else {
        set = new DisjointSet(next_id++);
        sets.emplace_back(set);
        found_set = set;
      }
    }

    pred_to_set.emplace(pred, set);
  };

  for (const auto &pred : clause->positive_predicates) {
    do_pred(pred.get(), nullptr);
  }

  for (const auto &pred : clause->negated_predicates) {
    do_pred(pred.get(), nullptr);
  }

  for (const auto &agg : clause->aggregates) {
    const auto set = new DisjointSet(next_id++);
    sets.emplace_back(set);
    do_pred(agg->predicate.get(), set);
    do_pred(agg->functor.get(), set);
  }

  for (const auto &assign : clause->assignments) {
    Node<ParsedVariable> *lhs_var = assign->lhs.used_var;
    auto &set = var_to_set[lhs_var->Id()];
    if (!set) {
      set = new DisjointSet(next_id++);
      sets.emplace_back(set);
    }
  }

  for (const auto &cmp : clause->comparisons) {
    Node<ParsedVariable> *lhs_var = cmp->lhs.used_var;
    Node<ParsedVariable> *rhs_var = cmp->rhs.used_var;

    auto &lhs_set = var_to_set[lhs_var->Id()];
    auto &rhs_set = var_to_set[rhs_var->Id()];

    if (lhs_set && rhs_set) {
      DisjointSet::Union(lhs_set, rhs_set);
    } else if (lhs_set) {
      rhs_set = lhs_set;
    } else if (rhs_set) {
      lhs_set = rhs_set;
    } else {
      lhs_set = new DisjointSet(next_id++);
      rhs_set = lhs_set;
      sets.emplace_back(lhs_set);
    }
  }

  std::vector<DisjointSet *> conditions;
  for (auto &[id, set] : var_to_set) {
    (void) id;
    set = set->Find();
    if (set->id != clause_head_id) {
      conditions.push_back(set);
    }
  }

  std::sort(conditions.begin(), conditions.end());
  auto it = std::unique(conditions.begin(), conditions.end());
  conditions.erase(it, conditions.end());

  // All head variables are related to every predicate/comparison/aggregate.
  if (conditions.empty()) {
    return;

  // There are no head variables, and there is only one condition set, i.e.
  // we are definition a condition variable.
  } else if (conditions.size() == 1 && clause->head_variables.empty()) {
    return;
  }

  const auto clause_range = ParsedClause(clause).SpellingRange();
  for (auto cond_set : conditions) {
    auto err = log.Append(clause_range);
    err << "The following elements in the body of this clause should be "
        << "factored out into a zero-argument predicate";

    for (const auto &pred : clause->positive_predicates) {
      if (pred_to_set[pred.get()]->Find() == cond_set) {
        err.Note(clause_range, ParsedPredicate(pred.get()).SpellingRange())
            << "This predicate";
      }
    }

    for (const auto &pred : clause->negated_predicates) {
      if (pred_to_set[pred.get()]->Find() == cond_set) {
        err.Note(clause_range, ParsedPredicate(pred.get()).SpellingRange())
            << "This negated predicate";
      }
    }

    for (const auto &assign : clause->assignments) {
      Node<ParsedVariable> *lhs_var = assign->lhs.used_var;
      if (var_to_set[lhs_var->Id()] == cond_set) {
        err.Note(clause_range, ParsedAssignment(assign.get()).SpellingRange())
            << "This assignment";
      }
    }

    for (const auto &cmp : clause->comparisons) {
      Node<ParsedVariable> *lhs_var = cmp->lhs.used_var;
      if (var_to_set[lhs_var->Id()] == cond_set) {
        err.Note(clause_range, ParsedComparison(cmp.get()).SpellingRange())
            << "This comparison";
      }
    }
  }
}

}  // namespace

// Try to parse `sub_range` as a clause.
void ParserImpl::ParseClause(Node<ParsedModule> *module, Token negation_tok,
                             Node<ParsedDeclaration> *decl) {

  auto clause = std::make_unique<Node<ParsedClause>>(module);
  prev_named_var.clear();

  Token tok;
  int state = 0;

  // Approximate state transition diagram for parsing clauses. It gets a bit
  // more complicated because we can have literals in the clause head, or
  // aggregates with inline
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

  // Link `pred` into `clause`.
  auto link_pred = [&](void) {
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

  for (next_pos = tok.NextPosition(); ReadNextSubToken(tok);
       next_pos = tok.NextPosition()) {

    if (clause) {
      clause->last_tok = tok;
    }

    const auto lexeme = tok.Lexeme();
    const auto tok_range = tok.SpellingRange();
    switch (state) {
      case 0:
        if (Lexeme::kIdentifierAtom == lexeme ||
            Lexeme::kIdentifierUnnamedAtom == lexeme) {
          clause->name = tok;
          state = 1;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected atom here (lower case identifier) for the name of "
              << "the clause head being declared, got '" << tok << "' instead";
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
          context->error_log.Append(scope_range, tok_range)
              << "Expected opening parenthesis here to begin parameter list of "
              << "clause head '" << clause->name << "', or a colon for a zero-"
              << "arity predicate, but got '" << tok << "' instead";
          return;
        }

      case 2:
        if (Lexeme::kIdentifierVariable == lexeme) {
          (void) CreateVariable(clause.get(), tok, true, false);

          state = 3;
          continue;

        } else if (Lexeme::kIdentifierUnnamedVariable == lexeme) {
          context->error_log.Append(scope_range, tok_range)
              << "Unnamed variables in the parameter list of a clause "
              << "are not allowed because they cannot be range restricted";
          return;

        // Support something like `foo(1, ...) : ...`, converting it into
        // `foo(V, ...) : V=1, ...`.
        } else if (Lexeme::kLiteralString == lexeme ||
                   Lexeme::kLiteralNumber == lexeme) {
          (void) CreateLiteralVariable(clause.get(), tok, true, false);
          state = 3;
          continue;

        // Kick-start type inference when using a named constant.
        } else if (Lexeme::kIdentifierConstant == lexeme) {
          auto unnamed_var = CreateLiteralVariable(
              clause.get(), tok, true, false);
          const TypeLoc type_loc(tok.TypeKind());
          unnamed_var->type = type_loc;
          auto foreign_const_it = context->foreign_constants.find(
              tok.IdentifierId());
          if (foreign_const_it != context->foreign_constants.end()) {
            unnamed_var->type = foreign_const_it->second->type;
          }
          state = 3;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected variable name (capitalized identifier) for "
              << "parameter in clause '" << clause->name << "', but got '"
              << tok << "' instead";
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
          context->error_log.Append(scope_range, tok_range)
              << "Expected comma (to continue parameter list) or closing "
              << "parenthesis (to end paramater list) for clause head '"
              << clause->name << "', but got '" << tok << "' instead";
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
          context->error_log.Append(scope_range, tok_range)
              << "Expected colon to denote the beginning of the body "
              << "of the clause '" << clause->name << "', but got '" << tok
              << "' instead";
          return;
        }

      case 5:
        if (clause->first_body_token.IsInvalid()) {
          clause->first_body_token = tok;
        }

        if (Lexeme::kIdentifierVariable == lexeme) {
          lhs = CreateVariable(clause.get(), tok, false, false);
          state = 6;
          continue;

        } else if (Lexeme::kLiteralString == lexeme ||
                   Lexeme::kLiteralNumber == lexeme ||
                   Lexeme::kIdentifierConstant == lexeme) {
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
          context->error_log.Append(scope_range, tok_range)
              << "Expected variable name, atom, or exclamation point, but got '"
              << tok << "' instead";
          return;
        }

      case 6:
        if (Lexeme::kPuncEqual == lexeme || Lexeme::kPuncNotEqual == lexeme ||
            Lexeme::kPuncLess == lexeme || Lexeme::kPuncGreater == lexeme) {
          compare_op = tok;
          state = 7;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected comparison operator, but got '" << tok
              << "' instead";
          return;
        }

      case 7:
        rhs = nullptr;

        // Allow comparisons with literals by converting the literals into
        // variables and assigning values to those variables.
        if (Lexeme::kLiteralString == lexeme ||
            Lexeme::kLiteralNumber == lexeme ||
            Lexeme::kIdentifierConstant == lexeme) {

          // If we're doing `<var> = <literal>` then we don't want to explode
          // it into `<temp> = literal, <var> = <temp>`.
          if (Lexeme::kPuncEqual == compare_op.Lexeme()) {
            auto assign = new Node<ParsedAssignment>(lhs);
            assign->rhs.literal = tok;
            assign->rhs.assigned_to = lhs;
            std::string_view data;
            if (context->display_manager.TryReadData(tok_range, &data)) {
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
            const DisplayRange assign_range(lhs->name.Position(),
                                            rhs->name.NextPosition());
            context->error_log.Append(scope_range, assign_range)
                << "Variable '" << lhs->name
                << "' cannot appear on both sides of a comparison";
            return;
          }

          const auto compare = new Node<ParsedComparison>(lhs, rhs, compare_op);

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
          context->error_log.Append(scope_range, tok_range)
              << "Expected variable name or number/string literal, but got '"
              << tok << "' instead";
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
          context->error_log.Append(scope_range, tok_range)
              << "Expected comma or period, but got '" << tok << "' instead";
          return;
        }

      case 9: {
        DisplayRange err_range(tok.Position(),
                               sub_tokens.back().NextPosition());
        context->error_log.Append(scope_range, err_range)
            << "Unexpected tokens following clause '" << clause->name << "'";
        state = 10;  // Ignore further errors, but add the local in.
        continue;
      }

      // We're just chugging tokens at the end, ignore them.
      case 10: continue;

      // We think we're parsing a negated predicate.
      case 11:
        if (Lexeme::kIdentifierAtom == lexeme) {
          pred.reset(new Node<ParsedPredicate>(module, clause.get()));
          pred->name = tok;
          pred->negation_pos = negation_pos;
          state = 12;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected atom here for negated predicate, but got '" << tok
              << "' instead";
          return;
        }

      case 12:
        if (Lexeme::kPuncOpenParen == lexeme) {
          state = 13;
          continue;

        } else if (Lexeme::kPuncPeriod == lexeme) {
          clause->dot = tok;
          if (!TryMatchPredicateWithDecl(module, pred.get())) {
            return;
          }
          state = 9;
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
          context->error_log.Append(scope_range, tok_range)
              << "Expected an opening parenthesis, comma, or period here to"
              << "test predicate '" << pred->name << "', but got '" << tok
              << "' instead";
          return;
        }

      case 13:
        arg = nullptr;

        // Convert literals into variables, just-in-time.
        if (Lexeme::kLiteralString == lexeme ||
            Lexeme::kLiteralNumber == lexeme ||
            Lexeme::kIdentifierConstant == lexeme) {
          arg = CreateLiteralVariable(clause.get(), tok, false, true);

        } else if (Lexeme::kIdentifierVariable == lexeme ||
                   Lexeme::kIdentifierUnnamedVariable == lexeme) {
          arg = CreateVariable(clause.get(), tok, false, true);
        }

        if (arg) {
          auto use = new Node<ParsedUse<ParsedPredicate>>(UseKind::kArgument,
                                                          arg, pred.get());

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
          context->error_log.Append(scope_range, tok_range)
              << "Expected variable or literal here as argument to predicate '"
              << pred->name << "', but got '" << tok << "' instead";
          return;
        }

      case 14:
        if (Lexeme::kPuncCloseParen == lexeme) {
          pred->rparen = tok;

          if (!TryMatchPredicateWithDecl(module, pred.get())) {
            return;
          }

          const auto pred_range =
              ParsedPredicate(pred.get()).SpellingRange();

          // Not allowed to negate inline declarations, as they might not be
          // backed by actual relations.
          if (pred->negation_pos.IsValid() &&
              pred->declaration->inline_attribute.IsValid()) {
            auto err = context->error_log.Append(scope_range, pred_range);
            err << "Cannot negate " << pred->declaration->KindName() << " '"
                << pred->name << "' because it has been marked as inline";

            auto note =
                err.Note(ParsedDeclaration(pred->declaration).SpellingRange(),
                         pred->declaration->inline_attribute.SpellingRange());
            note << "Marked as inline here";
            return;
          }

          // If it's an aggregating functor then we need to follow-up with
          // the `over` keyword.
          auto pred_decl = ParsedDeclaration::Of(ParsedPredicate(pred.get()));
          if (pred_decl.IsFunctor() &&
              ParsedFunctor::From(pred_decl).IsAggregate()) {

            if (pred->negation_pos.IsValid()) {
              context->error_log.Append(scope_range, pred_range)
                  << "Cannot negate aggregating functor '" << pred->name << "'";
              return;
            }

            state = 15;  // Go look for an `over`.
            continue;

          } else if (pred->negation_pos.IsValid()) {

            const auto kind = pred->declaration->context->kind;

            // We don't allow negations of messages because we think of them
            // as ephemeral, i.e. not even part of the database. They come in
            // to trigger some action, and leave.
            //
            // We *do* allow negation of queries because we proxy them
            // externally via later source-to-source transforms.
            if (kind == DeclarationKind::kMessage) {
              context->error_log.Append(scope_range, pred_range)
                  << "Cannot negate message '" << pred->name
                  << "'; if you want to test that a message has never been "
                  << "received then proxy it with a `#local` or `#export`";
              return;

            // A functor with a range of one-to-one or one-or-more is guaranteed
            // to produce at least one output, and so negating it would yield
            // and always-false situation.
            } else if (kind == DeclarationKind::kFunctor &&
                       (pred->declaration->range == FunctorRange::kOneToOne ||
                        pred->declaration->range == FunctorRange::kOneOrMore)) {
              auto err = context->error_log.Append(scope_range, pred_range);
              err << "Cannot negate functor '" << pred->name
                  << "' declared with a one-to-one or one-or-more range";

              err.Note(
                  ParsedDeclaration(pred->declaration).SpellingRange(),
                  DisplayRange(pred->declaration->range_begin_opt.Position(),
                               pred->declaration->range_end_opt.NextPosition()))
                  << "Range of functor '" << pred->name
                  << "' is specified here";
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
          context->error_log.Append(scope_range, tok_range)
              << "Expected comma or period, but got '" << tok << "' instead";
          return;
        }

      case 15:
        if (Lexeme::kKeywordOver == lexeme) {
          if (!ParseAggregatedPredicate(module, clause.get(), std::move(pred),
                                        tok, next_pos)) {
            return;

          } else {
            state = 8;
            continue;
          }

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected 'over' after usage of aggregate functor '"
              << pred->name << "', but got '" << tok << "' instead";
          return;
        }
    }
  }

  if (state != 9 && state != 10) {
    context->error_log.Append(scope_range, next_pos)
        << "Incomplete clause definition";
    return;
  }

  const auto is_query_clause =
      DeclarationKind::kQuery == clause->declaration->context->kind;
  const auto is_message_clause =
      DeclarationKind::kMessage == clause->declaration->context->kind;

  // Go see if we depend on one or more messages.
  Node<ParsedPredicate> *prev_message = nullptr;
  for (auto &used_pred : clause->positive_predicates) {
    const auto kind = used_pred->declaration->context->kind;
    if (kind == DeclarationKind::kMessage) {
      clause->depends_on_messages = true;
      prev_message = used_pred.get();
      continue;
    }
  }

  if (negation_tok.IsValid()) {
    const auto negation_tok_range = negation_tok.SpellingRange();

    // We don't let deletion clauses be specified on queries because a query
    // gives us point-in-time results according to some request.
    if (is_query_clause) {
      context->error_log.Append(scope_range, negation_tok_range)
          << "Deletion clauses cannot be specified on queries";
      return;

    // We also don't support negations of messages, as it's a message isn't
    // something that "exists" in the database. That is, we can publish the
    // fact that something was deleted/changed, but we can't publish the
    // deletion of a message because they are ephemeral, and even if we had
    // received a corresponding "equivalent" message, then we never really
    // stored it to begin with.
    } else if (is_message_clause) {
      context->error_log.Append(scope_range, negation_tok_range)
          << "Deletion clauses cannot be specified on messages";
      return;

    // Negation (i.e. removal) clauses must have a direct dependency on a
    // message. This keeps removal in the control of external users, and means
    // that, absent external messages, the system won't get into trivial cycles
    // that prevent fixpoints.
    } else if (!prev_message) {
      context->error_log.Append(scope_range, negation_tok_range)
          << "The explicit deletion clause for " << decl->name << '/'
          << decl->parameters.size() << " must directly depend on a message";
      return;

    // We're not allowed to directly delete things from k/v stores, as we can't
    // reasonably match up the values supplied for the values, and the keys of
    // the current value associated with the same keys.
    } else if (ParsedDeclaration(clause->declaration).HasMutableParameter()) {
      auto err = context->error_log.Append(scope_range, negation_tok_range);
      err << "Deletion clauses cannot be specified on declarations with "
          << "mutable paramaters";

      for (const auto &param_ : clause->declaration->parameters) {
        ParsedParameter param(param_.get());
        err.Note(param.SpellingRange(), param.Type().SpellingRange())
            << "Mutable parameter is here";
      }
      return;

    // We don't allow negation of zero-argument predicates, because if they
    // are dataflow-dependent, then there's no real way to "merge" multiple
    // positive and negative flows.
    } else if (clause->head_variables.empty()) {
      context->error_log.Append(scope_range, negation_tok_range)
          << "Deletion clauses cannot be specified on zero-argument predicates";
      return;
    }

  // Don't let us send out any messages if we have any uses of this message.
  } else if (is_message_clause && !decl->context->positive_uses.empty()) {
    auto err = context->error_log.Append(scope_range);
    err << "Cannot send output in message " << decl->name << '/'
        << decl->parameters.size()
        << "; the message is already used for receiving data";

    for (auto pred_ : decl->context->positive_uses) {
      auto pred = ParsedPredicate(pred_);
      auto clause = ParsedClause::Containing(pred);
      err.Note(clause.SpellingRange(), pred.SpellingRange())
          << "Message receipt is here";
    }

  // We've found a positive clause definition, and there are negative clause
  // definitions, and this positive clause definition does not actually use
  // any messages.
  } else if (!prev_message && !decl->context->deletion_clauses.empty()) {
    auto err = context->error_log.Append(scope_range, negation_tok.Position());
    err << "All positive clauses of " << decl->name << '/'
        << decl->parameters.size() << " must directly depend on a message "
        << "because of the presence of a deletion clause";

    auto del_clause = decl->context->deletion_clauses.front().get();
    auto note = err.Note(ParsedClause(del_clause).SpellingRange());
    note << "First deletion clause is here";
  }

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

  FindUnrelatedConditions(clause.get(), context->error_log);

  // Add this clause to its decl context.
  decl_clause_list->emplace_back(std::move(clause));
}

}  // namespace hyde
