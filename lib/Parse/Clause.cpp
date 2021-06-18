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
void ParserImpl::ParseClause(Node<ParsedModule> *module,
                             Node<ParsedDeclaration> *decl) {

  auto clause = std::make_unique<Node<ParsedClause>>(module);
  prev_named_var.clear();

  Token tok;
  std::vector<Token> clause_toks;
  bool multi_clause = false;
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
  Token negation_tok;
  Node<ParsedVariable> *arg = nullptr;
  Node<ParsedVariable> *lhs = nullptr;
  Node<ParsedVariable> *rhs = nullptr;
  Token compare_op;
  std::unique_ptr<Node<ParsedPredicate>> pred;

  // Link `pred` into `clause`.
  auto link_pred = [&](void) {
    if (negation_tok.IsValid()) {
      if (negation_tok.Lexeme() == Lexeme::kPragmaPerfNever) {
        if (pred->declaration->context->kind == DeclarationKind::kFunctor) {
          context->error_log.Append(
              scope_range, ParsedPredicate(pred.get()).SpellingRange())
              << "Functor applications cannot be negated with '@never'";

        } else if (pred->argument_uses.empty()) {
          context->error_log.Append(
              scope_range, ParsedPredicate(pred.get()).SpellingRange())
              << "Zero-argument predicates cannot be negated with '@never'";
        }
      }
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
    negation_tok = Token();
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
          clause_toks.push_back(tok);
          state = 1;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected atom here (lower case identifier) for the name of "
              << "the clause head being declared, got '" << tok << "' instead";
          return;
        }

      case 1:
        clause_toks.push_back(tok);  // add token even if we error
        if (Lexeme::kPuncOpenParen == lexeme) {
          state = 2;
          continue;

        // Zero-argument predicate, e.g. `foo : ...`.
        } else if (Lexeme::kPuncColon == lexeme) {
          if (decl) {
            clause->declaration = decl;
            state = 5;
            continue;

          } else if (!TryMatchClauseWithDecl(module, clause.get())) {
            return;

          } else {
            state = 5;
            continue;
          }

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected opening parenthesis here to begin parameter list of "
              << "clause head '" << clause->name << "', or a colon for a zero-"
              << "arity predicate, but got '" << tok << "' instead";
          return;
        }

      // We have see either an opening parenthesis, or we have just parsed
      // a parameter and have seen a comma, it's now time to try to parse
      // another clause head parameter.
      case 2:
        clause_toks.push_back(tok);  // add token even if we error
        if (Lexeme::kIdentifierVariable == lexeme) {
          auto param_var = CreateVariable(clause.get(), tok, true, false);
          auto param_use = new Node<ParsedUse<ParsedClause>>(
              UseKind::kParameter, param_var, clause.get());

          if (!clause->parameter_uses.empty()) {
            clause->parameter_uses.back()->next = param_use;
          }
          clause->parameter_uses.emplace_back(param_use);
          param_var->context->parameter_uses.push_back(param_use);

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
                   Lexeme::kLiteralNumber == lexeme ||
                   Lexeme::kLiteralTrue == lexeme ||
                   Lexeme::kLiteralFalse == lexeme) {
          (void) CreateLiteralVariable(clause.get(), tok, true, false);
          state = 3;
          continue;

        // Kick-start type inference when using a named constant.
        } else if (Lexeme::kIdentifierConstant == lexeme) {
          auto unnamed_var =
              CreateLiteralVariable(clause.get(), tok, true, false);
          const TypeLoc type_loc(tok.TypeKind());
          unnamed_var->type = type_loc;
          auto foreign_const_it =
              context->foreign_constants.find(tok.IdentifierId());
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

      // We've read a variable/literal/constant, now we expect a comma and more
      // clause head parameters, or a closing paren to end the clause head.
      case 3:
        clause_toks.push_back(tok);  // add token even if we error
        if (Lexeme::kPuncComma == lexeme) {
          state = 2;
          continue;

        // Done parsing this clause head.
        } else if (Lexeme::kPuncCloseParen == lexeme) {
          clause->rparen = tok;

          // If we're parsing an attached body, e.g. `head(...) : body1 : body2`
          // then we will have split out the tokens `head(...) : body2` and
          // passed `decl` in from the parse of `head(...) : body1`.
          if (decl) {
            clause->declaration = decl;
            state = 4;
            continue;

          // We matched it against a clasue head.
          } else if (TryMatchClauseWithDecl(module, clause.get())) {
            decl = clause->declaration;
            state = 4;
            continue;

          // `TryMatchClauseWithDecl` failed and will have reported an error.
          } else {
            assert(0 < context->error_log.Size());
            return;
          }

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected comma (to continue parameter list) or closing "
              << "parenthesis (to end paramater list) for clause head '"
              << clause->name << "', but got '" << tok << "' instead";
          return;
        }

      // Time to see if we have a clause body to parse or not.
      case 4:
        clause_toks.push_back(tok);  // add token even if we error
        if (Lexeme::kPuncColon == lexeme) {
          state = 5;
          continue;

        // We're done with the body.
        } else if (Lexeme::kPuncPeriod == lexeme) {
          clause->dot = tok;
          state = 9;
          continue;

        // Pragma to add a highlighting color to nodes in the GraphViz DOT
        // digraph format output of the dataflow representation.
        } else if (Lexeme::kPragmaDebugHighlight == lexeme) {
          if (clause->highlight.IsValid()) {
            auto err = context->error_log.Append(scope_range, tok_range);
            err << "Cannot repeat pragma '" << tok << "'";

            err.Note(scope_range, clause->highlight.SpellingRange())
                << "Previous use of the '" << tok << "' pragma was here";
            return;

          } else {
            clause->highlight = tok;
            state = 4;
            continue;
          }

        // Programmer annotation that gives the compiler permission to generate
        // cross-product nodes in the data flow of a particular clause. Cross-
        // products have bad runtime performance, so this acts as an explicit
        // opt-in to prevent their accidental introduction.
        } else if (Lexeme::kPragmaPerfProduct == lexeme) {
          if (clause->product.IsValid()) {
            auto err = context->error_log.Append(scope_range, tok_range);
            err << "Cannot repeat pragma '" << tok << "'";

            err.Note(scope_range, clause->product.SpellingRange())
                << "Previous use of the '" << tok << "' pragma was here";
            return;

          } else {
            clause->product = tok;
            state = 4;
            continue;
          }
        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected colon to denote the beginning of the body "
              << "of the clause '" << clause->name << "', period to mark a "
              << "fact, or a debug prgram; but got '" << tok << "' instead";
          return;
        }

      // We've just seen a `:`, time to parse a clause body.
      case 5:
        if (clause->first_body_token.IsInvalid()) {
          clause->first_body_token = tok;
        }

        // The `V` in `V = ...` or `V != ...`.
        if (Lexeme::kIdentifierVariable == lexeme) {
          lhs = CreateVariable(clause.get(), tok, false, false);
          state = 6;
          continue;

        // We're seeing a `false` token. It could be something like `false = V`
        // or `..., false, ...` on its own that "disables" the clause.
        } else if (Lexeme::kLiteralFalse == lexeme) {
          if (Token peek_tok; ReadNextSubToken(peek_tok)) {
            UnreadSubToken();
            switch (peek_tok.Lexeme()) {
              case Lexeme::kPuncComma:
              case Lexeme::kPuncColon:
              case Lexeme::kPuncPeriod: {
                break;
              }
              default: {
                lhs = CreateLiteralVariable(clause.get(), tok, false, false);
                state = 6;
                continue;
              }
            }
          }

          // No next token, or at the end of this clause body or predicate.
          clause->disabled_by = tok.SpellingRange();
          state = 8;
          continue;

        // We're seeing a `true` token. It could be something like `true = V`
        // or `..., true, ...` on its own that gets ignored.
        } else if (Lexeme::kLiteralFalse == lexeme) {
          if (Token peek_tok; ReadNextSubToken(peek_tok)) {
            UnreadSubToken();
            switch (peek_tok.Lexeme()) {
              case Lexeme::kPuncComma:
              case Lexeme::kPuncColon:
              case Lexeme::kPuncPeriod: {
                break;  // Ignore it.
              }
              default: {
                lhs = CreateLiteralVariable(clause.get(), tok, false, false);
                state = 6;
                continue;
              }
            }
          }

          // No next token, or at the end of this clause body or predicate.
          state = 8;
          continue;

        // The `1` in `1 = ...`.
        } else if (Lexeme::kLiteralString == lexeme ||
                   Lexeme::kLiteralNumber == lexeme ||
                   Lexeme::kIdentifierConstant == lexeme ||
                   Lexeme::kLiteralTrue == lexeme ||
                   Lexeme::kLiteralFalse == lexeme) {
          lhs = CreateLiteralVariable(clause.get(), tok, false, false);
          state = 6;
          continue;

        // The `!` in `!pred(...)` or in `!V` where `V` has Boolean type.
        } else if (Lexeme::kPuncExclaim == lexeme) {
          negation_tok = tok;
          state = 11;
          continue;

        // The `@never` in `@never pred(...)`.
        } else if (Lexeme::kPragmaPerfNever == lexeme) {
          negation_tok = tok;
          state = 11;
          continue;

        // The `pred` in `pred(...)`.
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

        // We've just seen a variable, literal, or constant; try to combine it
        // with a binary operator.
        if (Lexeme::kPuncEqual == lexeme || Lexeme::kPuncNotEqual == lexeme ||
            Lexeme::kPuncLess == lexeme || Lexeme::kPuncGreater == lexeme) {
          compare_op = tok;
          state = 7;
          continue;

        // We've just seen a variable, and now we see a comma, so we interpret
        // it as "variable is true."
        } else if (Lexeme::kPuncComma == lexeme ||
                   Lexeme::kPuncPeriod == lexeme ||
                   Lexeme::kPuncColon == lexeme) {
          assert(lhs);

          // Likely a constant/literal; we'll keep parsing anyway.
          if (lhs->name.Lexeme() == Lexeme::kIdentifierUnnamedVariable &&
              lhs->type.Kind() != TypeKind::kBoolean) {
            context->error_log.Append(scope_range, tok_range)
                << "Expected variable here but got '"
                << lhs->name.SpellingRange() << "' instead";
          }

          const auto assign = new Node<ParsedAssignment>(lhs);
          assign->rhs.literal =
              Token::Synthetic(Lexeme::kLiteralTrue, DisplayRange());
          assign->rhs.assigned_to = lhs;
          assign->rhs.data = "true";
          assign->rhs.type =
              TypeLoc(TypeKind::kBoolean, lhs->name.SpellingRange());

          // Add to the clause's assignment list.
          if (!clause->assignments.empty()) {
            clause->assignments.back()->next = assign;
          }
          clause->assignments.emplace_back(assign);

          // Add to the variable's assignment list. We support the list, but for
          // these auto-created variables, there can be only one use.
          lhs->context->assignment_uses.push_back(&(assign->lhs));

          pred.reset();
          if (Lexeme::kPuncComma == lexeme) {
            state = 5;
            continue;

          } else if (Lexeme::kPuncPeriod == lexeme) {
            clause->dot = tok;
            state = 9;
            continue;

          } else if (Lexeme::kPuncColon == lexeme) {

            // let the "dot" be the colon token
            clause->dot = tok;

            // there's another clause let's go accumulate the remaining tokens
            state = 16;
            multi_clause = true;
            continue;

          } else {
            assert(false);
            state = 9;
            continue;
          }

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
            Lexeme::kIdentifierConstant == lexeme ||
            Lexeme::kLiteralTrue == lexeme ||
            Lexeme::kLiteralFalse == lexeme) {

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


            // NOTE(pag): This will have been previously reported. It is likely
            //            a result of an invalid string literal (e.g. crossing
            //            a line boundary) that has been "converted" into a
            //            valid one for the sake of parsing being able to
            //            proceed.
            } else {
              assert(!context->error_log.IsEmpty());
            }

            // Infer the type of the assignment based off the constant.
            if (Lexeme::kIdentifierConstant == tok.Lexeme()) {
              auto const_ptr = context->foreign_constants[tok.IdentifierId()];
              assert(const_ptr != nullptr);
              assert(const_ptr->parent != nullptr);
              assign->rhs.type = const_ptr->type;
              assign->rhs.foreign_type = const_ptr->parent;
              assign->rhs.foreign_constant = const_ptr;
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

        } else if (Lexeme::kPuncColon == lexeme) {

          // let the "dot" be the colon token
          clause->dot = tok;

          // there's another clause let's go accumulate the remaining tokens
          state = 16;
          multi_clause = true;
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

      case 11:

        // We think we're parsing a negated predicate, i.e. `!pred(...)`.
        if (Lexeme::kIdentifierAtom == lexeme) {
          pred.reset(new Node<ParsedPredicate>(module, clause.get()));
          pred->name = tok;
          pred->negation = negation_tok;
          state = 12;
          continue;

        // `@never` can only apply to a predicate, not to a variable.
        } else if (negation_tok.Lexeme() == Lexeme::kPragmaPerfNever) {
          context->error_log.Append(scope_range, tok_range)
              << "Expected atom here after the '@never' negation pragma, "
              << "but got '" << tok << "' instead";
          return;

        // We think we're parsing a negated Boolean variable, e.g. `!V`;
        // this gets treated as `V = false`.
        } else if (Lexeme::kIdentifierVariable == lexeme) {
          lhs = CreateVariable(clause.get(), tok, false, false);
          const auto assign = new Node<ParsedAssignment>(lhs);
          assign->rhs.literal =
              Token::Synthetic(Lexeme::kLiteralFalse, DisplayRange());
          assign->rhs.assigned_to = lhs;
          assign->rhs.data = "false";
          assign->rhs.type =
              TypeLoc(TypeKind::kBoolean,
                      DisplayRange(negation_tok.Position(), tok.NextPosition()));

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

        // `!true`, i.e. `false`, this gets treated as `false`.
        } else if (Lexeme::kLiteralTrue == lexeme) {
          clause->disabled_by = DisplayRange(negation_tok.Position(),
                                             tok.NextPosition());
          state = 8;
          continue;

        // `!false`, i.e. `true`, we ignore this, and we anticipate either a
        // comma or a
        } else if (Lexeme::kLiteralFalse == lexeme) {
          state = 8;
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

        // It is a zero-argument predicate, go to the next clause.
        } else if (Lexeme::kPuncColon == lexeme) {
          clause->dot = tok;
          if (!TryMatchPredicateWithDecl(module, pred.get())) {
            return;
          }

          link_pred();

          // there's another clause let's go accumulate the remaining tokens
          state = 16;
          multi_clause = true;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected an opening parenthesis, comma, or period here to"
              << " test predicate '" << pred->name << "', but got '" << tok
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

          const auto pred_range = ParsedPredicate(pred.get()).SpellingRange();

          // Not allowed to negate inline declarations, as they might not be
          // backed by actual relations.
          if (pred->negation.IsValid() &&
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

            if (pred->negation.IsValid()) {
              context->error_log.Append(scope_range, pred_range)
                  << "Cannot negate aggregating functor '" << pred->name << "'";
              return;
            }

            state = 15;  // Go look for an `over`.
            continue;

          } else if (pred->negation.IsValid()) {

            const auto kind = pred->declaration->context->kind;

            //            // We don't allow negations of messages because we think of them
            //            // as ephemeral, i.e. not even part of the database. They come in
            //            // to trigger some action, and leave.
            //            //
            //            // We *do* allow negation of queries because we proxy them
            //            // externally via later source-to-source transforms.
            //            if (kind == DeclarationKind::kMessage) {
            //              context->error_log.Append(scope_range, pred_range)
            //                  << "Cannot negate message '" << pred->name
            //                  << "'; if you want to test that a message has never been "
            //                  << "received then proxy it with a `#local` or `#export`";
            //              return;

            // A functor with a range of one-to-one or one-or-more is guaranteed
            // to produce at least one output, and so negating it would yield
            // and always-false situation.
            if (kind == DeclarationKind::kFunctor &&
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
      case 16:

        // Accumulate multi-clause tokens
        assert(multi_clause);
        clause_toks.push_back(tok);
        if (Lexeme::kPuncPeriod == lexeme) {
          state = 9;
          continue;
        }
    }
  }

  if (state != 9 && state != 10) {
    context->error_log.Append(scope_range, next_pos)
        << "Incomplete clause definition; state " << state;
    return;
  }

  const auto is_message_clause =
      DeclarationKind::kMessage == clause->declaration->context->kind;

  // Don't let us send out any messages if we have any uses of this message.
  if (is_message_clause && !decl->context->positive_uses.empty()) {
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
  auto module_clause_list = &(module->clauses);
  auto decl_clause_list = &(clause_decl_context->clauses);

  FindUnrelatedConditions(clause.get(), context->error_log);

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

  // Call Parse Clause Recursively if there was more than one clause
  if (multi_clause) {
    auto end_index = next_sub_tok_index;

    sub_tokens.swap(clause_toks);
    next_sub_tok_index = 0;
    ParseClause(module, decl);

    // NOTE(sonya): restore previous token list and index for debugging in
    // ParseAllTokens()
    sub_tokens.swap(clause_toks);
    next_sub_tok_index = end_index;
  }
}

}  // namespace hyde
