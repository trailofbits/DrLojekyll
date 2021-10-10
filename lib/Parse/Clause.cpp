// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/Util/DisjointSet.h>

#include "Parser.h"

namespace hyde {
namespace {

// Go through the variables used in the clause and try to identify groups of
// predicates that should be extracted into boolean conditions (zero-argument
// clauses). It's simpler to demand that the writer of the code factor out these
// conditions instead of having us do it automatically.
static void FindUnrelatedConditions(ParsedClauseImpl *clause,
                                    const ErrorLog &log) {

  std::vector<std::unique_ptr<DisjointSet>> sets;
  std::unordered_map<uint64_t, DisjointSet *> var_to_set;
  std::unordered_map<ParsedPredicateImpl *, DisjointSet *> pred_to_set;

  auto next_id = 0u;
  const auto clause_head_id = next_id;
  const auto clause_set = new DisjointSet(next_id++);
  sets.emplace_back(clause_set);
  for (ParsedVariableImpl * const var : clause->head_variables) {
    var_to_set.emplace(var->Id(), clause_set);
  }

  auto do_pred = [&](ParsedPredicateImpl *pred, DisjointSet *set) {

    // Happens to be a convenient place to check.
    if (pred->declaration->context->kind == DeclarationKind::kMessage) {
      clause->depends_on_messages = true;
    }

    // This predicate has zero arguments, i.e. it is a condition already.
    // Associated it with the clause head set.
    if (pred->argument_uses.Empty()) {
      if (set) {
        set = DisjointSet::Union(set, clause_set);
      } else {
        set = clause_set;
      }
    }

    for (ParsedVariableImpl *var : pred->argument_uses) {
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

  for (ParsedPredicateImpl *pred : clause->positive_predicates) {
    do_pred(pred, nullptr);
  }

  for (ParsedPredicateImpl *pred : clause->negated_predicates) {
    do_pred(pred, nullptr);
  }

  for (ParsedAggregateImpl * const agg : clause->aggregates) {
    const auto set = new DisjointSet(next_id++);
    sets.emplace_back(set);
    do_pred(&(agg->predicate), set);
    do_pred(&(agg->functor), set);
  }

  for (ParsedAssignmentImpl * const assign : clause->assignments) {
    ParsedVariableImpl *lhs_var = assign->lhs.get();
    auto &set = var_to_set[lhs_var->Id()];
    if (!set) {
      set = new DisjointSet(next_id++);
      sets.emplace_back(set);
    }
  }

  for (ParsedComparisonImpl * const cmp : clause->comparisons) {
    ParsedVariableImpl *lhs_var = cmp->lhs.get();
    ParsedVariableImpl *rhs_var = cmp->rhs.get();

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
  } else if (conditions.size() == 1 && clause->head_variables.Empty()) {
    return;
  }

  const auto clause_range = ParsedClause(clause).SpellingRange();
  for (auto cond_set : conditions) {
    auto err = log.Append(clause_range);
    err << "The following elements in the body of this clause should be "
        << "factored out into a zero-argument predicate";

    for (ParsedPredicateImpl *pred : clause->positive_predicates) {
      if (pred_to_set[pred]->Find() == cond_set) {
        err.Note(clause_range, ParsedPredicate(pred).SpellingRange())
            << "This predicate";
      }
    }

    for (ParsedPredicateImpl *pred : clause->negated_predicates) {
      if (pred_to_set[pred]->Find() == cond_set) {
        err.Note(clause_range, ParsedPredicate(pred).SpellingRange())
            << "This negated predicate";
      }
    }

    for (ParsedAssignmentImpl *assign : clause->assignments) {
      ParsedVariableImpl *lhs_var = assign->lhs.get();
      if (var_to_set[lhs_var->Id()] == cond_set) {
        err.Note(clause_range, ParsedAssignment(assign).SpellingRange())
            << "This assignment";
      }
    }

    for (ParsedComparisonImpl *cmp : clause->comparisons) {
      ParsedVariableImpl *lhs_var = cmp->lhs.get();
      if (var_to_set[lhs_var->Id()] == cond_set) {
        err.Note(clause_range, ParsedComparison(cmp).SpellingRange())
            << "This comparison";
      }
    }
  }
}

}  // namespace

// Try to parse `sub_range` as a clause.
void ParserImpl::ParseClause(ParsedModuleImpl *module,
                             ParsedDeclarationImpl *decl) {

  ParsedClauseImpl *clause = module->clauses.Create(module);
  clause->declaration = decl;

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
  ParsedVariableImpl *arg = nullptr;
  ParsedVariableImpl *lhs = nullptr;
  ParsedVariableImpl *rhs = nullptr;
  Token compare_op;

  Token pred_name;

  ParsedDeclarationImpl *pred_decl = nullptr;
  std::vector<ParsedVariableImpl *> pred_vars;
  ParsedPredicateImpl *pred = nullptr;
  ParsedAggregateImpl *agg = nullptr;

  // Link `pred` into `clause`.
  auto link_pred = [&](void) {
    assert(pred_decl != nullptr);

    if (negation_tok.IsValid() &&
        negation_tok.Lexeme() == Lexeme::kPragmaPerfNever) {
      if (pred_decl->context->kind == DeclarationKind::kFunctor) {
        context->error_log.Append(
            scope_range, ParsedPredicate(pred).SpellingRange())
            << "Functor applications cannot be negated with '@never'";

      } else if (pred->argument_uses.Empty()) {
        context->error_log.Append(
            scope_range, ParsedPredicate(pred).SpellingRange())
            << "Zero-argument predicates cannot be negated with '@never'";
      }
    }

    if (!pred) {
      assert(!agg);
      if (negation_tok.IsValid()) {
        pred = clause->negated_predicates.Create(module, clause);
        pred->negation = negation_tok;
        pred_decl->context->negated_uses.AddUse(pred);

      } else {
        pred = clause->positive_predicates.Create(module, clause);
        pred_decl->context->positive_uses.AddUse(pred);
      }
    } else {
      assert(agg != nullptr);
    }

    pred->name = pred_name;
    pred->rparen = tok;
    pred->declaration = pred_decl;
    for (ParsedVariableImpl *pred_var : pred_vars) {
      pred->argument_uses.AddUse(pred_var);
    }

    pred_name = Token();
    negation_tok = Token();
    pred_decl = nullptr;
    pred = nullptr;
    pred_vars.clear();
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

          } else if (!TryMatchClauseWithDecl(module, clause)) {
            return;

          } else {
            decl = clause->declaration;
            assert(decl != nullptr);
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
          (void) CreateVariable(clause, tok, true, false);
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
          (void) CreateLiteralVariable(clause, tok, true, false);
          state = 3;
          continue;

        // Kick-start type inference when using a named constant.
        } else if (Lexeme::kIdentifierConstant == lexeme) {
          auto unnamed_var =
              CreateLiteralVariable(clause, tok, true, false);
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
        clause_toks.push_back(tok);  // Add token even if we error.

        // Advance to the next clause parameter.
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

          // `TryMatchClauseWithDecl` failed and will have reported an error.
          } else if (!TryMatchClauseWithDecl(module, clause)) {
            assert(0 < context->error_log.Size());
            return;

          // We matched it against a clasue head.
          } else {
            decl = clause->declaration;
            assert(decl != nullptr);
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
          lhs = CreateVariable(clause, tok, false, false);
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
                lhs = CreateLiteralVariable(clause, tok, false, false);
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
                lhs = CreateLiteralVariable(clause, tok, false, false);
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
          lhs = CreateLiteralVariable(clause, tok, false, false);
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
          pred_name = tok;
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

          const auto assign = clause->assignments.Create(lhs);

          assign->rhs.literal =
              Token::Synthetic(Lexeme::kLiteralTrue, DisplayRange());
          assign->rhs.data = "true";
          assign->rhs.type =
              TypeLoc(TypeKind::kBoolean, lhs->name.SpellingRange());

          pred = nullptr;

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
            auto assign = clause->assignments.Create(lhs);
            assign->rhs.literal = tok;
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

            state = 8;
            continue;

          } else {
            rhs = CreateLiteralVariable(clause, tok, false, false);
          }

        } else if (Lexeme::kIdentifierVariable == lexeme) {
          rhs = CreateVariable(clause, tok, false, false);
        }

        if (rhs) {

          // Don't allow comparisons against the same named variable. This
          // simplifies later checks, and makes sure that iteration over the
          // comparisons containing a given variable are well-founded.
          if (lhs->first_appearance == rhs->first_appearance) {
            const DisplayRange assign_range(lhs->name.Position(),
                                            rhs->name.NextPosition());
            context->error_log.Append(scope_range, assign_range)
                << "Variable '" << lhs->name
                << "' cannot appear on both sides of a comparison";
            return;
          }

          (void) clause->comparisons.Create(lhs, rhs, compare_op);
          state = 8;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected variable name or number/string literal, but got '"
              << tok << "' instead";
          return;
        }

      case 8:
        pred = nullptr;
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

        } else if (Lexeme::kKeywordOver == lexeme) {

          // We parsed the predicate for the aggregating functor.
          if (agg) {
            if (!ParseAggregatedPredicate(module, clause, agg,
                                          tok, next_pos)) {
              return;

            } else {
              agg = nullptr;
              state = 8;
              continue;
            }
          } else {
            context->error_log.Append(scope_range, tok_range)
                << "Expected comma or period, but got '" << tok << "' instead";
            return;
          }

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
          pred_name = tok;
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
          lhs = CreateVariable(clause, tok, false, false);
          ParsedAssignmentImpl *const assign = clause->assignments.Create(lhs);
          assign->rhs.literal =
              Token::Synthetic(Lexeme::kLiteralFalse, DisplayRange());
          assign->rhs.data = "false";
          assign->rhs.type =
              TypeLoc(TypeKind::kBoolean,
                      DisplayRange(negation_tok.Position(), tok.NextPosition()));
          state = 8;
          continue;

        // `!true`, i.e. `false`, this gets treated as `false`.
        } else if (Lexeme::kLiteralTrue == lexeme) {
          clause->disabled_by = DisplayRange(negation_tok.Position(),
                                             tok.NextPosition());
          state = 8;
          continue;

        // `!false`, i.e. `true`, we ignore this, and we anticipate either a
        // comma to continue the clause or a period to end the clause.
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
        assert(pred_name.IsValid());

        // Invoking an N-ary predicate.
        if (Lexeme::kPuncOpenParen == lexeme) {
          state = 13;
          continue;

        // Ending a clause with a zero-argument predicate.
        } else if (Lexeme::kPuncPeriod == lexeme) {
          clause->dot = tok;
          pred_decl = TryMatchPredicateWithDecl(
              module, pred_name, pred_vars, pred_name);
          if (pred_decl) {
            link_pred();
          }
          state = 9;

          continue;

        // Continue the clause after a zero-argument predicate.
        } else if (Lexeme::kPuncComma == lexeme) {
          pred_decl = TryMatchPredicateWithDecl(
              module, pred_name, pred_vars, pred_name);
          if (pred_decl) {
            link_pred();
          }
          state = 5;
          continue;

        // It is a zero-argument predicate, ending this clause body, and
        // followed by the beginning of a new clause body.
        } else if (Lexeme::kPuncColon == lexeme) {
          clause->dot = tok;
          pred_decl = TryMatchPredicateWithDecl(
              module, pred_name, pred_vars, pred_name);
          if (pred_decl) {
            link_pred();
          }

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
          arg = CreateLiteralVariable(clause, tok, false, true);

        } else if (Lexeme::kIdentifierVariable == lexeme ||
                   Lexeme::kIdentifierUnnamedVariable == lexeme) {
          arg = CreateVariable(clause, tok, false, true);
        }

        if (arg) {
          assert(!pred_decl);
          pred_vars.push_back(arg);
          state = 14;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected variable or literal here as argument to predicate '"
              << pred_name << "', but got '" << tok << "' instead";
          return;
        }

      case 14:
        assert(!pred);
        assert(!agg);

        if (Lexeme::kPuncCloseParen == lexeme) {
          DisplayRange pred_range(pred_name.Position(),
                                  tok.NextPosition());

          pred_decl = TryMatchPredicateWithDecl(
                module, pred_name, pred_vars, tok);

          if (!pred_decl) {
            return;
          }

          // Check to see if we're doing an aggregation.
          if (Token peek_tok; ReadNextSubToken(peek_tok)) {
            UnreadSubToken();
            if (peek_tok.Lexeme() == Lexeme::kKeywordOver) {
              agg = clause->aggregates.Create(clause);
              pred = &(agg->functor);
              pred_decl->context->positive_uses.AddUse(pred);

              // We need to be using a functor for aggregations.
              if (pred_decl->context->kind != DeclarationKind::kFunctor) {
                auto err = context->error_log.Append(scope_range, pred_range);
                err << "Cannot use non-functor predicate '" << pred_name
                    << "/" << pred_vars.size() << "' as an aggregating functor";

                err.Note(scope_range, peek_tok.SpellingRange())
                    << "Used as an aggregating functor here";

              // Functors used for aggregations must actually be aggregating
              // functors.
              } else if (!pred_decl->is_aggregate) {
                auto err = context->error_log.Append(scope_range, pred_range);
                err << "Cannot use " << pred_decl->KindName() << " '" << pred_name
                    << "/" << pred_vars.size() << "' as an aggregating functor";

                err.Note(ParsedDeclaration(pred_decl).SpellingRange())
                    << "Functor declared here has no 'summarize' or 'aggregate'"
                    << " parameters";

                err.Note(scope_range, peek_tok.SpellingRange())
                    << "Used as an aggregating functor here";

              // We are not allowed to negate an aggregating functor, as that
              // makes no sense.
              } else if (negation_tok.IsValid()) {
                DisplayRange neg_pred_range(negation_tok.Position(),
                                            tok.NextPosition());
                context->error_log.Append(scope_range, neg_pred_range)
                    << "Cannot negate use of aggregating functor '"
                    << pred_name << "/" << pred_vars.size() << "'";
              }
            }
          }

          // If it's an aggregating functor then we need to follow-up with
          // the `over` keyword.
          if (pred_decl->is_aggregate && !agg) {
            context->error_log.Append(scope_range, pred_range,
                                      tok.NextPosition())
                << "Must follow use of aggregating functor  '" << pred_name
                << "/" << pred_vars.size() << "' with an 'over' keyword";
          }

          // Not allowed to negate inline declarations, as they might not be
          // backed by actual relations.
          if (negation_tok.IsValid() &&
              pred_decl->inline_attribute.IsValid()) {
            auto err = context->error_log.Append(scope_range, pred_range);
            err << "Cannot negate " << pred_decl->KindName() << " '"
                << pred_name << "' because it has been marked as inline";

            err.Note(ParsedDeclaration(pred_decl).SpellingRange(),
                     pred_decl->inline_attribute.SpellingRange())
                << "Marked as inline here";
          }

          // A functor with a range of one-to-one or one-or-more is guaranteed
          // to produce at least one output, and so negating it would yield
          // and always-false situation.
          if (negation_tok.IsValid() &&
              pred_decl->context->kind == DeclarationKind::kFunctor &&
              (pred_decl->range == FunctorRange::kOneToOne ||
               pred_decl->range == FunctorRange::kOneOrMore)) {
            auto err = context->error_log.Append(scope_range, pred_range);
            err << "Cannot negate functor '" << pred_name
                << "' declared with a one-to-one or one-or-more range";

            err.Note(
                ParsedDeclaration(pred_decl).SpellingRange(),
                DisplayRange(pred_decl->range_begin_opt.Position(),
                             pred_decl->range_end_opt.NextPosition()))
                << "Range of functor '" << pred_name
                << "' is specified here";
          }

          link_pred();

          state = 8;
          continue;

        // Go read another parameter.
        } else if (Lexeme::kPuncComma == lexeme) {
          state = 13;
          continue;

        } else {
          context->error_log.Append(scope_range, tok_range)
              << "Expected comma or period, but got '" << tok << "' instead";
          return;
        }

      case 15:
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

  // Link the clause in to its respective declaration.
  assert(clause->declaration == decl);
  const auto clause_decl_context = decl->context.get();
  clause_decl_context->clauses.AddUse(clause);

  for (ParsedVariableImpl *var : clause->head_variables) {
    // NOTE(pag): This has a side-effect of filling in
    //            `var->first_appearance->id`.
    var->id.flat = var->Id();
  }
  for (ParsedVariableImpl *var : clause->body_variables) {
    // NOTE(pag): This has a side-effect of filling in
    //            `var->first_appearance->id`.
    var->id.flat = var->Id();
  }

  const auto is_message_clause =
      DeclarationKind::kMessage == clause_decl_context->kind;

  // Don't let us publish out any messages if we have any receipts
  // of this message.
  if (is_message_clause &&
      (!clause_decl_context->positive_uses.Empty() ||
       !clause_decl_context->negated_uses.Empty())) {
    auto err = context->error_log.Append(scope_range);
    err << "Cannot send output in message " << decl->name << '/'
        << decl->parameters.Size()
        << "; the message is already used for receiving data";

    for (ParsedPredicateImpl *pred_ : decl->context->positive_uses) {
      auto pred = ParsedPredicate(pred_);
      auto clause = ParsedClause::Containing(pred);
      err.Note(clause.SpellingRange(), pred.SpellingRange())
          << "Message receipt is here";
    }
  }

  FindUnrelatedConditions(clause, context->error_log);

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
