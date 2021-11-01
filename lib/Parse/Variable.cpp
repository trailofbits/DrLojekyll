// Copyright 2020, Trail of Bits. All rights reserved.

#include "Parser.h"

namespace hyde {

// Create a variable.
ParsedVariableImpl *ParserImpl::CreateVariable(ParsedClauseImpl *clause,
                                               Token name, bool is_param,
                                               bool is_arg) {
  ParsedVariableImpl *var = nullptr;

  if (is_param) {
    var = clause->head_variables.Create(clause, name, true, false);
    var->order_of_appearance = clause->head_variables.Size() - 1u;

  } else {
    var = clause->body_variables.Create(clause, name, false, is_arg);
    var->order_of_appearance = clause->body_variables.Size() + kMaxArity;
  }

  if (Lexeme::kIdentifierVariable == name.Lexeme()) {
    auto &prev = prev_named_var[name.IdentifierId()];
    if (prev) {
      assert(!prev->next_appearance);
      prev->next_appearance = var;
      var->first_appearance = prev->first_appearance;

    } else {
      var->first_appearance = var;
    }

    prev = var;

  // Unnamed variable.
  } else {
    var->first_appearance = var;
  }

  return var;
}

// Create a variable to name a literal.
ParsedVariableImpl *
ParserImpl::CreateLiteralVariable(ParsedClauseImpl *clause, Token tok,
                                  bool is_param, bool is_arg) {
  const auto lhs = CreateVariable(
      clause,
      Token::Synthetic(Lexeme::kIdentifierUnnamedVariable, tok.SpellingRange()),
      false, false);

  const auto assign = clause->groups.back()->assignments.Create(lhs);
  assign->rhs.literal = tok;

  const auto tok_lexeme = tok.Lexeme();

  // Infer the type of the assignment based off the constant.
  if (Lexeme::kIdentifierConstant == tok_lexeme) {
    auto const_ptr = context->foreign_constants[tok.IdentifierId()];
    assert(const_ptr != nullptr);
    assert(const_ptr->parent != nullptr);

    lhs->type = const_ptr->type;

    assign->rhs.type = const_ptr->type;
    assign->rhs.foreign_type = const_ptr->parent;
    assign->rhs.foreign_constant = const_ptr;

  // Boolean constants bring along their types as well.
  } else if (Lexeme::kLiteralTrue == tok_lexeme) {
    lhs->type = TypeLoc(TypeKind::kBoolean, tok.SpellingRange());
    assign->rhs.type = lhs->type;
    assign->rhs.data = "true";

  } else if (Lexeme::kLiteralFalse == tok_lexeme) {
    lhs->type = TypeLoc(TypeKind::kBoolean, tok.SpellingRange());
    assign->rhs.type = lhs->type;
    assign->rhs.data = "false";
  }

  if (assign->rhs.data.empty()) {
    std::string_view data;
    if (context->display_manager.TryReadData(tok.SpellingRange(), &data)) {
      assert(!data.empty());
      assign->rhs.data = data;

    } else if (Lexeme::kLiteralNumber == tok_lexeme) {
      assign->rhs.data = "0";
    }
  }

  // Now create the version of the variable that gets used.
  ParsedVariableImpl *var = nullptr;
  if (is_param) {
    var = clause->head_variables.Create(clause, lhs->name, true, false);
    var->order_of_appearance = clause->head_variables.Size() - 1u;

  } else {
    var = clause->body_variables.Create(clause, lhs->name, false, is_arg);
    var->order_of_appearance = clause->body_variables.Size() + kMaxArity;
  }

  assert(lhs->first_appearance == lhs);
  assert(!lhs->next_appearance);
  lhs->next_appearance = var;

  var->first_appearance = lhs->first_appearance;
  var->type = lhs->type;

  return var;
}

}  // namespace hyde
