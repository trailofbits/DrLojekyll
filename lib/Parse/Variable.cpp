// Copyright 2020, Trail of Bits. All rights reserved.

#include "Parser.h"

namespace hyde {

// Create a variable.
Node<ParsedVariable> *ParserImpl::CreateVariable(Node<ParsedClause> *clause,
                                                 Token name, bool is_param,
                                                 bool is_arg) {

  auto var = new Node<ParsedVariable>;
  if (is_param) {
    if (!clause->head_variables.empty()) {
      clause->head_variables.back()->next = var;
    }
    var->appearance = static_cast<unsigned>(clause->head_variables.size());
    clause->head_variables.emplace_back(var);

  } else {
    if (!clause->body_variables.empty()) {
      clause->body_variables.back()->next = var;
    }
    var->appearance =
        static_cast<unsigned>(clause->body_variables.size() + kMaxArity);
    clause->body_variables.emplace_back(var);
  }

  var->name = name;
  var->is_parameter = is_param;
  var->is_argument = is_arg;

  if (Lexeme::kIdentifierVariable == name.Lexeme()) {
    auto &prev = prev_named_var[name.IdentifierId()];
    if (prev) {
      var->context = prev->context;
      prev->next_use = var;

    } else {
      var->context = std::make_shared<parse::VariableContext>(clause, var);
    }

    prev = var;

  // Unnamed variable.
  } else {
    var->context = std::make_shared<parse::VariableContext>(clause, var);
  }

  return var;
}

// Create a variable to name a literal.
Node<ParsedVariable> *
ParserImpl::CreateLiteralVariable(Node<ParsedClause> *clause, Token tok,
                                  bool is_param, bool is_arg) {
  const auto lhs = CreateVariable(
      clause,
      Token::Synthetic(Lexeme::kIdentifierUnnamedVariable, tok.SpellingRange()),
      false, false);

  const auto assign = new Node<ParsedAssignment>(lhs);
  assign->rhs.literal = tok;

  const auto tok_lexeme = tok.Lexeme();

  // Infer the type of the assignment based off the constant.
  if (Lexeme::kIdentifierConstant == tok_lexeme) {
    auto const_ptr = context->foreign_constants[tok.IdentifierId()];
    assert(const_ptr != nullptr);
    assert(const_ptr->parent != nullptr);
    assign->rhs.type = const_ptr->type;
    assign->rhs.foreign_type = const_ptr->parent;

  // Boolean constants bring along their types as well.
  } else if (Lexeme::kLiteralTrue == tok_lexeme ||
             Lexeme::kLiteralFalse == tok_lexeme) {
    assign->rhs.type = TypeLoc(TypeKind::kBoolean, tok.SpellingRange());
  }

  std::string_view data;
  if (context->display_manager.TryReadData(tok.SpellingRange(), &data)) {
    assert(!data.empty());
    assign->rhs.data = data;
  } else {
    switch (tok_lexeme) {
      case Lexeme::kLiteralNumber: assign->rhs.data = "0"; break;
      case Lexeme::kLiteralTrue: assign->rhs.data = "true"; break;
      case Lexeme::kLiteralFalse: assign->rhs.data = "false"; break;
      default: break;
    }
  }
  assign->rhs.assigned_to = lhs;

  // Add to the clause's assignment list.
  if (!clause->assignments.empty()) {
    clause->assignments.back()->next = assign;
  }
  clause->assignments.emplace_back(assign);

  // Add to the variable's assignment list. We support the list, but for
  // these auto-created variables, there can be only one use.
  lhs->context->assignment_uses.push_back(&(assign->lhs));

  // Now create the version of the variable that gets used.
  const auto var = new Node<ParsedVariable>;
  if (is_param) {
    if (!clause->head_variables.empty()) {
      clause->head_variables.back()->next = var;
    }
    var->appearance = static_cast<unsigned>(clause->head_variables.size());
    clause->head_variables.emplace_back(var);

  } else {
    if (!clause->body_variables.empty()) {
      clause->body_variables.back()->next = var;
    }
    var->appearance =
        static_cast<unsigned>(clause->body_variables.size() + kMaxArity);
    clause->body_variables.emplace_back(var);
  }

  var->name = lhs->name;
  var->context = lhs->context;
  var->is_parameter = is_param;
  var->is_argument = is_arg;
  lhs->next_use = var;  // Link it in.

  return var;
}

}  // namespace hyde
