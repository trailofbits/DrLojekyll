// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include "Parse.h"

namespace hyde {
namespace parse {

void *NodeTraverser::Next(void *node, intptr_t offset) {
  return *reinterpret_cast<void **>(reinterpret_cast<intptr_t>(node) + offset);
}

// Compute a unique identifier for this declaration.
uint64_t DeclarationBase::Id(void) const {
  auto &id = context->id;
  if (id.flat) {
    return id.flat;
  }

  id.flat = ParsedModule(module).Id();
  id.info.atom_name_id = head.name.IdentifierId();
  id.info.arity = head.parameters.size();
  assert(head.parameters.size() == id.info.arity);

  // If it's not a local thing, then use `~0` for the ID.
  if (strcmp(context->kind, "local")) {
    id.info.module_id = ~0u;
  }

  return id.flat;
}

// Return a list of clauses associated with this declaration.
ParsedNodeRange<ParsedClause> DeclarationBase::Clauses(void) const {
  if (context->clauses.empty()) {
    return parse::ParsedNodeRange<ParsedClause>();
  } else {
    return parse::ParsedNodeRange<ParsedClause>(context->clauses.front().get());
  }
}

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Winvalid-offsetof"

// Return a list of positive uses of this definition.
parse::ParsedNodeRange<ParsedPredicate>
DeclarationBase::PositiveUses(void) const {
  if (context->positive_uses.empty()) {
    return parse::ParsedNodeRange<ParsedPredicate>();
  } else {
    return parse::ParsedNodeRange<ParsedPredicate>(
        context->positive_uses.front(),
        __builtin_offsetof(parse::Impl<ParsedPredicate>, next_use));
  }
}

// Return a list of negative uses of this definition.
parse::ParsedNodeRange<ParsedPredicate>
DeclarationBase::NegativeUses(void) const {
  if (context->negated_uses.empty()) {
    return parse::ParsedNodeRange<ParsedPredicate>();
  } else {
    return parse::ParsedNodeRange<ParsedPredicate>(
        context->negated_uses.front(),
        __builtin_offsetof(parse::Impl<ParsedPredicate>, next_use));
  }
}

#pragma clang diagnostic pop

// Compute the unique identifier for this variable.
uint64_t Impl<ParsedVariable>::Id(void) noexcept {
  if (id.flat) {
    return id.flat;
  }

  id.flat = clause->Id();
  if (name.Lexeme() == Lexeme::kIdentifierUnnamedVariable) {
    id.info.var_id = clause->next_var_id++;

  } else {
    auto &prev_id = clause->named_var_ids[name.IdentifierId()];
    if (!prev_id) {
      prev_id = clause->next_var_id++;
    }
    id.info.var_id = prev_id;
  }

  return id.flat;
}

// Compute the identifier for this clause.
uint64_t Impl<ParsedPredicate>::Id(void) const noexcept {
  return declaration->Id();
}

// Compute the identifier for this clause.
uint64_t Impl<ParsedClause>::Id(void) const noexcept {
  assert(declaration != nullptr);
  if (!id.flat) {
    id.flat = declaration->Id();
  }
  return id.flat;
}

}  // namespace parse

DisplayRange ParsedVariable::SpellingRange(void) const noexcept {
  return impl->name.SpellingRange();
}

// Returns the token corresponding with the name of this variable.
Token ParsedVariable::Name(void) const noexcept {
  return impl->name;
}

// Returns `true` if this variable is an argument to its clause.
bool ParsedVariable::IsArgument(void) const noexcept {
  return impl->first_use->is_argument;
}

// Returns `true` if this variable is an unnamed variable.
bool ParsedVariable::IsUnnamed(void) const noexcept {
  return impl->name.Lexeme() == Lexeme::kIdentifierUnnamedVariable;
}

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Winvalid-offsetof"
// Uses of this variable within the clause in which it appears.
parse::ParsedNodeRange<ParsedVariable>
ParsedVariable::UsesInClause(void) const noexcept {
  return parse::ParsedNodeRange<ParsedVariable>(
      impl->first_use,
      __builtin_offsetof(parse::Impl<ParsedVariable>, next_use));
}
#pragma clang diagnostic pop

// Return a unique integer that identifies this variable.
uint64_t ParsedVariable::Id(void) const noexcept {
  return impl->Id();
}

DisplayRange ParsedComparison::SpellingRange(void) const noexcept {
  return DisplayRange(impl->lhs->name.Position(),
                      impl->rhs->name.NextPosition());
}

ParsedVariable ParsedComparison::LHS(void) const noexcept {
  return ParsedVariable(impl->lhs);
}

ParsedVariable ParsedComparison::RHS(void) const noexcept {
  return ParsedVariable(impl->rhs);
}

ComparisonOperator ParsedComparison::Operator(void) const noexcept {
  switch (impl->compare_op.Lexeme()) {
    case Lexeme::kPuncEqual:
      return ComparisonOperator::kEqual;
    case Lexeme::kPuncNotEqual:
      return ComparisonOperator::kNotEqual;
    case Lexeme::kPuncLess:
      return ComparisonOperator::kLessThan;
    case Lexeme::kPuncLessEqual:
      return ComparisonOperator::kLessThanEqual;
    case Lexeme::kPuncGreater:
      return ComparisonOperator::kGreaterThan;
    case Lexeme::kPuncGreaterEqual:
      return ComparisonOperator::kGreaterThanEqual;
    default:
      assert(false);
      return ComparisonOperator::kInvalid;
  }
}

DisplayRange ParsedAssignment::SpellingRange(void) const noexcept {
  return DisplayRange(impl->lhs->name.Position(),
                      impl->rhs.literal.NextPosition());
}

ParsedVariable ParsedAssignment::LHS(void) const noexcept {
  return ParsedVariable(impl->lhs);
}

ParsedLiteral ParsedAssignment::RHS(void) const noexcept {
  return ParsedLiteral(&(impl->rhs));
}

DisplayRange ParsedPredicate::SpellingRange(void) const noexcept {
  return DisplayRange(
      impl->negation_pos.IsValid() ? impl->negation_pos : impl->name.Position(),
      impl->rparen.NextPosition());
}

// Returns `true` if this is a negated predicate.
bool ParsedPredicate::IsNegated(void) const noexcept {
  return impl->negation_pos.IsValid();
}

DisplayRange ParsedParameter::SpellingRange(void) const noexcept {
  auto begin = impl->name.Position();
  if (impl->opt_binding.IsValid()) {
    begin = impl->opt_binding.Position();
  } else if (impl->opt_type.IsValid()) {
    begin = impl->opt_type.Position();
  }
  return DisplayRange(begin, impl->name.NextPosition());
}

Token ParsedParameter::Name(void) const noexcept {
  return impl->name;
}

Token ParsedParameter::Type(void) const noexcept {
  return impl->opt_type;
}

ParameterBinding ParsedParameter::Binding(void) const noexcept {
  if (impl->opt_binding.Lexeme() == Lexeme::kKeywordBound) {
    return ParameterBinding::kBound;
  }
  return ParameterBinding::kFree;
}

DisplayRange ParsedDeclaration::SpellingRange(void) const noexcept {
  return DisplayRange(impl->name.Position(),
                  impl->rparen.NextPosition());
}

Token ParsedDeclaration::Name(void) const noexcept {
  return impl->name;
}

parse::ParsedNodeRange<ParsedParameter>
    ParsedDeclaration::Parameters(void) const {
  if (impl->parameters.empty()) {
    return parse::ParsedNodeRange<ParsedParameter>();
  } else {
    return parse::ParsedNodeRange<ParsedParameter>(
        impl->parameters.front().get());
  }
}

ParsedClause ParsedClause::Containing(ParsedVariable var) noexcept {
  return ParsedClause(var.impl->clause);
}

ParsedClause ParsedClause::Containing(ParsedPredicate pred) noexcept {
  return ParsedClause(pred.impl->clause);
}

ParsedClause ParsedClause::Containing(ParsedAssignment assignment) noexcept {
  return ParsedClause(assignment.impl->lhs->clause);
}

ParsedClause ParsedClause::Containing(ParsedComparison compare) noexcept {
  return ParsedClause(compare.impl->lhs->clause);
}

DisplayRange ParsedClause::SpellingRange(void) const noexcept {
  return DisplayRange(impl->name.Position(),
                       impl->dot.NextPosition());
}

// All variables used in the clause. Some variables might be repeated.
parse::ParsedNodeRange<ParsedVariable> ParsedClause::Variables(void) const {
  if (!impl->variables.empty()) {
    return parse::ParsedNodeRange<ParsedVariable>(
        impl->variables.front().get());
  } else {
    return parse::ParsedNodeRange<ParsedVariable>();
  }
}

// All positive positive_predicates in the clause.
parse::ParsedNodeRange<ParsedPredicate>
ParsedClause::PositivePredicates(void) const {
  if (!impl->positive_predicates.empty()) {
    return parse::ParsedNodeRange<ParsedPredicate>(
        impl->positive_predicates.front().get());
  } else {
    return parse::ParsedNodeRange<ParsedPredicate>();
  }
}

// All negated predicates in the clause.
parse::ParsedNodeRange<ParsedPredicate>
ParsedClause::NegatedPredicates(void) const {
  if (!impl->negated_predicates.empty()) {
    return parse::ParsedNodeRange<ParsedPredicate>(
        impl->negated_predicates.front().get());
  } else {
    return parse::ParsedNodeRange<ParsedPredicate>();
  }
}

// All assignments of variables to constant literals.
parse::ParsedNodeRange<ParsedAssignment>
ParsedClause::Assignments(void) const {
  if (!impl->assignments.empty()) {
    return parse::ParsedNodeRange<ParsedAssignment>(
        impl->assignments.front().get());
  } else {
    return parse::ParsedNodeRange<ParsedAssignment>();
  }
}

// All comparisons between two variables.
parse::ParsedNodeRange<ParsedComparison>
ParsedClause::Comparisons(void) const {
  if (!impl->comparisons.empty()) {
    return parse::ParsedNodeRange<ParsedComparison>(
        impl->comparisons.front().get());
  } else {
    return parse::ParsedNodeRange<ParsedComparison>();
  }
}

DisplayRange ParsedQuery::SpellingRange(void) const noexcept {
  return DisplayRange(
      impl->directive_pos, Declaration().SpellingRange().To());
}

ParsedDeclaration ParsedQuery::Declaration(void) const noexcept {
  return ParsedDeclaration(&(impl->head));
}

parse::ParsedNodeRange<ParsedClause> ParsedQuery::Clauses(void) const {
  return impl->Clauses();
}

parse::ParsedNodeRange<ParsedPredicate> ParsedQuery::PositiveUses(void) const {
  return impl->PositiveUses();
}

parse::ParsedNodeRange<ParsedPredicate> ParsedQuery::NegativeUses(void) const {
  return impl->NegativeUses();
}

DisplayRange ParsedExport::SpellingRange(void) const noexcept {
  return DisplayRange(
      impl->directive_pos, Declaration().SpellingRange().To());
}

ParsedDeclaration ParsedExport::Declaration(void) const noexcept {
  return ParsedDeclaration(&(impl->head));
}

parse::ParsedNodeRange<ParsedClause> ParsedExport::Clauses(void) const {
  return impl->Clauses();
}

parse::ParsedNodeRange<ParsedPredicate> ParsedExport::PositiveUses(void) const {
  return impl->PositiveUses();
}

parse::ParsedNodeRange<ParsedPredicate> ParsedExport::NegativeUses(void) const {
  return impl->NegativeUses();
}

DisplayRange ParsedLocal::SpellingRange(void) const noexcept {
  return DisplayRange(impl->directive_pos, Declaration().SpellingRange().To());
}

ParsedDeclaration ParsedLocal::Declaration(void) const noexcept {
  return ParsedDeclaration(&(impl->head));
}

parse::ParsedNodeRange<ParsedPredicate> ParsedLocal::PositiveUses(void) const {
  return impl->PositiveUses();
}

parse::ParsedNodeRange<ParsedPredicate> ParsedLocal::NegativeUses(void) const {
  return impl->NegativeUses();
}

parse::ParsedNodeRange<ParsedClause> ParsedLocal::Clauses(void) const {
  return impl->Clauses();
}

DisplayRange ParsedFunctor::SpellingRange(void) const noexcept {
  return DisplayRange(
      impl->directive_pos, Declaration().SpellingRange().To());
}

ParsedDeclaration ParsedFunctor::Declaration(void) const noexcept {
  return ParsedDeclaration(&(impl->head));
}

parse::ParsedNodeRange<ParsedPredicate> ParsedFunctor::PositiveUses(void) const {
  return impl->PositiveUses();
}

DisplayRange ParsedMessage::SpellingRange(void) const noexcept {
  return DisplayRange(
      impl->directive_pos, Declaration().SpellingRange().To());
}

ParsedDeclaration ParsedMessage::Declaration(void) const noexcept {
  return ParsedDeclaration(&(impl->head));
}

parse::ParsedNodeRange<ParsedClause> ParsedMessage::Clauses(void) const {
  return impl->Clauses();
}

parse::ParsedNodeRange<ParsedPredicate> ParsedMessage::PositiveUses(void) const {
  return impl->PositiveUses();
}

DisplayRange ParsedModule::SpellingRange(void) const noexcept {
  return DisplayRange(impl->first.Position(),
                      impl->last.Position());
}

// Return the ID of this module. Returns `~0u` if not valid.
uint64_t ParsedModule::Id(void) const noexcept {
  parse::IdInterpreter interpreter = {};
  interpreter.info.module_id = impl->first.DisplayId();
  interpreter.info.atom_name_id = ~0u;
  interpreter.info.var_id = ~0u;
  interpreter.info.arity = ~0u;
  return interpreter.flat;
}

parse::ParsedNodeRange<ParsedImport> ParsedModule::Imports(void) const {
  if (impl->imports.empty()) {
    return parse::ParsedNodeRange<ParsedImport>();
  } else {
    return parse::ParsedNodeRange<ParsedImport>(impl->imports.front().get());
  }
}

parse::ParsedNodeRange<ParsedLocal> ParsedModule::Locals(void) const {
  if (impl->locals.empty()) {
    return parse::ParsedNodeRange<ParsedLocal>();
  } else {
    return parse::ParsedNodeRange<ParsedLocal>(impl->locals.front().get());
  }
}

parse::ParsedNodeRange<ParsedExport> ParsedModule::Exports(void) const {
  if (impl->exports.empty()) {
    return parse::ParsedNodeRange<ParsedExport>();
  } else {
    return parse::ParsedNodeRange<ParsedExport>(impl->exports.front().get());
  }
}

parse::ParsedNodeRange<ParsedMessage> ParsedModule::Messages(void) const {
  if (impl->messages.empty()) {
    return parse::ParsedNodeRange<ParsedMessage>();
  } else {
    return parse::ParsedNodeRange<ParsedMessage>(impl->messages.front().get());
  }
}

parse::ParsedNodeRange<ParsedFunctor> ParsedModule::Functors(void) const {
  if (impl->functors.empty()) {
    return parse::ParsedNodeRange<ParsedFunctor>();
  } else {
    return parse::ParsedNodeRange<ParsedFunctor>(impl->functors.front().get());
  }
}

DisplayRange ParsedImport::SpellingRange(void) const noexcept {
  return DisplayRange(impl->directive_pos, impl->path.NextPosition());
}

ParsedModule ParsedImport::ImportedModule(void) const noexcept {
  return ParsedModule(impl->imported_module);
}

}  // namespace hyde
