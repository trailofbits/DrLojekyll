// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include "Parse.h"
#include <cstring>
#include <cassert>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Winvalid-offsetof"

namespace hyde {
namespace parse {

void *NodeTraverser::Next(void *node, intptr_t offset) {
  return *reinterpret_cast<void **>(reinterpret_cast<intptr_t>(node) + offset);
}


DisplayRange UseAccessor::GetUseSpellingRange(void *impl_) {
  const auto impl = reinterpret_cast<UseBase *>(impl_);
  return ParsedVariable(impl->used_var).SpellingRange();
}

UseKind UseAccessor::GetUseKind(void *impl_) {
  return reinterpret_cast<UseBase *>(impl_)->use_kind;
}

const void *UseAccessor::GetUser(void *impl_) {
  switch (reinterpret_cast<UseBase *>(impl_)->use_kind) {
    case UseKind::kArgument: {
      auto impl = reinterpret_cast<Impl<ParsedUse<ParsedPredicate>> *>(impl_);
      return &(impl->user);
    }
    case UseKind::kAssignmentLHS: {
      auto impl = reinterpret_cast<Impl<ParsedUse<ParsedAssignment>> *>(impl_);
      return &(impl->user);
    }
    case UseKind::kComparisonLHS: {
      auto impl = reinterpret_cast<Impl<ParsedUse<ParsedComparison>> *>(impl_);
      return &(impl->user);
    }
    case UseKind::kComparisonRHS: {
      auto impl = reinterpret_cast<Impl<ParsedUse<ParsedComparison>> *>(impl_);
      return &(impl->user);
    }
  }
}

const char *Impl<ParsedDeclaration>::KindName(void) const {
  switch (context->kind) {
    case DeclarationKind::kQuery: return "query";
    case DeclarationKind::kMessage: return "message";
    case DeclarationKind::kFunctor: return "functor";
    case DeclarationKind::kExport: return "export";
    case DeclarationKind::kLocal: return "local";
  }
}

// Compute a unique identifier for this declaration.
uint64_t Impl<ParsedDeclaration>::Id(void) const noexcept {
  auto &id = context->id;
  if (id.flat) {
    return id.flat;
  }

  id.flat = ParsedModule(module).Id();
  id.info.atom_name_id = name.IdentifierId();
  id.info.arity = parameters.size();
  assert(parameters.size() == id.info.arity);

  // If it's not a local thing, then use `~0` for the ID.
  if (context->kind == DeclarationKind::kLocal) {
    id.info.module_id = ~0u;
  }

  return id.flat;
}

// Return a list of clauses associated with this declaration.
ParsedNodeRange<ParsedClause> Impl<ParsedDeclaration>::Clauses(void) const {
  if (context->clauses.empty()) {
    return parse::ParsedNodeRange<ParsedClause>();
  } else {
    return parse::ParsedNodeRange<ParsedClause>(
        context->clauses.front().get());
  }
}

// Return a list of positive uses of this definition.
parse::ParsedNodeRange<ParsedPredicate>
Impl<ParsedDeclaration>::PositiveUses(void) const {
  if (context->positive_uses.empty()) {
    return parse::ParsedNodeRange<ParsedPredicate>();
  } else {
    return parse::ParsedNodeRange<ParsedPredicate>(
        context->positive_uses.front(),
        static_cast<intptr_t>(__builtin_offsetof(
            parse::Impl<ParsedPredicate>, next_use_in_clause)));
  }
}

// Return a list of negative uses of this definition.
parse::ParsedNodeRange<ParsedPredicate>
Impl<ParsedDeclaration>::NegativeUses(void) const {
  if (context->negated_uses.empty()) {
    return parse::ParsedNodeRange<ParsedPredicate>();
  } else {
    return parse::ParsedNodeRange<ParsedPredicate>(
        context->negated_uses.front(),
        static_cast<intptr_t>(__builtin_offsetof(
            parse::Impl<ParsedPredicate>, next_use_in_clause)));
  }
}

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

// Returns `true` if this variable is an parameter to its clause.
bool ParsedVariable::IsParameter(void) const noexcept {
  return impl->first_use->is_parameter;
}

// Returns `true` if this variable is an argument to a predicate.
bool ParsedVariable::IsArgument(void) const noexcept {
  return impl->is_argument;
}

// Returns `true` if this variable, or any other used of this variable,
// is assigned to any literals.
bool ParsedVariable::IsAssigned(void) const noexcept {
  return !impl->assignment_uses->empty();
}

// Returns `true` if this variable, or any other used of this variable,
// is assigned to any literals.
bool ParsedVariable::IsCompared() const noexcept {
  return !impl->comparison_uses->empty();
}

// Returns `true` if this variable is an unnamed variable.
bool ParsedVariable::IsUnnamed(void) const noexcept {
  return impl->name.Lexeme() == Lexeme::kIdentifierUnnamedVariable;
}

// Return a unique integer that identifies this variable.
uint64_t ParsedVariable::Id(void) const noexcept {
  return impl->Id();
}

// Return the variable to which `literal` assigned.
ParsedVariable ParsedVariable::AssignedTo(ParsedLiteral literal) noexcept {
  return ParsedVariable(literal.impl->assigned_to);
}

DisplayRange ParsedLiteral::SpellingRange(void) const noexcept {
  return impl->literal.SpellingRange();
}

bool ParsedLiteral::IsNumber(void) const noexcept {
  return impl->literal.Lexeme() == Lexeme::kLiteralNumber;
}

bool ParsedLiteral::IsString(void) const noexcept {
  return impl->literal.Lexeme() == Lexeme::kLiteralString;
}

DisplayRange ParsedComparison::SpellingRange(void) const noexcept {
  return DisplayRange(impl->lhs.used_var->name.Position(),
                      impl->rhs.used_var->name.NextPosition());
}

ParsedVariable ParsedComparison::LHS(void) const noexcept {
  return ParsedVariable(impl->lhs.used_var);
}

ParsedVariable ParsedComparison::RHS(void) const noexcept {
  return ParsedVariable(impl->rhs.used_var);
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
      return ComparisonOperator::kEqual;
  }
}

// Return the list of all comparisons with `var`.
parse::ParsedNodeRange<ParsedComparisonUse>
ParsedComparison::Using(ParsedVariable var) {
  if (var.impl->comparison_uses->empty()) {
    return parse::ParsedNodeRange<ParsedComparisonUse>();
  } else {
    return parse::ParsedNodeRange<ParsedComparisonUse>(
        var.impl->comparison_uses->front());
  }
}

DisplayRange ParsedAssignment::SpellingRange(void) const noexcept {
  return DisplayRange(impl->lhs.used_var->name.Position(),
                      impl->rhs.literal.NextPosition());
}

ParsedVariable ParsedAssignment::LHS(void) const noexcept {
  return ParsedVariable(impl->lhs.used_var);
}

ParsedLiteral ParsedAssignment::RHS(void) const noexcept {
  return ParsedLiteral(&(impl->rhs));
}

// Return the list of all assignments to `var`.
parse::ParsedNodeRange<ParsedAssignmentUse>
ParsedAssignment::Using(ParsedVariable var) {
  if (var.impl->assignment_uses->empty()) {
    return parse::ParsedNodeRange<ParsedAssignmentUse>();
  } else {
    return parse::ParsedNodeRange<ParsedAssignmentUse>(
        var.impl->assignment_uses->front());
  }
}

// Return the list of all uses of `var` as an argument to a predicate.
parse::ParsedNodeRange<ParsedArgumentUse>
ParsedPredicate::Using(ParsedVariable var) {
  if (var.impl->argument_uses->empty()) {
    return parse::ParsedNodeRange<ParsedArgumentUse>();
  } else {
    return parse::ParsedNodeRange<ParsedArgumentUse>(
        var.impl->argument_uses->front());
  }
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

// Returns the arity of this predicate.
unsigned ParsedPredicate::Arity(void) const noexcept {
  return static_cast<unsigned>(impl->argument_uses.size());
}

// Return the `n`th argument of this predicate.
ParsedVariable ParsedPredicate::NthArgument(unsigned n) const noexcept {
  assert(n < Arity());
  return ParsedVariable(impl->argument_uses[n]->used_var);
}

// All variables used as arguments to this predicate.
parse::ParsedNodeRange<ParsedVariable> ParsedPredicate::Arguments(void) const {
  assert(0 < Arity());
  return parse::ParsedNodeRange<ParsedVariable>(
      impl->argument_uses.front()->used_var,
      static_cast<intptr_t>(__builtin_offsetof(
          parse::Impl<ParsedVariable>, next_var_in_arg_list)));
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
  switch (impl->opt_binding.Lexeme()) {
    case Lexeme::kKeywordBound:
      return ParameterBinding::kBound;
    case Lexeme::kKeywordFree:
      return ParameterBinding::kFree;
    case Lexeme::kKeywordAggregate:
      return ParameterBinding::kAggregate;
    case Lexeme::kKeywordSummary:
      return ParameterBinding::kSummary;
    default:
      return ParameterBinding::kImplicit;
  }
}

ParsedDeclaration::ParsedDeclaration(const ParsedQuery &query)
    : parse::ParsedNode<ParsedDeclaration>(query.impl) {}

ParsedDeclaration::ParsedDeclaration(const ParsedMessage &message)
    : parse::ParsedNode<ParsedDeclaration>(message.impl) {}

ParsedDeclaration::ParsedDeclaration(const ParsedFunctor &functor)
    : parse::ParsedNode<ParsedDeclaration>(functor.impl) {}

ParsedDeclaration::ParsedDeclaration(const ParsedExport &exp)
    : parse::ParsedNode<ParsedDeclaration>(exp.impl) {}

ParsedDeclaration::ParsedDeclaration(const ParsedLocal &local)
    : parse::ParsedNode<ParsedDeclaration>(local.impl) {}

DisplayRange ParsedDeclaration::SpellingRange(void) const noexcept {
  return DisplayRange(
      impl->name.Position(),
      (impl->complexity_attribute.IsValid() ?
       impl->complexity_attribute.NextPosition() :
       impl->rparen.NextPosition()));
}

// Return the ID of this declaration.
uint64_t ParsedDeclaration::Id(void) const {
  return impl->Id();
}

uint64_t ParsedFunctor::Id(void) const noexcept {
  return impl->Id();
}

uint64_t ParsedMessage::Id(void) const noexcept {
  return impl->Id();
}

uint64_t ParsedQuery::Id(void) const noexcept {
  return impl->Id();
}

uint64_t ParsedExport::Id(void) const noexcept {
  return impl->Id();
}

uint64_t ParsedLocal::Id(void) const noexcept {
  return impl->Id();
}

Token ParsedDeclaration::Name(void) const noexcept {
  return impl->name;
}

bool ParsedDeclaration::IsQuery(void) const noexcept {
  return Kind() == DeclarationKind::kQuery;
}

bool ParsedDeclaration::IsMessage(void) const noexcept {
  return Kind() == DeclarationKind::kMessage;
}

bool ParsedDeclaration::IsFunctor(void) const noexcept {
  return Kind() == DeclarationKind::kFunctor;
}

bool ParsedDeclaration::IsExport(void) const noexcept {
  return Kind() == DeclarationKind::kExport;
}

bool ParsedDeclaration::IsLocal(void) const noexcept {
  return Kind() == DeclarationKind::kLocal;
}

// The kind of this declaration.
DeclarationKind ParsedDeclaration::Kind(void) const noexcept {
  return impl->context->kind;
}

// The string version of this kind name.
const char *ParsedDeclaration::KindName(void) const noexcept {
  return impl->KindName();
}

// Returns the arity of this clause.
unsigned ParsedDeclaration::Arity(void) const noexcept {
  return static_cast<unsigned>(impl->parameters.size());
}

// Return the `n`th parameter of this clause.
ParsedParameter ParsedDeclaration::NthParameter(unsigned n) const noexcept {
  assert(n < Arity());
  return ParsedParameter(impl->parameters[n].get());
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

parse::ParsedNodeRange<ParsedDeclaration>
ParsedDeclaration::Redeclarations(void) const {
  return parse::ParsedNodeRange<ParsedDeclaration>(
      reinterpret_cast<parse::Impl<ParsedDeclaration> *>(
          impl->context->redeclarations.front()),
      static_cast<intptr_t>(__builtin_offsetof(
          parse::Impl<ParsedDeclaration>, next_redecl)));
}

parse::ParsedNodeRange<ParsedClause> ParsedDeclaration::Clauses(void) const {
  return impl->Clauses();
}

parse::ParsedNodeRange<ParsedPredicate>
ParsedDeclaration::PositiveUses(void) const {
  return impl->PositiveUses();
}

parse::ParsedNodeRange<ParsedPredicate>
ParsedDeclaration::NegativeUses(void) const {
  return impl->NegativeUses();
}

unsigned ParsedDeclaration::NumPositiveUses(void) const noexcept {
  return static_cast<unsigned>(impl->context->positive_uses.size());
}

unsigned ParsedDeclaration::NumNegatedUses(void) const noexcept {
  return static_cast<unsigned>(impl->context->negated_uses.size());
}

// Return the declaration associated with a clause. This is the first
// parsed declaration, so it could be in a different module.
ParsedDeclaration ParsedDeclaration::Of(ParsedClause clause) {
  return ParsedDeclaration(clause.impl->declaration);
}

// Return the declaration associated with a predicate. This is the first
// parsed declaration, so it could be in a different module.
ParsedDeclaration ParsedDeclaration::Of(ParsedPredicate pred) {
  return ParsedDeclaration(pred.impl->declaration);
}

ParsedClause ParsedClause::Containing(ParsedVariable var) noexcept {
  return ParsedClause(var.impl->clause);
}

ParsedClause ParsedClause::Containing(ParsedPredicate pred) noexcept {
  return ParsedClause(pred.impl->clause);
}

ParsedClause ParsedClause::Containing(ParsedAssignment assignment) noexcept {
  return ParsedClause(assignment.impl->lhs.used_var->clause);
}

ParsedClause ParsedClause::Containing(ParsedComparison compare) noexcept {
  return ParsedClause(compare.impl->lhs.used_var->clause);
}

// Return the total number of uses of `var` in its clause body.
unsigned ParsedClause::NumUsesInBody(ParsedVariable var) noexcept {
  return static_cast<unsigned>(var.impl->assignment_uses->size() +
                               var.impl->comparison_uses->size() +
                               var.impl->argument_uses->size());
}

DisplayRange ParsedClause::SpellingRange(void) const noexcept {
  return DisplayRange(impl->name.Position(),
                       impl->dot.NextPosition());
}

// Returns the arity of this clause.
unsigned ParsedClause::Arity(void) const noexcept {
  return static_cast<unsigned>(impl->head_variables.size());
}

// Return the `n`th parameter of this clause.
ParsedVariable ParsedClause::NthParameter(unsigned n) const noexcept {
  assert(n < Arity());
  return ParsedVariable(impl->head_variables[n].get());
}

// All variables used in the body of the clause.
parse::ParsedNodeRange<ParsedVariable> ParsedClause::Parameters(void) const {
  assert(!impl->head_variables.empty());
  return parse::ParsedNodeRange<ParsedVariable>(
      impl->head_variables.front().get());
}

// All body_variables used in the clause. Some variables might be repeated.
parse::ParsedNodeRange<ParsedVariable> ParsedClause::Variables(void) const {
  assert(!impl->body_variables.empty());
  return parse::ParsedNodeRange<ParsedVariable>(
      impl->body_variables.front().get());
}

// All instances of `var` in its clause.
parse::ParsedNodeRange<ParsedVariable> ParsedClause::Uses(ParsedVariable var) {
  return parse::ParsedNodeRange<ParsedVariable>(
      var.impl->first_use,
      static_cast<intptr_t>(__builtin_offsetof(
          parse::Impl<ParsedVariable>, next_use_in_clause)));
}

// All positive predicates in the clause.
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
  if (!impl->assignment_uses.empty()) {
    return parse::ParsedNodeRange<ParsedAssignment>(
        impl->assignment_uses.front().get());
  } else {
    return parse::ParsedNodeRange<ParsedAssignment>();
  }
}

// All comparisons between two variables.
parse::ParsedNodeRange<ParsedComparison>
ParsedClause::Comparisons(void) const {
  if (!impl->comparison_uses.empty()) {
    return parse::ParsedNodeRange<ParsedComparison>(
        impl->comparison_uses.front().get());
  } else {
    return parse::ParsedNodeRange<ParsedComparison>();
  }
}

const ParsedQuery &ParsedQuery::From(const ParsedDeclaration &decl) {
  assert(decl.IsQuery());
  return reinterpret_cast<const ParsedQuery &>(decl);
}

DisplayRange ParsedQuery::SpellingRange(void) const noexcept {
  return DisplayRange(
      impl->directive_pos, impl->rparen.NextPosition());
}

parse::ParsedNodeRange<ParsedQuery> ParsedQuery::Redeclarations(void) const {
  return parse::ParsedNodeRange<ParsedQuery>(
      reinterpret_cast<parse::Impl<ParsedQuery> *>(
          impl->context->redeclarations.front()),
      static_cast<intptr_t>(__builtin_offsetof(
          parse::Impl<ParsedQuery>, next_redecl)));
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

unsigned ParsedQuery::NumPositiveUses(void) const noexcept {
  return static_cast<unsigned>(impl->context->positive_uses.size());
}

unsigned ParsedQuery::NumNegatedUses(void) const noexcept {
  return static_cast<unsigned>(impl->context->negated_uses.size());
}

const ParsedExport &ParsedExport::From(const ParsedDeclaration &decl) {
  assert(decl.IsExport());
  return reinterpret_cast<const ParsedExport &>(decl);
}

DisplayRange ParsedExport::SpellingRange(void) const noexcept {
  return DisplayRange(
      impl->directive_pos, impl->rparen.NextPosition());
}

parse::ParsedNodeRange<ParsedExport> ParsedExport::Redeclarations(void) const {
  return parse::ParsedNodeRange<ParsedExport>(
      reinterpret_cast<parse::Impl<ParsedExport> *>(
          impl->context->redeclarations.front()),
      static_cast<intptr_t>(__builtin_offsetof(
          parse::Impl<ParsedExport>, next_redecl)));
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

unsigned ParsedExport::NumPositiveUses(void) const noexcept {
  return static_cast<unsigned>(impl->context->positive_uses.size());
}

unsigned ParsedExport::NumNegatedUses(void) const noexcept {
  return static_cast<unsigned>(impl->context->negated_uses.size());
}

const ParsedLocal &ParsedLocal::From(const ParsedDeclaration &decl) {
  assert(decl.IsLocal());
  return reinterpret_cast<const ParsedLocal &>(decl);
}

DisplayRange ParsedLocal::SpellingRange(void) const noexcept {
  return DisplayRange(
      impl->directive_pos, impl->rparen.NextPosition());
}

parse::ParsedNodeRange<ParsedLocal>
ParsedLocal::Redeclarations(void) const {
  return parse::ParsedNodeRange<ParsedLocal>(
      reinterpret_cast<parse::Impl<ParsedLocal> *>(
          impl->context->redeclarations.front()),
      static_cast<intptr_t>(__builtin_offsetof(
          parse::Impl<ParsedLocal>, next_redecl)));
}

parse::ParsedNodeRange<ParsedPredicate> ParsedLocal::PositiveUses(void) const {
  return impl->PositiveUses();
}

parse::ParsedNodeRange<ParsedPredicate> ParsedLocal::NegativeUses(void) const {
  return impl->NegativeUses();
}

unsigned ParsedLocal::NumPositiveUses(void) const noexcept {
  return static_cast<unsigned>(impl->context->positive_uses.size());
}

unsigned ParsedLocal::NumNegatedUses(void) const noexcept {
  return static_cast<unsigned>(impl->context->negated_uses.size());
}

parse::ParsedNodeRange<ParsedClause> ParsedLocal::Clauses(void) const {
  return impl->Clauses();
}

bool ParsedFunctor::IsComplex(void) const noexcept {
  return impl->complexity_attribute.Lexeme() == Lexeme::kKeywordComplex;
}

bool ParsedFunctor::IsTrivial(void) const noexcept {
  return impl->complexity_attribute.Lexeme() == Lexeme::kKeywordTrivial;
}

const ParsedFunctor &ParsedFunctor::From(const ParsedDeclaration &decl) {
  assert(decl.IsFunctor());
  return reinterpret_cast<const ParsedFunctor &>(decl);
}

DisplayRange ParsedFunctor::SpellingRange(void) const noexcept {
  return DisplayRange(
      impl->directive_pos, impl->complexity_attribute.NextPosition());
}

parse::ParsedNodeRange<ParsedFunctor>
ParsedFunctor::Redeclarations(void) const {
  return parse::ParsedNodeRange<ParsedFunctor>(
      reinterpret_cast<parse::Impl<ParsedFunctor> *>(
          impl->context->redeclarations.front()),
      static_cast<intptr_t>(__builtin_offsetof(
          parse::Impl<ParsedFunctor>, next_redecl)));
}

parse::ParsedNodeRange<ParsedPredicate>
ParsedFunctor::PositiveUses(void) const {
  return impl->PositiveUses();
}

unsigned ParsedFunctor::NumPositiveUses(void) const noexcept {
  return static_cast<unsigned>(impl->context->positive_uses.size());
}

const ParsedMessage &ParsedMessage::From(const ParsedDeclaration &decl) {
  assert(decl.IsMessage());
  return reinterpret_cast<const ParsedMessage &>(decl);
}

DisplayRange ParsedMessage::SpellingRange(void) const noexcept {
  return DisplayRange(
      impl->directive_pos, impl->rparen.NextPosition());
}

parse::ParsedNodeRange<ParsedMessage>
ParsedMessage::Redeclarations(void) const {
  return parse::ParsedNodeRange<ParsedMessage>(
      reinterpret_cast<parse::Impl<ParsedMessage> *>(
          impl->context->redeclarations.front()),
      static_cast<intptr_t>(__builtin_offsetof(
          parse::Impl<ParsedMessage>, next_redecl)));
}

parse::ParsedNodeRange<ParsedClause> ParsedMessage::Clauses(void) const {
  return impl->Clauses();
}

parse::ParsedNodeRange<ParsedPredicate>
ParsedMessage::PositiveUses(void) const {
  return impl->PositiveUses();
}

unsigned ParsedMessage::NumPositiveUses(void) const noexcept {
  return static_cast<unsigned>(impl->context->positive_uses.size());
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

parse::ParsedNodeRange<ParsedQuery> ParsedModule::Queries(void) const {
  if (impl->queries.empty()) {
    return parse::ParsedNodeRange<ParsedQuery>();
  } else {
    return parse::ParsedNodeRange<ParsedQuery>(impl->queries.front().get());
  }
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

parse::ParsedNodeRange<ParsedClause> ParsedModule::Clauses(void) const {
  if (impl->clauses.empty()) {
    return parse::ParsedNodeRange<ParsedClause>();
  } else {
    return parse::ParsedNodeRange<ParsedClause>(impl->clauses.front());
  }
}

DisplayRange ParsedImport::SpellingRange(void) const noexcept {
  return DisplayRange(impl->directive_pos, impl->path.NextPosition());
}

ParsedModule ParsedImport::ImportedModule(void) const noexcept {
  return ParsedModule(impl->imported_module);
}

}  // namespace hyde

#pragma clang diagnostic pop