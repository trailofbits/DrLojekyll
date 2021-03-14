// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include "Parse.h"

#include <cassert>
#include <cstring>

#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Winvalid-offsetof"
#endif

namespace hyde {
namespace parse {

DisplayRange UseAccessor::GetUseSpellingRange(void *impl_) {
  const auto impl = reinterpret_cast<UseBase *>(impl_);
  return ParsedVariable(impl->used_var).SpellingRange();
}

UseKind UseAccessor::GetUseKind(void *impl_) {
  return reinterpret_cast<UseBase *>(impl_)->use_kind;
}

const void *UseAccessor::GetUser(void *impl_) {
  switch (reinterpret_cast<UseBase *>(impl_)->use_kind) {
    case UseKind::kParameter: {
      auto impl = reinterpret_cast<Node<ParsedUse<ParsedClause>> *>(impl_);
      return &(impl->user);
    }
    case UseKind::kArgument: {
      auto impl = reinterpret_cast<Node<ParsedUse<ParsedPredicate>> *>(impl_);
      return &(impl->user);
    }
    case UseKind::kAssignmentLHS: {
      auto impl = reinterpret_cast<Node<ParsedUse<ParsedAssignment>> *>(impl_);
      return &(impl->user);
    }
    case UseKind::kComparisonLHS: {
      auto impl = reinterpret_cast<Node<ParsedUse<ParsedComparison>> *>(impl_);
      return &(impl->user);
    }
    case UseKind::kComparisonRHS: {
      auto impl = reinterpret_cast<Node<ParsedUse<ParsedComparison>> *>(impl_);
      return &(impl->user);
    }
  }
  return nullptr;
}

}  // namespace parse

const char *Node<ParsedDeclaration>::KindName(void) const {
  switch (context->kind) {
    case DeclarationKind::kQuery: return "query";
    case DeclarationKind::kMessage: return "message";
    case DeclarationKind::kFunctor: return "functor";
    case DeclarationKind::kExport: return "export";
    case DeclarationKind::kLocal: return "local";
  }
  return "";
}

#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Wconversion"
#  pragma GCC diagnostic ignored "-Woverflow"
#endif

// Compute a unique identifier for this declaration.
uint64_t Node<ParsedDeclaration>::Id(void) const noexcept {
  auto &id = context->id;
  if (id.flat) {
    return id.flat;
  }

  id.flat = ParsedModule(module->shared_from_this()).Id();

  // NOTE(pag): All anonymous declarations and by definition
  if (name.Lexeme() == Lexeme::kIdentifierUnnamedAtom) {
    id.info.atom_name_id = module->next_anon_decl_id++;
    id.info.atom_name_id = ~id.info.atom_name_id;

  } else {
    id.info.atom_name_id = name.IdentifierId();

    // If it's not a local thing, then use `~0` for the ID.
    if (context->kind != DeclarationKind::kLocal) {
      id.info.module_id = ~0u;
    }
  }
  id.info.arity = parameters.size();
  return id.flat;
}

#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic pop
#endif

// Return a list of clauses associated with this declaration.
NodeRange<ParsedClause> Node<ParsedDeclaration>::Clauses(void) const {
  if (context->clauses.empty()) {
    return NodeRange<ParsedClause>();
  } else {
    return NodeRange<ParsedClause>(context->clauses.front().get());
  }
}

// Return a list of clauses associated with this declaration.
NodeRange<ParsedClause> Node<ParsedDeclaration>::DeletionClauses(void) const {
  if (context->deletion_clauses.empty()) {
    return NodeRange<ParsedClause>();
  } else {
    return NodeRange<ParsedClause>(context->deletion_clauses.front().get());
  }
}

// Return a list of positive uses of this definition.
NodeRange<ParsedPredicate> Node<ParsedDeclaration>::PositiveUses(void) const {
  if (context->positive_uses.empty()) {
    return NodeRange<ParsedPredicate>();
  } else {
    return NodeRange<ParsedPredicate>(context->positive_uses.front(),
                                      static_cast<intptr_t>(__builtin_offsetof(
                                          Node<ParsedPredicate>, next_use)));
  }
}

// Return a list of negative uses of this definition.
NodeRange<ParsedPredicate> Node<ParsedDeclaration>::NegativeUses(void) const {
  if (context->negated_uses.empty()) {
    return NodeRange<ParsedPredicate>();
  } else {
    return NodeRange<ParsedPredicate>(context->negated_uses.front(),
                                      static_cast<intptr_t>(__builtin_offsetof(
                                          Node<ParsedPredicate>, next_use)));
  }
}

// Compute the unique identifier for this variable.
uint64_t Node<ParsedVariable>::Id(void) noexcept {
  auto &id = context->id;
  if (id.flat) {
    return id.flat;
  }

#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Wconversion"
#endif

  const auto clause = context->clause;
  id.flat = clause->Id();
  if (name.Lexeme() == Lexeme::kIdentifierUnnamedVariable) {
    id.info.var_id = clause->next_var_id++;

  } else {
    assert(0 < name.IdentifierId());
    auto &prev_id = clause->named_var_ids[name.IdentifierId()];
    if (!prev_id) {
      prev_id = clause->next_var_id++;
    }
    id.info.var_id = prev_id;
  }

  assert(0 < id.info.var_id);

#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic pop
#endif

  return id.flat;
}

// Compute the identifier for this clause.
uint64_t Node<ParsedPredicate>::Id(void) const noexcept {
  return declaration->Id();
}

// Compute the identifier for this clause.
uint64_t Node<ParsedClause>::Id(void) const noexcept {
  assert(declaration != nullptr);
  if (!id.flat) {
    id.flat = declaration->Id();
  }
  return id.flat;
}

DisplayRange ParsedVariable::SpellingRange(void) const noexcept {
  return impl->name.SpellingRange();
}

// Returns the token corresponding with the name of this variable.
Token ParsedVariable::Name(void) const noexcept {
  return impl->name;
}

TypeLoc ParsedVariable::Type(void) const noexcept {
  return impl->type;
}

// Returns `true` if this variable is an parameter to its clause.
bool ParsedVariable::IsParameter(void) const noexcept {
  return impl->context->first_use ? impl->context->first_use->is_parameter
                                  : false;
}

// Returns `true` if this variable is an argument to a predicate.
bool ParsedVariable::IsArgument(void) const noexcept {
  return impl->is_argument;
}

// Returns `true` if this variable, or any other used of this variable,
// is assigned to any literals.
bool ParsedVariable::IsAssigned(void) const noexcept {
  return !impl->context->assignment_uses.empty();
}

// Returns `true` if this variable, or any other use of this variable,
// is compared with any other variables.
bool ParsedVariable::IsCompared() const noexcept {
  return !impl->context->comparison_uses.empty();
}

// Returns `true` if this variable is an unnamed variable.
bool ParsedVariable::IsUnnamed(void) const noexcept {
  return impl->name.Lexeme() == Lexeme::kIdentifierUnnamedVariable;
}

// Return a unique integer that identifies this variable.
uint64_t ParsedVariable::Id(void) const noexcept {
  return impl->Id();
}

// A number corresponding to the order of appearance of this variable.
unsigned ParsedVariable::Order(void) const noexcept {
  if (impl->appearance >= kMaxArity) {
    return (impl->appearance - kMaxArity) +
           static_cast<unsigned>(impl->context->clause->head_variables.size());
  } else {
    return impl->appearance;
  }
}

// Iterate over each use of this variable.
NodeRange<ParsedVariable> ParsedVariable::Uses(void) const {
  return NodeRange<ParsedVariable>(impl->context->first_use,
                                   static_cast<intptr_t>(__builtin_offsetof(
                                       Node<ParsedVariable>, next_use)));
}

// Return the number of uses of this variable.
unsigned ParsedVariable::NumUses(void) const {
  auto context = impl->context.get();
  return static_cast<unsigned>(
      context->parameter_uses.size() + context->argument_uses.size() +
      context->assignment_uses.size() + context->comparison_uses.size());
}

// Replace all uses of this variable with another variable.
bool ParsedVariable::ReplaceAllUses(ParsedVariable that) const {

  // TODO(pag): Test this code.

  auto context = impl->context;  // Hold a ref.
  auto that_context = that.impl->context.get();
  if (context.get() == that_context) {
    return true;
  } else if (context->clause != that_context->clause) {
    return false;
  } else if (impl->type.Kind() != that.impl->type.Kind()) {
    return false;
  }

  // Follow the next use chain of the other variable and find the last pointer
  // in it, and then link that to the first use of this variable.
  auto nupp = &(that_context->first_use);
  for (; *nupp; nupp = &((*nupp)->next_use)) {

    // Do nothing;
  }
  *nupp = context->first_use;

  // Replace the variable names and contexts in the use list.
  auto replace_uses = [=](auto &use_list, auto &that_use_list) {
    if (!use_list.empty() && !that_use_list.empty()) {
      that_use_list.back()->next = use_list.front();
    }
    for (auto &use : use_list) {
      use->used_var->name = that.impl->name;
      use->used_var->context = that.impl->context;
      that_use_list.emplace_back(std::move(use));
    }
  };

  replace_uses(context->parameter_uses, that_context->parameter_uses);
  replace_uses(context->argument_uses, that_context->argument_uses);
  replace_uses(context->comparison_uses, that_context->comparison_uses);
  replace_uses(context->assignment_uses, that_context->assignment_uses);

  return true;
}

// Return the variable to which `literal` assigned.
ParsedVariable ParsedVariable::AssignedTo(ParsedLiteral literal) noexcept {
  return ParsedVariable(literal.impl->assigned_to);
}

DisplayRange ParsedLiteral::SpellingRange(void) const noexcept {
  return impl->literal.SpellingRange();
}

std::optional<std::string_view>
ParsedLiteral::Spelling(Language lang) const noexcept {
  if (IsConstant()) {
    const auto &info = impl->foreign_type->info[static_cast<unsigned>(lang)];
    const auto id = impl->literal.IdentifierId();
    for (const auto &const_ptr : info.constants) {
      if (const_ptr->lang == lang && const_ptr->name.IdentifierId() == id) {
        return const_ptr->code;
      }
    }
    return std::nullopt;

  } else {
    return impl->data;
  }
}

// Is this a foreign constant?
bool ParsedLiteral::IsConstant(void) const noexcept {
  return impl->literal.Lexeme() == Lexeme::kIdentifierConstant;
}

bool ParsedLiteral::IsNumber(void) const noexcept {
  return impl->literal.Lexeme() == Lexeme::kLiteralNumber;
}

bool ParsedLiteral::IsString(void) const noexcept {
  return impl->literal.Lexeme() == Lexeme::kLiteralString;
}

bool ParsedLiteral::IsBoolean(void) const noexcept {
  return impl->literal.Lexeme() == Lexeme::kLiteralTrue ||
         impl->literal.Lexeme() == Lexeme::kLiteralFalse;
}

TypeLoc ParsedLiteral::Type(void) const noexcept {
  return impl->type;
}

// Token representing the use of this constant.
Token ParsedLiteral::Literal(void) const noexcept {
  return impl->literal;
}

DisplayRange ParsedComparison::SpellingRange(void) const noexcept {
  return DisplayRange(impl->lhs.UseBase::used_var->name.Position(),
                      impl->rhs.UseBase::used_var->name.NextPosition());
}

ParsedVariable ParsedComparison::LHS(void) const noexcept {
  return ParsedVariable(impl->lhs.UseBase::used_var);
}

ParsedVariable ParsedComparison::RHS(void) const noexcept {
  return ParsedVariable(impl->rhs.UseBase::used_var);
}

ComparisonOperator ParsedComparison::Operator(void) const noexcept {
  switch (impl->compare_op.Lexeme()) {
    case Lexeme::kPuncEqual: return ComparisonOperator::kEqual;
    case Lexeme::kPuncNotEqual: return ComparisonOperator::kNotEqual;
    case Lexeme::kPuncLess: return ComparisonOperator::kLessThan;
    case Lexeme::kPuncGreater: return ComparisonOperator::kGreaterThan;
    default: assert(false); return ComparisonOperator::kEqual;
  }
}

// Return the list of all comparisons with `var`.
NodeRange<ParsedComparisonUse> ParsedComparison::Using(ParsedVariable var) {
  if (var.impl->context->comparison_uses.empty()) {
    return NodeRange<ParsedComparisonUse>();
  } else {
    return NodeRange<ParsedComparisonUse>(
        var.impl->context->comparison_uses.front());
  }
}

DisplayRange ParsedAssignment::SpellingRange(void) const noexcept {
  return DisplayRange(impl->lhs.UseBase::used_var->name.Position(),
                      impl->rhs.literal.NextPosition());
}

ParsedVariable ParsedAssignment::LHS(void) const noexcept {
  return ParsedVariable(impl->lhs.UseBase::used_var);
}

ParsedLiteral ParsedAssignment::RHS(void) const noexcept {
  return ParsedLiteral(&(impl->rhs));
}

// Return the list of all assignments to `var`.
NodeRange<ParsedAssignmentUse> ParsedAssignment::Using(ParsedVariable var) {
  if (var.impl->context->assignment_uses.empty()) {
    return NodeRange<ParsedAssignmentUse>();
  } else {
    return NodeRange<ParsedAssignmentUse>(
        var.impl->context->assignment_uses.front());
  }
}

// Return the list of all uses of `var` as an argument to a predicate.
NodeRange<ParsedArgumentUse> ParsedPredicate::Using(ParsedVariable var) {
  if (var.impl->context->argument_uses.empty()) {
    return NodeRange<ParsedArgumentUse>();
  } else {
    return NodeRange<ParsedArgumentUse>(
        var.impl->context->argument_uses.front());
  }
}

DisplayRange ParsedPredicate::SpellingRange(void) const noexcept {
  return DisplayRange(
      impl->negation_pos.IsValid() ? impl->negation_pos : impl->name.Position(),
      impl->rparen.IsValid() ? impl->rparen.NextPosition()
                             : impl->name.NextPosition());
}

// Returns `true` if this is a positive predicate.
bool ParsedPredicate::IsPositive(void) const noexcept {
  return impl->negation_pos.IsInvalid();
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
  return ParsedVariable(impl->argument_uses[n]->UseBase::used_var);
}

// All variables used as arguments to this predicate.
NodeRange<ParsedVariable> ParsedPredicate::Arguments(void) const {
  assert(0 < Arity());
  return NodeRange<ParsedVariable>(
      impl->argument_uses.front()->UseBase::used_var,
      static_cast<intptr_t>(
          __builtin_offsetof(Node<ParsedVariable>, next_var_in_arg_list)));
}

DisplayRange ParsedAggregate::SpellingRange(void) const noexcept {
  return impl->spelling_range;
}

ParsedPredicate ParsedAggregate::Functor(void) const noexcept {
  return ParsedPredicate(impl->functor.get());
}

ParsedPredicate ParsedAggregate::Predicate(void) const noexcept {
  return ParsedPredicate(impl->predicate.get());
}

// List of parameters to the predicate that are not paired with anything of
// the arguments to the aggregating functor.
NodeRange<ParsedVariable>
ParsedAggregate::GroupVariablesFromPredicate(void) const {
  if (impl->first_group_var) {
    return NodeRange<ParsedVariable>(
        impl->first_group_var, static_cast<intptr_t>(__builtin_offsetof(
                                   Node<ParsedVariable>, next_group_var)));
  } else {
    return NodeRange<ParsedVariable>();
  }
}

// List of parameters from the predicate that are paired with a `aggregate`-
// attributed variable in the functor.
NodeRange<ParsedVariable>
ParsedAggregate::AggregatedVariablesFromPredicate(void) const {
  if (impl->first_aggregate_var) {
    return NodeRange<ParsedVariable>(
        impl->first_aggregate_var,
        static_cast<intptr_t>(
            __builtin_offsetof(Node<ParsedVariable>, next_aggregate_var)));
  } else {
    return NodeRange<ParsedVariable>();
  }
}

// List of parameters from the predicate that are paired with a `bound`-
// attributed variables in the functor.
NodeRange<ParsedVariable>
ParsedAggregate::ConfigurationVariablesFromPredicate(void) const {
  if (impl->first_config_var) {
    return NodeRange<ParsedVariable>(
        impl->first_config_var, static_cast<intptr_t>(__builtin_offsetof(
                                    Node<ParsedVariable>, next_config_var)));
  } else {
    return NodeRange<ParsedVariable>();
  }
}

DisplayRange ParsedParameter::SpellingRange(void) const noexcept {
  auto begin = impl->name.Position();
  if (impl->opt_binding.IsValid()) {
    begin = impl->opt_binding.Position();
  } else if (impl->parsed_opt_type && impl->opt_type.IsValid()) {
    begin = impl->opt_type.Position();
  }
  return DisplayRange(begin, impl->name.NextPosition());
}

Token ParsedParameter::Name(void) const noexcept {
  return impl->name;
}

TypeLoc ParsedParameter::Type(void) const noexcept {
  return impl->opt_type;
}

ParameterBinding ParsedParameter::Binding(void) const noexcept {
  switch (impl->opt_binding.Lexeme()) {
    case Lexeme::kKeywordBound: return ParameterBinding::kBound;
    case Lexeme::kKeywordFree: return ParameterBinding::kFree;
    case Lexeme::kKeywordAggregate: return ParameterBinding::kAggregate;
    case Lexeme::kKeywordSummary: return ParameterBinding::kSummary;
    case Lexeme::kKeywordMutable: return ParameterBinding::kMutable;
    default: return ParameterBinding::kImplicit;
  }
}

unsigned ParsedParameter::Index(void) const noexcept {
  return impl->index;
}

// Returns `true` if this variable is an unnamed variable.
bool ParsedParameter::IsUnnamed(void) const noexcept {
  return impl->name.Lexeme() == Lexeme::kIdentifierUnnamedVariable;
}

// Applies only to `bound` parameters of functors.
bool ParsedParameter::CanBeReordered(void) const noexcept {
  return impl->opt_unordered_name.IsValid();
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

ParsedDeclaration::ParsedDeclaration(const ParsedPredicate &pred)
: parse::ParsedNode<ParsedDeclaration>(ParsedDeclaration::Of(pred)) {}

DisplayRange ParsedDeclaration::SpellingRange(void) const noexcept {
  if (impl->rparen.IsValid()) {
    auto last_tok = impl->last_tok.IsValid() ? impl->last_tok : impl->rparen;
    return DisplayRange(impl->directive_pos.IsValid() ? impl->directive_pos
                                                      : impl->name.Position(),
                        last_tok.NextPosition());
  } else {
    return impl->name.SpellingRange();
  }
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

Token ParsedFunctor::Name(void) const noexcept {
  return impl->name;
}

Token ParsedMessage::Name(void) const noexcept {
  return impl->name;
}

Token ParsedQuery::Name(void) const noexcept {
  return impl->name;
}

Token ParsedLocal::Name(void) const noexcept {
  return impl->name;
}

Token ParsedExport::Name(void) const noexcept {
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

// Does this declaration have a `mutable`-attributed parameter? If so, then
// this relation must be materialized.
bool ParsedDeclaration::HasMutableParameter(void) const noexcept {
  return impl->has_mutable_parameter;
}

// Does this declaration have a clause that directly depends on a `#message`?
bool ParsedDeclaration::HasDirectInputDependency(void) const noexcept {
  auto context = impl->context.get();
  if (context->checked_takes_input) {
    return context->takes_input;
  }

  context->checked_takes_input = true;
  for (const auto &clause : context->clauses) {
    if (clause->depends_on_messages) {
      context->takes_input = true;
      return true;
    }
  }

  return false;
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

unsigned ParsedFunctor::Arity(void) const noexcept {
  return static_cast<unsigned>(impl->parameters.size());
}

unsigned ParsedQuery::Arity(void) const noexcept {
  return static_cast<unsigned>(impl->parameters.size());
}

unsigned ParsedMessage::Arity(void) const noexcept {
  return static_cast<unsigned>(impl->parameters.size());
}

unsigned ParsedLocal::Arity(void) const noexcept {
  return static_cast<unsigned>(impl->parameters.size());
}

unsigned ParsedExport::Arity(void) const noexcept {
  return static_cast<unsigned>(impl->parameters.size());
}

// Return the `n`th parameter of this clause.
ParsedParameter ParsedDeclaration::NthParameter(unsigned n) const noexcept {
  assert(n < Arity());
  return ParsedParameter(impl->parameters[n].get());
}

// Return the `n`th parameter of this clause.
ParsedParameter ParsedFunctor::NthParameter(unsigned n) const noexcept {
  assert(n < Arity());
  return ParsedParameter(impl->parameters[n].get());
}

// Return the `n`th parameter of this clause.
ParsedParameter ParsedMessage::NthParameter(unsigned n) const noexcept {
  assert(n < Arity());
  return ParsedParameter(impl->parameters[n].get());
}

// Return the `n`th parameter of this clause.
ParsedParameter ParsedQuery::NthParameter(unsigned n) const noexcept {
  assert(n < Arity());
  return ParsedParameter(impl->parameters[n].get());
}

// Return the `n`th parameter of this clause.
ParsedParameter ParsedLocal::NthParameter(unsigned n) const noexcept {
  assert(n < Arity());
  return ParsedParameter(impl->parameters[n].get());
}


// Return the `n`th parameter of this clause.
ParsedParameter ParsedExport::NthParameter(unsigned n) const noexcept {
  assert(n < Arity());
  return ParsedParameter(impl->parameters[n].get());
}

NodeRange<ParsedParameter> ParsedDeclaration::Parameters(void) const {
  if (impl->parameters.empty()) {
    return NodeRange<ParsedParameter>();
  } else {
    return NodeRange<ParsedParameter>(impl->parameters.front().get());
  }
}

NodeRange<ParsedDeclaration> ParsedDeclaration::Redeclarations(void) const {
  assert(!impl->context->redeclarations.empty());
  return NodeRange<ParsedDeclaration>(
      reinterpret_cast<Node<ParsedDeclaration> *>(
          impl->context->redeclarations.front()),
      static_cast<intptr_t>(
          __builtin_offsetof(Node<ParsedDeclaration>, next_redecl)));
}

NodeRange<ParsedClause> ParsedDeclaration::Clauses(void) const {
  return impl->Clauses();
}

NodeRange<ParsedClause> ParsedDeclaration::DeletionClauses(void) const {
  return impl->DeletionClauses();
}

NodeRange<ParsedPredicate> ParsedDeclaration::PositiveUses(void) const {
  return impl->PositiveUses();
}

NodeRange<ParsedPredicate> ParsedDeclaration::NegativeUses(void) const {
  return impl->NegativeUses();
}

unsigned ParsedDeclaration::NumPositiveUses(void) const noexcept {
  return static_cast<unsigned>(impl->context->positive_uses.size());
}

unsigned ParsedDeclaration::NumNegatedUses(void) const noexcept {
  return static_cast<unsigned>(impl->context->negated_uses.size());
}

unsigned ParsedDeclaration::NumClauses(void) const noexcept {
  return static_cast<unsigned>(impl->context->clauses.size());
}

unsigned ParsedDeclaration::NumDeletionClauses(void) const noexcept {
  return static_cast<unsigned>(impl->context->deletion_clauses.size());
}

bool ParsedDeclaration::IsInline(void) const noexcept {
  return IsQuery() || impl->inline_attribute.Lexeme() == Lexeme::kPragmaPerfInline;
}

std::string_view ParsedDeclaration::BindingPattern(void) const noexcept {
  if (impl->binding_pattern.empty()) {
    impl->binding_pattern.reserve(impl->parameters.size());
    for (const auto &param : impl->parameters) {
      switch (ParsedParameter(param.get()).Binding()) {
        case ParameterBinding::kImplicit:
          impl->binding_pattern.push_back('i');
          break;
        case ParameterBinding::kMutable:
          impl->binding_pattern.push_back('m');
          break;
        case ParameterBinding::kFree:
          impl->binding_pattern.push_back('f');
          break;

        case ParameterBinding::kBound:
          impl->binding_pattern.push_back('b');
          break;

        case ParameterBinding::kSummary:
          impl->binding_pattern.push_back('s');
          break;

        case ParameterBinding::kAggregate:
          impl->binding_pattern.push_back('a');
          break;
      }
    }
  }

  return impl->binding_pattern;
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

// Create a new variable in this context of this clause.
ParsedVariable ParsedClause::CreateVariable(TypeLoc type) {
  assert(type.UnderlyingKind() != TypeKind::kInvalid);
  auto var = new Node<ParsedVariable>;
  var->type = type;
  var->name =
      Token::Synthetic(Lexeme::kIdentifierUnnamedVariable, SpellingRange());
  var->context = std::make_shared<parse::VariableContext>(impl, nullptr);
  impl->body_variables.emplace_back(var);
  return ParsedVariable(var);
}

ParsedClause ParsedClause::Containing(ParsedVariable var) noexcept {
  return ParsedClause(var.impl->context->clause);
}

ParsedClause ParsedClause::Containing(ParsedPredicate pred) noexcept {
  return ParsedClause(pred.impl->clause);
}

ParsedClause ParsedClause::Containing(ParsedAssignment assignment) noexcept {
  return ParsedClause(assignment.impl->lhs.UseBase::used_var->context->clause);
}

ParsedClause ParsedClause::Containing(ParsedComparison compare) noexcept {
  return ParsedClause(compare.impl->lhs.UseBase::used_var->context->clause);
}

ParsedClause ParsedClause::Containing(ParsedAggregate agg) noexcept {
  return ParsedClause(agg.impl->functor->clause);
}

// Is this a deletion clause?
bool ParsedClause::IsDeletion(void) const noexcept {
  return impl->negation.IsValid();
}

// Return the total number of uses of `var` in its clause body.
unsigned ParsedClause::NumUsesInBody(ParsedVariable var) noexcept {
  return static_cast<unsigned>(var.impl->context->assignment_uses.size() +
                               var.impl->context->comparison_uses.size() +
                               var.impl->context->argument_uses.size());
}

DisplayRange ParsedClause::SpellingRange(void) const noexcept {
  auto last_tok =
      impl->last_tok.IsValid() ? impl->last_tok : impl->dot;
  return DisplayRange((impl->negation.IsValid() ? impl->negation.Position()
                                                : impl->name.Position()),
                      last_tok.NextPosition());
}

// Should this clause be highlighted in the data flow representation?
bool ParsedClause::IsHighlighted(void) const noexcept {
  return impl->highlight.IsValid();
}

// Returns `true` if this clause body is disabled. A disabled clause body
// is one that contains a free `false` or `!true` predicate.
bool ParsedClause::IsDisabled(DisplayRange *disabled_by) const noexcept {
  if (impl->disabled_by.From().IsValid()) {
    if (disabled_by) {
      *disabled_by = impl->disabled_by;
    }
    return true;
  } else {
    return false;
  }
}

// Are cross-products permitted when building the data flow representation
// for this clause?
bool ParsedClause::CrossProductsArePermitted(void) const noexcept {
  return impl->product.IsValid();
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
NodeRange<ParsedVariable> ParsedClause::Parameters(void) const {
  if (impl->head_variables.empty()) {
    return NodeRange<ParsedVariable>();
  } else {
    return NodeRange<ParsedVariable>(impl->head_variables.front().get());
  }
}

// All body_variables used in the clause. Some variables might be repeated.
NodeRange<ParsedVariable> ParsedClause::Variables(void) const {
  if (impl->body_variables.empty()) {
    return NodeRange<ParsedVariable>();
  } else {
    return NodeRange<ParsedVariable>(impl->body_variables.front().get());
  }
}

// All instances of `var` in its clause.
NodeRange<ParsedVariable> ParsedClause::Uses(ParsedVariable var) {
  return NodeRange<ParsedVariable>(var.impl->context->first_use,
                                   static_cast<intptr_t>(__builtin_offsetof(
                                       Node<ParsedVariable>, next_use)));
}

// All positive predicates in the clause.
NodeRange<ParsedPredicate> ParsedClause::PositivePredicates(void) const {
  if (!impl->positive_predicates.empty()) {
    return NodeRange<ParsedPredicate>(impl->positive_predicates.front().get());
  } else {
    return NodeRange<ParsedPredicate>();
  }
}

// All negated predicates in the clause.
NodeRange<ParsedPredicate> ParsedClause::NegatedPredicates(void) const {
  if (!impl->negated_predicates.empty()) {
    return NodeRange<ParsedPredicate>(impl->negated_predicates.front().get());
  } else {
    return NodeRange<ParsedPredicate>();
  }
}

// All assignments of variables to constant literals.
NodeRange<ParsedAssignment> ParsedClause::Assignments(void) const {
  if (!impl->assignments.empty()) {
    return NodeRange<ParsedAssignment>(impl->assignments.front().get());
  } else {
    return NodeRange<ParsedAssignment>();
  }
}

// All comparisons between two variables.
NodeRange<ParsedComparison> ParsedClause::Comparisons(void) const {
  if (!impl->comparisons.empty()) {
    return NodeRange<ParsedComparison>(impl->comparisons.front().get());
  } else {
    return NodeRange<ParsedComparison>();
  }
}

// All aggregations.
NodeRange<ParsedAggregate> ParsedClause::Aggregates(void) const {
  if (!impl->aggregates.empty()) {
    return NodeRange<ParsedAggregate>(impl->aggregates.front().get());
  } else {
    return NodeRange<ParsedAggregate>();
  }
}

DisplayRange ParsedClauseHead::SpellingRange(void) const noexcept {
  return DisplayRange(clause.impl->name.Position(),
                      clause.impl->rparen.NextPosition());
}

DisplayRange ParsedClauseBody::SpellingRange(void) const noexcept {
  return DisplayRange(clause.impl->first_body_token.Position(),
                      clause.impl->dot.NextPosition());
}

const ParsedQuery &ParsedQuery::From(const ParsedDeclaration &decl) {
  assert(decl.IsQuery());
  return reinterpret_cast<const ParsedQuery &>(decl);
}

DisplayRange ParsedQuery::SpellingRange(void) const noexcept {
  auto last_tok = impl->last_tok.IsValid() ? impl->last_tok : impl->rparen;
  return DisplayRange(impl->directive_pos, last_tok.NextPosition());
}

NodeRange<ParsedQuery> ParsedQuery::Redeclarations(void) const {
  return NodeRange<ParsedQuery>(reinterpret_cast<Node<ParsedQuery> *>(
                                    impl->context->redeclarations.front()),
                                static_cast<intptr_t>(__builtin_offsetof(
                                    Node<ParsedQuery>, next_redecl)));
}

NodeRange<ParsedClause> ParsedQuery::Clauses(void) const {
  return impl->Clauses();
}

NodeRange<ParsedPredicate> ParsedQuery::PositiveUses(void) const {
  return impl->PositiveUses();
}

NodeRange<ParsedPredicate> ParsedQuery::NegativeUses(void) const {
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
  auto last_tok = impl->last_tok.IsValid() ? impl->last_tok : impl->rparen;
  return DisplayRange(impl->directive_pos, last_tok.NextPosition());
}

NodeRange<ParsedExport> ParsedExport::Redeclarations(void) const {
  return NodeRange<ParsedExport>(reinterpret_cast<Node<ParsedExport> *>(
                                     impl->context->redeclarations.front()),
                                 static_cast<intptr_t>(__builtin_offsetof(
                                     Node<ParsedExport>, next_redecl)));
}

NodeRange<ParsedClause> ParsedExport::Clauses(void) const {
  return impl->Clauses();
}

NodeRange<ParsedPredicate> ParsedExport::PositiveUses(void) const {
  return impl->PositiveUses();
}

NodeRange<ParsedPredicate> ParsedExport::NegativeUses(void) const {
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
  auto last_tok = impl->last_tok.IsValid() ? impl->last_tok : impl->rparen;
  return DisplayRange(impl->directive_pos, last_tok.NextPosition());
}

NodeRange<ParsedLocal> ParsedLocal::Redeclarations(void) const {
  return NodeRange<ParsedLocal>(reinterpret_cast<Node<ParsedLocal> *>(
                                    impl->context->redeclarations.front()),
                                static_cast<intptr_t>(__builtin_offsetof(
                                    Node<ParsedLocal>, next_redecl)));
}

NodeRange<ParsedPredicate> ParsedLocal::PositiveUses(void) const {
  return impl->PositiveUses();
}

NodeRange<ParsedPredicate> ParsedLocal::NegativeUses(void) const {
  return impl->NegativeUses();
}

unsigned ParsedLocal::NumPositiveUses(void) const noexcept {
  return static_cast<unsigned>(impl->context->positive_uses.size());
}

unsigned ParsedLocal::NumNegatedUses(void) const noexcept {
  return static_cast<unsigned>(impl->context->negated_uses.size());
}

bool ParsedLocal::IsInline(void) const noexcept {
  return impl->inline_attribute.Lexeme() == Lexeme::kPragmaPerfInline;
}

NodeRange<ParsedClause> ParsedLocal::Clauses(void) const {
  return impl->Clauses();
}

bool ParsedFunctor::IsAggregate(void) const noexcept {
  return impl->is_aggregate;
}

bool ParsedFunctor::IsMerge(void) const noexcept {
  return impl->is_merge;
}

bool ParsedFunctor::IsPure(void) const noexcept {
  return impl->is_pure;
}

// Is this a filter-like functor? This is `true` if the functor is `pure`
// and if the number of free parameters is zero and if the range is
// `FunctorRange::kZeroOrOne`.
bool ParsedFunctor::IsFilter(void) const noexcept {
  if (impl->is_pure && impl->range == FunctorRange::kZeroOrOne) {
    for (const auto &param : impl->parameters) {
      if (ParameterBinding::kFree == ParsedParameter(param.get()).Binding()) {
        return false;
      }
    }
    return true;
  } else {
    return false;
  }
}

const ParsedFunctor &ParsedFunctor::From(const ParsedDeclaration &decl) {
  assert(decl.IsFunctor());
  return reinterpret_cast<const ParsedFunctor &>(decl);
}

const ParsedFunctor ParsedFunctor::MergeOperatorOf(ParsedParameter param) {
  assert(param.impl->opt_merge != nullptr);
  return ParsedFunctor(param.impl->opt_merge);
}

DisplayRange ParsedFunctor::SpellingRange(void) const noexcept {
  auto last_tok = impl->last_tok.IsValid() ? impl->last_tok : impl->rparen;
  return DisplayRange(impl->directive_pos, last_tok.NextPosition());
}

NodeRange<ParsedFunctor> ParsedFunctor::Redeclarations(void) const {
  return NodeRange<ParsedFunctor>(reinterpret_cast<Node<ParsedFunctor> *>(
                                      impl->context->redeclarations.front()),
                                  static_cast<intptr_t>(__builtin_offsetof(
                                      Node<ParsedFunctor>, next_redecl)));
}

NodeRange<ParsedParameter> ParsedFunctor::Parameters(void) const {
  if (impl->parameters.empty()) {
    return NodeRange<ParsedParameter>();
  } else {
    return NodeRange<ParsedParameter>(impl->parameters.front().get());
  }
}

NodeRange<ParsedPredicate> ParsedFunctor::PositiveUses(void) const {
  return impl->PositiveUses();
}

unsigned ParsedFunctor::NumPositiveUses(void) const noexcept {
  return static_cast<unsigned>(impl->context->positive_uses.size());
}

FunctorRange ParsedFunctor::Range(void) const noexcept {
  return impl->range;
}

const ParsedMessage &ParsedMessage::From(const ParsedDeclaration &decl) {
  assert(decl.IsMessage());
  return reinterpret_cast<const ParsedMessage &>(decl);
}

DisplayRange ParsedMessage::SpellingRange(void) const noexcept {
  auto last_tok = impl->last_tok.IsValid() ? impl->last_tok : impl->rparen;
  return DisplayRange(impl->directive_pos, last_tok.NextPosition());
}

NodeRange<ParsedParameter> ParsedMessage::Parameters(void) const {
  if (impl->parameters.empty()) {
    return NodeRange<ParsedParameter>();
  } else {
    return NodeRange<ParsedParameter>(impl->parameters.front().get());
  }
}

NodeRange<ParsedMessage> ParsedMessage::Redeclarations(void) const {
  return NodeRange<ParsedMessage>(reinterpret_cast<Node<ParsedMessage> *>(
                                      impl->context->redeclarations.front()),
                                  static_cast<intptr_t>(__builtin_offsetof(
                                      Node<ParsedMessage>, next_redecl)));
}

NodeRange<ParsedClause> ParsedMessage::Clauses(void) const {
  return impl->Clauses();
}

NodeRange<ParsedPredicate> ParsedMessage::PositiveUses(void) const {
  return impl->PositiveUses();
}

// Returns `true` if this message is the head of any clause, i.e. if there
// are rules that publish this message.
bool ParsedMessage::IsPublished(void) const noexcept {
  return !impl->context->clauses.empty();
}

unsigned ParsedMessage::NumPositiveUses(void) const noexcept {
  return static_cast<unsigned>(impl->context->positive_uses.size());
}

DisplayRange ParsedModule::SpellingRange(void) const noexcept {
  return DisplayRange(impl->first.Position(), impl->last.Position());
}

// Return the ID of this module. Returns `~0u` if not valid.
uint64_t ParsedModule::Id(void) const noexcept {

#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Wconversion"
#  pragma GCC diagnostic ignored "-Woverflow"
#endif

  parse::IdInterpreter interpreter = {};
  interpreter.info.module_id = impl->first.DisplayId();
  interpreter.info.atom_name_id = ~0u;
  interpreter.info.var_id = ~0u;
  interpreter.info.arity = ~0u;
  return interpreter.flat;

#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic pop
#endif
}

NodeRange<ParsedQuery> ParsedModule::Queries(void) const {
  if (impl->queries.empty()) {
    return NodeRange<ParsedQuery>();
  } else {
    return NodeRange<ParsedQuery>(impl->queries.front().get());
  }
}

NodeRange<ParsedImport> ParsedModule::Imports(void) const {
  if (impl->imports.empty()) {
    return NodeRange<ParsedImport>();
  } else {
    return NodeRange<ParsedImport>(impl->imports.front().get());
  }
}

NodeRange<ParsedInline> ParsedModule::Inlines(void) const {
  if (impl->inlines.empty()) {
    return NodeRange<ParsedInline>();
  } else {
    return NodeRange<ParsedInline>(impl->inlines.front().get());
  }
}

NodeRange<ParsedLocal> ParsedModule::Locals(void) const {
  if (impl->locals.empty()) {
    return NodeRange<ParsedLocal>();
  } else {
    return NodeRange<ParsedLocal>(impl->locals.front().get());
  }
}

NodeRange<ParsedExport> ParsedModule::Exports(void) const {
  if (impl->exports.empty()) {
    return NodeRange<ParsedExport>();
  } else {
    return NodeRange<ParsedExport>(impl->exports.front().get());
  }
}

NodeRange<ParsedMessage> ParsedModule::Messages(void) const {
  if (impl->messages.empty()) {
    return NodeRange<ParsedMessage>();
  } else {
    return NodeRange<ParsedMessage>(impl->messages.front().get());
  }
}

NodeRange<ParsedFunctor> ParsedModule::Functors(void) const {
  if (impl->functors.empty()) {
    return NodeRange<ParsedFunctor>();
  } else {
    return NodeRange<ParsedFunctor>(impl->functors.front().get());
  }
}

NodeRange<ParsedClause> ParsedModule::Clauses(void) const {
  if (impl->clauses.empty()) {
    return NodeRange<ParsedClause>();
  } else {
    return NodeRange<ParsedClause>(
        impl->clauses.front(),
        __builtin_offsetof(Node<ParsedClause>, next_in_module));
  }
}

NodeRange<ParsedClause> ParsedModule::DeletionClauses(void) const {
  if (impl->deletion_clauses.empty()) {
    return NodeRange<ParsedClause>();
  } else {
    return NodeRange<ParsedClause>(
        impl->deletion_clauses.front(),
        __builtin_offsetof(Node<ParsedClause>, next_in_module));
  }
}

NodeRange<ParsedForeignType> ParsedModule::ForeignTypes(void) const {

  if (impl->root_module->types.empty()) {
    return NodeRange<ParsedForeignType>();
  } else {
    return NodeRange<ParsedForeignType>(
        impl->root_module->types.front().get(),
        __builtin_offsetof(Node<ParsedForeignType>, next));
  }
}

// Try to return the foreign type associated with a particular type location
// or type kind.
std::optional<ParsedForeignType>
ParsedModule::ForeignType(TypeLoc loc) const noexcept {
  return ForeignType(loc.Kind());
}

std::optional<ParsedForeignType>
ParsedModule::ForeignType(TypeKind kind_) const noexcept {
  const auto kind = static_cast<uint32_t>(kind_);
  const auto hi = kind >> 8u;
  const auto lo = kind & 0xffu;

  if (static_cast<TypeKind>(lo) != TypeKind::kForeignType) {
    return std::nullopt;
  }

  const auto &types = impl->root_module->foreign_types;
  auto it = types.find(hi);
  if (it == types.end()) {
    return std::nullopt;
  }

  assert(it->second != nullptr);
  return ParsedForeignType(it->second);
}

// The root module of this parse.
ParsedModule ParsedModule::RootModule(void) const {
  if (impl->root_module == impl.get()) {
    return *this;
  } else {
    return ParsedModule(impl->root_module->shared_from_this());
  }
}

DisplayRange ParsedImport::SpellingRange(void) const noexcept {
  return DisplayRange(impl->directive_pos, impl->dot.NextPosition());
}

ParsedModule ParsedImport::ImportedModule(void) const noexcept {
  return ParsedModule(impl->imported_module->shared_from_this());
}

std::filesystem::path ParsedImport::ImportedPath(void) const noexcept {
  return impl->resolved_path;
}

// Type name of this token.
Token ParsedForeignType::Name(void) const noexcept {
  return impl->name;
}

// Is this type actually built-in?
bool ParsedForeignType::IsBuiltIn(void) const noexcept {
  return impl->is_built_in;
}

std::optional<DisplayRange>
ParsedForeignType::SpellingRange(Language lang_) const noexcept {
  const auto lang = static_cast<unsigned>(lang_);
  if (impl->info[lang].is_present) {
    return impl->info[lang].range;
  } else {
    return std::nullopt;
  }
}

// Optional code to inline, specific to a language.
std::optional<std::string_view>
ParsedForeignType::CodeToInline(Language lang_) const noexcept {
  const auto lang = static_cast<unsigned>(lang_);
  if (impl->info[lang].is_present) {
    return impl->info[lang].code;
  } else {
    return std::nullopt;
  }
}

// Returns `true` if there is a specialized `lang`-specific instance, and
// `false` is none is present, or if the default `Language::kUnknown` is used.
bool ParsedForeignType::IsSpecialized(Language lang_) const noexcept {
  const auto lang = static_cast<unsigned>(lang_);
  return impl->info[lang].is_present || impl->info[lang].can_override;
}

// Returns `true` if the representation of this foreign type in the target
// language `lang` is referentially transparent, i.e. if equality implies
// identity. This is the case for trivial types, e.g. integers.
bool ParsedForeignType::IsReferentiallyTransparent(Language lang_) const noexcept {
  const auto lang = static_cast<unsigned>(lang_);
    return impl->is_built_in || impl->info[lang].is_transparent;
}

// Return the prefix and suffix for construction for this language.
std::optional<std::pair<std::string_view, std::string_view>>
ParsedForeignType::Constructor(Language lang_) const noexcept {
  const auto lang = static_cast<unsigned>(lang_);
  const auto &info = impl->info[lang];
  if (lang_ != Language::kUnknown && info.is_present &&
      (!info.constructor_prefix.empty() || !info.constructor_suffix.empty())) {
    return std::make_pair<std::string_view, std::string_view>(
        info.constructor_prefix,
        info.constructor_suffix);
  } else {
    return std::nullopt;
  }
}

// List of constants defined on this type for a particular language.
NodeRange<ParsedForeignConstant> ParsedForeignType::Constants(
    Language lang_) const noexcept {
  const auto lang = static_cast<unsigned>(lang_);
  const auto &info = impl->info[lang];
  if (info.constants.empty()) {
    return NodeRange<ParsedForeignConstant>();
  } else {
    return NodeRange<ParsedForeignConstant>(
        info.constants.front().get(),
        __builtin_offsetof(Node<ParsedForeignConstant>, next));
  }
}

TypeLoc ParsedForeignConstant::Type(void) const noexcept {
  return impl->type;
}

// Name of this constant.
Token ParsedForeignConstant::Name(void) const noexcept {
  return impl->name;
}

::hyde::Language ParsedForeignConstant::Language(void) const noexcept {
  return impl->lang;
}

DisplayRange ParsedForeignConstant::SpellingRange(void) const noexcept {
  return impl->range;
}

std::string_view ParsedForeignConstant::Constructor(void) const noexcept {
  return impl->code;
}

DisplayRange ParsedInline::SpellingRange(void) const noexcept {
  return impl->range;
}

std::string_view ParsedInline::CodeToInline(void) const noexcept {
  return impl->code;
}

::hyde::Language ParsedInline::Language(void) const noexcept {
  return impl->language;
}

bool ParsedInline::IsPrologue(void) const noexcept {
  return impl->is_prologue;
}

bool ParsedInline::IsEpilogue(void) const noexcept {
  return !impl->is_prologue;
}

}  // namespace hyde

#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic pop
#endif
