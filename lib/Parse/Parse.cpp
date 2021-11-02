// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include "Parse.h"

#include <cassert>
#include <cstring>

#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Winvalid-offsetof"
#endif

#define DEFINED_RANGE(name, method, type, access) \
  DefinedNodeRange<type> name::method(void) const { \
    return {DefinedNodeIterator<type>(impl->access.begin()), \
            DefinedNodeIterator<type>(impl->access.end())}; \
  }

#define USED_RANGE(name, method, type, access) \
  UsedNodeRange<type> name::method(void) const { \
    return {impl->access.begin(), impl->access.end()}; \
  }


#define DEFINED_RANGE_N(name, method, type, access, sub_access) \
  DefinedNodeRange<type> name::method(unsigned n) const { \
    auto &range = impl->access; \
    if (n >= range.size()) { \
      return {DefinedNodeIterator<type>(), DefinedNodeIterator<type>()}; \
    } else { \
      return {DefinedNodeIterator<type>(range[n]->sub_access.begin()), \
              DefinedNodeIterator<type>(range[n]->sub_access.end())}; \
    } \
  }

#define USED_RANGE_N(name, method, type, access, sub_access) \
  UsedNodeRange<type> name::method(unsigned n) const { \
    auto &range = impl->access; \
    if (n >= range.size()) { \
      return {UsedNodeIterator<T>(), UsedNodeIterator<T>()}; \
    } else { \
      return {range[n]->sub_access.begin()), range[n]->sub_access.end()}; \
    } \
  }

namespace hyde {
namespace parse {

DeclarationContext::~DeclarationContext(void) {
  redeclarations.ClearWithoutErasure();
  unique_redeclarations.ClearWithoutErasure();
  clauses.ClearWithoutErasure();
  positive_uses.ClearWithoutErasure();
  negated_uses.ClearWithoutErasure();
}

DeclarationContext::DeclarationContext(const DeclarationKind kind_)
    : User(this),
      kind(kind_),
      redeclarations(this),
      unique_redeclarations(this),
      clauses(this),
      positive_uses(this),
      negated_uses(this) {}

}  // namespace parse

ParsedLiteralImpl::~ParsedLiteralImpl(void) {}

ParsedLiteralImpl::ParsedLiteralImpl(ParsedAssignmentImpl *assignment_)
    : Def<ParsedLiteralImpl>(this),
      assignment(assignment_) {}

ParsedVariableImpl::~ParsedVariableImpl(void) {}

ParsedVariableImpl::ParsedVariableImpl(ParsedClauseImpl *clause_, Token name_,
                                       bool is_parameter_, bool is_argument_)
    : Def<ParsedVariableImpl>(this),
      clause(clause_),
      name(name_),
      is_parameter(is_parameter_),
      is_argument(is_argument_) {}

ParsedModuleImpl::~ParsedModuleImpl(void) {

  for (auto decl : declarations) {
    if (auto context_ptr = decl->context.get()) {
      context_ptr->redeclarations.ClearWithoutErasure();
      context_ptr->clauses.ClearWithoutErasure();
      context_ptr->positive_uses.ClearWithoutErasure();
      context_ptr->negated_uses.ClearWithoutErasure();

      decl->context.reset();
    }
  }

  // Clear these out because they are uses
  exports.ClearWithoutErasure();
  locals.ClearWithoutErasure();
  queries.ClearWithoutErasure();
  functors.ClearWithoutErasure();
  messages.ClearWithoutErasure();
}

ParsedModuleImpl::ParsedModuleImpl(const DisplayConfiguration &config_)
    : User(this),
      config(config_),
      names(this),
      imports(this),
      inlines(this),
      foreign_types(this),
      builtin_types(this),
      foreign_constants(this),
      declarations(this),
      exports(this),
      locals(this),
      queries(this),
      functors(this),
      messages(this),
      clauses(this) {}

ParsedClauseImpl::~ParsedClauseImpl(void) {}

ParsedClauseImpl::ParsedClauseImpl(ParsedModuleImpl *module_)
    : Def<ParsedClauseImpl>(this),
      User(this),
      module(module_),
      head_variables(this),
      body_variables(this) {
  groups.emplace_back(new Group(this));
}

ParsedDeclarationImpl::~ParsedDeclarationImpl(void) {}

ParsedDeclarationImpl::ParsedDeclarationImpl(ParsedModuleImpl *module_,
                                             const DeclarationKind kind_)
    : Def<ParsedDeclarationImpl>(this),
      User(this),
      module(module_),
      context(std::make_shared<parse::DeclarationContext>(kind_)),
      parameters(this) {
  context->redeclarations.AddUse(this);
}

ParsedDeclarationImpl::ParsedDeclarationImpl(
    ParsedModuleImpl *module_,
    const std::shared_ptr<parse::DeclarationContext> &context_)
    : Def<ParsedDeclarationImpl>(this),
      User(this),
      module(module_),
      context(context_),
      parameters(this) {
  assert(!context->redeclarations.Empty());
  context->redeclarations.AddUse(this);
}

ParsedForeignTypeImpl::~ParsedForeignTypeImpl(void) {
  for (auto &entry : info) {
    entry.constants->ClearWithoutErasure();
    entry.constants.reset();
  }
}

ParsedEnumTypeImpl::~ParsedEnumTypeImpl(void) {}

ParsedForeignTypeImpl::ParsedForeignTypeImpl(void)
    : Def<ParsedForeignTypeImpl>(this),
      User(this) {
  for (auto &entry : info) {
    entry.constants.reset(new UseList<ParsedForeignConstantImpl>(this));
  }
}

ParsedAggregateImpl::~ParsedAggregateImpl(void) {
  group_vars.ClearWithoutErasure();
  config_vars.ClearWithoutErasure();
  aggregate_vars.ClearWithoutErasure();
}

ParsedAggregateImpl::ParsedAggregateImpl(ParsedClauseImpl *clause_)
    : User(this),
      clause(clause_),
      functor(clause->module, clause),
      predicate(clause->module, clause),
      group_vars(this),
      config_vars(this),
      aggregate_vars(this) {}

ParsedComparisonImpl::~ParsedComparisonImpl(void) {
  lhs.ClearWithoutErasure();
  rhs.ClearWithoutErasure();
}

ParsedComparisonImpl::ParsedComparisonImpl(ParsedVariableImpl *lhs_,
                                           ParsedVariableImpl *rhs_,
                                           Token compare_op_)
    : Def<ParsedComparisonImpl>(this),
      User(this),
      lhs(this, lhs_),
      rhs(this, rhs_),
      compare_op(compare_op_) {}

ParsedAssignmentImpl::~ParsedAssignmentImpl(void) {
  lhs.ClearWithoutErasure();
}

ParsedAssignmentImpl::ParsedAssignmentImpl(ParsedVariableImpl *lhs_)
    : Def<ParsedAssignmentImpl>(this),
      User(this),
      lhs(this, lhs_),
      rhs(this) {}

ParsedPredicateImpl::~ParsedPredicateImpl(void) {
  argument_uses.ClearWithoutErasure();
}

ParsedPredicateImpl::ParsedPredicateImpl(ParsedModuleImpl *module_,
                                         ParsedClauseImpl *clause_)
    : Def<ParsedPredicateImpl>(this),
      User(this),
      module(module_),
      clause(clause_),
      argument_uses(this) {}

const char *ParsedDeclarationImpl::KindName(void) const {
  switch (context->kind) {
    case DeclarationKind::kQuery: return "query";
    case DeclarationKind::kMessage: return "message";
    case DeclarationKind::kFunctor: return "functor";
    case DeclarationKind::kExport: return "export";
    case DeclarationKind::kLocal: return "local";
  }
  return "";
}

#define EQUAL_DECL(type) \
    bool type::operator==(const type &that) const noexcept { \
      return impl->context == that.impl->context; \
    } \
    bool type::operator!=(const type &that) const noexcept { \
      return impl->context != that.impl->context; \
    }

EQUAL_DECL(ParsedDeclaration)
EQUAL_DECL(ParsedExport)
EQUAL_DECL(ParsedQuery)
EQUAL_DECL(ParsedMessage)
EQUAL_DECL(ParsedFunctor)
EQUAL_DECL(ParsedLocal)

// TODO(pag): Eliminate comparison on pointers.
bool ParsedExport::operator<(const ParsedExport &that) const noexcept {
  return impl->context.get() < that.impl->context.get();
}

#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Wconversion"
#  pragma GCC diagnostic ignored "-Woverflow"
#endif

// Compute a unique identifier for this declaration.
uint64_t ParsedDeclarationImpl::Id(void) const noexcept {
  auto &id = context->id;
  if (id.flat) {
    return id.flat;
  }

  id.flat = ~id.flat;

  // If we're dealing with a local, then embed the module ID, otherwise leave it
  // out.
  switch (context->kind) {
    case DeclarationKind::kExport:
    case DeclarationKind::kQuery:
    case DeclarationKind::kMessage:
    case DeclarationKind::kFunctor:
      break;
    case DeclarationKind::kLocal:
      id.info.module_id = module->first.DisplayId();
      break;
  }

  // NOTE(pag): All anonymous declarations and by definition.
  if (name.Lexeme() == Lexeme::kIdentifierUnnamedAtom) {
    id.info.atom_name_id = module->next_anon_decl_id++;
    id.info.atom_name_id = ~id.info.atom_name_id;

  } else {
    id.info.atom_name_id = name.IdentifierId();
  }
  id.info.arity = std::min<unsigned>(parameters.Size(), kMaxArity);
  return id.flat;
}

#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic pop
#endif

// Compute the unique identifier for this variable.
uint64_t ParsedVariableImpl::Id(void) noexcept {
  auto &id_ref = first_appearance->id;
  if (id_ref.flat) {
    return id_ref.flat;
  }

#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Wconversion"
#endif

  id_ref.flat = clause->Id();
  if (name.Lexeme() == Lexeme::kIdentifierUnnamedVariable) {
    id_ref.info.var_id = clause->next_var_id++;

  } else {
    assert(0 < name.IdentifierId());
    auto &prev_id = clause->named_var_ids[name.IdentifierId()];
    if (!prev_id) {
      prev_id = clause->next_var_id++;
    }
    id_ref.info.var_id = prev_id;
  }

  assert(0 < id_ref.info.var_id);

#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic pop
#endif

  return id_ref.flat;
}

// Compute the unique identifier for this variable, local to its clause.
uint64_t ParsedVariableImpl::IdInClause(void) noexcept {
  if (!id.flat) {
    id.flat = Id();
  }
  return id.info.var_id;
}

// Compute the identifier for this clause.
uint64_t ParsedPredicateImpl::Id(void) const noexcept {
  return declaration->Id();
}

// Compute the identifier for this clause.
uint64_t ParsedClauseImpl::Id(void) const noexcept {
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

// Returns `true` if this variable is an unnamed variable.
bool ParsedVariable::IsUnnamed(void) const noexcept {
  return impl->name.Lexeme() == Lexeme::kIdentifierUnnamedVariable;
}

// Return a unique integer that identifies this variable.
uint64_t ParsedVariable::Id(void) const noexcept {
  return impl->Id();
}

// Compute the unique identifier for this variable, local to its clause.
uint64_t ParsedVariable::IdInClause(void) const noexcept {
  return impl->IdInClause();
}

// A number corresponding to the order of appearance of this variable.
unsigned ParsedVariable::Order(void) const noexcept {
  if (impl->order_of_appearance >= kMaxArity) {
    return (impl->order_of_appearance - kMaxArity) +
           static_cast<unsigned>(impl->clause->head_variables.Size());
  } else {
    return impl->order_of_appearance;
  }
}

bool ParsedVariable::operator==(const ParsedVariable &that) const noexcept {
  return impl->first_appearance == that.impl->first_appearance;
}

bool ParsedVariable::operator!=(const ParsedVariable &that) const noexcept {
  return impl->first_appearance != that.impl->first_appearance;
}

// Return whether or not this variable is used more than once. Appearances
// in the head of a clause count as a use.
bool ParsedVariable::HasMoreThanOneUse(void) const noexcept {
  return impl->first_appearance->next_appearance != nullptr;
}

// Return the variable to which `literal` assigned.
ParsedVariable ParsedVariable::AssignedTo(ParsedLiteral literal) noexcept {
  return ParsedVariable(literal.impl->assignment->lhs.get());
}

DisplayRange ParsedLiteral::SpellingRange(void) const noexcept {
  return impl->literal.SpellingRange();
}

std::optional<std::string_view>
ParsedLiteral::Spelling(Language lang) const noexcept {
  if (IsConstant()) {
    const auto &info = impl->foreign_type->info[static_cast<unsigned>(lang)];
    const auto id = impl->literal.IdentifierId();
    ParsedForeignConstantImpl *backup = nullptr;
    for (ParsedForeignConstantImpl * const const_ptr : *(info.constants)) {
      if (const_ptr->name.IdentifierId() == id) {
        if (const_ptr->lang == lang) {
          return const_ptr->code;
        } else if (const_ptr->lang == Language::kUnknown) {
          backup = const_ptr;
        }
      }
    }
    if (backup) {
      return backup->code;
    }
    return std::nullopt;

  } else {
    return impl->data;
  }
}

// Is this a foreign constant?
bool ParsedLiteral::IsConstant(void) const noexcept {
  return impl->foreign_constant != nullptr;
}

// Is this an enumeration constant?
bool ParsedLiteral::IsEnumerator(void) const noexcept {
  return impl->foreign_constant &&
         impl->foreign_type->is_enum;
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
  return DisplayRange(impl->lhs->name.Position(),
                      impl->rhs->name.NextPosition());
}

ParsedVariable ParsedComparison::LHS(void) const noexcept {
  return ParsedVariable(impl->lhs.get());
}

ParsedVariable ParsedComparison::RHS(void) const noexcept {
  return ParsedVariable(impl->rhs.get());
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

DisplayRange ParsedAssignment::SpellingRange(void) const noexcept {
  return DisplayRange(impl->lhs->name.Position(),
                      impl->rhs.literal.NextPosition());
}

ParsedVariable ParsedAssignment::LHS(void) const noexcept {
  return ParsedVariable(impl->lhs.get());
}

ParsedLiteral ParsedAssignment::RHS(void) const noexcept {
  return ParsedLiteral(&(impl->rhs));
}

DisplayRange ParsedPredicate::SpellingRange(void) const noexcept {
  return DisplayRange(
      impl->negation.IsValid() ? impl->negation.Position() :
                                 impl->name.Position(),
      impl->rparen.IsValid() ? impl->rparen.NextPosition()
                             : impl->name.NextPosition());
}

// Returns `true` if this is a positive predicate.
bool ParsedPredicate::IsPositive(void) const noexcept {
  return impl->negation.IsInvalid();
}

// Returns `true` if this is a negated predicate.
bool ParsedPredicate::IsNegated(void) const noexcept {
  return impl->negation.IsValid();
}

// Returns `true` if this is a negated predicate, and the negation uses
// `@never`.
bool ParsedPredicate::IsNegatedWithNever(void) const noexcept {
  return impl->negation.Lexeme() == Lexeme::kPragmaPerfNever;
}

// Return the negation token used, if any.
Token ParsedPredicate::Negation(void) const noexcept {
  return impl->negation;
}

// Returns the arity of this predicate.
unsigned ParsedPredicate::Arity(void) const noexcept {
  return impl->argument_uses.Size();
}

// Return the `n`th argument of this predicate.
ParsedVariable ParsedPredicate::NthArgument(unsigned n) const noexcept {
  assert(n < Arity());
  return ParsedVariable(impl->argument_uses[n]);
}

// All variables used as arguments to this predicate.
USED_RANGE(ParsedPredicate, Arguments, ParsedVariable, argument_uses)

DisplayRange ParsedAggregate::SpellingRange(void) const noexcept {
  return impl->spelling_range;
}

ParsedPredicate ParsedAggregate::Functor(void) const noexcept {
  return ParsedPredicate(&(impl->functor));
}

ParsedPredicate ParsedAggregate::Predicate(void) const noexcept {
  return ParsedPredicate(&(impl->predicate));
}

// List of parameters to the predicate that are not paired with anything of
// the arguments to the aggregating functor.
USED_RANGE(ParsedAggregate, GroupVariablesFromPredicate, ParsedVariable, group_vars)

// List of parameters from the predicate that are paired with a `aggregate`-
// attributed variable in the functor.
USED_RANGE(ParsedAggregate, AggregatedVariablesFromPredicate, ParsedVariable, aggregate_vars)

// List of parameters from the predicate that are paired with a `bound`-
// attributed variables in the functor.
USED_RANGE(ParsedAggregate, ConfigurationVariablesFromPredicate, ParsedVariable, config_vars)

// Return an integer that identifies this parameter.
uint64_t ParsedParameter::Id(void) const noexcept {
#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Wconversion"
#  pragma GCC diagnostic ignored "-Woverflow"
#endif
  // Cached ID of this declaration.
  parse::IdInterpreter id;
  id.flat = impl->decl->Id();
  id.info.var_id = impl->index;
#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic pop
#endif
  return id.flat;
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

ParsedDeclaration::ParsedDeclaration(const ParsedQuery &query)
    : ParsedDeclaration(query.impl) {}

ParsedDeclaration::ParsedDeclaration(const ParsedMessage &message)
    : ParsedDeclaration(message.impl) {}

ParsedDeclaration::ParsedDeclaration(const ParsedFunctor &functor)
    : ParsedDeclaration(functor.impl) {}

ParsedDeclaration::ParsedDeclaration(const ParsedExport &exp)
    : ParsedDeclaration(exp.impl) {}

ParsedDeclaration::ParsedDeclaration(const ParsedLocal &local)
    : ParsedDeclaration(local.impl) {}

ParsedDeclaration::ParsedDeclaration(const ParsedPredicate &pred)
    : ParsedDeclaration(ParsedDeclaration::Of(pred)) {}

DisplayRange ParsedDeclaration::SpellingRange(void) const noexcept {
  if (impl->parsed_tokens.empty()) {
    if (impl->rparen.IsValid()) {
      auto last_tok = impl->last_tok.IsValid() ? impl->last_tok : impl->rparen;
      return DisplayRange(impl->directive_pos.IsValid() ? impl->directive_pos
                                                        : impl->name.Position(),
                          last_tok.NextPosition());
    } else {
      return impl->name.SpellingRange();
    }
  } else {
    return impl->ParsedRange();
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

// Is this the first declaration?
bool ParsedDeclaration::IsFirstDeclaration(void) const noexcept {
  return impl == impl->context->redeclarations[0];
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
  for (ParsedClauseImpl * const clause : context->clauses) {
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
  return impl->parameters.Size();
}

unsigned ParsedFunctor::Arity(void) const noexcept {
  return impl->parameters.Size();
}

unsigned ParsedQuery::Arity(void) const noexcept {
  return impl->parameters.Size();
}

unsigned ParsedMessage::Arity(void) const noexcept {
  return impl->parameters.Size();
}

unsigned ParsedLocal::Arity(void) const noexcept {
  return impl->parameters.Size();
}

unsigned ParsedExport::Arity(void) const noexcept {
  return impl->parameters.Size();
}

// Return the `n`th parameter of this clause.
ParsedParameter ParsedDeclaration::NthParameter(unsigned n) const noexcept {
  assert(n < Arity());
  return ParsedParameter(impl->parameters[n]);
}

// Return the `n`th parameter of this clause.
ParsedParameter ParsedFunctor::NthParameter(unsigned n) const noexcept {
  assert(n < Arity());
  return ParsedParameter(impl->parameters[n]);
}

// Return the `n`th parameter of this clause.
ParsedParameter ParsedMessage::NthParameter(unsigned n) const noexcept {
  assert(n < Arity());
  return ParsedParameter(impl->parameters[n]);
}

// Return the `n`th parameter of this clause.
ParsedParameter ParsedQuery::NthParameter(unsigned n) const noexcept {
  assert(n < Arity());
  return ParsedParameter(impl->parameters[n]);
}

// Return the `n`th parameter of this clause.
ParsedParameter ParsedLocal::NthParameter(unsigned n) const noexcept {
  assert(n < Arity());
  return ParsedParameter(impl->parameters[n]);
}

// Return the `n`th parameter of this clause.
ParsedParameter ParsedExport::NthParameter(unsigned n) const noexcept {
  assert(n < Arity());
  return ParsedParameter(impl->parameters[n]);
}

DEFINED_RANGE(ParsedDeclaration, Parameters, ParsedParameter, parameters)
USED_RANGE(ParsedDeclaration, Redeclarations, ParsedDeclaration, context->redeclarations)
USED_RANGE(ParsedDeclaration, UniqueRedeclarations, ParsedDeclaration, context->unique_redeclarations)
USED_RANGE(ParsedDeclaration, Clauses, ParsedClause, context->clauses)
USED_RANGE(ParsedDeclaration, PositiveUses, ParsedPredicate, context->positive_uses)
USED_RANGE(ParsedDeclaration, NegativeUses, ParsedPredicate, context->negated_uses)

unsigned ParsedDeclaration::NumPositiveUses(void) const noexcept {
  return impl->context->positive_uses.Size();
}

unsigned ParsedDeclaration::NumNegatedUses(void) const noexcept {
  return impl->context->negated_uses.Size();
}

unsigned ParsedDeclaration::NumClauses(void) const noexcept {
  return impl->context->clauses.Size();
}

bool ParsedDeclaration::IsInline(void) const noexcept {
  return IsQuery() ||
         impl->inline_attribute.Lexeme() == Lexeme::kPragmaPerfInline;
}

// Is this declaration marked with the `@divergent` pragma?
bool ParsedDeclaration::IsDivergent(void) const noexcept {

  // TODO(pag): Implement me.
  return true;
}

std::string_view ParsedDeclaration::BindingPattern(void) const noexcept {
  if (impl->binding_pattern.empty()) {
    impl->binding_pattern.reserve(impl->parameters.Size());
    for (ParsedParameterImpl * const param : impl->parameters) {
      switch (ParsedParameter(param).Binding()) {
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
  return ParsedDeclaration(
      clause.impl->declaration->context->redeclarations[0]);
}

// Return the declaration associated with a predicate. This is the first
// parsed declaration, so it could be in a different module.
ParsedDeclaration ParsedDeclaration::Of(ParsedPredicate pred) {
  return ParsedDeclaration(pred.impl->declaration->context->redeclarations[0]);
}

// Return the declaration associated with a parameter. This is the first
// parsed declaration, so it could be in a different module.
ParsedDeclaration ParsedDeclaration::Containing(ParsedParameter param) {
  return ParsedDeclaration(param.impl->decl->context->redeclarations[0]);
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

ParsedClause ParsedClause::Containing(ParsedAggregate agg) noexcept {
  return ParsedClause(agg.impl->functor->clause);
}

// Is this a deletion clause?
bool ParsedClause::IsDeletion(void) const noexcept {
  return impl->negation.IsValid();
}

DisplayRange ParsedClause::SpellingRange(void) const noexcept {
  auto last_tok = impl->last_tok.IsValid() ? impl->last_tok : impl->dot;
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
  return impl->head_variables.Size();
}

// Returns the number of groups of this clause. Each group is separated by
// a `@barrier` pragma. Most clauses just have a single group.
unsigned ParsedClause::NumGroups(void) const noexcept {
  return static_cast<unsigned>(impl->groups.size());
}

// Return the `n`th parameter of this clause.
ParsedVariable ParsedClause::NthParameter(unsigned n) const noexcept {
  assert(n < Arity());
  return ParsedVariable(impl->head_variables[n]);
}

// All variables used in the body of the clause.
DEFINED_RANGE(ParsedClause, Parameters, ParsedVariable, head_variables)

// All body_variables used in the clause. Some variables might be repeated.
DEFINED_RANGE(ParsedClause, Variables, ParsedVariable, body_variables)

// All positive predicates in the clause.
DEFINED_RANGE_N(ParsedClause, PositivePredicates, ParsedPredicate, groups, positive_predicates)

// All negated predicates in the clause.
DEFINED_RANGE_N(ParsedClause, NegatedPredicates, ParsedPredicate, groups, negated_predicates)

// All assignments of variables to constant literals.
DEFINED_RANGE_N(ParsedClause, Assignments, ParsedAssignment, groups, assignments)

// All comparisons between two variables.
DEFINED_RANGE_N(ParsedClause, Comparisons, ParsedComparison, groups, comparisons)

// All aggregations.
DEFINED_RANGE_N(ParsedClause, Aggregates, ParsedAggregate, groups, aggregates)

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

unsigned ParsedQuery::NumPositiveUses(void) const noexcept {
  return impl->context->positive_uses.Size();
}

unsigned ParsedQuery::NumNegatedUses(void) const noexcept {
  return impl->context->negated_uses.Size();
}

bool ParsedQuery::ReturnsAtMostOneResult(void) const noexcept {
  return impl->first_attribute.IsValid();
}

const ParsedExport &ParsedExport::From(const ParsedDeclaration &decl) {
  assert(decl.IsExport());
  return reinterpret_cast<const ParsedExport &>(decl);
}

DisplayRange ParsedExport::SpellingRange(void) const noexcept {
  auto last_tok = impl->last_tok.IsValid() ? impl->last_tok : impl->rparen;
  return DisplayRange(impl->directive_pos, last_tok.NextPosition());
}

unsigned ParsedExport::NumPositiveUses(void) const noexcept {
  return impl->context->positive_uses.Size();
}

unsigned ParsedExport::NumNegatedUses(void) const noexcept {
  return impl->context->negated_uses.Size();
}

const ParsedLocal &ParsedLocal::From(const ParsedDeclaration &decl) {
  assert(decl.IsLocal());
  return reinterpret_cast<const ParsedLocal &>(decl);
}

DisplayRange ParsedLocal::SpellingRange(void) const noexcept {
  auto last_tok = impl->last_tok.IsValid() ? impl->last_tok : impl->rparen;
  return DisplayRange(impl->directive_pos, last_tok.NextPosition());
}

unsigned ParsedLocal::NumPositiveUses(void) const noexcept {
  return impl->context->positive_uses.Size();
}

unsigned ParsedLocal::NumNegatedUses(void) const noexcept {
  return impl->context->negated_uses.Size();
}

bool ParsedLocal::IsInline(void) const noexcept {
  return impl->inline_attribute.Lexeme() == Lexeme::kPragmaPerfInline;
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
    for (ParsedParameterImpl * const param : impl->parameters) {
      if (ParameterBinding::kFree == ParsedParameter(param).Binding()) {
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

unsigned ParsedFunctor::NumPositiveUses(void) const noexcept {
  return impl->context->positive_uses.Size();
}

unsigned ParsedFunctor::NumNegatedUses(void) const noexcept {
  return impl->context->negated_uses.Size();
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

// Returns `true` if this message is the head of any clause, i.e. if there
// are rules that publish this message.
bool ParsedMessage::IsPublished(void) const noexcept {
  return !impl->context->clauses.Empty();
}

bool ParsedMessage::IsReceived(void) const noexcept {
  return impl->context->clauses.Empty();
}

// Can this message receive/publish removals?
bool ParsedMessage::IsDifferential(void) const noexcept {
  return impl->differential_attribute.IsValid();
}

Token ParsedMessage::Differential(void) const noexcept {
  return impl->differential_attribute;
}

unsigned ParsedMessage::NumPositiveUses(void) const noexcept {
  return impl->context->positive_uses.Size();
}

unsigned ParsedMessage::NumNegatedUses(void) const noexcept {
  return impl->context->negated_uses.Size();
}

DisplayRange ParsedModule::SpellingRange(void) const noexcept {
  return DisplayRange(impl->first.Position(), impl->last.Position());
}


DisplayRange ParsedDatabaseName::SpellingRange(void) const noexcept {
  return DisplayRange(impl->introducer_tok.Position(),
                      impl->dot_tok.NextPosition());
}

Token ParsedDatabaseName::Name(void) const noexcept {
  return impl->name_tok;
}

std::string ParsedDatabaseName::NameAsString(void) const noexcept {
  return impl->name;
}

// Return the name of the database, if any.
std::optional<ParsedDatabaseName>
ParsedModule::DatabaseName(void) const noexcept {
  auto &names = impl->root_module->names;
  if (names.Empty()) {
    return std::nullopt;
  } else {
    return ParsedDatabaseName(names[0]);
  }
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

USED_RANGE(ParsedModule, Queries, ParsedQuery, queries)
USED_RANGE(ParsedModule, Locals, ParsedLocal, locals)
USED_RANGE(ParsedModule, Exports, ParsedExport, exports)
USED_RANGE(ParsedModule, Messages, ParsedMessage, messages)
USED_RANGE(ParsedModule, Functors, ParsedFunctor, functors)

DEFINED_RANGE(ParsedModule, Imports, ParsedImport, imports)
DEFINED_RANGE(ParsedModule, Inlines, ParsedInline, inlines)
DEFINED_RANGE(ParsedModule, Clauses, ParsedClause, clauses)
DEFINED_RANGE(ParsedModule, ForeignTypes, ParsedForeignType, root_module->foreign_types)
DEFINED_RANGE(ParsedModule, EnumTypes, ParsedEnumType, root_module->enum_types)
DEFINED_RANGE(ParsedModule, ForeignConstants, ParsedForeignConstant, root_module->foreign_constants)

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

  const auto &types = impl->root_module->id_to_foreign_type;
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

ParsedForeignType ParsedForeignType::Of(ParsedForeignConstant that) {
  return ParsedForeignType(that.impl->parent);
}

std::optional<ParsedForeignType> ParsedForeignType::Of(ParsedLiteral that) {
  if (that.impl->foreign_type) {
    return ParsedForeignType(that.impl->foreign_type);
  } else {
    return std::nullopt;
  }
}

// A representation of this foreign type as a `TypeLoc`.
TypeLoc ParsedForeignType::Type(void) const noexcept {
  return TypeLoc(impl->name);
}

// Name of this type.
Token ParsedForeignType::Name(void) const noexcept {
  return impl->name;
}

// Name of this type.
std::string_view ParsedForeignType::NameAsString(void) const noexcept {
  return impl->name_view;
}

// Is this type actually built-in?
bool ParsedForeignType::IsBuiltIn(void) const noexcept {
  return impl->is_built_in;
}

// Is this type actually an enumeration type?
bool ParsedForeignType::IsEnum(void) const noexcept {
  return impl->is_enum;
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
bool ParsedForeignType::IsReferentiallyTransparent(
    Language lang_) const noexcept {
  if (impl->is_enum) {
    return true;
  } else {
    const auto lang = static_cast<unsigned>(lang_);
    return impl->is_built_in || impl->info[lang].is_transparent;
  }
}

// Return the prefix and suffix for construction for this language.
std::optional<std::pair<std::string_view, std::string_view>>
ParsedForeignType::Constructor(Language lang_) const noexcept {
  const auto lang = static_cast<unsigned>(lang_);
  const auto &info = impl->info[lang];
  if (lang_ != Language::kUnknown && info.is_present &&
      (!info.constructor_prefix.empty() || !info.constructor_suffix.empty())) {
    return std::make_pair<std::string_view, std::string_view>(
        info.constructor_prefix, info.constructor_suffix);
  } else {
    return std::nullopt;
  }
}

// List of constants defined on this type for a particular language.
UsedNodeRange<ParsedForeignConstant>
ParsedForeignType::Constants(Language lang_) const noexcept {
  const auto lang = static_cast<unsigned>(lang_);
  const auto &info = impl->info[lang];
  return {info.constants->begin(), info.constants->end()};
}

std::optional<ParsedEnumType> ParsedEnumType::From(ParsedForeignType type) {
  if (auto new_impl = dynamic_cast<ParsedEnumTypeImpl *>(type.impl)) {
    return ParsedEnumType(new_impl);
  } else {
    return std::nullopt;
  }
}

// A representation of this enumeration type as a `TypeLoc`.
TypeLoc ParsedEnumType::Type(void) const noexcept {
  return TypeLoc(impl->name);
}

// A representation of this enumeration type as a `TypeLoc`.
TypeLoc ParsedEnumType::UnderlyingType(void) const noexcept {
  return TypeLoc(impl->builtin_type);
}

// Name of this type.
Token ParsedEnumType::Name(void) const noexcept {
  return impl->name;
}

// Name of this type.
std::string_view ParsedEnumType::NameAsString(void) const noexcept {
  return impl->name_view;
}

// Name of this type.
DisplayRange ParsedEnumType::SpellingRange(void) const noexcept {
  return impl->decls.front();
}

// List of constants defined on this type for a particular language.
UsedNodeRange<ParsedForeignConstant>
ParsedEnumType::Enumerators(void) const noexcept {
  const auto lang = static_cast<unsigned>(Language::kUnknown);
  const auto &info = impl->info[lang];
  return {info.constants->begin(), info.constants->end()};
}

ParsedForeignConstant ParsedForeignConstant::From(const ParsedLiteral &lit) {
  assert(lit.impl->foreign_constant != nullptr);
  return ParsedForeignConstant(lit.impl->foreign_constant);
}

TypeLoc ParsedForeignConstant::Type(void) const noexcept {
  return impl->type;
}

// Name of this constant.
Token ParsedForeignConstant::Name(void) const noexcept {
  return impl->name;
}

std::string_view ParsedForeignConstant::NameAsString(void) const noexcept {
  return impl->name_view;
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

// Can the optimizers assume that this constant has a unique value (w.r.t.
// any other constant, marked `@unique` or not).
bool ParsedForeignConstant::IsUnique(void) const noexcept {
  return impl->unique.IsValid();
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

InlineLocation ParsedInline::Location(void) const noexcept {
  return impl->location;
}

}  // namespace hyde

#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic pop
#endif
