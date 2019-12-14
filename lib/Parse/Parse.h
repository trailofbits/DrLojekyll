// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <drlojekyll/Parse/Parse.h>

#include <memory>
#include <unordered_map>
#include <vector>

#include <drlojekyll/Display/Display.h>
#include <drlojekyll/Display/DisplayConfiguration.h>
#include <drlojekyll/Lex/Token.h>

namespace hyde {
namespace parse {

union IdInterpreter {
  uint64_t flat{0};
  struct {
    uint64_t module_id:12;
    uint64_t arity:4;
    uint64_t atom_name_id:24;
    uint64_t var_id:24;
  } info;
};

static_assert(sizeof(IdInterpreter) == 8);

template <typename T>
class Node {
 public:
  T *next{nullptr};
};

template <>
class Impl<ParsedLiteral> : public Node<Impl<ParsedLiteral>> {
 public:
  Impl<ParsedVariable> *assigned_to{nullptr};
  Token literal;
};

class UseBase : public Node<UseBase> {
 public:
  inline UseBase(UseKind use_kind_,
                 parse::Impl<ParsedVariable> *used_var_)
      : use_kind(use_kind_),
        used_var(used_var_) {}

  const UseKind use_kind;
  parse::Impl<ParsedVariable> * const used_var;
};

template<typename T>
class Impl<ParsedUse<T>> : public UseBase {
 public:
  inline Impl(UseKind use_kind_,
              parse::Impl<ParsedVariable> *used_var_,
              parse::Impl<T> *user_)
      : UseBase(use_kind_, used_var_),
        user(user_) {}

  const T user;
};

template <typename T>
using UseList = std::vector<parse::Impl<ParsedUse<T>> *>;

class VariableContext {
 public:
  inline explicit VariableContext(Impl<ParsedClause> *clause_,
                                  Impl<ParsedVariable> *first_use_)
      : clause(clause_),
        first_use(first_use_) {}

  // Clause containing this variable.
  Impl<ParsedClause> * const clause;

  // First use of this variable in `clause`.
  Impl<ParsedVariable> * const first_use;

  // List of assignments to this variable.
  UseList<ParsedAssignment> assignment_uses;

  // List of comparisons against this variable.
  UseList<ParsedComparison> comparison_uses;

  // List of uses of this variable as an argument.
  UseList<ParsedPredicate> argument_uses;

  // List of uses of this variable as a parameter.
  UseList<ParsedClause> parameter_uses;

  // Cached unique identifier.
  IdInterpreter id;
};

template <>
class Impl<ParsedVariable> : public Node<Impl<ParsedVariable>> {
 public:

  // Compute the unique identifier for this variable.
  uint64_t Id(void) noexcept;

  Token name;

  // What was the order of appearance of this variable?
  unsigned appearance{0};

  // Next use of the same logical variable in `clause`.
  Impl<ParsedVariable> *next_use{nullptr};

  // Next variable (not necessarily the same) to be used in an argument list,
  // if this variable is used in an argument list.
  Impl<ParsedVariable> *next_var_in_arg_list{nullptr};

  std::shared_ptr<VariableContext> context;

  // Whether or not this variable is an parameter to its clause.
  bool is_parameter{false};

  // Whether or not this variable is an argument to a predicate.
  bool is_argument{false};
};

template <>
class Impl<ParsedComparison> : public Node<Impl<ParsedComparison>> {
 public:
  inline Impl(Impl<ParsedVariable> *lhs_, Impl<ParsedVariable> *rhs_,
              Token compare_op_)
      : lhs(UseKind::kComparisonLHS, lhs_, this),
        rhs(UseKind::kComparisonRHS, rhs_, this),
        compare_op(compare_op_) {}
  Impl<ParsedUse<ParsedComparison>> lhs;
  Impl<ParsedUse<ParsedComparison>> rhs;
  const Token compare_op;
};

template <>
class Impl<ParsedAssignment> : public Node<Impl<ParsedAssignment>> {
 public:
  inline Impl(Impl<ParsedVariable> *lhs_)
      : lhs(UseKind::kAssignmentLHS, lhs_, this) {}

  Impl<ParsedUse<ParsedAssignment>> lhs;
  Impl<ParsedLiteral> rhs;
};

template <>
class Impl<ParsedPredicate> : public Node<Impl<ParsedPredicate>> {
 public:
  inline Impl(Impl<ParsedModule> *module_, Impl<ParsedClause> *clause_)
      : module(module_),
        clause(clause_) {}

  // Compute the identifier for this predicate.
  uint64_t Id(void) const noexcept;

  // Module in which this predicate application exists.
  Impl<ParsedModule> * const module;

  // The clause containing this predicate.
  Impl<ParsedClause> * const clause;

  // The declaration associated with this predicate.
  Impl<ParsedDeclaration> *declaration{nullptr};

  // The next use this predicate. This use may be in a different clause body.
  Impl<ParsedPredicate> *next_use{nullptr};

  // Location information.
  DisplayPosition negation_pos;
  Token name;
  Token rparen;

  // The argument variables used in this predicate.
  std::vector<std::unique_ptr<Impl<ParsedUse<ParsedPredicate>>>> argument_uses;
};

template <>
class Impl<ParsedAggregate> : public Node<Impl<ParsedAggregate>> {
 public:
  DisplayRange spelling_range;
  std::unique_ptr<Impl<ParsedPredicate>> functor;
  std::unique_ptr<Impl<ParsedPredicate>> predicate;
};

template <>
class Impl<ParsedParameter> : public Node<Impl<ParsedParameter>> {
 public:
  Token opt_binding;
  Token name;
  Token opt_type;
};

template <>
class Impl<ParsedClause> : public Node<Impl<ParsedClause>> {
 public:
  inline Impl(Impl<ParsedModule> *module_)
      : module(module_) {}

  mutable IdInterpreter id;

  // The module containing this clause.
  Impl<ParsedModule> * const module;

  // The next clause defined in `module`.
  Impl<ParsedClause> *next_in_module{nullptr};

  // The declaration associated with this clause (definition).
  Impl<ParsedDeclaration> *declaration{nullptr};

  Token name;
  Token rparen;
  Token dot;

  // Variables used in this clause.
  std::vector<std::unique_ptr<Impl<ParsedVariable>>> head_variables;
  std::vector<std::unique_ptr<Impl<ParsedVariable>>> body_variables;

  std::vector<std::unique_ptr<Impl<ParsedComparison>>> comparisons;
  std::vector<std::unique_ptr<Impl<ParsedAssignment>>> assignments;
  std::vector<std::unique_ptr<Impl<ParsedAggregate>>> aggregates;
  std::vector<std::unique_ptr<Impl<ParsedPredicate>>> positive_predicates;
  std::vector<std::unique_ptr<Impl<ParsedPredicate>>> negated_predicates;

  std::unordered_map<unsigned, unsigned> named_var_ids;
  unsigned next_var_id{1};

  // Compute the identifier for this clause.
  uint64_t Id(void) const noexcept;
};

// Contextual information relevant to all redeclarations.
class DeclarationContext {
 public:
  inline DeclarationContext(const DeclarationKind kind_)
      : kind(kind_) {}

  // Cached ID of this declaration.
  IdInterpreter id;

  // The kind of this declaration.
  const DeclarationKind kind;

  // The list of all re-declarations. They may be spread across multiple
  // modules. Some of the redeclarations may have different parameter bindings.
  std::vector<Impl<ParsedDeclaration> *> redeclarations;

  // All clauses associated with a given `DeclarationBase` and each of its
  // redeclarations.
  std::vector<std::unique_ptr<Impl<ParsedClause>>> clauses;

  // All positive uses of this declaration.
  std::vector<Impl<ParsedPredicate> *> positive_uses;

  // All negative uses of this declaration.
  std::vector<Impl<ParsedPredicate> *> negated_uses;
};

template <>
class Impl<ParsedDeclaration> : public Node<Impl<ParsedDeclaration>> {
 public:
  inline Impl(Impl<ParsedModule> *module_,
              const DeclarationKind kind_)
      : module(module_),
        context(std::make_shared<DeclarationContext>(kind_)) {}

  inline Impl(Impl<ParsedModule> *module_,
              const std::shared_ptr<DeclarationContext> &context_)
      : module(module_),
        context(context_) {}

  const char *KindName(void) const;

  // Compute a unique identifier for this declaration.
  uint64_t Id(void) const noexcept;

  // Return a list of clauses associated with this declaration.
  parse::ParsedNodeRange<ParsedClause> Clauses(void) const;

  // Return a list of positive uses of this definition.
  parse::ParsedNodeRange<ParsedPredicate> PositiveUses(void) const;

  // Return a list of negative uses of this definition.
  parse::ParsedNodeRange<ParsedPredicate> NegativeUses(void) const;

  // The module containing this declaration.
  Impl<ParsedModule> * const module;

  Impl<ParsedDeclaration> *next_redecl{nullptr};

  // The context that collects all of the declarations together.
  const std::shared_ptr<DeclarationContext> context;

  // The position of the declaration.
  DisplayPosition directive_pos;

  Token name;
  Token rparen;
  Token complexity_attribute;
  bool is_aggregate{false};
  std::vector<std::unique_ptr<Impl<ParsedParameter>>> parameters;

 private:
  Impl(void) = delete;
};

template <>
class Impl<ParsedQuery> : public Impl<ParsedDeclaration> {
 public:
  using Impl<ParsedDeclaration>::Impl;
};

template <>
class Impl<ParsedExport> : public Impl<ParsedDeclaration> {
 public:
  using Impl<ParsedDeclaration>::Impl;
};

template <>
class Impl<ParsedLocal> : public Impl<ParsedDeclaration> {
 public:
  using Impl<ParsedDeclaration>::Impl;
};

template <>
class Impl<ParsedFunctor> : public Impl<ParsedDeclaration> {
 public:
  using Impl<ParsedDeclaration>::Impl;
};

template <>
class Impl<ParsedMessage> : public Impl<ParsedDeclaration> {
 public:
  using Impl<ParsedDeclaration>::Impl;
};

template <>
class Impl<ParsedImport> : public Node<Impl<ParsedImport>> {
 public:
  DisplayPosition directive_pos;
  Token path;
  Impl<ParsedModule> *imported_module{nullptr};
};

template <>
 class Impl<ParsedModule>
     : public std::enable_shared_from_this<Impl<ParsedModule>> {
 public:
  Impl(const DisplayConfiguration &config_)
      : config(config_) {}

  const DisplayConfiguration config;

  // Used by anonymous declarations.
  unsigned next_anon_decl_id{1};

  // Used for the spelling range of the module, as well as getting the
  // module ID (which corresponds with the display ID from which the module
  // is derived).
  Token first;
  Token last;

  Impl<ParsedModule> *root_module{nullptr};
  std::vector<std::shared_ptr<Impl<ParsedModule>>> non_root_modules;

  std::vector<std::unique_ptr<Impl<ParsedImport>>> imports;
  std::vector<std::unique_ptr<Impl<ParsedExport>>> exports;
  std::vector<std::unique_ptr<Impl<ParsedLocal>>> locals;
  std::vector<std::unique_ptr<Impl<ParsedQuery>>> queries;
  std::vector<std::unique_ptr<Impl<ParsedFunctor>>> functors;
  std::vector<std::unique_ptr<Impl<ParsedMessage>>> messages;

  // All clauses defined in this module.
  std::vector<Impl<ParsedClause> *> clauses;

  // Local declarations, grouped by `id=(name_id, arity)`.
  std::unordered_map<uint64_t, Impl<ParsedDeclaration> *> local_declarations;
};

}  // namespace parse
}  // namespace hyde
