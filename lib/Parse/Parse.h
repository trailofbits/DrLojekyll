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

class DeclarationBase;

template <typename T>
class Node {
 public:
  T *next{nullptr};
};

template <>
class Impl<ParsedLiteral> : public Node<Impl<ParsedLiteral>> {
 public:
  Token literal;
};

template <>
class Impl<ParsedVariable> : public Node<Impl<ParsedVariable>> {
 public:

  // Compute the unique identifier for this variable.
  uint64_t Id(void) noexcept;

  Token name;
  Impl<ParsedClause> *clause{nullptr};

  // Use chain of this variable within its clause.
  Impl<ParsedVariable> *next_use{nullptr};
  Impl<ParsedVariable> *first_use{nullptr};

  // Cached unique identifier.
  IdInterpreter id;

  // Whether or not this variable is an argument to its clause.
  bool is_argument{false};
};

template <>
class Impl<ParsedComparison> : public Node<Impl<ParsedComparison>> {
 public:
  Impl<ParsedVariable> *lhs{nullptr};
  Impl<ParsedVariable> *rhs{nullptr};
  Token compare_op;
};

template <>
class Impl<ParsedAssignment> : public Node<Impl<ParsedAssignment>> {
 public:
  Impl<ParsedVariable> *lhs{nullptr};
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
  DeclarationBase *declaration{nullptr};

  // The next use this predicate. This use may be in a different clause
  // body.
  Impl<ParsedPredicate> *next_use{nullptr};

  DisplayPosition negation_pos;
  Token name;
  Token rparen;
  std::vector<Impl<ParsedVariable> *> arguments;
};

template <>
class Impl<ParsedParameter> : public Node<Impl<ParsedParameter>> {
 public:
  Token opt_binding;
  Token name;
  Token opt_type;
};

template <>
class Impl<ParsedDeclaration> : public Node<Impl<ParsedDeclaration>> {
 public:
  Token name;
  Token rparen;
  std::vector<std::unique_ptr<Impl<ParsedParameter>>> parameters;
};

template <>
class Impl<ParsedClause> : public Node<Impl<ParsedClause>> {
 public:
  inline Impl(Impl<ParsedModule> *module_)
      : module(module_) {}

  mutable IdInterpreter id;

  // The module containing this clause.
  Impl<ParsedModule> * const module;

  // The declaration associated with this clause (definition).
  DeclarationBase *declaration{nullptr};

  Token name;
  Token rparen;
  Token dot;

  // The parameter variables.
  std::vector<Impl<ParsedVariable> *> parameters;

  // Variables used in this clause. The first `N` variables correspond with
  // the clause head.
  std::vector<std::unique_ptr<Impl<ParsedVariable>>> variables;
  std::vector<std::unique_ptr<Impl<ParsedComparison>>> comparisons;
  std::vector<std::unique_ptr<Impl<ParsedAssignment>>> assignments;
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
  inline DeclarationContext(const char *kind_)
      : kind(kind_) {}

  // Cached ID of this declaration.
  IdInterpreter id;

  // The kind of this declaration, e.g. "functor", "export", "local",
  // "message".
  const char * const kind;

  // The list of all re-declarations. They may be spread across multiple
  // modules. Some of the redeclarations may have different parameter bindings.
  std::vector<DeclarationBase *> redeclarations;

  // All clauses associated with a given `DeclarationBase` and each of its
  // redeclarations.
  std::vector<std::unique_ptr<Impl<ParsedClause>>> clauses;

  // All positive uses of this declaration.
  std::vector<Impl<ParsedPredicate> *> positive_uses;

  // All negative uses of this declaration.
  std::vector<Impl<ParsedPredicate> *> negated_uses;
};

// Basic information about each declarati9n.
class DeclarationBase : public Node<DeclarationBase> {
 public:
  inline DeclarationBase(Impl<ParsedModule> *module_,
                         const char *kind_)
      : module(module_),
        context(std::make_shared<DeclarationContext>(kind_)) {}

  inline DeclarationBase(Impl<ParsedModule> *module_,
                         const std::shared_ptr<DeclarationContext> &context_)
      : module(module_),
        context(context_) {}

  // Compute a unique identifier for this declaration.
  uint64_t Id(void) const;

  // Return a list of clauses associated with this declaration.
  parse::ParsedNodeRange<ParsedClause> Clauses(void) const;

  // Return a list of positive uses of this definition.
  parse::ParsedNodeRange<ParsedPredicate> PositiveUses(void) const;

  // Return a list of negative uses of this definition.
  parse::ParsedNodeRange<ParsedPredicate> NegativeUses(void) const;

  // The module containing this declaration.
  Impl<ParsedModule> * const module;

  // The context that collects all of the declarations together.
  const std::shared_ptr<DeclarationContext> context;

  Impl<ParsedDeclaration> head;

  // The position of the declaration.
  DisplayPosition directive_pos;

 private:
  DeclarationBase(void) = delete;
};

template <>
class Impl<ParsedQuery> : public DeclarationBase {
 public:
  using DeclarationBase::DeclarationBase;
};

template <>
class Impl<ParsedExport> : public DeclarationBase {
 public:
  using DeclarationBase::DeclarationBase;
};

template <>
class Impl<ParsedLocal> : public DeclarationBase {
 public:
  using DeclarationBase::DeclarationBase;
};

template <>
class Impl<ParsedFunctor> : public DeclarationBase {
 public:
  using DeclarationBase::DeclarationBase;
};

template <>
class Impl<ParsedMessage> : public DeclarationBase {
 public:
  using DeclarationBase::DeclarationBase;
};

template <>
class Impl<ParsedImport> : public Node<Impl<ParsedImport>> {
 public:
  DisplayPosition directive_pos;
  Token path;
  Impl<ParsedModule> *imported_module{nullptr};
};

template <>
class Impl<ParsedModule> : public Node<Impl<ParsedModule>> {
 public:
  Impl(const DisplayConfiguration &config_)
      : config(config_) {}

  const DisplayConfiguration config;

  // Used for the spelling range of the module, as well as getting the
  // module ID (which corresponds with the display ID from which the module
  // is derived).
  Token first;
  Token last;

  std::vector<std::unique_ptr<Impl<ParsedImport>>> imports;
  std::vector<std::unique_ptr<Impl<ParsedExport>>> exports;
  std::vector<std::unique_ptr<Impl<ParsedLocal>>> locals;
  std::vector<std::unique_ptr<Impl<ParsedQuery>>> queries;
  std::vector<std::unique_ptr<Impl<ParsedFunctor>>> functors;
  std::vector<std::unique_ptr<Impl<ParsedMessage>>> messages;

  // Local declarations, grouped by `id=(name_id, arity)`.
  std::unordered_map<uint64_t, DeclarationBase *> local_declarations;
};

}  // namespace parse
}  // namespace hyde
