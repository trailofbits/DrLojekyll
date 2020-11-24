// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <drlojekyll/Display/Display.h>
#include <drlojekyll/Display/DisplayConfiguration.h>
#include <drlojekyll/Lex/Token.h>
#include <drlojekyll/Parse/ErrorLog.h>
#include <drlojekyll/Parse/Parse.h>

#include <cassert>
#include <memory>
#include <unordered_map>
#include <vector>

namespace hyde {

static constexpr unsigned kMaxArity = 63u;

namespace parse {

union IdInterpreter {
  uint64_t flat{0};
  struct {
    uint64_t module_id : 10;
    uint64_t arity : 6;
    uint64_t atom_name_id : 24;
    uint64_t var_id : 24;
  } info;
};

static_assert(sizeof(IdInterpreter) == 8);

template <typename T>
using UseList = std::vector<Node<ParsedUse<T>> *>;

// Contextual information relevant to all uses of a logical variable within
// a clause.
class VariableContext {
 public:
  inline explicit VariableContext(Node<ParsedClause> *clause_,
                                  Node<ParsedVariable> *first_use_)
      : clause(clause_),
        first_use(first_use_) {}

  // Clause containing this variable.
  Node<ParsedClause> *const clause;

  // First use of this variable in `clause`.
  Node<ParsedVariable> *first_use;

  // List of assignments to this variable.
  UseList<ParsedAssignment> assignment_uses;

  // List of comparisons against this variable.
  UseList<ParsedComparison> comparison_uses;

  // List of uses of this variable as an argument.
  UseList<ParsedPredicate> argument_uses;

  // List of uses of this variable as a parameter.
  UseList<ParsedClause> parameter_uses;

  // Cached unique identifier.
  parse::IdInterpreter id;
};

// Contextual information relevant to all redeclarations.
class DeclarationContext {
 public:
  inline DeclarationContext(const DeclarationKind kind_) : kind(kind_) {}

  // Cached ID of this declaration.
  parse::IdInterpreter id;

  // The kind of this declaration.
  const DeclarationKind kind;

  // The list of all re-declarations. They may be spread across multiple
  // modules. Some of the redeclarations may have different parameter bindings.
  std::vector<Node<ParsedDeclaration> *> redeclarations;

  // All clauses associated with a given `DeclarationBase` and each of its
  // redeclarations.
  std::vector<std::unique_ptr<Node<ParsedClause>>> clauses;
  std::vector<std::unique_ptr<Node<ParsedClause>>> deletion_clauses;

  // All positive uses of this declaration.
  std::vector<Node<ParsedPredicate> *> positive_uses;

  // All negative uses of this declaration.
  std::vector<Node<ParsedPredicate> *> negated_uses;

  bool takes_input{false};
  bool checked_takes_input{false};

  bool generates_value{false};
  bool checked_generates_value{false};

  bool can_be_deleted{false};
};

}  // namespace parse

template <>
class Node<ParsedLiteral> {
 public:
  Node<ParsedLiteral> *next{nullptr};
  Node<ParsedVariable> *assigned_to{nullptr};
  Token literal;
  TypeLoc type;
  std::string data;
};

class UseBase {
 public:
  inline UseBase(UseKind use_kind_, Node<ParsedVariable> *used_var_)
      : use_kind(use_kind_),
        used_var(used_var_) {}

  const UseKind use_kind;
  Node<ParsedVariable> *used_var;
};

template <typename T>
class Node<ParsedUse<T>> : public UseBase {
 public:
  inline Node(UseKind use_kind_, Node<ParsedVariable> *used_var_,
              Node<T> *user_)
      : UseBase(use_kind_, used_var_),
        user(user_) {}

  const T user;
  Node<ParsedUse<T>> *next{nullptr};
};

template <>
class Node<ParsedVariable> {
 public:
  // Compute the unique identifier for this variable.
  uint64_t Id(void) noexcept;

  Token name;
  TypeLoc type;

  // What was the order of appearance of this variable? The head variables,
  // even invented ones, have appearance values in the range `[0, N)` if there
  // are `N` head variables, and the body variables have appearance values in
  // the range `[kMaxArity, kMaxArity+M)` if there are `M` body variables.
  //
  // The function `ParsedVariable::Order` normalizes this value such that if
  // `N < kMaxArity`, then the order numbers range from `[0, N+M)`.
  unsigned appearance{0};

  // Next variable used in `clause`, which may be a logically different
  // variable. Variables that are in a clause head are not linked to
  // variables in a clause body.
  Node<ParsedVariable> *next{nullptr};

  // Next use of the same logical variable in `clause`.
  Node<ParsedVariable> *next_use{nullptr};

  // Next variable (not necessarily the same) to be used in an argument list,
  // if this variable is used in an argument list.
  Node<ParsedVariable> *next_var_in_arg_list{nullptr};

  // Next group variable in an aggregate.
  Node<ParsedVariable> *next_group_var{nullptr};
  Node<ParsedVariable> *next_config_var{nullptr};
  Node<ParsedVariable> *next_aggregate_var{nullptr};

  std::shared_ptr<parse::VariableContext> context;

  // Whether or not this variable is an parameter to its clause.
  bool is_parameter{false};

  // Whether or not this variable is an argument to a predicate.
  bool is_argument{false};
};

template <>
class Node<ParsedComparison> {
 public:
  inline Node(Node<ParsedVariable> *lhs_, Node<ParsedVariable> *rhs_,
              Token compare_op_)
      : lhs(UseKind::kComparisonLHS, lhs_, this),
        rhs(UseKind::kComparisonRHS, rhs_, this),
        compare_op(compare_op_) {}

  // Next in the clause body.
  Node<ParsedComparison> *next{nullptr};

  Node<ParsedUse<ParsedComparison>> lhs;
  Node<ParsedUse<ParsedComparison>> rhs;
  const Token compare_op;
};

template <>
class Node<ParsedAssignment> {
 public:
  inline Node(Node<ParsedVariable> *lhs_)
      : lhs(UseKind::kAssignmentLHS, lhs_, this) {}

  // Next in the clause body.
  Node<ParsedAssignment> *next{nullptr};

  Node<ParsedUse<ParsedAssignment>> lhs;
  Node<ParsedLiteral> rhs;
};

template <>
class Node<ParsedPredicate> {
 public:
  inline Node(Node<ParsedModule> *module_, Node<ParsedClause> *clause_)
      : module(module_),
        clause(clause_) {}

  // Compute the identifier for this predicate.
  uint64_t Id(void) const noexcept;

  // Module in which this predicate application exists.
  Node<ParsedModule> *const module;

  // The clause containing this predicate.
  Node<ParsedClause> *const clause;

  // The declaration associated with this predicate.
  Node<ParsedDeclaration> *declaration{nullptr};

  // Next parsed predicate in the clause body. Positive predicates never link
  // to negative predicates, and vice versa.
  Node<ParsedPredicate> *next{nullptr};

  // The next use this predicate. This use may be in a different clause body.
  Node<ParsedPredicate> *next_use{nullptr};

  // Location information.
  DisplayPosition negation_pos;
  Token name;
  Token rparen;

  // The argument variables used in this predicate.
  std::vector<std::unique_ptr<Node<ParsedUse<ParsedPredicate>>>> argument_uses;
};

template <>
class Node<ParsedAggregate> {
 public:
  // Next in the clause.
  Node<ParsedAggregate> *next{nullptr};

  DisplayRange spelling_range;
  std::unique_ptr<Node<ParsedPredicate>> functor;
  std::unique_ptr<Node<ParsedPredicate>> predicate;

  Node<ParsedVariable> *first_group_var{nullptr};
  Node<ParsedVariable> *first_config_var{nullptr};
  Node<ParsedVariable> *first_aggregate_var{nullptr};
};

template <>
class Node<ParsedParameter> {
 public:
  // Next in the declaration head.
  Node<ParsedParameter> *next{nullptr};

  // Next in the unordered chain.
  Node<ParsedParameter> *next_unordered{nullptr};

  // If this parameter is `mutable(func)`, then this points to the functor
  // associated with `func`.
  Node<ParsedFunctor> *opt_merge{nullptr};

  // This can be `bound`, `free`, `aggregate`, `summary`, `mutable`, or empty.
  Token opt_binding;

  // Spelling range of this mutable parameter.
  DisplayRange opt_mutable_range;

  // The parameter name.
  Token name;

  // Last use of the same-named parameter in an unordered set.
  Token opt_unordered_name;

  // Optional type.
  TypeLoc opt_type;

  unsigned index{~0u};

  // `true` if `opt_type` was produced from parsing, as opposed to type
  // propagation.
  bool parsed_opt_type{false};
};

template <>
class Node<ParsedClause> {
 public:
  inline Node(Node<ParsedModule> *module_) : module(module_) {}

  mutable parse::IdInterpreter id;

  // The module containing this clause.
  Node<ParsedModule> *const module;

  // The next clause associated with this `declaration`.
  Node<ParsedClause> *next{nullptr};

  // The next clause defined in `module`.
  Node<ParsedClause> *next_in_module{nullptr};

  // The declaration associated with this clause (definition).
  Node<ParsedDeclaration> *declaration{nullptr};

  Token negation;
  Token name;
  Token rparen;
  Token first_body_token;
  Token dot;
  Token last_tok;

  // Variables used in this clause.
  std::vector<std::unique_ptr<Node<ParsedVariable>>> head_variables;
  std::vector<std::unique_ptr<Node<ParsedVariable>>> body_variables;

  std::vector<std::unique_ptr<Node<ParsedComparison>>> comparisons;
  std::vector<std::unique_ptr<Node<ParsedAssignment>>> assignments;
  std::vector<std::unique_ptr<Node<ParsedAggregate>>> aggregates;
  std::vector<std::unique_ptr<Node<ParsedPredicate>>> positive_predicates;
  std::vector<std::unique_ptr<Node<ParsedPredicate>>> negated_predicates;

  std::unordered_map<unsigned, unsigned> named_var_ids;
  unsigned next_var_id{1};

  // Does this clause depend on any messages?
  bool depends_on_messages{false};

  // Compute the identifier for this clause.
  uint64_t Id(void) const noexcept;
};

template <>
class Node<ParsedDeclaration> {
 public:
  inline Node(Node<ParsedModule> *module_, const DeclarationKind kind_)
      : module(module_),
        context(std::make_shared<parse::DeclarationContext>(kind_)) {
    context->redeclarations.push_back(this);
  }

  inline Node(Node<ParsedModule> *module_,
              const std::shared_ptr<parse::DeclarationContext> &context_)
      : module(module_),
        context(context_) {
    assert(!context->redeclarations.empty());
    context->redeclarations.back()->next_redecl = this;
    context->redeclarations.push_back(this);
  }

  const char *KindName(void) const;

  // Compute a unique identifier for this declaration.
  uint64_t Id(void) const noexcept;

  // Return a list of clauses associated with this declaration.
  NodeRange<ParsedClause> Clauses(void) const;

  // Return a list of clauses associated with this declaration.
  NodeRange<ParsedClause> DeletionClauses(void) const;

  // Return a list of positive uses of this definition.
  NodeRange<ParsedPredicate> PositiveUses(void) const;

  // Return a list of negative uses of this definition.
  NodeRange<ParsedPredicate> NegativeUses(void) const;

  // The module containing this declaration.
  Node<ParsedModule> *const module;

  // The next declaration in this module, possibly unrelated to this one.
  Node<ParsedDeclaration> *next{nullptr};

  // The next redeclaration of this declaration. This could be in any module.
  Node<ParsedDeclaration> *next_redecl{nullptr};

  // The context that collects all of the declarations together.
  const std::shared_ptr<parse::DeclarationContext> context;

  // The position of the declaration.
  DisplayPosition directive_pos;

  Token name;
  Token rparen;
  Token range_begin_opt;
  Token range_end_opt;
  FunctorRange range{FunctorRange::kZeroOrMore};
  Token inline_attribute;
  Token last_tok;

  // Is this decl a functor, and if so, does it have `aggregate`- and
  // `summary`-attributed parameters.
  bool is_aggregate{false};

  // Is this decl a functor, and if so, does it always produce the same outputs
  // given the same inputs, or has it been attributed with `impure` and thus
  // might behave non-deterministically.
  bool is_pure{true};

  // Is this decl a functor, and if so, is it used as a merge functor to a
  // `mutable`-attributed parameter of another decl.
  bool is_merge{false};

  // Does this declaration have any `mutable`-attributed parameter?
  bool has_mutable_parameter{false};

  std::vector<std::unique_ptr<Node<ParsedParameter>>> parameters;
  std::string binding_pattern;

 private:
  Node(void) = delete;
};

template <>
class Node<ParsedQuery> : public Node<ParsedDeclaration> {
 public:
  using Node<ParsedDeclaration>::Node;
};

template <>
class Node<ParsedExport> : public Node<ParsedDeclaration> {
 public:
  using Node<ParsedDeclaration>::Node;
};

template <>
class Node<ParsedLocal> : public Node<ParsedDeclaration> {
 public:
  using Node<ParsedDeclaration>::Node;
};

template <>
class Node<ParsedFunctor> : public Node<ParsedDeclaration> {
 public:
  using Node<ParsedDeclaration>::Node;
};

template <>
class Node<ParsedMessage> : public Node<ParsedDeclaration> {
 public:
  using Node<ParsedDeclaration>::Node;
};

template <>
class Node<ParsedImport> {
 public:
  // Next import in this module.
  Node<ParsedImport> *next{nullptr};

  DisplayPosition directive_pos;
  Token path;
  Node<ParsedModule> *imported_module{nullptr};
};

template <>
class Node<ParsedForeignType> {
 public:
};

template <>
class Node<ParsedInline> {
 public:
  inline Node(DisplayRange range_, std::string_view code_, Language language_,
              bool is_prologue_)
      : range(range_),
        code(code_),
        language(language_),
        is_prologue(is_prologue_) {}

  // Next inline in this module.
  Node<ParsedInline> *next{nullptr};

  const DisplayRange range;
  const std::string code;
  const Language language;
  const bool is_prologue;
};

template <>
class Node<ParsedModule>
    : public std::enable_shared_from_this<Node<ParsedModule>> {
 public:
  Node(const DisplayConfiguration &config_) : config(config_) {}

  const DisplayConfiguration config;

  // Used by anonymous declarations.
  unsigned next_anon_decl_id{1};

  // Used for the spelling range of the module, as well as getting the
  // module ID (which corresponds with the display ID from which the module
  // is derived).
  Token first;
  Token last;

  Node<ParsedModule> *root_module{nullptr};

  // If this is the root module, then `all_modules` contains a list of all
  // modules, starting with the root module itself.
  std::vector<Node<ParsedModule> *> all_modules;

  std::vector<std::shared_ptr<Node<ParsedModule>>> non_root_modules;

  std::vector<std::unique_ptr<Node<ParsedImport>>> imports;
  std::vector<std::unique_ptr<Node<ParsedInline>>> inlines;
  std::vector<std::unique_ptr<Node<ParsedExport>>> exports;
  std::vector<std::unique_ptr<Node<ParsedLocal>>> locals;
  std::vector<std::unique_ptr<Node<ParsedQuery>>> queries;
  std::vector<std::unique_ptr<Node<ParsedFunctor>>> functors;
  std::vector<std::unique_ptr<Node<ParsedMessage>>> messages;

  // All clauses defined in this module.
  std::vector<Node<ParsedClause> *> clauses;
  std::vector<Node<ParsedClause> *> deletion_clauses;

  // Local declarations, grouped by `id=(name_id, arity)`.
  std::unordered_map<uint64_t, Node<ParsedDeclaration> *> local_declarations;
};

}  // namespace hyde
