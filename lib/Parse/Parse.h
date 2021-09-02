// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <drlojekyll/Display/Display.h>
#include <drlojekyll/Display/DisplayConfiguration.h>
#include <drlojekyll/Lex/Token.h>
#include <drlojekyll/Parse/ErrorLog.h>
#include <drlojekyll/Parse/Parse.h>
#include <drlojekyll/Util/DefUse.h>

#include <cassert>
#include <filesystem>
#include <memory>
#include <string>
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

// Contextual information relevant to all uses of a logical variable within
// a clause.
class VariableContext {
 public:
  inline explicit VariableContext(ParsedClauseImpl *clause_,
                                  ParsedVariableImpl *first_use_)
      : clause(clause_),
        first_use(first_use_) {}

  // Clause containing this variable.
  ParsedClauseImpl *const clause;

  // First use of this variable in `clause`.
  ParsedVariableImpl *first_use;

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
class DeclarationContext : public User {
 public:
  virtual ~DeclarationContext(void);

  inline DeclarationContext(const DeclarationKind kind_)
      : User(this),
        kind(kind_),
        redeclarations(this),
        clauses(this),
        positive_uses(this),
        negated_uses(this) {}

  // Cached ID of this declaration.
  parse::IdInterpreter id;

  // The kind of this declaration.
  const DeclarationKind kind;

  // The list of all re-declarations. They may be spread across multiple
  // modules. Some of the redeclarations may have different parameter bindings.
  WeakUseList<ParsedDeclarationImpl> redeclarations;

  // All clauses associated with a given `DeclarationBase` and each of its
  // redeclarations.
  UseList<ParsedClauseImpl> clauses;

  // All positive uses of this declaration.
  WeakUseList<ParsedPredicateImpl> positive_uses;

  // All negative uses of this declaration.
  WeakUseList<ParsedPredicateImpl> negated_uses;

  bool takes_input{false};
  bool checked_takes_input{false};

  bool generates_value{false};
  bool checked_generates_value{false};

  bool can_be_deleted{false};
};

}  // namespace parse

class ParsedLiteralImpl : public Def<ParsedLiteralImpl> {
 public:
  inline ParsedLiteralImpl(void)
      : Def<ParsedLiteralImpl>(this) {}

  ParsedLiteralImpl *next{nullptr};
  ParsedVariableImpl *assigned_to{nullptr};
  Token literal;
  TypeLoc type;
  std::string data;
  ParsedForeignTypeImpl *foreign_type{nullptr};
  ParsedForeignConstantImpl *foreign_constant{nullptr};
};

class ParsedVariableImpl : public Def<ParsedVariableImpl> {
 public:
  inline ParsedVariableImpl(void)
      : Def<ParsedVariableImpl>(this) {}

  // Compute the unique identifier for this variable.
  uint64_t Id(void) noexcept;

  // Compute the unique identifier for this variable, local to its clause.
  uint64_t IdInClause(void) noexcept;

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
  ParsedVariableImpl *next{nullptr};

  // Next use of the same logical variable in `clause`.
  ParsedVariableImpl *next_use{nullptr};

  // Next variable (not necessarily the same) to be used in an argument list,
  // if this variable is used in an argument list.
  ParsedVariableImpl *next_var_in_arg_list{nullptr};

  // Next group variable in an aggregate.
  ParsedVariableImpl *next_group_var{nullptr};
  ParsedVariableImpl *next_config_var{nullptr};
  ParsedVariableImpl *next_aggregate_var{nullptr};

  std::shared_ptr<parse::VariableContext> context;

  // Whether or not this variable is an parameter to its clause.
  bool is_parameter{false};

  // Whether or not this variable is an argument to a predicate.
  bool is_argument{false};
};

class ParsedComparisonImpl : public Def<ParsedComparisonImpl>, public User {
 public:
  virtual ~ParsedComparisonImpl(void);

  inline ParsedComparisonImpl(ParsedVariableImpl *lhs_,
                              ParsedVariableImpl *rhs_,
                              Token compare_op_)
      : Def<ParsedComparisonImpl>(this),
        User(this),
        lhs(this, lhs_),
        rhs(this, rhs_),
        compare_op(compare_op_) {}

  // Next in the clause body.
  ParsedComparisonImpl *next{nullptr};

  UseRef<ParsedVariableImpl> lhs;
  UseRef<ParsedVariableImpl> rhs;
  const Token compare_op;
};

class ParsedAssignmentImpl : public Def<ParsedAssignmentImpl>, public User {
 public:
  virtual ~ParsedAssignmentImpl(void);

  inline ParsedAssignmentImpl(ParsedVariableImpl *lhs_)
      : Def<ParsedAssignmentImpl>(this),
        User(this),
        lhs(this, lhs_) {}

  // Next in the clause body.
  ParsedAssignmentImpl *next{nullptr};

  UseRef<ParsedVariableImpl> lhs;
  ParsedLiteralImpl rhs;
};

class ParsedPredicateImpl : public Def<ParsedPredicateImpl>, public User {
 public:
  virtual ~ParsedPredicateImpl(void);

  inline ParsedPredicateImpl(ParsedModuleImpl *module_,
                             ParsedClauseImpl *clause_)
      : Def<ParsedPredicateImpl>(this),
        User(this),
        module(module_),
        clause(clause_),
        argument_uses(this) {}

  // Compute the identifier for this predicate.
  uint64_t Id(void) const noexcept;

  // Module in which this predicate application exists.
  ParsedModuleImpl *const module;

  // The clause containing this predicate.
  ParsedClauseImpl *const clause;

  // The declaration associated with this predicate.
  ParsedDeclarationImpl *declaration{nullptr};

  // Next parsed predicate in the clause body. Positive predicates never link
  // to negative predicates, and vice versa.
  ParsedPredicateImpl *next{nullptr};

  // The next use this predicate. This use may be in a different clause body.
  ParsedPredicateImpl *next_use{nullptr};

  // Location information.
  Token negation;
  Token name;
  Token rparen;

  // The argument variables used in this predicate.
  UseList<ParsedVariableImpl> argument_uses;
};

class ParsedAggregateImpl {
 public:
  // Next in the clause.
  ParsedAggregateImpl *next{nullptr};

  DisplayRange spelling_range;
  std::unique_ptr<ParsedPredicateImpl> functor;
  std::unique_ptr<ParsedPredicateImpl> predicate;

  ParsedVariableImpl *first_group_var{nullptr};
  ParsedVariableImpl *first_config_var{nullptr};
  ParsedVariableImpl *first_aggregate_var{nullptr};
};

class ParsedParameterImpl : public Def<ParsedParameterImpl> {
 public:
  inline ParsedParameterImpl(void)
      : Def<ParsedParameterImpl>(this) {}

  // Next in the declaration head.
  ParsedParameterImpl *next{nullptr};

  // Next in the unordered chain.
  ParsedParameterImpl *next_unordered{nullptr};

  // If this parameter is `mutable(func)`, then this points to the functor
  // associated with `func`.
  ParsedFunctorImpl *opt_merge{nullptr};

  // This can be `bound`, `free`, `aggregate`, `summary`, `mutable`, or empty.
  Token opt_binding;

  // Spelling range of this mutable parameter.
  DisplayRange opt_mutable_range;

  // The parameter name.
  Token name;

  // Optional type.
  TypeLoc opt_type;

  unsigned index{~0u};

  // `true` if `opt_type` was produced from parsing, as opposed to type
  // propagation.
  bool parsed_opt_type{false};
};

class ParsedClauseImpl : public Def<ParsedClauseImpl>, public User {
 public:
  inline ParsedClauseImpl(ParsedModuleImpl *module_)
      : Def<ParsedClauseImpl>(this),
        User(this),
        module(module_),
        head_variables(this),
        body_variables(this),
        comparisons(this),
        assignments(this),
        aggregates(this),
        positive_predicates(this),
        negated_predicates(this) {}

  mutable parse::IdInterpreter id;

  // The module containing this clause.
  ParsedModuleImpl *const module;

  // The next clause associated with this `declaration`.
  ParsedClauseImpl *next{nullptr};

  // The next clause defined in `module`.
  ParsedClauseImpl *next_in_module{nullptr};

  // The declaration associated with this clause (definition).
  ParsedDeclarationImpl *declaration{nullptr};

  Token negation;
  Token name;
  Token rparen;
  Token first_body_token;
  Token dot;
  Token last_tok;

  // Signals whether or not we should enable highlighting in the data flow
  // representation to highlight the nodes associated with this clause body.
  Token highlight;

  // Signals whether or not the programmer has explicitly allowed the compiler
  // to generate a cross-product (if needed) when building the data flow for
  // this particular clause.
  Token product;

  // If there's a `false` or a `!true` in the body of the clause, then we mark
  // this clause as being disabled by this token.
  DisplayRange disabled_by;

  // Variables used in this clause.
  DefList<ParsedVariableImpl> head_variables;
  DefList<ParsedVariableImpl> body_variables;

  DefList<ParsedComparisonImpl> comparisons;
  DefList<ParsedAssignmentImpl> assignments;
  DefList<ParsedAggregateImpl> aggregates;
  DefList<ParsedPredicateImpl> positive_predicates;
  DefList<ParsedPredicateImpl> negated_predicates;

  std::unordered_map<unsigned, unsigned> named_var_ids;
  unsigned next_var_id{1};

  // Does this clause depend on any messages?
  bool depends_on_messages{false};

  // Compute the identifier for this clause.
  uint64_t Id(void) const noexcept;
};

class ParsedDeclarationImpl : public Def<ParsedDeclarationImpl>, public User {
 public:
  inline ParsedDeclarationImpl(ParsedModuleImpl *module_,
                               const DeclarationKind kind_)
      : Def<ParsedDeclarationImpl>(this),
        User(this),
        module(module_),
        context(std::make_shared<parse::DeclarationContext>(kind_)),
        parameters(this) {
    context->redeclarations.AddUse(this);
  }

  inline ParsedDeclarationImpl(
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

  inline DisplayRange ParsedRange(void) const {
    return DisplayRange(parsed_tokens.front().Position(),
                        parsed_tokens.back().NextPosition());
  }

  const char *KindName(void) const;

  // Compute a unique identifier for this declaration.
  uint64_t Id(void) const noexcept;

  // Return a list of clauses associated with this declaration.
  DefinedNodeRange<ParsedClause> Clauses(void) const;

  // Return a list of positive uses of this definition.
  UsedNodeRange<ParsedPredicate> PositiveUses(void) const;

  // Return a list of negative uses of this definition.
  NodeRange<ParsedPredicate> NegativeUses(void) const;

  // The module containing this declaration.
  ParsedModuleImpl *const module;

  // The next declaration in this module, possibly unrelated to this one.
  ParsedDeclarationImpl *next{nullptr};

  // The next redeclaration of this declaration. This could be in any module.
  ParsedDeclarationImpl *next_redecl{nullptr};

  // The context that collects all of the declarations together.
  const std::shared_ptr<parse::DeclarationContext> context;

  // The position of the declaration.
  DisplayPosition directive_pos;

  std::vector<Token> parsed_tokens;

  Token name;
  Token rparen;
  Token range_begin_opt;
  Token range_end_opt;
  FunctorRange range{FunctorRange::kZeroOrMore};
  Token inline_attribute;
  Token differential_attribute;
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

  DefList<ParsedParameterImpl> parameters;
  std::string binding_pattern;

 private:
  ParsedDeclarationImpl(void) = delete;
};

class ParsedQueryImpl : public ParsedDeclarationImpl {
 public:
  using ParsedDeclarationImpl::ParsedDeclarationImpl;
};

class ParsedExportImpl : public ParsedDeclarationImpl {
 public:
  using ParsedDeclarationImpl::ParsedDeclarationImpl;
};

class ParsedLocalImpl : public ParsedDeclarationImpl {
 public:
  using ParsedDeclarationImpl::ParsedDeclarationImpl;
};

class ParsedFunctorImpl : public ParsedDeclarationImpl {
 public:
  using ParsedDeclarationImpl::ParsedDeclarationImpl;
};

class ParsedMessageImpl : public ParsedDeclarationImpl {
 public:
  using ParsedDeclarationImpl::ParsedDeclarationImpl;
};

class ParsedImportImpl : public Def<ParsedImportImpl> {
 public:
  inline ParsedImportImpl(void)
      : Def<ParsedImportImpl>(this) {}

  // Next import in this module.
  ParsedImportImpl *next{nullptr};

  DisplayPosition directive_pos;
  Token path;
  Token dot;
  std::filesystem::path resolved_path;
  ParsedModuleImpl *imported_module{nullptr};
};

class ParsedForeignConstantImpl : public Def<ParsedForeignConstantImpl> {
 public:
  inline ParsedForeignConstantImpl(void)
      : Def<ParsedForeignConstantImpl>(this) {}

  // The next foreign constant defined on this type for a particular language.
  ParsedForeignConstantImpl *next{nullptr};
  ParsedForeignConstantImpl *next_with_same_name{nullptr};
  ParsedForeignTypeImpl *parent{nullptr};

  Language lang{Language::kUnknown};
  DisplayRange range;
  std::string code;
  Token name;
  Token unique;
  TypeLoc type;
  bool can_overide{true};
};

class ParsedForeignTypeImpl : public Def<ParsedForeignTypeImpl> {
 public:
  inline ParsedForeignTypeImpl(void)
      : Def<ParsedForeignTypeImpl>(this) {}

  // The next foreign type anywhere in the parse.
  ParsedForeignTypeImpl *next{nullptr};

  // The name of this type.
  Token name;

  // Display ranges for all declarations.
  std::vector<DisplayRange> decls;

  // Is this a built-in type?
  bool is_built_in{false};

  struct Info {
    DisplayRange range;
    std::string code;
    std::string constructor_prefix;
    std::string constructor_suffix;
    bool can_override{true};
    bool is_present{false};
    bool is_transparent{false};
    std::vector<std::unique_ptr<ParsedForeignConstantImpl>> constants;
  } info[kNumLanguages];
};

class ParsedInlineImpl : public Def<ParsedInlineImpl> {
 public:
  inline ParsedInlineImpl(
      DisplayRange range_, std::string_view code_, Language language_,
      bool is_prologue_)
      : Def<ParsedInlineImpl>(this),
        range(range_),
        code(code_),
        language(language_),
        is_prologue(is_prologue_) {}

  // Next inline in this module.
  ParsedInlineImpl *next{nullptr};

  const DisplayRange range;
  const std::string code;
  const Language language;
  const bool is_prologue;
};

class ParsedModuleImpl
    : public std::enable_shared_from_this<ParsedModuleImpl> {
 public:
  ParsedModuleImpl(const DisplayConfiguration &config_)
      : config(config_),
        imports(this),
        inlines(this),
        foreign_types(this),
        foreign_constants(this),
        declarations(this),
        exports(this),
        locals(this),
        queries(this),
        functors(this),
        messages(this),
        clauses(this) {}

  const DisplayConfiguration config;

  // Used by anonymous declarations.
  unsigned next_anon_decl_id{1};

  // Used for the spelling range of the module, as well as getting the
  // module ID (which corresponds with the display ID from which the module
  // is derived).
  Token first;
  Token last;

  ParsedModuleImpl *root_module{nullptr};

  // If this is the root module, then `all_modules` contains a list of all
  // modules, starting with the root module itself.
  std::vector<ParsedModuleImpl *> all_modules;

  std::vector<std::shared_ptr<ParsedModuleImpl>> non_root_modules;


  DefList<ParsedImportImpl> imports;
  DefList<ParsedInlineImpl> inlines;
  DefList<ParsedForeignTypeImpl> foreign_types;
  DefList<ParsedForeignConstantImpl> foreign_constants;

  DefList<ParsedDeclarationImpl> declarations;
  UseList<ParsedExportImpl> exports;
  UseList<ParsedLocalImpl> locals;
  UseList<ParsedQueryImpl> queries;
  UseList<ParsedFunctorImpl> functors;
  UseList<ParsedMessageImpl> messages;

  // All clauses defined in this module.
  DefList<ParsedClauseImpl> clauses;

  // Mapping of identifier IDs to foreign types.
  std::unordered_map<uint32_t, ParsedForeignTypeImpl *> id_to_foreign_type;

  // Maps identifier IDs to foreign constants.
  std::unordered_map<uint32_t, ParsedForeignConstantImpl *>
      id_to_foreign_constant;


  // Local declarations, grouped by `id=(name_id, arity)`.
  std::unordered_map<uint64_t, ParsedDeclarationImpl *> local_declarations;
};

}  // namespace hyde
