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

// Contextual information relevant to all redeclarations.
class DeclarationContext : public User {
 public:
  virtual ~DeclarationContext(void);

  DeclarationContext(const DeclarationKind kind_);

  // Cached ID of this declaration.
  parse::IdInterpreter id;

  // The kind of this declaration.
  const DeclarationKind kind;

  // The list of all re-declarations. They may be spread across multiple
  // modules. Some of the redeclarations may have different parameter bindings.
  WeakUseList<ParsedDeclarationImpl> redeclarations;

  // The list of unique redeclarations (in terms of binding parameters).
  WeakUseList<ParsedDeclarationImpl> unique_redeclarations;

  // All clauses associated with a given `DeclarationBase` and each of its
  // redeclarations.
  UseList<ParsedClauseImpl> clauses;

  // All positive uses of this declaration.
  WeakUseList<ParsedPredicateImpl> positive_uses;

  // All negative uses of this declaration.
  WeakUseList<ParsedPredicateImpl> negated_uses;

  // All forcing uses of this declaration.
  WeakUseList<ParsedPredicateImpl> forcing_uses;

  bool takes_input{false};
  bool checked_takes_input{false};

  bool generates_value{false};
  bool checked_generates_value{false};

  bool can_be_deleted{false};
};

}  // namespace parse

// A literal constant that appears somewhere in the code. Literal constants
// are always synthesized as being part of assignments, even if they appear
// outside of assignments.
class ParsedLiteralImpl : public Def<ParsedLiteralImpl> {
 public:
  virtual ~ParsedLiteralImpl(void);
  explicit ParsedLiteralImpl(ParsedAssignmentImpl *assignment_);

  ParsedAssignmentImpl * const assignment;

  Token literal;
  TypeLoc type;
  std::string data;
  ParsedForeignTypeImpl *foreign_type{nullptr};
  ParsedForeignConstantImpl *foreign_constant{nullptr};
};

// A variable. Mostly these are variables that appear in the code, but these
// also correspond to synthesized variables, i.e. ones that have been invented
// for assignments to
class ParsedVariableImpl : public Def<ParsedVariableImpl> {
 public:
  virtual ~ParsedVariableImpl(void);

  ParsedVariableImpl(ParsedClauseImpl *clause_, Token name_,
                       bool is_parameter_, bool is_argument_);

  // Compute the unique identifier for this variable.
  uint64_t Id(void) noexcept;

  // Compute the unique identifier for this variable, local to its clause.
  uint64_t IdInClause(void) noexcept;

  // The clause to which this variable belongs.
  ParsedClauseImpl *const clause;

  // Name of this variable.
  const Token name;
  std::string name_view;

  // Whether or not this variable is an parameter to its clause.
  const bool is_parameter;

  // Whether or not this variable is an argument to a predicate.
  const bool is_argument;

  TypeLoc type;

  // Unique ID of this variable.
  parse::IdInterpreter id;

  // First appearance of this variable in the clause.
  ParsedVariableImpl *first_appearance{nullptr};

  // Next appearance of this variable in the clause.
  ParsedVariableImpl *next_appearance{nullptr};

  // What was the order of appearance of this variable? The head variables,
  // even invented ones, have appearance values in the range `[0, N)` if there
  // are `N` head variables, and the body variables have appearance values in
  // the range `[kMaxArity, kMaxArity+M)` if there are `M` body variables.
  //
  // The function `ParsedVariable::Order` normalizes this value such that if
  // `N < kMaxArity`, then the order numbers range from `[0, N+M)`.
  unsigned order_of_appearance{0};
};

// Represents a comparison between two variables. If we have a a comparison
// like `A < 1` then that is turned into an assignment of `1` to a temporary
// variable, followed by a comparison between `A` and that temporary.
class ParsedComparisonImpl : public Def<ParsedComparisonImpl>, public User {
 public:
  virtual ~ParsedComparisonImpl(void);

  ParsedComparisonImpl(ParsedVariableImpl *lhs_,
                       ParsedVariableImpl *rhs_,
                       Token compare_op_);

  UseRef<ParsedVariableImpl> lhs;
  UseRef<ParsedVariableImpl> rhs;
  const Token compare_op;
};

// Represents an assignment of a literal to a variable. This comes up explicitly
// as a result of things like `A = 1`, but also implicitly due to constant uses
// like `foo(1)`, which are converted into `foo(A), A = 1`.
class ParsedAssignmentImpl : public Def<ParsedAssignmentImpl>, public User {
 public:
  virtual ~ParsedAssignmentImpl(void);
  ParsedAssignmentImpl(ParsedVariableImpl *lhs_);

  // Next in the clause body.
  ParsedAssignmentImpl *next{nullptr};

  UseRef<ParsedVariableImpl> lhs;
  ParsedLiteralImpl rhs;
};

// Represents the application of a predicate, which is any form of declaration
// (message, export, local, or functor). All arguments to predicates are
// variables, even if they don't appear as such in the original source code.
class ParsedPredicateImpl : public Def<ParsedPredicateImpl>, public User {
 public:
  virtual ~ParsedPredicateImpl(void);

  ParsedPredicateImpl(ParsedModuleImpl *module_,
                      ParsedClauseImpl *clause_);

  // Compute the identifier for this predicate.
  uint64_t Id(void) const noexcept;

  // Module in which this predicate application exists.
  ParsedModuleImpl *const module;

  // The clause containing this predicate.
  ParsedClauseImpl *const clause;

  // The declaration associated with this predicate.
  ParsedDeclarationImpl *declaration{nullptr};

  // Location information.
  Token negation;
  Token force;
  Token name;
  std::string_view name_view;
  Token rparen;

  // The argument variables used in this predicate.
  UseList<ParsedVariableImpl> argument_uses;
};

class ParsedAggregateImpl : public User {
 public:
  virtual ~ParsedAggregateImpl(void);
  explicit ParsedAggregateImpl(ParsedClauseImpl *clause_);

  ParsedClauseImpl * const clause;

  DisplayRange spelling_range;

  ParsedPredicateImpl functor;
  ParsedPredicateImpl predicate;

  UseList<ParsedVariableImpl> group_vars;
  UseList<ParsedVariableImpl> config_vars;
  UseList<ParsedVariableImpl> aggregate_vars;
};

class ParsedParameterImpl : public Def<ParsedParameterImpl> {
 public:
  inline explicit ParsedParameterImpl(ParsedDeclarationImpl *decl_)
      : Def<ParsedParameterImpl>(this),
        decl(decl_) {}

  // The declaration containing this parameter.
  ParsedDeclarationImpl * const decl;

  // If this parameter is `mutable(func)`, then this points to the functor
  // associated with `func`.
  ParsedFunctorImpl *opt_merge{nullptr};

  // This can be `bound`, `free`, `aggregate`, `summary`, `mutable`, or empty.
  Token opt_binding;

  // Spelling range of this mutable parameter.
  DisplayRange opt_mutable_range;

  // The parameter name.
  Token name;
  std::string_view name_view;

  // Optional type.
  TypeLoc opt_type;

  // The index of this parameter within its declaration.
  unsigned index{~0u};

  // `true` if `opt_type` was produced from parsing, as opposed to type
  // propagation.
  bool parsed_opt_type{false};
};

class ParsedClauseImpl : public Def<ParsedClauseImpl>, public User {
 public:
  virtual ~ParsedClauseImpl(void);
  ParsedClauseImpl(ParsedModuleImpl *module_);

  mutable parse::IdInterpreter id;

  // The module containing this clause.
  ParsedModuleImpl *const module;

  // The declaration associated with this clause (definition).
  ParsedDeclarationImpl *declaration{nullptr};

  Token negation;
  Token name;
  std::string_view name_view;
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

  // Maps identifiers of the form `parse::IdInterpreter` to uses of those
  // identifiers in this clause.
  std::unordered_map<uint64_t, UseList<ParsedVariableImpl>> variables;

  // Variables used in this clause.
  DefList<ParsedVariableImpl> head_variables;
  DefList<ParsedVariableImpl> body_variables;

  // Really, there should only be one, but predicates that are messages that we
  // will send just prior to a query's execution.
  DefList<ParsedPredicateImpl> forcing_predicates;

  struct Group {
    inline Group(ParsedClauseImpl *owner_)
        : comparisons(owner_),
          assignments(owner_),
          aggregates(owner_),
          positive_predicates(owner_),
          negated_predicates(owner_) {}

    Token barrier;
    DefList<ParsedComparisonImpl> comparisons;
    DefList<ParsedAssignmentImpl> assignments;
    DefList<ParsedAggregateImpl> aggregates;
    DefList<ParsedPredicateImpl> positive_predicates;
    DefList<ParsedPredicateImpl> negated_predicates;
  };

  std::vector<std::unique_ptr<Group>> groups;

  std::unordered_map<unsigned, unsigned> named_var_ids;
  unsigned next_var_id{1};

  // Does this clause depend on any messages?
  bool depends_on_messages{false};

  // Compute the identifier for this clause.
  uint64_t Id(void) const noexcept;
};

class ParsedDeclarationImpl : public Def<ParsedDeclarationImpl>, public User {
 public:
  virtual ~ParsedDeclarationImpl(void);

  ParsedDeclarationImpl(ParsedModuleImpl *module_,
                        const DeclarationKind kind_);

  ParsedDeclarationImpl(
        ParsedModuleImpl *module_,
        const std::shared_ptr<parse::DeclarationContext> &context_);

  inline DisplayRange ParsedRange(void) const {
    return DisplayRange(parsed_tokens.front().Position(),
                        parsed_tokens.back().NextPosition());
  }

  const char *KindName(void) const;

  // Compute a unique identifier for this declaration.
  uint64_t Id(void) const noexcept;

  // The module containing this declaration.
  ParsedModuleImpl *const module;

  // The next declaration in this module, possibly unrelated to this one.
  ParsedDeclarationImpl *next{nullptr};

  // The next redeclaration of this declaration. This could be in any module.
  // This redecl will exactly match in terms of bound/free attributes.
  ParsedDeclarationImpl *next_redecl{nullptr};
  ParsedDeclarationImpl *first_redecl{nullptr};

  // The context that collects all of the declarations together.
  std::shared_ptr<parse::DeclarationContext> context;

  // The position of the declaration.
  DisplayPosition directive_pos;

  std::vector<Token> parsed_tokens;

  Token name;
  std::string_view name_view;
  Token rparen;
  Token range_begin_opt;
  Token range_end_opt;
  FunctorRange range{FunctorRange::kZeroOrMore};
  Token inline_attribute;
  Token differential_attribute;
  Token first_attribute;
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

  std::string inline_code[kNumLanguages];
  bool inline_code_is_default[kNumLanguages];
  bool inline_code_is_generic[kNumLanguages];

  const std::string &BindingPattern(void) noexcept;

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
  std::string_view name_view;
  Token unique;
  TypeLoc type;
  bool can_overide{true};
};

class ParsedForeignTypeImpl : public Def<ParsedForeignTypeImpl>, public User {
 public:
  virtual ~ParsedForeignTypeImpl(void);
  ParsedForeignTypeImpl(void);

  // The name of this type.
  Token name;
  std::string_view name_view;

  // The underlying builtin type of this foreign type, if any.
  Token builtin_type;

  // Display ranges for all declarations.
  std::vector<DisplayRange> decls;

  // Is this a built-in type?
  bool is_built_in{false};

  // Is this an enumeration type?
  bool is_enum{false};

  struct Info {
    DisplayRange range;
    std::string code;
    std::string constructor_prefix;
    std::string constructor_suffix;
    bool can_override{true};
    bool is_present{false};
    bool is_transparent{false};
    std::unique_ptr<UseList<ParsedForeignConstantImpl>> constants;
  } info[kNumLanguages];
};

class ParsedEnumTypeImpl : public ParsedForeignTypeImpl {
 public:
  virtual ~ParsedEnumTypeImpl(void);
};

class ParsedInlineImpl : public Def<ParsedInlineImpl> {
 public:
  ParsedInlineImpl(
      DisplayRange range_, std::string_view code_, Language language_,
      std::string stage_name_);

  // Next inline in this module.
  ParsedInlineImpl *next{nullptr};

  const DisplayRange range;
  const std::string code;
  const Language language;
  const std::string stage_name;
};

class ParsedDatabaseNameImpl : public Def<ParsedDatabaseNameImpl> {
 public:
  ParsedDatabaseNameImpl(Token introducer_tok_, Token name_tok_,
                                Token dot_,
                                std::vector<std::string> name_parts_);

  const Token introducer_tok;
  const Token name_tok;
  const Token dot_tok;
  const std::vector<std::string> name_parts;
};

class ParsedModuleImpl
    : public std::enable_shared_from_this<ParsedModuleImpl>,
      public User {
 public:

  ~ParsedModuleImpl(void);
  ParsedModuleImpl(const DisplayConfiguration &config_);

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

  DefList<ParsedDatabaseNameImpl> names;
  DefList<ParsedImportImpl> imports;
  DefList<ParsedInlineImpl> inlines;
  DefList<ParsedForeignTypeImpl> foreign_types;
  DefList<ParsedEnumTypeImpl> enum_types;
  DefList<ParsedForeignTypeImpl> builtin_types;
  DefList<ParsedForeignConstantImpl> foreign_constants;

  // All declarations defined in this module, in the order in which they are
  // defined.
  DefList<ParsedDeclarationImpl> declarations;

  // Type-specific lists of the declarations within `declarations`. These lists
  // let us focus on specific subsets of the declarations within the module.
  UseList<ParsedExportImpl, ParsedDeclarationImpl> exports;
  UseList<ParsedLocalImpl, ParsedDeclarationImpl> locals;
  UseList<ParsedQueryImpl, ParsedDeclarationImpl> queries;
  UseList<ParsedFunctorImpl, ParsedDeclarationImpl> functors;
  UseList<ParsedMessageImpl, ParsedDeclarationImpl> messages;

  // All clauses defined in this module, in the order in which they are defined.
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
