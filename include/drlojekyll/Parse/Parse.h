// Copyright 2019, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/Display/DisplayPosition.h>
#include <drlojekyll/Lex/Token.h>
#include <drlojekyll/Parse/Type.h>
#include <drlojekyll/Util/DefUse.h>
#include <drlojekyll/Util/Node.h>

#include <filesystem>
#include <functional>
#include <memory>
#include <string_view>
#include <utility>

namespace hyde {

class Parser;
class ParserImpl;

enum class ParameterBinding {
  kImplicit,
  kMutable,
  kFree,
  kBound,
  kAggregate,
  kSummary
};

class ParsedAssignment;
class ParsedClause;
class ParsedComparison;
class ParsedPredicate;

enum class Language : unsigned { kUnknown, kCxx, kPython, kFlatBuffer };
static constexpr auto kNumLanguages = 4u;

// Represents a literal.
class ParsedLiteralImpl;
class ParsedLiteral : public Node<ParsedLiteral, ParsedLiteralImpl> {
 public:
  DisplayRange SpellingRange(void) const noexcept;

  std::optional<std::string_view> Spelling(Language lang) const noexcept;

  // Is this a foreign constant?
  bool IsConstant(void) const noexcept;

  // Is this an enumeration constant?
  bool IsEnumerator(void) const noexcept;

  // Is this a numeric immediate literal? This could encompass both integral
  // and floating point values, including integers of hexadecimal, octal, and
  // binary representations.
  bool IsNumber(void) const noexcept;

  // Is this a string literal? This does not include code literals.
  bool IsString(void) const noexcept;

  // Is this a Boolean literal, i.e. `true` or `false`.
  bool IsBoolean(void) const noexcept;

  // What is the type of this literal? The returned `TypeLoc` refers to the
  // source of the type that we used to infer the type, based off of usage.
  TypeLoc Type(void) const noexcept;

  // Token representing the use of this constant.
  Token Literal(void) const noexcept;

 protected:
  friend class ParsedVariable;
  friend class ParsedForeignConstant;
  friend class ParsedForeignType;

  using Node<ParsedLiteral, ParsedLiteralImpl>::Node;
};

// Type of a use.
enum class UseKind {
  kParameter,
  kArgument,
  kAssignmentLHS,
  kComparisonLHS,
  kComparisonRHS
};

// Represents a parsed variable.
class ParsedVariableImpl;
class ParsedVariable : public Node<ParsedVariable, ParsedVariableImpl> {
 public:
  DisplayRange SpellingRange(void) const noexcept;

  // Returns the token corresponding with the name of this variable.
  Token Name(void) const noexcept;
  std::string_view NameAsString(void) const noexcept;

  // Returns the type of the variable.
  TypeLoc Type(void) const noexcept;

  // Returns `true` if this variable is an unnamed variable.
  bool IsUnnamed(void) const noexcept;

  // Return the variable to which `literal` assigned.
  static ParsedVariable AssignedTo(ParsedLiteral literal) noexcept;

  // Return a unique integer that identifies this variable. Within a given
  // clause, all body_variables with the same name will have the same identifier.
  uint64_t Id(void) const noexcept;

  // Compute the unique identifier for this variable, local to its clause.
  uint64_t IdInClause(void) const noexcept;

  // A number corresponding to the order of appearance of this variable.
  unsigned Order(void) const noexcept;

  // Override `operator==` so that different uses of the same variable appear
  // the same.
  bool operator==(const ParsedVariable &that) const noexcept;
  bool operator!=(const ParsedVariable &that) const noexcept;

  // Return whether or not this variable is used more than once. Appearances
  // in the head of a clause count as a use.
  bool HasMoreThanOneUse(void) const noexcept;

 protected:
  friend class ParsedAssignment;
  friend class ParsedClause;
  friend class ParsedComparison;
  friend class ParsedPredicate;

  using Node<ParsedVariable, ParsedVariableImpl>::Node;
};

enum class ComparisonOperator : int {
  kEqual,
  kNotEqual,
  kLessThan,
  kGreaterThan
};

// Represents an attempt to unify two body_variables. E.g. `V1=V2`, `V1<V2`, etc.
class ParsedComparisonImpl;
class ParsedComparison : public Node<ParsedComparison, ParsedComparisonImpl> {
 public:
  DisplayRange SpellingRange(void) const noexcept;
  ParsedVariable LHS(void) const noexcept;
  ParsedVariable RHS(void) const noexcept;
  ComparisonOperator Operator(void) const noexcept;

 protected:
  friend class ParsedClause;
  using Node<ParsedComparison, ParsedComparisonImpl>::Node;
};

// Represents and attempt to assign a literal to a variable, e.g. `V=1`.
class ParsedAssignmentImpl;
class ParsedAssignment : public Node<ParsedAssignment, ParsedAssignmentImpl> {
 public:
  DisplayRange SpellingRange(void) const noexcept;
  ParsedVariable LHS(void) const noexcept;
  ParsedLiteral RHS(void) const noexcept;

  // Return the assignment using the literal.
  static ParsedAssignment Using(ParsedLiteral literal);

 protected:
  friend class ParsedClause;
  using Node<ParsedAssignment, ParsedAssignmentImpl>::Node;
};

// Represents a call to a functor, receipt of a message, etc.
class ParsedPredicateImpl;
class ParsedPredicate : public Node<ParsedPredicate, ParsedPredicateImpl> {
 public:
  Token Name(void) const noexcept;
  std::string_view NameAsString(void) const noexcept;

  DisplayRange SpellingRange(void) const noexcept;

  // Returns `true` if this is a positive predicate.
  bool IsPositive(void) const noexcept;

  // Returns `true` if this is a negated predicate.
  bool IsNegated(void) const noexcept;

  // Returns `true` if this is a negated predicate, and the negation uses
  // `@never`.
  bool IsNegatedWithNever(void) const noexcept;

  // Returns the arity of this predicate.
  unsigned Arity(void) const noexcept;

  // Return the negation token used, if any.
  Token Negation(void) const noexcept;

  // Return the `n`th argument of this predicate.
  ParsedVariable NthArgument(unsigned n) const noexcept;

  // All variables used as arguments to this predicate.
  UsedNodeRange<ParsedVariable> Arguments(void) const;

 protected:
  friend class ParsedClause;
  friend class ParsedDeclaration;
  using Node<ParsedPredicate, ParsedPredicateImpl>::Node;
};

// Represents a call to an aggregation functor over some predicate. For example:
//
//    #functor count_i32(aggregate i32 Val, summary i32 NumVals)
//    #local node(i32 Id)
//    #local num_nodes(i32 NumNodes) : count_i32(Id, NumNodes) over node(Id).
//
// The requirement for use is that any summary value
class ParsedAggregateImpl;
class ParsedAggregate : public Node<ParsedAggregate, ParsedAggregateImpl> {
 public:
  DisplayRange SpellingRange(void) const noexcept;
  ParsedPredicate Functor(void) const noexcept;
  ParsedPredicate Predicate(void) const noexcept;

  // List of parameters from the predicate that are not paired with anything of
  // the arguments to the aggregating functor.
  //
  // This corresponds with `A` in the following:
  //
  //    count_grouped_As(A, NumXs)
  //      : count(X, NumXs) over (i32 A, i32 X) { blah(X). }.
  //
  // This pattern is interpreted like a "group by `A`". Another way of thinking
  // of it is that we want to have a distinct aggregation object per unique `A`.
  //
  // To the rest of the clause body, these are treated as free variables which
  // are bound by the aggregate.
  UsedNodeRange<ParsedVariable> GroupVariablesFromPredicate(void) const;

  // List of parameters from the predicate that are paired with a `aggregate`-
  // attributed variable in the functor.
  UsedNodeRange<ParsedVariable> AggregatedVariablesFromPredicate(void) const;

  // List of parameters from the predicate that are paired with a `bound`-
  // attributed variables in the functor. These behave in a similar way to group
  // variables, in that they do end up grouping the results; however, the
  // difference is that these are passed into the aggregating functor when its
  // aggregation state is initialized, and thus they act as configuration or
  // initialization values for the functor's state.
  UsedNodeRange<ParsedVariable> ConfigurationVariablesFromPredicate(void) const;

 protected:
  friend class ParsedClause;
  using Node<ParsedAggregate, ParsedAggregateImpl>::Node;
};

// Represents a parsed parameter. The following are valid forms:
//
//    free type A
//    bound type A
//    free A
//    bound A
//    type A
//    mutable(merge_functor) A
//    aggregate type A
//    summary type A
//
// Parameter names (`A` in the above example) must be identifiers beginning with
// an upper case character, or `_`.
//
// Things like the binding specification are optional in some contexts but
// not others (e.g. in export directives).
class ParsedParameterImpl;
class ParsedParameter : public Node<ParsedParameter, ParsedParameterImpl> {
 public:

  // Return an integer that identifies this parameter.
  uint64_t Id(void) const noexcept;

  DisplayRange SpellingRange(void) const noexcept;
  Token Name(void) const noexcept;
  std::string_view NameAsString(void) const noexcept;
  TypeLoc Type(void) const noexcept;
  ParameterBinding Binding(void) const noexcept;
  unsigned Index(void) const noexcept;

  // Returns `true` if this variable is an unnamed variable.
  bool IsUnnamed(void) const noexcept;

 protected:
  friend class ParsedFunctor;

  using Node<ParsedParameter, ParsedParameterImpl>::Node;
};

class ParsedClauseHead;
class ParsedClauseBody;

// Represents a parsed clause, which defines either an internal or exported
// predicate.
class ParsedClauseImpl;
class ParsedClause : public Node<ParsedClause, ParsedClauseImpl> {
 public:

  // Traverse upward in the AST.
  static ParsedClause Containing(ParsedVariable var) noexcept;
  static ParsedClause Containing(ParsedPredicate pred) noexcept;
  static ParsedClause Containing(ParsedAssignment var) noexcept;
  static ParsedClause Containing(ParsedComparison cmp) noexcept;
  static ParsedClause Containing(ParsedAggregate agg) noexcept;

  Token Name(void) const noexcept;
  std::string_view NameAsString(void) const noexcept;

  DisplayRange SpellingRange(void) const noexcept;

  // Should this clause be highlighted in the data flow representation?
  bool IsHighlighted(void) const noexcept;

  // Returns `true` if this clause body is disabled. A disabled clause body
  // is one that contains a free `false` or `!true` predicate.
  bool IsDisabled(DisplayRange *disabled_by = nullptr) const noexcept;

  // Are cross-products permitted when building the data flow representation
  // for this clause?
  bool CrossProductsArePermitted(void) const noexcept;

  // Returns the arity of this clause.
  unsigned Arity(void) const noexcept;

  // Returns the number of groups of this clause. Each group is separated by
  // a `@barrier` pragma. Most clauses just have a single group.
  unsigned NumGroups(void) const noexcept;

  // Return the `n`th parameter of this clause.
  ParsedVariable NthParameter(unsigned n) const noexcept;

  // All variables used as parameters to this clause.
  DefinedNodeRange<ParsedVariable> Parameters(void) const;

  // All variables used in the body of the clause.
  DefinedNodeRange<ParsedVariable> Variables(void) const;

  // All positive predicates in the clause.
  DefinedNodeRange<ParsedPredicate> PositivePredicates(unsigned group_num) const;

  // All negated predicates in the clause.
  DefinedNodeRange<ParsedPredicate> NegatedPredicates(unsigned group_num) const;

  // All assignments of variables to constant literals.
  DefinedNodeRange<ParsedAssignment> Assignments(unsigned group_num) const;

  // All comparisons between two variables.
  DefinedNodeRange<ParsedComparison> Comparisons(unsigned group_num) const;

  // All aggregations.
  DefinedNodeRange<ParsedAggregate> Aggregates(unsigned group_num) const;

  // Is this a deletion clause?
  bool IsDeletion(void) const noexcept;

  // Returns a possible "forcing message." This only applies to queries, where
  // the message parameters must all be constants or unify against the query
  // parameters.
  std::optional<ParsedPredicate> ForcingMessage(void) const;

 protected:
  friend class ParsedClauseHead;
  friend class ParsedClauseBody;
  friend class ParsedDeclaration;

  using Node<ParsedClause, ParsedClauseImpl>::Node;
};

class ParsedClauseHead {
 public:
  inline explicit ParsedClauseHead(ParsedClause clause_) : clause(clause_) {}

  DisplayRange SpellingRange(void) const noexcept;

  const ParsedClause clause;
};

class ParsedClauseBody {
 public:
  inline explicit ParsedClauseBody(ParsedClause clause_) : clause(clause_) {}

  DisplayRange SpellingRange(void) const noexcept;

  const ParsedClause clause;
};

enum class DeclarationKind { kQuery, kMessage, kFunctor, kExport, kLocal };

class ParsedQuery;
class ParsedMessage;
class ParsedFunctor;
class ParsedExport;
class ParsedLocal;

// The head of a declaration. This includes the name of the clause.
// Clause head names must be identifiers beginning with a lower case character.
class ParsedDeclarationImpl;
class ParsedDeclaration : public Node<ParsedDeclaration, ParsedDeclarationImpl> {
 public:
  ParsedDeclaration(const ParsedQuery &query);
  ParsedDeclaration(const ParsedMessage &message);
  ParsedDeclaration(const ParsedFunctor &functor);
  ParsedDeclaration(const ParsedExport &exp);
  ParsedDeclaration(const ParsedLocal &local);
  ParsedDeclaration(const ParsedPredicate &pred);

  DisplayRange SpellingRange(void) const noexcept;

  bool operator==(const ParsedDeclaration &that) const noexcept;
  bool operator!=(const ParsedDeclaration &that) const noexcept;

  // Return the ID of this declaration.
  uint64_t Id(void) const;

  // Return the name of this declaration as a token.
  Token Name(void) const noexcept;

  // Return the name of this declaration as a string.
  std::string_view NameAsString(void) const noexcept;

  // Is this the first declaration?
  bool IsFirstDeclaration(void) const noexcept;

  bool IsQuery(void) const noexcept;
  bool IsMessage(void) const noexcept;
  bool IsFunctor(void) const noexcept;
  bool IsExport(void) const noexcept;
  bool IsLocal(void) const noexcept;

  // Does this declaration have a `mutable`-attributed parameter? If so, then
  // this relation must be materialized.
  bool HasMutableParameter(void) const noexcept;

  // Does this declaration have a clause that directly depends on a `#message`?
  bool HasDirectInputDependency(void) const noexcept;

  // The kind of this declaration.
  DeclarationKind Kind(void) const noexcept;

  // The string version of this kind name.
  const char *KindName(void) const noexcept;

  // Returns the arity of this declaration.
  unsigned Arity(void) const noexcept;

  // Return the `n`th parameter of this declaration.
  ParsedParameter NthParameter(unsigned n) const noexcept;

  UsedNodeRange<ParsedDeclaration> Redeclarations(void) const;
  UsedNodeRange<ParsedDeclaration> UniqueRedeclarations(void) const;
  DefinedNodeRange<ParsedParameter> Parameters(void) const;
  UsedNodeRange<ParsedClause> Clauses(void) const;
  UsedNodeRange<ParsedPredicate> PositiveUses(void) const;
  UsedNodeRange<ParsedPredicate> NegativeUses(void) const;

  unsigned NumPositiveUses(void) const noexcept;
  unsigned NumNegatedUses(void) const noexcept;
  unsigned NumClauses(void) const noexcept;

  // Is this declaration marked with the `@inline` pragma?
  bool IsInline(void) const noexcept;

  // Is this declaration marked with the `@divergent` pragma?
  bool IsDivergent(void) const noexcept;

  inline unsigned NumUses(void) const noexcept {
    return NumPositiveUses() + NumNegatedUses();
  }

  // Return the declaration associated with a clause. This is the first
  // parsed declaration, so it could be in a different module.
  static ParsedDeclaration Of(ParsedClause clause);

  // Return the declaration associated with a predicate. This is the first
  // parsed declaration, so it could be in a different module.
  static ParsedDeclaration Of(ParsedPredicate pred);

  // Return the declaration associated with a parameter. This is the first
  // parsed declaration, so it could be in a different module.
  static ParsedDeclaration Containing(ParsedParameter pred);

  // A string representing a binding pattern of the parameters. A bound
  // parameter is `b`, a free one is `f`, mutable is `m`, aggregate is `a`,
  // and summary is `s`. This returns a string of letters, e.g. `bbf` means
  // two bound parameters, followed by a free parameter.
  std::string_view BindingPattern(void) const noexcept;

 protected:
  friend class ParserImpl;
  using Node<ParsedDeclaration, ParsedDeclarationImpl>::Node;
};

class ParsedDeclarationName {
 public:
  inline explicit ParsedDeclarationName(ParsedDeclaration decl_)
      : decl(decl_) {}
  const ParsedDeclaration decl;
};

// Represents a rule that has been exported to the user for querying the
// database. These rules must have global unique names. For example:
//
//    #query rule(bound type Var, free type Var)
//
// Exported rules are unique in that they limit the scope of how they can be
// used, by requiring that users bind all `bound`-attributed arguments. The
// same logical rule can have multiple exports, so long as they all have
// different rule binding types.
//
// Specifying what is bound is a proxy for indexing. That is, you can think of
// the set of `bound`-attributed parameters of a `#query` as being an index
// on an SQL table.
//
// Query declarations and defined clauses can be defined in any module.
class ParsedQueryImpl;
class ParsedQuery : public Node<ParsedQuery, ParsedQueryImpl> {
 public:
  static const ParsedQuery &From(const ParsedDeclaration &decl);

  bool operator==(const ParsedQuery &that) const noexcept;
  bool operator!=(const ParsedQuery &that) const noexcept;

  DisplayRange SpellingRange(void) const noexcept;
  uint64_t Id(void) const noexcept;
  Token Name(void) const noexcept;
  std::string_view NameAsString(void) const noexcept;
  unsigned Arity(void) const noexcept;
  ParsedParameter NthParameter(unsigned n) const noexcept;

  unsigned NumPositiveUses(void) const noexcept;
  unsigned NumNegatedUses(void) const noexcept;

  inline unsigned NumUses(void) const noexcept {
    return NumPositiveUses() + NumNegatedUses();
  }

  bool ReturnsAtMostOneResult(void) const noexcept;

  // Returns a possible "forcing message." This only applies to queries, where
  // the message parameters must all be constants or unify against the query
  // parameters.
  std::optional<ParsedPredicate> ForcingMessage(void) const;

 protected:
  friend class ParsedDeclaration;
  using Node<ParsedQuery, ParsedQueryImpl>::Node;
};

// Represents a rule that has been exported to other modules. These rules
// must have global unique names. For example:
//
//    #export rule(type Var, type Var)
//
// Exports must correspond with clauses defined within the current module, and
// only the current module. The same export cannot be defined in multiple
// modules. If that functionality is desired, then messages should be used.
//
// Type names on parameters in `#export` declarations are optional.
class ParsedExportImpl;
class ParsedExport : public Node<ParsedExport, ParsedExportImpl> {
 public:
  static const ParsedExport &From(const ParsedDeclaration &decl);

  bool operator==(const ParsedExport &that) const noexcept;
  bool operator!=(const ParsedExport &that) const noexcept;

  // TODO(pag): Eliminate comparison on pointers.
  bool operator<(const ParsedExport &that) const noexcept;

  DisplayRange SpellingRange(void) const noexcept;
  uint64_t Id(void) const noexcept;
  Token Name(void) const noexcept;
  std::string_view NameAsString(void) const noexcept;
  unsigned Arity(void) const noexcept;
  ParsedParameter NthParameter(unsigned n) const noexcept;

  unsigned NumPositiveUses(void) const noexcept;
  unsigned NumNegatedUses(void) const noexcept;

  inline unsigned NumUses(void) const noexcept {
    return NumPositiveUses() + NumNegatedUses();
  }

 protected:
  friend class ParsedDeclaration;
  using Node<ParsedExport, ParsedExportImpl>::Node;
};

// Represents a rule that is specific to this module. Across modules, there
// can be several locals with the same names/prototypes, and they will not
// be treated as referencing the same things. For example:
//
//    #local rule(type Var, type Var)
//
// Locals must correspond with clauses defined within the current module, and
// only the current module. Type names on parameters in `#local` declarations
// are optional.
class ParsedLocalImpl;
class ParsedLocal : public Node<ParsedLocal, ParsedLocalImpl> {
 public:
  static const ParsedLocal &From(const ParsedDeclaration &decl);

  bool operator==(const ParsedLocal &that) const noexcept;
  bool operator!=(const ParsedLocal &that) const noexcept;

  DisplayRange SpellingRange(void) const noexcept;
  uint64_t Id(void) const noexcept;
  Token Name(void) const noexcept;
  std::string_view NameAsString(void) const noexcept;
  unsigned Arity(void) const noexcept;
  ParsedParameter NthParameter(unsigned n) const noexcept;

  unsigned NumPositiveUses(void) const noexcept;
  unsigned NumNegatedUses(void) const noexcept;

  bool IsInline(void) const noexcept;

  inline unsigned NumUses(void) const noexcept {
    return NumPositiveUses() + NumNegatedUses();
  }

 protected:
  friend class ParsedDeclaration;
  using Node<ParsedLocal, ParsedLocalImpl>::Node;
};

enum class FunctorRange {
  kZeroOrMore,  // Default.
  kZeroOrOne,
  kOneToOne,
  kOneOrMore
};

// Represents a rule that is supplied by a plugin. These rules must have
// globally unique names, and follow similar declaration rules as exports.
//
//    #functor add1(bound i32 Pred, free i32 Succ)
//    #functor add1(free i32 Pred, bound i32 Succ)
//    #functor add1(bound i32 Pred, bound i32 Succ)
//
// The above example feasibly adds one to `Pred`, subtracts one from `Succ`, or
// checks that `Pred+1 == Succ`.
//
// The `impure` qualifier in the below example tells Dr. Lojekyll that it can't
// trust a functor to produce the same outputs given the same inputs. This
// qualifier cannot be used on aggregating functors, functors that have no
// bound parameters (treated by default as impure), or on functors used to
// merge mutable parameters.
//
//    #functor blah(...) impure
//
// Usage of an `impure` qualifier implies additional state tracking and also
// differential data flow. For example: lets say `blah(10, A)` produces a value
// `A=0` on the first use. At a later time, it produces `A=1`. The implication
// is that the data flow node will have to produce a deletion record of
// `-(10, 0)` before it produces an insertion record `+(10, 1)`.
//
// The `range` qualified in the below example tells Dr. Lojekyll whether or not
// a functor will output zero-or-one, zero-or-more (default), or one-or-more
// outputs given its inputs. If all parameters to a functor are bound, then the
// range of the functor is fixed as zero-or-one, i.e. treated like a filter
// function.
class ParsedFunctorImpl;
class ParsedFunctor : public Node<ParsedFunctor, ParsedFunctorImpl> {
 public:
  static const ParsedFunctor &From(const ParsedDeclaration &decl);
  static const ParsedFunctor MergeOperatorOf(ParsedParameter param);

  bool operator==(const ParsedFunctor &that) const noexcept;
  bool operator!=(const ParsedFunctor &that) const noexcept;

  DisplayRange SpellingRange(void) const noexcept;
  uint64_t Id(void) const noexcept;
  Token Name(void) const noexcept;
  std::string_view NameAsString(void) const noexcept;
  unsigned Arity(void) const noexcept;
  ParsedParameter NthParameter(unsigned n) const noexcept;

  // Is this an aggregating functor?
  bool IsAggregate(void) const noexcept;

  // Is this functor used to merge values in a `mutable`-attributed parameter?
  // If so, it has three parameters (prev value, proposed value, merged value).
  bool IsMerge(void) const noexcept;

  // Is this a "pure" functor? That is, do we expect that if we re-execute it
  // given the same inputs, that it will produce the same outputs?
  bool IsPure(void) const noexcept;

  // Is this a filter-like functor? This is `true` if the functor is `pure`
  // and if the number of free parameters is zero and if the range is
  // `FunctorRange::kZeroOrOne`.
  bool IsFilter(void) const noexcept;

  // Is this an inline functor? This means that is will have a direct definition
  // provided in some file or in the auto-generated code (via a `#prologue` or
  // `#epilogue`). This is really a symbol visibility thing.
  //
  // An alternative meaning is when a custom symbol is provided, e.g. via
  // the `@inline(```c++ foo```)` syntax; in this case, it means the target code
  // should call this function using the code `foo`.
  bool IsInline(Language lang) const noexcept;

  // Returns the custom inline name, if any. For example:
  //
  //      #functor foo(bound u32 X, free u32 Y) @inline(```c++ foo```).
  //
  // Normally, in C++, the function name would be `foo_bf` for the `bound`
  // and `free`, but in this case, it would just be `foo`.
  std::optional<std::string> InlineName(Language lang) const noexcept;

  unsigned NumPositiveUses(void) const noexcept;
  unsigned NumNegatedUses(void) const noexcept;

  FunctorRange Range(void) const noexcept;

  inline unsigned NumUses(void) const noexcept {
    return NumPositiveUses() + NumNegatedUses();
  }

 protected:
  friend class ParsedDeclaration;
  using Node<ParsedFunctor, ParsedFunctorImpl>::Node;
};

// Parsed messages are all extern by default, and so must follow all the same
// rules as exports. The one key difference between messages and exports is
// that all parameters are implicitly bound, and only a single message can be
// used in a clause body. Thus binding of parameter body variables doesn't need
// to be specified.
//
// The same message (albeit with different associated clause bodies) can be
// (re)defined in any module.
//
// Messages are either receive-only, or send-only, never both. Thus, a given
// message must only appear either always as a clause head (send), or always in
// clause bodies (receive).
class ParsedMessageImpl;
class ParsedMessage : public Node<ParsedMessage, ParsedMessageImpl> {
 public:
  static const ParsedMessage &From(const ParsedDeclaration &decl);

  bool operator==(const ParsedMessage &that) const noexcept;
  bool operator!=(const ParsedMessage &that) const noexcept;

  DisplayRange SpellingRange(void) const noexcept;
  uint64_t Id(void) const noexcept;
  Token Name(void) const noexcept;
  std::string_view NameAsString(void) const noexcept;
  unsigned Arity(void) const noexcept;
  ParsedParameter NthParameter(unsigned n) const noexcept;

  // Returns `true` if this message is the head of any clause, i.e. if there
  // are rules that publish this message.
  bool IsPublished(void) const noexcept;
  bool IsReceived(void) const noexcept;

  // Can this message receive/publish removals?
  bool IsDifferential(void) const noexcept;
  Token Differential(void) const noexcept;

  unsigned NumPositiveUses(void) const noexcept;

  unsigned NumNegatedUses(void) const noexcept;

  // Number of uses as a `@first` forcing message inside of a query clause.
  unsigned NumForcedUses(void) const noexcept;

  inline unsigned NumUses(void) const noexcept {
    return NumPositiveUses() + NumNegatedUses() + NumForcedUses();
  }

 protected:
  friend class ParsedDeclaration;
  using Node<ParsedMessage, ParsedMessageImpl>::Node;
};

class ParsedForeignConstant;
class ParsedForeignType;
class ParsedEnumType;
class ParsedImport;
class ParsedInline;
class ParsedModuleIterator;

class ParsedDatabaseNameImpl;
class ParsedDatabaseName
    : public Node<ParsedDatabaseName, ParsedDatabaseNameImpl> {
 public:

  // Spelling range of the
  DisplayRange SpellingRange(void) const noexcept;

  // Name of the database.
  Token Name(void) const noexcept;

  // Name of this database as a string.
  std::string NamespaceName(Language lang) const noexcept;

  // Name of this database as a string, acceptable for a file name for
  // any language.
  std::string FileName(void) const noexcept;

 protected:
  friend class ParsedModule;
  using Node<ParsedDatabaseName, ParsedDatabaseNameImpl>::Node;
};

// Represents a module parsed from a display.
class ParsedModuleImpl;
class ParsedModule {
 public:
  DisplayRange SpellingRange(void) const noexcept;

  // Return the name of the database, if any.
  std::optional<ParsedDatabaseName> DatabaseName(void) const noexcept;

  // Return the ID of this module. Returns `~0u` if not valid.
  uint64_t Id(void) const noexcept;

  UsedNodeRange<ParsedQuery> Queries(void) const;
  UsedNodeRange<ParsedLocal> Locals(void) const;
  UsedNodeRange<ParsedExport> Exports(void) const;
  UsedNodeRange<ParsedMessage> Messages(void) const;
  UsedNodeRange<ParsedFunctor> Functors(void) const;

  DefinedNodeRange<ParsedImport> Imports(void) const;
  DefinedNodeRange<ParsedInline> Inlines(void) const;
  DefinedNodeRange<ParsedClause> Clauses(void) const;

  // NOTE(pag): This returns the list of /all/ foreign types, as they are
  //            globally visible.
  DefinedNodeRange<ParsedForeignType> ForeignTypes(void) const;

  // NOTE(pag): This returns the list of /all/ enum types, as they are
  //            globally visible.
  DefinedNodeRange<ParsedEnumType> EnumTypes(void) const;

  // NOTE(pag): This returns the list of /all/ foreign constants, as they are
  //            globally visible.
  DefinedNodeRange<ParsedForeignConstant> ForeignConstants(void) const;

  // Try to return the foreign type associated with a particular type location
  // or type kind.
  std::optional<ParsedForeignType> ForeignType(TypeLoc loc) const noexcept;
  std::optional<ParsedForeignType> ForeignType(TypeKind kind) const noexcept;

  // The root module of this parse.
  ParsedModule RootModule(void) const;

  inline ParsedModule(const std::shared_ptr<ParsedModuleImpl> &impl_)
      : impl(impl_) {}

  inline bool operator<(const ParsedModule &that) const noexcept {
    return impl.get() < that.impl.get();
  }

  inline bool operator==(const ParsedModule &that) const noexcept {
    return impl.get() == that.impl.get();
  }

  inline bool operator!=(const ParsedModule &that) const noexcept {
    return impl.get() != that.impl.get();
  }

 protected:
  friend class ParsedImport;
  friend class ParsedModuleIterator;
  friend class Parser;
  friend class ParserImpl;

  std::shared_ptr<ParsedModuleImpl> impl;

 private:
  ParsedModule(void) = delete;
};

// Represents a parsed import declaration, e.g.
//
//    #import "../hello.dr"
//
// Any imports must be the first things parsed in a module.
class ParsedImportImpl;
class ParsedImport : public Node<ParsedImport, ParsedImportImpl> {
 public:
  DisplayRange SpellingRange(void) const noexcept;
  ParsedModule ImportedModule(void) const noexcept;

  std::filesystem::path ImportedPath(void) const noexcept;

 protected:
  using Node<ParsedImport, ParsedImportImpl>::Node;
};

// Represents a parsed foreign constant. These let us explicitly represent
// values from a target language. For example, we can map foreign constants
// to C++ expressions, such as enumerators, `sizeof(...)` expressions, even
// function calls!
//
//    #constant type_name const_name ```<lang> expansion```
//
// Where `type_name` is a foreign type declared with `#foreign`.
class ParsedForeignConstantImpl;
class ParsedForeignConstant : public Node<ParsedForeignConstant, ParsedForeignConstantImpl> {
 public:
  static ParsedForeignConstant From(const ParsedLiteral &lit);

  TypeLoc Type(void) const noexcept;

  // Name of this constant.
  Token Name(void) const noexcept;
  std::string_view NameAsString(void) const noexcept;

  ::hyde::Language Language(void) const noexcept;

  DisplayRange SpellingRange(void) const noexcept;

  std::string_view Constructor(void) const noexcept;

  // Can the optimizers assume that this constant has a unique value (w.r.t.
  // any other constant, marked `@unique` or not).
  bool IsUnique(void) const noexcept;

 protected:
  friend class ParsedForeignType;

  using Node<ParsedForeignConstant, ParsedForeignConstantImpl>::Node;
};

// Represents a parsed foreign type. These let us explicitly represent value/
// serializable types from the codegen target language in Dr. Lojekyll's code.
// They can be forward declared as:
//
//    #foreign type_name
//
// And defined as:
//
//    #foreign type_name ```name for all languages here```
//
// Or:
//
//    #foreign type_name "name for all languages here"
//
// Alternatively, language-specific codegen names can be provided with:
//
//    #foreign std_string ```c++ std::string```
//    #foreign std_string ```python str```
//
// Sometimes, one needs to specify how to construct the type given a default
// value in the target language. For example, this can be done with:
//
//    #foreign std_string ```c++ std::string``` ```std::string($)```
//
// The meta-variable `$` must appear in the constructor string exactly once.
//
// Foreign type declarations logically follow code inlined into the target
// via `#prologue` statements. Thus, a foreign type can safely refer to a type
// declared within a `#prologue` statement.
class ParsedForeignTypeImpl;
class ParsedForeignType : public Node<ParsedForeignType, ParsedForeignTypeImpl> {
 public:
  static ParsedForeignType Of(ParsedForeignConstant that);
  static std::optional<ParsedForeignType> Of(ParsedLiteral that);

  // A representation of this foreign type as a `TypeLoc`.
  TypeLoc Type(void) const noexcept;

  // Name of this type.
  Token Name(void) const noexcept;

  // Name of this type.
  std::string_view NameAsString(void) const noexcept;

  // Is this type actually built-in?
  bool IsBuiltIn(void) const noexcept;

  // Is this type actually an enumeration type?
  bool IsEnum(void) const noexcept;

  std::optional<DisplayRange> SpellingRange(Language lang) const noexcept;

  // Optional code to inline, specific to a language.
  std::optional<std::string_view> CodeToInline(Language lang) const noexcept;

  // Returns `true` if there is a specialized `lang`-specific instance, and
  // `false` is none is present, or if the default `Language::kUnknown` is used.
  bool IsSpecialized(Language lang) const noexcept;

  // Returns `true` if the representation of this foreign type in the target
  // language `lang` is referentially transparent, i.e. if equality implies
  // identity. This is the case for trivial types, e.g. integers.
  bool IsReferentiallyTransparent(Language lang) const noexcept;

  // Returns `true` if the representation of this foreign type in the target
  // language `lang` is nullable, i.e. if there exists in the space of types a
  // value that is distinguished and implicitly converted in the target language
  // as Boolean false.
  bool IsNullable(Language lang) const noexcept;

  // Return the prefix and suffix for construction for this language.
  std::optional<std::pair<std::string_view, std::string_view>>
  Constructor(Language lang) const noexcept;

  // List of constants defined on this type for a particular language.
  UsedNodeRange<ParsedForeignConstant> Constants(Language lang) const noexcept;

 protected:
  friend class ParsedModule;
  friend class ParsedEnumType;

  using Node<ParsedForeignType, ParsedForeignTypeImpl>::Node;
};

class ParsedEnumTypeImpl;
class ParsedEnumType : public Node<ParsedEnumType, ParsedEnumTypeImpl> {
 public:
  static std::optional<ParsedEnumType> From(ParsedForeignType type);

  // A representation of this enumeration type as a `TypeLoc`.
  TypeLoc Type(void) const noexcept;

  // A representation of this foreign type's underlying integral type
  // as a `TypeLoc`.
  TypeLoc UnderlyingType(void) const noexcept;

  // Name of this type.
  Token Name(void) const noexcept;
  std::string_view NameAsString(void) const noexcept;

  DisplayRange SpellingRange(void) const noexcept;

  // List of constants defined on this type for a particular language.
  UsedNodeRange<ParsedForeignConstant> Enumerators(void) const noexcept;

 protected:
  friend class ParsedModule;

  using Node<ParsedEnumType, ParsedEnumTypeImpl>::Node;
};

// Represents a parsed `#prologue` or `#epilogue` statement, that lets us write
// C/C++ code directly inside of a datalog module and have it pasted directly
// into generated C/C++ code. This can be useful for making sure that certain
// functors are inlined / inlinable, and thus visible to the compiler.
class ParsedInlineImpl;
class ParsedInline : public Node<ParsedInline, ParsedInlineImpl> {
 public:
  DisplayRange SpellingRange(void) const noexcept;
  std::string_view CodeToInline(void) const noexcept;
  ::hyde::Language Language(void) const noexcept;

  // Tells us to what compiler code generation stage this inline should be
  // emitted.
  std::string_view Stage(void) const noexcept;

 protected:
  using Node<ParsedInline, ParsedInlineImpl>::Node;
};

}  // namespace hyde

namespace std {

template <>
struct hash<::hyde::ParsedClause> {
  using argument_type = ::hyde::ParsedClause;
  using result_type = uint64_t;
  inline uint64_t operator()(::hyde::ParsedClause clause) const noexcept {
    return clause.UniqueId();
  }
};

template <>
struct hash<::hyde::ParsedParameter> {
  using argument_type = ::hyde::ParsedParameter;
  using result_type = uint64_t;
  inline uint64_t operator()(::hyde::ParsedParameter param) const noexcept {
    return param.Id();
  }
};

template <>
struct hash<::hyde::ParsedModule> {
  using argument_type = ::hyde::ParsedModule;
  using result_type = uint64_t;
  inline uint64_t operator()(::hyde::ParsedModule module) const noexcept {
    return module.Id();
  }
};

template <>
struct hash<::hyde::ParsedVariable> {
  using argument_type = ::hyde::ParsedVariable;
  using result_type = uint64_t;
  inline uint64_t operator()(::hyde::ParsedVariable var) const noexcept {
    return var.Id();
  }
};

template <>
struct hash<::hyde::ParsedDeclaration> {
  using argument_type = ::hyde::ParsedDeclaration;
  using result_type = uint64_t;
  inline uint64_t operator()(::hyde::ParsedDeclaration decl) const noexcept {
    return decl.Id();
  }
};

template <>
struct hash<::hyde::ParsedFunctor> {
  using argument_type = ::hyde::ParsedFunctor;
  using result_type = uint64_t;
  inline uint64_t operator()(::hyde::ParsedFunctor decl) const noexcept {
    return decl.Id();
  }
};

template <>
struct hash<::hyde::ParsedMessage> {
  using argument_type = ::hyde::ParsedMessage;
  using result_type = uint64_t;
  inline uint64_t operator()(::hyde::ParsedMessage decl) const noexcept {
    return decl.Id();
  }
};

template <>
struct hash<::hyde::ParsedQuery> {
  using argument_type = ::hyde::ParsedQuery;
  using result_type = uint64_t;
  inline uint64_t operator()(::hyde::ParsedQuery decl) const noexcept {
    return decl.Id();
  }
};

template <>
struct hash<::hyde::ParsedExport> {
  using argument_type = ::hyde::ParsedExport;
  using result_type = uint64_t;
  inline uint64_t operator()(::hyde::ParsedExport decl) const noexcept {
    return decl.Id();
  }
};

template <>
struct hash<::hyde::ParsedLocal> {
  using argument_type = ::hyde::ParsedLocal;
  using result_type = uint64_t;
  inline uint64_t operator()(::hyde::ParsedLocal decl) const noexcept {
    return decl.Id();
  }
};

template <>
struct hash<::hyde::ParsedPredicate> {
  using argument_type = ::hyde::ParsedPredicate;
  using result_type = uint64_t;
  inline uint64_t operator()(::hyde::ParsedPredicate pred) const noexcept {
    return pred.UniqueId();
  }
};

template <>
struct hash<::hyde::ParsedComparison> {
  using argument_type = ::hyde::ParsedComparison;
  using result_type = uint64_t;
  inline uint64_t operator()(::hyde::ParsedComparison cmp) const noexcept {
    return cmp.UniqueId();
  }
};

template <typename PublicT, typename PrivateT>
struct hash<::hyde::Node<PublicT, PrivateT>> {
  using argument_type = ::hyde::Node<PublicT, PrivateT>;
  using result_type = uint64_t;

  inline uint64_t operator()(::hyde::Node<PublicT, PrivateT> node) const noexcept {
    return node.Hash();
  }
};

}  // namespace std
