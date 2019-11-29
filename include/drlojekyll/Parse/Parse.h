// Copyright 2019, Trail of Bits. All rights reserved.

#pragma once

#include <utility>

#include <drlojekyll/Display/DisplayPosition.h>
#include <drlojekyll/Lex/Token.h>

namespace hyde {

class Parser;
class ParserImpl;

namespace parse {

template<typename T>
class Impl;

template <typename T>
class ParsedNodeIterator;

template <typename T>
class ParsedNodeRange;

// Used for traversing nodes that are arranged in a list. Class based so that
// the use of `Next` and `Prev` are privileged.
class NodeTraverser {
 private:
  template <typename T>
  friend class ParsedNodeIterator;

  static void *Next(void *, intptr_t);
};

// Base class of all parsed nodes. Parsed nodes are thin wrappers around
// an implementation class pointer, where the data is managed by the parser.
template<typename T>
class ParsedNode {
 public:
  inline ParsedNode(Impl<T> *impl_)
      : impl(impl_) {}

  inline bool operator==(const ParsedNode<T> &that) const {
    return impl == that.impl;
  }

  inline bool operator!=(const ParsedNode<T> &that) const {
    return impl != that.impl;
  }

  inline bool operator<(const ParsedNode<T> &that) const {
    return impl < that.impl;
  }

  inline uintptr_t UniqueId(void) const {
    return reinterpret_cast<uintptr_t>(impl);
  }

  inline uintptr_t Hash(void) const {
    return reinterpret_cast<uintptr_t>(impl) >> 3;
  }

 protected:
  friend class Parser;
  friend class ParserImpl;

  template <typename U>
  friend class ParsedNode;

  template <typename U>
  friend class ParsedNodeIterator;

  template <typename U>
  friend class ParsedNodeRange;

  Impl<T> *impl{nullptr};
};

// Iterator over a parsed node.
template <typename T>
class ParsedNodeIterator {
 public:
  ParsedNodeIterator(const ParsedNodeIterator<T> &) noexcept = default;
  ParsedNodeIterator(ParsedNodeIterator<T> &&) noexcept = default;

  ParsedNodeIterator<T> &operator=(const ParsedNodeIterator<T> &) noexcept = default;
  ParsedNodeIterator<T> &operator=(ParsedNodeIterator<T> &&) noexcept = default;

  T operator*(void) const {
    return T(impl);
  }

  T operator->(void) const = delete;

  bool operator==(ParsedNodeIterator<T> that) const {
    return impl == that.impl;
  }

  bool operator!=(ParsedNodeIterator<T> that) const {
    return impl != that.impl;
  }

  inline ParsedNodeIterator<T> &operator++(void) {
    impl = reinterpret_cast<Impl<T> *>(NodeTraverser::Next(impl, offset));
    return *this;
  }

  inline ParsedNodeIterator<T> operator++(int) const {
    auto ret = *this;
    impl = reinterpret_cast<Impl<T> *>(NodeTraverser::Next(impl, offset));
    return ret;
  }

 private:
  friend class ParsedNodeRange<T>;

  inline explicit ParsedNodeIterator(Impl<T> *impl_, intptr_t offset_)
      : impl(impl_),
        offset(offset_) {}

  ParsedNodeIterator(void) = default;

  Impl<T> *impl{nullptr};
  intptr_t offset{0};
};

template <typename T>
class ParsedNodeRange {
 public:
  ParsedNodeRange(void) = default;

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Winvalid-offsetof"
  inline ParsedNodeRange(
      Impl<T> *impl_,
      intptr_t offset_=static_cast<intptr_t>(__builtin_offsetof(Impl<T>, next)))
      : impl(impl_),
        offset(offset_) {}
#pragma clang diagnostic pop

  inline ParsedNodeIterator<T> begin(void) const {
    return ParsedNodeIterator<T>(impl, offset);
  }

  inline ParsedNodeIterator<T> end(void) const {
    return {};
  }

 private:
  template <typename U>
  friend class ParsedNode;

  template <typename U>
  friend class Impl;

  Impl<T> *impl{nullptr};
  intptr_t offset{0};
};

}  // namespace parse

enum class ParameterBinding {
  kImplicit,
  kFree,
  kBound,
  kAggregate,
  kSummary
};

class ParsedAssignment;
class ParsedClause;
class ParsedComparison;
class ParsedPredicate;

// Represents a literal.
class ParsedLiteral : public parse::ParsedNode<ParsedLiteral> {
 public:
  DisplayRange SpellingRange(void) const noexcept;

  bool IsNumber(void) const noexcept;
  bool IsString(void) const noexcept;

 protected:
  friend class ParsedVariable;

  using parse::ParsedNode<ParsedLiteral>::ParsedNode;
};

// Type of a use.
enum class UseKind {
  kArgument,
  kAssignmentLHS,
  kComparisonLHS,
  kComparisonRHS
};

template <typename T>
class ParsedUse;

namespace parse {

// Used to access information about use nodes.
class UseAccessor {
 private:
  UseAccessor(void) = delete;

  template <typename>
  friend class ::hyde::ParsedUse;

  DisplayRange GetUseSpellingRange(void *);
  UseKind GetUseKind(void *);
  const void *GetUser(void *);
};
}  // namespace

// A variable use.
template <typename T>
class ParsedUse : public parse::ParsedNode<ParsedUse<T>> {
 public:
  DisplayRange SpellingRange(void) const noexcept {
    return parse::UseAccessor::GetUseSpellingRange(this->impl);
  }

  inline UseKind Kind(void) const noexcept {
    return parse::UseAccessor::GetUseKind(this->impl);
  }

  inline const T &User(void) const noexcept {
    return *reinterpret_cast<const T *>(parse::UseAccessor::GetUser(this->impl));
  }

 protected:
  using parse::ParsedNode<ParsedUse<T>>::ParsedNode;
};

using ParsedAssignmentUse = ParsedUse<ParsedAssignment>;
using ParsedComparisonUse = ParsedUse<ParsedComparison>;
using ParsedArgumentUse = ParsedUse<ParsedPredicate>;

// Represents a parsed variable.
class ParsedVariable : public parse::ParsedNode<ParsedVariable> {
 public:
  DisplayRange SpellingRange(void) const noexcept;

  // Returns the token corresponding with the name of this variable.
  Token Name(void) const noexcept;

  // Returns `true` if this variable is an parameter to its clause.
  bool IsParameter(void) const noexcept;

  // Returns `true` if this variable is being used as an argument to a
  // predicate.
  bool IsArgument(void) const noexcept;

  // Returns `true` if this variable, or any other used of this variable,
  // is assigned to any literals.
  bool IsAssigned(void) const noexcept;

  // Returns `true` if this variable, or any other use of this variable,
  // is compared with any other body_variables.
  bool IsCompared(void) const noexcept;

  // Returns `true` if this variable is an unnamed variable.
  bool IsUnnamed(void) const noexcept;

  // Return the variable to which `literal` assigned.
  static ParsedVariable AssignedTo(ParsedLiteral literal) noexcept;

  // Return a unique integer that identifies this variable. Within a given
  // clause, all body_variables with the same name will have the same identifier.
  uint64_t Id(void) const noexcept;

  // Override `operator==` so that different uses of the same variable appear
  // the same.
  inline bool operator==(const ParsedVariable &that) const {
    return Id() == that.Id();
  }

  // Override `operator==` so that different uses of the same variable appear
  // the same.
  inline bool operator!=(const ParsedVariable &that) const {
    return Id() != that.Id();
  }

 protected:
  friend class ParsedAssignment;
  friend class ParsedClause;
  friend class ParsedComparison;
  friend class ParsedPredicate;

  using parse::ParsedNode<ParsedVariable>::ParsedNode;
};

enum class ComparisonOperator {
  kEqual,
  kNotEqual,
  kLessThan,
  kLessThanEqual,
  kGreaterThan,
  kGreaterThanEqual
};

// Represents an attempt to unify two body_variables. E.g. `V1=V2`, `V1<V2`, etc.
class ParsedComparison : public parse::ParsedNode<ParsedComparison> {
 public:
  DisplayRange SpellingRange(void) const noexcept;
  ParsedVariable LHS(void) const noexcept;
  ParsedVariable RHS(void) const noexcept;
  ComparisonOperator Operator(void) const noexcept;

  // Return the list of all comparisons with `var`.
  static parse::ParsedNodeRange<ParsedComparisonUse> Using(ParsedVariable var);

 protected:
  friend class ParsedClause;
  using parse::ParsedNode<ParsedComparison>::ParsedNode;
};

// Represents and attempt to assign a literal to a variable, e.g. `V=1`.
class ParsedAssignment
    : public parse::ParsedNode<ParsedAssignment> {
 public:
  DisplayRange SpellingRange(void) const noexcept;
  ParsedVariable LHS(void) const noexcept;
  ParsedLiteral RHS(void) const noexcept;

  // Return the list of all assignments to `var`.
  static parse::ParsedNodeRange<ParsedAssignmentUse> Using(ParsedVariable var);

 protected:
  friend class ParsedClause;
  using parse::ParsedNode<ParsedAssignment>::ParsedNode;
};

// Represents a call to a functor, receipt of a message, etc.
class ParsedPredicate : public parse::ParsedNode<ParsedPredicate> {
 public:
  DisplayRange SpellingRange(void) const noexcept;

  // Returns `true` if this is a negated predicate.
  bool IsNegated(void) const noexcept;

  // Returns the arity of this predicate.
  unsigned Arity(void) const noexcept;

  // Return the `n`th argument of this predicate.
  ParsedVariable NthArgument(unsigned n) const noexcept;

  // All variables used as arguments to this predicate.
  parse::ParsedNodeRange<ParsedVariable> Arguments(void) const;

  // Return the list of all uses of `var` as an argument to a predicate.
  static parse::ParsedNodeRange<ParsedArgumentUse> Using(ParsedVariable var);

 protected:
  friend class ParsedClause;
  friend class ParsedDeclaration;
  using parse::ParsedNode<ParsedPredicate>::ParsedNode;
};

// Represents a parsed parameter. The following are valid forms:
//
//    free @type A
//    bound @type A
//    free A
//    bound A
//    @type A
//
// Parameter names (`A` in the above example) must be identifiers beginning with
// an upper case character, or `_`.
//
// Things like the binding specification are optional in some contexts but
// not others (e.g. in export directives).
class ParsedParameter : public parse::ParsedNode<ParsedParameter> {
 public:
  DisplayRange SpellingRange(void) const noexcept;
  Token Name(void) const noexcept;
  Token Type(void) const noexcept;
  ParameterBinding Binding(void) const noexcept;

  // Other declarations of this parameter. This goes and gets the list of
  // parameters that
  parse::ParsedNodeRange<ParsedParameter> Redeclarations(void) const;

 protected:
  using parse::ParsedNode<ParsedParameter>::ParsedNode;
};

// Represents a parsed clause, which defines either an internal or exported
// predicate.
class ParsedClause : public parse::ParsedNode<ParsedClause> {
 public:

  // Traverse upward in the AST.
  static ParsedClause Containing(ParsedVariable var) noexcept;
  static ParsedClause Containing(ParsedPredicate pred) noexcept;
  static ParsedClause Containing(ParsedAssignment var) noexcept;
  static ParsedClause Containing(ParsedComparison var) noexcept;

  // Return the total number of uses of `var` in its clause body.
  static unsigned NumUsesInBody(ParsedVariable var) noexcept;

  DisplayRange SpellingRange(void) const noexcept;

  // Returns the arity of this clause.
  unsigned Arity(void) const noexcept;

  // Return the `n`th parameter of this clause.
  ParsedVariable NthParameter(unsigned n) const noexcept;

  // All variables used as parameters to this clause.
  parse::ParsedNodeRange<ParsedVariable> Parameters(void) const;

  // All variables used in the body of the clause.
  parse::ParsedNodeRange<ParsedVariable> Variables(void) const;

  // All instances of `var` in its clause.
  static parse::ParsedNodeRange<ParsedVariable> Uses(ParsedVariable var);

  // All positive predicates in the clause.
  parse::ParsedNodeRange<ParsedPredicate> PositivePredicates(void) const;

  // All negated predicates in the clause.
  parse::ParsedNodeRange<ParsedPredicate> NegatedPredicates(void) const;

  // All assignments of variables to constant literals.
  parse::ParsedNodeRange<ParsedAssignment> Assignments(void) const;

  // All comparisons between two variables.
  parse::ParsedNodeRange<ParsedComparison> Comparisons(void) const;

 protected:
  friend class ParsedDeclaration;

  using parse::ParsedNode<ParsedClause>::ParsedNode;
};

enum class DeclarationKind {
  kQuery,
  kMessage,
  kFunctor,
  kExport,
  kLocal
};

class ParsedQuery;
class ParsedMessage;
class ParsedFunctor;
class ParsedExport;
class ParsedLocal;

// The head of a declaration. This includes the name of the clause.
// Clause head names must be identifiers beginning with a lower case character.
class ParsedDeclaration : public parse::ParsedNode<ParsedDeclaration> {
 public:
  ParsedDeclaration(const ParsedQuery &query);
  ParsedDeclaration(const ParsedMessage &message);
  ParsedDeclaration(const ParsedFunctor &functor);
  ParsedDeclaration(const ParsedExport &exp);
  ParsedDeclaration(const ParsedLocal &local);

  DisplayRange SpellingRange(void) const noexcept;

  // Return the ID of this declaration.
  uint64_t Id(void) const;

  // Return the name of this declaration as a token.
  Token Name(void) const noexcept;

  bool IsQuery(void) const noexcept;
  bool IsMessage(void) const noexcept;
  bool IsFunctor(void) const noexcept;
  bool IsExport(void) const noexcept;
  bool IsLocal(void) const noexcept;

  // The kind of this declaration.
  DeclarationKind Kind(void) const noexcept;

  // The string version of this kind name.
  const char *KindName(void) const noexcept;

  // Returns the arity of this declaration.
  unsigned Arity(void) const noexcept;

  // Return the `n`th parameter of this declaration.
  ParsedParameter NthParameter(unsigned n) const noexcept;

  parse::ParsedNodeRange<ParsedDeclaration> Redeclarations(void) const;
  parse::ParsedNodeRange<ParsedParameter> Parameters(void) const;
  parse::ParsedNodeRange<ParsedClause> Clauses(void) const;
  parse::ParsedNodeRange<ParsedPredicate> PositiveUses(void) const;
  parse::ParsedNodeRange<ParsedPredicate> NegativeUses(void) const;

  unsigned NumPositiveUses(void) const noexcept;
  unsigned NumNegatedUses(void) const noexcept;

  inline unsigned NumUses(void) const noexcept {
    return NumPositiveUses() + NumNegatedUses();
  }

  // Return the declaration associated with a clause. This is the first
  // parsed declaration, so it could be in a different module.
  static ParsedDeclaration Of(ParsedClause clause);

  // Return the declaration associated with a predicate. This is the first
  // parsed declaration, so it could be in a different module.
  static ParsedDeclaration Of(ParsedPredicate pred);

 protected:
  using parse::ParsedNode<ParsedDeclaration>::ParsedNode;
};

// Represents a rule that has been exported to the user for querying the
// database. These rules must have global unique names. For example:
//
//    #query rule(bound @type Var, free @type Var)
//
// Exported rules are unique in that they limit the scope of how they can be
// used, by requiring that users bind all `bound`-attributed arguments. The
// same logical rule can have multiple exports, so long as they all have
// different rule binding types.
//
// Specifying what is bound is a proxy for indexing. If iteration over the
// entire set of facts is permissible, then an `#extern` declaration marking
// all parameters as free is required.
//
// Query declarations and defined clauses can be defined in any module.
class ParsedQuery : public parse::ParsedNode<ParsedQuery> {
 public:
  static const ParsedQuery &From(const ParsedDeclaration &decl);

  DisplayRange SpellingRange(void) const noexcept;
  uint64_t Id(void) const noexcept;

  parse::ParsedNodeRange<ParsedQuery> Redeclarations(void) const;
  parse::ParsedNodeRange<ParsedClause> Clauses(void) const;
  parse::ParsedNodeRange<ParsedPredicate> PositiveUses(void) const;
  parse::ParsedNodeRange<ParsedPredicate> NegativeUses(void) const;

  unsigned NumPositiveUses(void) const noexcept;
  unsigned NumNegatedUses(void) const noexcept;

  inline unsigned NumUses(void) const noexcept {
    return NumPositiveUses() + NumNegatedUses();
  }

 protected:
  friend class ParsedDeclaration;
  using parse::ParsedNode<ParsedQuery>::ParsedNode;
};

// Represents a rule that has been exported to other modules. These rules
// must have global unique names. For example:
//
//    #export rule(@type Var, @type Var)
//
// Exports must correspond with clauses defined within the current module, and
// only the current module. The same extern cannot be defined in multiple
// modules. If that functionality is desired, then messages should be used.
class ParsedExport : public parse::ParsedNode<ParsedExport> {
 public:
  static const ParsedExport &From(const ParsedDeclaration &decl);

  DisplayRange SpellingRange(void) const noexcept;
  uint64_t Id(void) const noexcept;

  parse::ParsedNodeRange<ParsedExport> Redeclarations(void) const;
  parse::ParsedNodeRange<ParsedClause> Clauses(void) const;
  parse::ParsedNodeRange<ParsedPredicate> PositiveUses(void) const;
  parse::ParsedNodeRange<ParsedPredicate> NegativeUses(void) const;

  unsigned NumPositiveUses(void) const noexcept;
  unsigned NumNegatedUses(void) const noexcept;

  inline unsigned NumUses(void) const noexcept {
    return NumPositiveUses() + NumNegatedUses();
  }

 protected:
  friend class ParsedDeclaration;
  using parse::ParsedNode<ParsedExport>::ParsedNode;
};

// Represents a rule that is specific to this module. Across modules, there
// can be several locals with different names/prototypes, and they will not
// be treated as referencing the same things. For example:
//
//    #local rule(@type Var, @type Var)
//
// Locals must correspond with clauses defined within the current module, and
// only the current module.
class ParsedLocal : public parse::ParsedNode<ParsedLocal> {
 public:
  static const ParsedLocal &From(const ParsedDeclaration &decl);

  DisplayRange SpellingRange(void) const noexcept;
  uint64_t Id(void) const noexcept;

  parse::ParsedNodeRange<ParsedLocal> Redeclarations(void) const;
  parse::ParsedNodeRange<ParsedClause> Clauses(void) const;
  parse::ParsedNodeRange<ParsedPredicate> PositiveUses(void) const;
  parse::ParsedNodeRange<ParsedPredicate> NegativeUses(void) const;

  unsigned NumPositiveUses(void) const noexcept;
  unsigned NumNegatedUses(void) const noexcept;

  inline unsigned NumUses(void) const noexcept {
    return NumPositiveUses() + NumNegatedUses();
  }

 protected:
  friend class ParsedDeclaration;
  using parse::ParsedNode<ParsedLocal>::ParsedNode;
  ParsedDeclaration Declaration(void) const;
};

// Represents a rule that is supplied by a plugin. These rules must have
// globally unique names, and follow similar declaration rules as exports.
//
//    #functor add1(bound @i32 Pred, free @i32 Succ)
//    #functor add1(free @i32 Pred, bound @i32 Succ)
//    #functor add1(bound @i32 Pred, bound @i32 Succ)
//
// The above example feasibly adds one to `Pred`, subtracts one from `Succ`, or
// checks that `Pred+1 == Succ`.
class ParsedFunctor : public parse::ParsedNode<ParsedFunctor> {
 public:
  static const ParsedFunctor &From(const ParsedDeclaration &decl);

  DisplayRange SpellingRange(void) const noexcept;
  uint64_t Id(void) const noexcept;
  bool IsComplex(void) const noexcept;
  bool IsTrivial(void) const noexcept;

  parse::ParsedNodeRange<ParsedFunctor> Redeclarations(void) const;
  parse::ParsedNodeRange<ParsedPredicate> PositiveUses(void) const;

  unsigned NumPositiveUses(void) const noexcept ;

  inline unsigned NumNegatedUses(void) const noexcept {
    return 0;
  }

  inline unsigned NumUses(void) const noexcept {
    return NumPositiveUses();
  }

 protected:
  friend class ParsedDeclaration;
  using parse::ParsedNode<ParsedFunctor>::ParsedNode;
};

// Parsed messages are all extern by default, and so must follow all the same
// rules as exports. The one key difference between messages and exports is
// that all parameters are implicitly bound, and only a single message can be
// used in the dynamic extent of a clause body. Thus binding of parameter
// body_variables doesn't need to be specified.
//
// The same message (albeit with different associated clause bodies) can be
// (re)defined in any module.
class ParsedMessage : public parse::ParsedNode<ParsedMessage> {
 public:
  static const ParsedMessage &From(const ParsedDeclaration &decl);

  DisplayRange SpellingRange(void) const noexcept;
  uint64_t Id(void) const noexcept;

  parse::ParsedNodeRange<ParsedMessage> Redeclarations(void) const;
  parse::ParsedNodeRange<ParsedClause> Clauses(void) const;
  parse::ParsedNodeRange<ParsedPredicate> PositiveUses(void) const;

  unsigned NumPositiveUses(void) const noexcept;

  inline unsigned NumNegatedUses(void) const noexcept {
    return 0;
  }

  inline unsigned NumUses(void) const noexcept {
    return NumPositiveUses();
  }

 protected:
  friend class ParsedDeclaration;
  using parse::ParsedNode<ParsedMessage>::ParsedNode;
};

class ParsedImport;

// Represents a module parsed from a display.
class ParsedModule : public parse::ParsedNode<ParsedModule> {
 public:
  DisplayRange SpellingRange(void) const noexcept;

  // Return the ID of this module. Returns `~0u` if not valid.
  uint64_t Id(void) const noexcept;

  parse::ParsedNodeRange<ParsedQuery> Queries(void) const;
  parse::ParsedNodeRange<ParsedImport> Imports(void) const;
  parse::ParsedNodeRange<ParsedLocal> Locals(void) const;
  parse::ParsedNodeRange<ParsedExport> Exports(void) const;
  parse::ParsedNodeRange<ParsedMessage> Messages(void) const;
  parse::ParsedNodeRange<ParsedFunctor> Functors(void) const;
  parse::ParsedNodeRange<ParsedClause> Clauses(void) const;

 protected:
  friend class Parser;
  friend class ParserImpl;

  using parse::ParsedNode<ParsedModule>::ParsedNode;
};

// Represents a parsed import declaration, e.g.
//
//    #import "../hello.dr"
//
// Any imports must be the first things parsed in a module.
class ParsedImport : public parse::ParsedNode<ParsedImport> {
 public:
  DisplayRange SpellingRange(void) const noexcept;
  ParsedModule ImportedModule(void) const noexcept;

 protected:
  using parse::ParsedNode<ParsedImport>::ParsedNode;
};

}  // namespace hyde

namespace std {

template<>
struct hash<::hyde::ParsedModule> {
  inline uint64_t operator()(::hyde::ParsedModule module) const noexcept {
    return module.Id();
  }
};

template<>
struct hash<::hyde::ParsedVariable> {
  inline uint64_t operator()(::hyde::ParsedVariable var) const noexcept {
    return var.Id();
  }
};

template<>
struct hash<::hyde::ParsedDeclaration> {
  inline uint64_t operator()(::hyde::ParsedDeclaration decl) const noexcept {
    return decl.Id();
  }
};

template<>
struct hash<::hyde::ParsedFunctor> {
  inline uint64_t operator()(::hyde::ParsedFunctor decl) const noexcept {
    return decl.Id();
  }
};

template<>
struct hash<::hyde::ParsedMessage> {
  inline uint64_t operator()(::hyde::ParsedMessage decl) const noexcept {
    return decl.Id();
  }
};

template<>
struct hash<::hyde::ParsedQuery> {
  inline uint64_t operator()(::hyde::ParsedQuery decl) const noexcept {
    return decl.Id();
  }
};

template<>
struct hash<::hyde::ParsedExport> {
  inline uint64_t operator()(::hyde::ParsedExport decl) const noexcept {
    return decl.Id();
  }
};

template<>
struct hash<::hyde::ParsedLocal> {
  inline uint64_t operator()(::hyde::ParsedLocal decl) const noexcept {
    return decl.Id();
  }
};

template<typename T>
struct hash<::hyde::parse::ParsedNode<T>> {
  inline uintptr_t operator()(
      ::hyde::parse::ParsedNode<T> node) const noexcept {
    return node.Hash();
  }
};

}  // namespace std