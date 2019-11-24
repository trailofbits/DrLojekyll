// Copyright 2019, Trail of Bits. All rights reserved.

#pragma once

#include <utility>

#if defined(__linux__)
#include <functional>
#endif

#include <drlojekyll/Display/DisplayPosition.h>

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
  T operator*(void) const {
    return T(impl);
  }

  T operator->(void) const = delete;

  bool operator==(ParsedNodeIterator<T> that) const {
    return impl == that.impl;
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
      intptr_t offset_=__builtin_offsetof(Impl<T>, next))
      : impl(impl_),
        offset(offset_) {}
#pragma clang diagnostic pop

  ParsedNodeIterator<T> begin(void) const {
    return {impl, offset};
  }

  ParsedNodeIterator<T> end(void) const {
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
  kFree,
  kBound,
};

class ParsedClause;

// Represents a parsed variable.
class ParsedVariable : public parse::ParsedNode<ParsedVariable> {
 public:
  DisplayRange SpellingRange(void) const noexcept;

  // Returns the token corresponding with the name of this variable.
  Token Name(void) const noexcept;

  // Returns `true` if this variable is an argument to its clause.
  bool IsArgument(void) const noexcept;

  // Returns `true` if this variable is an unnamed variable.
  bool IsUnnamed(void) const noexcept;

  // Uses of this variable within the clause in which it appears.
  parse::ParsedNodeRange<ParsedVariable> UsesInClause(void) const noexcept;

  // Return a unique integer that identifies this variable. Within a given
  // clause, all variables with the same name will have the same identifier.
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
  friend class ParsedClause;
  using parse::ParsedNode<ParsedVariable>::ParsedNode;
};

// Represents a literal.
class ParsedLiteral : public parse::ParsedNode<ParsedLiteral> {
 public:
  DisplayRange SpellingRange(void) const;

  bool IsNumber(void) const;
  bool IsString(void) const;

 protected:
  using parse::ParsedNode<ParsedLiteral>::ParsedNode;
};

enum class ComparisonOperator {
  kInvalid,
  kEqual,
  kNotEqual,
  kLessThan,
  kLessThanEqual,
  kGreaterThan,
  kGreaterThanEqual
};

// Represents an attempt to unify two variables. E.g. `V1=V2`, `V1<V2`, etc.
class ParsedComparison : public parse::ParsedNode<ParsedComparison> {
 public:
  DisplayRange SpellingRange(void) const noexcept;
  ParsedVariable LHS(void) const noexcept;
  ParsedVariable RHS(void) const noexcept;
  ComparisonOperator Operator(void) const noexcept;

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

 protected:
  friend class ParsedClause;
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

 protected:
  using parse::ParsedNode<ParsedParameter>::ParsedNode;
};

// The head of a declaration. This includes the name of the clause.
// Clause head names must be identifiers beginning with a lower case character.
class ParsedDeclaration : public parse::ParsedNode<ParsedDeclaration> {
 public:
  DisplayRange SpellingRange(void) const noexcept;
  Token Name(void) const noexcept;
  parse::ParsedNodeRange<ParsedParameter> Parameters(void) const;

 protected:
  using parse::ParsedNode<ParsedDeclaration>::ParsedNode;
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

  DisplayRange SpellingRange(void) const noexcept;

  // All variables used in the clause. Some variables might be repeated.
  parse::ParsedNodeRange<ParsedVariable> Variables(void) const;

  // All positive positive_predicates in the clause.
  parse::ParsedNodeRange<ParsedPredicate> PositivePredicates(void) const;

  // All negated positive_predicates in the clause.
  parse::ParsedNodeRange<ParsedPredicate> NegatedPredicates(void) const;

  // All assignments of variables to constant literals.
  parse::ParsedNodeRange<ParsedAssignment> Assignments(void) const;

  // All comparisons between two variables.
  parse::ParsedNodeRange<ParsedComparison> Comparisons(void) const;

 protected:
  using parse::ParsedNode<ParsedClause>::ParsedNode;
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
  DisplayRange SpellingRange(void) const noexcept;
  ParsedDeclaration Declaration(void) const noexcept;

  parse::ParsedNodeRange<ParsedClause> Clauses(void) const;
  parse::ParsedNodeRange<ParsedPredicate> PositiveUses(void) const;
  parse::ParsedNodeRange<ParsedPredicate> NegativeUses(void) const;

 protected:
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
  DisplayRange SpellingRange(void) const noexcept;
  ParsedDeclaration Declaration(void) const noexcept;

  parse::ParsedNodeRange<ParsedClause> Clauses(void) const;
  parse::ParsedNodeRange<ParsedPredicate> PositiveUses(void) const;
  parse::ParsedNodeRange<ParsedPredicate> NegativeUses(void) const;

 protected:
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
  DisplayRange SpellingRange(void) const noexcept;
  ParsedDeclaration Declaration(void) const noexcept;

  parse::ParsedNodeRange<ParsedClause> Clauses(void) const;
  parse::ParsedNodeRange<ParsedPredicate> PositiveUses(void) const;
  parse::ParsedNodeRange<ParsedPredicate> NegativeUses(void) const;

 protected:
  using parse::ParsedNode<ParsedLocal>::ParsedNode;
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
  DisplayRange SpellingRange(void) const noexcept;
  ParsedDeclaration Declaration(void) const noexcept;

  parse::ParsedNodeRange<ParsedPredicate> PositiveUses(void) const;

  // Return the ID of this functor.
  uint64_t Id(void) const;

 protected:
  using parse::ParsedNode<ParsedFunctor>::ParsedNode;
};

// Parsed messages are all extern by default, and so must follow all the same
// rules as exports. The one key difference between messages and exports is
// that all parameters are implicitly bound, and only a single message can be
// used in the dynamic extent of a clause body. Thus binding of parameter
// variables doesn't need to be specified.
//
// The same message (albeit with different associated clause bodies) can be
// (re)defined in any module.
class ParsedMessage : public parse::ParsedNode<ParsedMessage> {
 public:
  DisplayRange SpellingRange(void) const noexcept;
  ParsedDeclaration Declaration(void) const noexcept;

  parse::ParsedNodeRange<ParsedClause> Clauses(void) const;
  parse::ParsedNodeRange<ParsedPredicate> PositiveUses(void) const;

 protected:
  using parse::ParsedNode<ParsedMessage>::ParsedNode;
};

class ParsedImport;

// Represents a module parsed from a display.
class ParsedModule : public parse::ParsedNode<ParsedModule> {
 public:
  DisplayRange SpellingRange(void) const noexcept;

  // Return the ID of this module. Returns `~0u` if not valid.
  uint64_t Id(void) const noexcept;

  parse::ParsedNodeRange<ParsedImport> Imports(void) const;
  parse::ParsedNodeRange<ParsedLocal> Locals(void) const;
  parse::ParsedNodeRange<ParsedExport> Exports(void) const;
  parse::ParsedNodeRange<ParsedMessage> Messages(void) const;
  parse::ParsedNodeRange<ParsedFunctor> Functors(void) const;

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
// custom specialization of std::hash can be injected in namespace std
namespace std {
template<>
struct hash<::hyde::ParsedVariable> {
  uint64_t operator()(const ::hyde::ParsedVariable &var) const noexcept {
    return var.Id();
  }
};
}  // namespace std
