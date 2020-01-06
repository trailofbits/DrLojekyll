# Dr. Lojekyll

Dr. Lojekyll, pronounced Dr. Logical, is a Datalog compiler and execution
engine. It is designed around a publish/subscribe model, as well as Dr. Stefan
Brass's "push method" of pipelined bottom-up Datalog execution.

## Syntax Examples

### Transitive Closure
The following code describes how to create a program that computes the
transitive closure of a directed graph.

This program receives messages, each being a tuple of 64-bit integers
representing graph node IDs. These tuples represent edges in the graph. The
program provides a query interface, where all reachable node IDs (`To`) from
a given node ID (`From`) can be queried.

```
#message directed_edge(@i64 From, @i64 To)
#query transitive_closure(bound @i64 From, free @i64 To)

transitive_closure(From, To) : directed_edge(From, To).
transitive_closure(From, To) : transitive_closure(From, X)
                             , transitive_closure(X, To).
```

## Formal Syntax

The following BNF grammar describes the formal syntax of Dr. Lojekyll modules.
Dr. Lojekyll modules begin with zero-or-more import directives, followed by
zero-or-more declarations, and end with zero-or-more clause definitions.

All defined clauses must be preceded by a declaration of the same name and
arity. Clauses cannot be defined for functors, which are implemented by native
code extension modules.

```
module: import_list decl_list clause_list

import_list: import import_list
import_list:

decl_list: decl decl_list
decl_list:

clause_list: clause clause_list
clause_list

// Decls generally must fit inside a single line. They are allowed
// to span multiple lines, but only if the new line characters exist
// within the matched parantheses of their parameter lists.
decl: export_decl
decl: local_decl
decl: functor_decl
decl: message_decl

export_decl: "#export" atom "(" param_list_0 ")" "\n"
message_decl: "#message" atom "(" param_list_0 ")" "\n"
local_decl: "#local" atom "(" param_list_1 ")" "\n"

functor_decl: "#functor" atom "(" param_list_2 ")" "trivial" "\n"
functor_decl: "#functor" atom "(" param_list_2 ")" "complex" "\n"

param_list_0: type named_var "," param_list_0
param_list_0: type named_var

param_list_1: type named_var "," param_list_1
param_list_1: named_var "," param_list_1
param_list_1: type named_var
param_list_1: named_var

param_list_2: binding_specifier_2 type named_var "," param_list_2
param_list_2: binding_specifier_2 type named_var

type: "@i8"
type: "@i16"
type: "@i32"
type: "@i64"
type: "@u8"
type: "@u16"
type: "@u32"
type: "@u64"
type: "@f32"
type: "@f64"
type: "@str"
type: "@uuid"

atom: r"[a-z][A-Za-z0-9_]*"
named_var: r"[A-Z][A-Za-z0-9_]*"

var: named_var
var: "_"

binding_specifier: "bound"
binding_specifier: "free"

binding_specifier_2: binding_specifier
binding_specifier_2: "aggregate"
binding_specifier_2: "summary"

clause: atom "(" named_var_list ")" ":" conjunct_list "."

named_var_list: named_var "," named_var_list
named_var_list: named_var

conjunct_list: comparison conjunct_list_tail
conjunct_list: predicate conjunct_list_tail
conjunct_list: negation conjunct_list_tail
conjunct_list: predicate "over" aggregation conjunct_list_tail

aggregation: predicate
aggregation: "(" param_list_0 ")" "{" conjunct_list "}"

var_or_literal: var
var_or_literal: literal

comparison: var_or_literal "=" var_or_literal
comparison: var_or_literal "!=" var_or_literal
comparison: var_or_literal "<" var_or_literal
comparison: var_or_literal ">" var_or_literal

predicate: atom "(" arg_list ")"
negation: "!" predicate

conjunct_list_tail: "," conjunct_list
conjunct_list_tail:

arg_list: var_or_literal "," arg_list
arg_list: var_or_literal

literal: "0"
literal: r"[1-9][0-9]*"
literal: r"0[1-7][0-7]*"
literal: r"0x[1-9a-fA-F][0-9a-fA-F]*"
literal: r"[1-9][0-9]*[.][0-9]+"
literal: <double quoted string literal>
```

## Compiler

The compiler front-end, which implements input management, lexing, and parsing
is designed to be re-usable by third parties looking to experiment with new
execution backend strategies, whilst still having access to a high-quality
parser and nice error messages.

## Engine