[![Continuous Integration](https://github.com/trailofbits/DrLojekyll/workflows/Continuous%20Integration/badge.svg?branch=master)](https://github.com/trailofbits/DrLojekyll/actions?query=workflow%3A%22Continuous+Integration%22+branch%3Amaster)
[![Fuzz Testing](https://github.com/trailofbits/DrLojekyll/workflows/Fuzz%20Testing/badge.svg?branch=master)](https://github.com/trailofbits/DrLojekyll/actions?query=workflow%3A%22Fuzz+Testing%22+branch%3Amaster)

# Dr. Lojekyll

Dr. Lojekyll, pronounced Dr. Logical, is a Datalog compiler and execution
engine. It is designed around a publish/subscribe model, as well as Dr. Stefan
Brass's "push method" of pipelined bottom-up Datalog execution. It operates
somewhere between full materialization and vectorized execution with respect
to operating on tuples.

## Use cases

 * **Incremental static analysis.** You have a codebase that you want to analyze, e.g. solidity code. You normally have to re-run Crytic on each commit. Ideally, you want to load up the analysis results from the prior commit, then differentially update them based only on what code has changed.
 * **Interactive disassembly.** You want to mark addresses as function heads, and then let the system figure out what it can based off of those actions. However, you also want to be able to selectively undo any such decision. Instead of `Ctrl-Z`, you want to have an undo checkbox list.
 * **Mixed static/dynamic analysis.** You want to do a points-to analysis of some program, and you also want to augment the results with real-world data. You implement your basic points-to analysis in Datalog, but then you also instrument your program (e.g. with something like AddressSanitizer) so that when a pointer is passed to a function as an argument at runtime, you can find who allocated it and where, and instantly send a message to Datalog to update your previously static-only based analysis.

## Quick Start

Dr. Lojekyll can be built on macOS, Linux, or Windows, using CMake and a
C++17-compliant toolchain.

**Developer debug build on Linux:**
```bash
$ cmake -B build -DCMAKE_CXX_COMPILER=clang-10 -DENABLE_SANITIZERS=1 -DCMAKE_BUILD_TYPE=Debug -DWARNINGS_AS_ERRORS=1 -DENABLE_LIBFUZZER=1 -DCMAKE_INSTALL_PREFIX="$PWD"/install
$ cmake --build build
$ cmake --install build
```

See the [continuous integration](.github/workflows/ci.yml) for
additional concrete examples of how this software is built.

**Listing available CMake options:**
```bash
$ cmake -B build -LAH
```

The Dr. Lojekyll-specific options are mostly found in the top-level
[CMakeLists.txt](CMakeLists.txt).


## Okay, so what is Dr. Lojekyll, anyway?

The right way to think of a compiled Dr. Lojekyll program is as a combination of
an orchestration engine and database system.

### Orchestration
A Dr. Lojekyll program specified messages that it receives and publishes. Think of
message receipt as being similar in spirit to an `INSERT` query on database, but where
the usage of the data contained in the message defines whether or not it will be
persistently stored. Think of messages published by a Dr. Lojekyll program as being
a kind of broadcast of "new facts of interst."

The key idea is that a user of a Dr. Lojekyll-compiled program `P` will itself be a separate
program -- likely a server that receives messages published by `P` -- that does work,
possibly querying `P` (like a database) and then publishing messages back to `P` for `P`
to receive.

### Database
A Dr. Lojekyll-compiled program is a kind of specialized database. Messages received may trigger
the persistent storage of data. The compiled program may have also specified that it makes
certain data available for querying via RPC.

### Compared to Redis
Redis is a key/value database that also contains a publish/subscribe system, similar to a program
`P` compiled with Dr. Lojekyll. Thus, Redis can and often does operate as both an orchestration
system and as a database.

The key difference between Dr. Lojekyll and Redis is where orchestration logic lies. In the case of
Dr. Lojekyll, that logic is embedded in `P`. That is, `P` can make orchestration decisions with uniform
access to incoming messages, current database state, and access to arbitrary logic via user-defined
C++ functions.

In the case of Redis, clients of a Redis server must themselves listen to messages and query Redis
to implement the logic that publishes messages or puts database values that can then influence other
clients.

A good example of where the logic gets tricky for clients to implement consistently is when a message
`Z` should be published iff both messages `X` and `Y` have been published, and if a predicate applied
to the data in those messages is true. In Dr. Lojekyll, this pattern is trivial to express; however, a Redis
client trying to implement the same mechanics would have to manage local state.

If the logic of orchestration is separate from the database and messages that guide that logic, then
consistency guarantees that are available on the data cannot (easily) be translated to the logic. That is,
if data and logic are separate, then by the time a decision is made, the data may have changed, which
might invalidate the decision. In Dr. Lojekyll, data and logic are coupled so that the same consistency
that is applied to the data also applies to the logic.

## Syntax Examples

### Transitive Closure
The following code describes how to create a program that computes the
transitive closure of a directed graph.

This program receives messages, each being a tuple of 64-bit integers
representing graph node IDs. These tuples represent edges in the graph. The
program provides a query interface, where all reachable node IDs (`To`) from
a given node ID (`From`) can be queried.

```
#message directed_edge(i64 From, i64 To)
#query transitive_closure(bound i64 From, free i64 To)

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
decl_list: inline_code decl_list
decl_list:

clause_list: clause clause_list
clause_list: inline_code clause_list
clause_list

import: "#import" <double quoted string literal>

// Single and multi-line code blocks
code_data: "```" <anything...> "```"
code_data: "```c++" <anything...> "```"
code_data: "```python" <anything...> "```"
code_data: <double quoted string literal>

// Inline code statements to be emitted to the generated code.
code_decl: "#prologue" code_data
code_decl: "#epilogue" code_data

// Decls generally must fit inside a single line. They are allowed
// to span multiple lines, but only if the new line characters exist
// within the matched parantheses of their parameter lists.
decl: export_decl
decl: local_decl
decl: functor_decl
decl: message_decl
decl: foreign_decl
decl: constant_decl

message_decl: "#message" atom "(" param_list_0 ")" "\n"
export_decl: "#export" atom "(" param_list_1 ")" "\n"
local_decl: "#local" atom "(" param_list_1 ")" maybe_inline "\n"

foreign_type_name: atom
foreign_type_name: variable

foreign_decl: "#foreign" foreign_type_name
foreign_decl: "#foreign" foreign_type_name code_data
foreign_decl: "#foreign" foreign_type_name code_data "```" <anything containing one $> "```"
foreign_decl: "#foreign" foreign_type_name code_data <double quoted string containing one $>

constant_decl: "#constant" foreign_type_name code_data

maybe_inline: "inline"
maybe_inline:

functor_decl: "#functor" atom "(" param_list_2 ")" constraints "\n"

constraints:
constraints: "range" "(" "." ")" constraints
constraints: "range" "(" "+" ")" constraints
constraints: "range" "(" "*" ")" constraints
constraints: "range" "(" "?" ")" constraints
constraints: "impure" constraints

param_list_0: type named_var "," param_list_0
param_list_0: type named_var

param_list_1: type named_var "," param_list_1
param_list_1: named_var "," param_list_1
param_list_1: type named_var
param_list_1: "mutable" "(" atom ")" named_var
param_list_1: named_var

param_list_2: binding_specifier_2 type named_var "," param_list_2
param_list_2: binding_specifier_2 type named_var

type: "i8"
type: "i16"
type: "i32"
type: "i64"
type: "u8"
type: "u16"
type: "u32"
type: "u64"
type: "f32"
type: "f64"
type: "utf8"
type: "ascii"
type: "bytes"
type: "uuid"

atom: r"[a-z][A-Za-z0-9_]*"
named_var: r"[A-Z][A-Za-z0-9_]*"

var: named_var
var: "_"

binding_specifier: "bound"
binding_specifier: "free"

binding_specifier_2: binding_specifier
binding_specifier_2: "aggregate"
binding_specifier_2: "summary"

clause: atom "(" named_var_list ")" "."
clause: atom "(" named_var_list ")" ":" conjunct_list "."
clause: "!" atom "(" named_var_list ")" "."
clause: "!" atom "(" named_var_list ")" ":" conjunct_list "."
clause: atom ":" conjunct_list "."

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

predicate: atom
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
