[![Continuous Integration](https://github.com/trailofbits/DrLojekyll/workflows/Continuous%20Integration/badge.svg?branch=master)](https://github.com/trailofbits/DrLojekyll/actions?query=workflow%3A%22Continuous+Integration%22+branch%3Amaster)
[![Fuzz Testing](https://github.com/trailofbits/DrLojekyll/workflows/Fuzz%20Testing/badge.svg?branch=master)](https://github.com/trailofbits/DrLojekyll/actions?query=workflow%3A%22Fuzz+Testing%22+branch%3Amaster)

# Dr. Lojekyll

Dr. Lojekyll, pronounced Dr. Logical, is a Datalog compiler and execution
engine. It is designed around a publish/subscribe model and operates
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
$ cmake -B build -DCMAKE_CXX_COMPILER=clang++-10 -DENABLE_SANITIZERS=1 -DCMAKE_BUILD_TYPE=Debug -DWARNINGS_AS_ERRORS=1 -DENABLE_LIBFUZZER=1 -DCMAKE_INSTALL_PREFIX="$PWD"/install
$ cmake --build build
$ cmake --install build
```

**Developer debug build on macOS:**
```bash
$ cmake -B build -DENABLE_SANITIZERS=1 -DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX="$PWD"/install
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

```eclipse
#message directed_edge(i64 From, i64 To)
#query transitive_closure(bound i64 From, free i64 To)

transitive_closure(From, To) : directed_edge(From, To).
transitive_closure(From, To) : transitive_closure(From, X)
                             , transitive_closure(X, To).
```

## Documentation
 
 * [Coding Style](docs/CodingStyle.md) 
 * [Grammar](docs/Grammar.md)
 * [Tutorial 1: Keeping track of the family](docs/Tutorial/1_KeepingTrackOfTheFamily.md)
