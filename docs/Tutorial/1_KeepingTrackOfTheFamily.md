# Tutorial 1: Keeping track of the family

In this tutorial, we will create a Datalog database that keeps track of family
members. Let's start by declaring a relation to keep track of parentage.

## Tracking relations with relations

We will base everything around the `child_of` relation, which will contain two
pieces of information: the name of a child, and the name of that child's parent.

```eclipse
#local child_of(ChildName, ParentName).
```

One way to think about the above declaration is that it is a terse form of
SQL's `CREATE TABLE` syntax. For example, you could read the above `#local`
declaration as being equivalent to the following SQL.

```sql
CREATE TABLE child_of (
  ChildName TEXT,
  ParentName TEXT,

  PRIMARY KEY(ChildName, ParentName)
);
```

Another way of thinking about the above `#local` declaration is that it defines
the following Python variable.

```python
child_of: Set[Tuple[str, str]] = set()
```

## Our first rules

Let's keep track of all the people in our database. We're not going to use this
in any way, but it's a good first example. We can leverage the `child_of`
relation to find all the people in our database.

```eclipse
#local person(Name).
person(Name) : child_of(Name, _).
person(Name) : child_of(_, Name).
```

Underscore (`_`) has the meaning of a "don't care" or "match anything" variable.
Two independent uses of the `_` are unrelated.

An English language interpretation of the above two clauses would be:

 * If you are child of someone else, then you are a person.
 * If you are a parent of someone, then you are a person.

An imperative interpretation of the above two clauses would be:

```python
child_of: Set[Tuple[str, str]] = set()
person: Set[str] = set()

# ...

for (Name, _) in child_of:
  person.add(Name)


for (_, Name) in child_of:
  person.add(Name)
```

## Who is related to who?

We might also want to know if two people are related, e.g. if someone is a cousin
of someone else. We'll use a `#query` declaration for this, which will instruct
the compiler's code generator to emit a function that we can call later to
query for data from the system. In this case, we are saying that given a
`SearchName`, we will receive a list of `CousinName` values for which `SearchName`
and `CousinName` are cousins.

```eclipse
#query cousin_of(bound str SearchName, free str CousinName).

cousin_of(A, CousinOfA)
    : child_of(A, Parent)
    , child_of(Parent, GrandParent)
    , child_of(AuntOrUncle, GrandParent)
    , Parent != AuntOrUncle
    , child_of(CousinOfA, AuntOrUncle).
```

Woah that's a lot going on. The following is an English interpretation of the
above clause.

 * If `A` is the child of `Parent`, and
 * If `Parent` is the child of `GrandParent`, and
 * If `AuntOrUncle` is also a child of `GrandParent`, and
 * If `Parent` is a different person than `AuntOrUncle`, and
 * If `CousinOfA` is a child of `AuntOrUncle`,
 * Then `A` is the cousin of `CousinOfA`.

The following is an imperative interpretation of the above clauses.

```python
# ...

cousin_of: Set[Tuple[str, str]] = set()

# child_of(A, Parent)
for (A, Parent) in child_of:

  # child_of(Parent, GrandParent)
  for (Parent_1, GrandParent) in child_of:
    if Parent != Parent_1:
      continue

    # child_of(AuntOrUncle, GrandParent)
    for (AuntOrUncle, GrandParent_1) in child_of:
      if GrandParent != GrandParent_1:
        continue

      # Parent != AuntOrUncle
      if Parent != AuntOrUncle:

        # child_of(CousinOfA, AuntOrUncle)
        for (CousinOfA, AuntOrUncle_1) in child_of:
          if AuntOrUncle != AuntOrUncle_1:
            continue

          # Done! Proven `cousin_of(A, CousinOfA)`.
          cousin_of.add((A, CousinOfA))
```

Here are a few important points to take away when establishing your mental model
about how Datalog works. The first is that variable names can be used and re-used
and that the first use acts as a kind of definition, and all later uses introduce
implicit constraints. This is visible in the Python translation in the first few
lines: where the Datalog code uses `child_of(A, Parent), child_of(Parent, GrandParent)`,
the Python code uses `Parent` and `Parent_1`, and requires that `Parent` match
`Parent_1` before other parts of the clause body are evaluated.

This style of variable binding is called "sideways information passing style."
The first usage of a variable is actually a kind of definition for it. Though,
perhaps it's more accurate to say that the first usage of a variable binds a
a value to that variable, and all other usages of the same variable are checked
against that bound value.

The second important takeaway is a direct consequence of Datalog's relation to
first-order logic; the order in which predicates/conjuncts in a clause body are
evaluated is irrelevant (at least, for now). The is, the nesting order of the
`for` loops in the above Python interpretation could be re-ordered and the
correctness of the program would not be altered. This also follows from the first
takeway: re-ordering predicates can change which variable usages are treated as
the definitions and which are treated as the checks, but it doesn't change what
values will inhabit those variables.

The final takeaway is about the structure of the imperative translation of the
Datalog code. The imperative code starts with the body, and then eventually proves
the head. This is called "bottom-up" proving. Another aspect of the imperative
translation is that each use of a relation in a clause body is associated with a
`for` loop. Here we see that we are really trying to find *all* satisfying 
assignments to variables that will make the entire clause body evaluate to true.

## How is babby formed?

Thus far we've declared relations and rules, but not how data enters into our
system. For that, we need a message.

```eclipse
#message born(utf8 ChildName, utf8 ParentName).

child_of(ChildName, ParentName) : born(ChildName, ParentName).
```

The English interpretation of the clause is:

  * If a `born` message is received with contents `(ChildName, ParentName)`,
  * Then add `(ChildName, ParentName)` to the `child_of` relation.

But then, where does the `born` message come from? How is it received? This answer
is two-part.

First, it is the compiler's responsibility to generate a function named after
`born`, which it will call `born_2` (the additional `_2` is there because the
`born` message has two parameters). This function can be called by user code
in order to feed data into the database.

Second, it is up to "the user" (human that is interfacing with generated code)
to program in a mechanism to actually call these generated functions. This
mechanism might, for instance, subscribe to a message queue and wait for a "real"
message to be received, and pass its data into the function. Alternatively, it
might just read a CSV file and feed those records into the function. How the
generated code interacts with the rest of the world is really up to the user.

## Compiling our Datalog

Let's compile our Datalog code. Create a file, `/tmp/tutorial_1.dr`, and into that
file, copy and paste the following code.

```eclipse
#message born(utf8 ChildName, utf8 ParentName).

#local child_of(ChildName, ParentName).
child_of(ChildName, ParentName) : born(ChildName, ParentName).

#query cousin_of(bound utf8 SearchName, free utf8 CousinName).
cousin_of(A, CousinOfA)
    : child_of(A, Parent)
    , child_of(Parent, GrandParent)
    , child_of(AuntOrUncle, GrandParent)
    , Parent != AuntOrUncle
    , child_of(CousinOfA, AuntOrUncle).
```

Next, invoke the Dr. Lojekyll compiler at the command line.

```bash
drlojekyll /tmp/tutorial_1.dr -py-out /tmp/tutorial_1.py
```

Next, we'll create some Python code to interact with the generated Python code.
In `/tmp/tutorial_1_test.py`, paste the following code.

```python
# Import the Python code, stored in tutorial_1`, generated by the above compiler
# invocation. All generated code contains three classes:
#
#   `Database`: Where the data is stored, where the query methods are defined,
#               and where the methods associated with received methods are
#               defined.
#
#   `DatabaseLog`: Where the methods associated with published methods are
#                  defined. These can be overriden by extending the `DatabaseLog`
#                  class. This will be explained more in a later tutorial.
#
#   `DatabaseFunctors`: Where the methods associated with declared functors are
#                       defined. The default definition looks into the global
#                       scope of their containing module, `tutorial_1` in this
#                       case, for functions that might provide implementations
#                       of the functors. This will be explained more in a later
#                       tutorial.
from tutorial_1 import Database, DatabaseLog, DatabaseFunctors

# Create an instance of the Datalog program. We also pass in instances of
# `DatabaseLog` and `DatabaseFunctors`, which hint at the possibility of them
# being extendable with custom functionality!
db = Database(DatabaseLog(), DatabaseFunctors())

# `born_2` refers to our `born` message declaration, which has two parameters.
# This feeds data into the engine and kicks off eager, bottom-up proving of all
# cousins.
db.born_2([("Prince Charles", "Queen Elizabeth II"),
           ("Princess Anne", "Queen Elizabeth II"),
           ("Prince William", "Prince Charles"),
           ("Prince Harry", "Prince Charles"),
           ("Peter Phillips", "Princess Anne"),
           ("Zara Tindall", "Princess Anne")])

# `cousin_of_bf` refers to our `cousin_of` query declaration, which has two
# parameters. The first parameter is declared with a `bound` attribute, which
# corresponds with the `b` in the `_bf` suffix. The `bound` attribute means
# that a concrete value will be provided when invoking the generated function.
# In this case, that concrete value is `"Prince William"`. The second parameter
# is declared with a `free` attribute, which corresponds with the `f` in the
# `_bf` suffix. The `free` attribute means that when called, the generated
# function will produce this value. In Python, this manifests as a generator or
# iterator of values.
for cousin_of_william in db.cousin_of_bf("Prince William"):
  print("{} is the Duke's cousin".format(cousin_of_william))
```

Now, you should be able to run this code and see the following output.

```bash
% python3 /tmp/tutorial_1_test.py                         
Peter Phillips is the Duke's cousin
Zara Tindall is the Duke's cousin
```
