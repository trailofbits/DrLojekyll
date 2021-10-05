// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <drlojekyll/Parse/Parse.h>

#include <optional>

namespace hyde {

class OutputStream;

OutputStream &operator<<(OutputStream &os, TypeKind type);
OutputStream &operator<<(OutputStream &os, TypeLoc type);
OutputStream &operator<<(OutputStream &os, ParsedLiteral val);
OutputStream &operator<<(OutputStream &os, ParsedParameter var);
OutputStream &operator<<(OutputStream &os, ParsedVariable var);
OutputStream &operator<<(OutputStream &os,
                         const std::optional<ParsedVariable> &var);
OutputStream &operator<<(OutputStream &os, ParsedDeclarationName decl);
OutputStream &operator<<(OutputStream &os, ParsedDeclaration decl);
OutputStream &operator<<(OutputStream &os, ParsedPredicate pred);
OutputStream &operator<<(OutputStream &os, ParsedAssignment assign);
OutputStream &operator<<(OutputStream &os, ComparisonOperator op);
OutputStream &operator<<(OutputStream &os, ParsedComparison compare);
OutputStream &operator<<(OutputStream &os, ParsedAggregate aggregate);
OutputStream &operator<<(OutputStream &os, ParsedClauseHead clause);
OutputStream &operator<<(OutputStream &os, ParsedClauseBody clause);
OutputStream &operator<<(OutputStream &os, ParsedClause clause);
OutputStream &operator<<(OutputStream &os, ParsedInline code);
OutputStream &operator<<(OutputStream &os, ParsedEnumType type);
OutputStream &operator<<(OutputStream &os, ParsedForeignType type);
OutputStream &operator<<(OutputStream &os, ParsedForeignConstant constant);
OutputStream &operator<<(OutputStream &os, ParsedDatabaseName name);
OutputStream &operator<<(OutputStream &os, ParsedModule module);

}  // namespace hyde
