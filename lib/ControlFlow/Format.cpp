// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/ControlFlow/Format.h>

#include "Program.h"

#include <drlojekyll/DataFlow/Format.h>
#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Lex/Format.h>
#include <drlojekyll/Parse/Format.h>

namespace hyde {

OutputStream &operator<<(OutputStream &os, ProgramLetBindingRegion region) {
  if (auto maybe_body = region.Body(); maybe_body) {
    os << os.Indent() << "let-binding {\n";
    os.PushIndent();
    os << (*maybe_body) << '\n';
    os.PopIndent();
    os << os.Indent() << '}';
  } else {
    os << os.Indent() << "empty-let-binding";
  }
  return os;
}

OutputStream &operator<<(OutputStream &os, ProgramVectorLoopRegion region) {
  if (auto maybe_body = region.Body(); maybe_body) {
    os << os.Indent() << "vector-loop {\n";
    os.PushIndent();
    os << (*maybe_body) << '\n';
    os.PopIndent();
    os << os.Indent() << '}';
  } else {
    os << os.Indent() << "empty-vector-loop";
  }
  return os;
}

OutputStream &operator<<(OutputStream &os, ProgramVectorAppendRegion region) {
  os << os.Indent() << "vector-append";
  return os;
}

OutputStream &operator<<(OutputStream &os, ProgramViewInsertRegion region) {
  if (auto maybe_body = region.Body(); maybe_body) {
    os << os.Indent() << "if-insert-into-view {\n";
    os.PushIndent();
    os << (*maybe_body) << '\n';
    os.PopIndent();
    os << os.Indent() << '}';
  } else {
    os << os.Indent() << "insert-into-view";
  }
  return os;
}

OutputStream &operator<<(OutputStream &os, ProgramViewJoinRegion region) {
  if (auto maybe_body = region.Body(); maybe_body) {
    os << os.Indent() << "equi-join {\n";
    os.PushIndent();
    os << (*maybe_body) << '\n';
    os.PopIndent();
    os << os.Indent() << '}';
  } else {
    os << os.Indent() << "empty-equi-join";
  }
  return os;
}

OutputStream &operator<<(OutputStream &os, ProgramInductionRegion region) {
  os << os.Indent() << "induction\n";
  os.PushIndent();
  os << os.Indent() << "init\n";
  os.PushIndent();
  os << region.Initializer() << '\n';
  os.PopIndent();
  os << os.Indent() << "fixpoint-loop\n";
  os.PushIndent();
  os << region.FixpointLoop() << '\n';
  os.PopIndent();
  os << os.Indent() << "output\n";
  os.PushIndent();
  os << region.Output() << '\n';
  os.PopIndent();
  os.PopIndent();
  os << os.Indent() << '}';
  return os;
}

OutputStream &operator<<(OutputStream &os, ProgramSeriesRegion region) {
  auto sep = "";
  for (auto sub_region : region.Regions()) {
    os << sep << sub_region;
    sep = "\n";
  }
  return os;
}

OutputStream &operator<<(OutputStream &os, ProgramParallelRegion region) {
  auto sep = "";
  os << os.Indent() << "par\n";
  os.PushIndent();
  for (auto sub_region : region.Regions()) {
    os << sep << os.Indent() << "{\n";
    os.PushIndent();
    os << sub_region << '\n';
    os.PopIndent();
    os << os.Indent() << '}';
    sep = "\n";
  }
  os.PopIndent();
  return os;
}

OutputStream &operator<<(OutputStream &os, ProgramRegion region) {
  if (region.IsSeries()) {
    os << ProgramSeriesRegion::From(region);
  } else if (region.IsParallel()) {
    os << ProgramParallelRegion::From(region);
  } else if (region.IsLetBinding()) {
    os << ProgramLetBindingRegion::From(region);
  } else if (region.IsInduction()) {
    os << ProgramInductionRegion::From(region);
  } else if (region.IsVectorLoop()) {
    os << ProgramVectorLoopRegion::From(region);
  } else if (region.IsVectorAppend()) {
    os << ProgramVectorAppendRegion::From(region);
  } else if (region.IsViewInsert()) {
    os << ProgramViewInsertRegion::From(region);
  } else if (region.IsViewJoin()) {
    os << ProgramViewJoinRegion::From(region);
  } else {
    assert(false);
    os << "<<unknown region>>";
  }
  return os;
}

OutputStream &operator<<(OutputStream &os, ProgramVectorProcedure proc) {
  os << "vector-proc {\n";
  os.PushIndent();
  os << proc.Body() << '\n';
  os.PopIndent();
  os << '}';
  return os;
}

OutputStream &operator<<(OutputStream &os, ProgramProcedure proc) {
  if (proc.OperatesOnVector()) {
    os << ProgramVectorProcedure::From(proc);

  } else if (proc.OperatesOnTuple()) {
    assert(!"TODO!");

  } else {
    assert(false);
    os << "<<invalid procedure>>";
  }
  return os;
}

OutputStream &operator<<(OutputStream &os, Program program) {
  auto sep = "";
  for (auto proc : program.Procedures()) {
    os << sep << proc;
    sep = "\n\n";
  }
  return os;
}

}  // namespace hyde
