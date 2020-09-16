// Copyright 2019, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/ControlFlow/Program.h>

namespace hyde {

class OutputStream;

OutputStream &operator<<(OutputStream &os, DataView view);
OutputStream &operator<<(OutputStream &os, DataTable table);
OutputStream &operator<<(OutputStream &os, DataVector vec);
OutputStream &operator<<(OutputStream &os, DataVariable var);
OutputStream &operator<<(OutputStream &os, ProgramExistenceCheckRegion region);
OutputStream &operator<<(OutputStream &os, ProgramLetBindingRegion region);
OutputStream &operator<<(OutputStream &os, ProgramVectorLoopRegion region);
OutputStream &operator<<(OutputStream &os, ProgramVectorAppendRegion region);
OutputStream &operator<<(OutputStream &os, ProgramVectorClearRegion region);
OutputStream &operator<<(OutputStream &os, ProgramViewInsertRegion region);
OutputStream &operator<<(OutputStream &os, ProgramViewJoinRegion region);
OutputStream &operator<<(OutputStream &os, ProgramInductionRegion region);
OutputStream &operator<<(OutputStream &os, ProgramSeriesRegion region);
OutputStream &operator<<(OutputStream &os, ProgramParallelRegion region);
OutputStream &operator<<(OutputStream &os, ProgramRegion region);
OutputStream &operator<<(OutputStream &os, ProgramProcedure proc);
OutputStream &operator<<(OutputStream &os, Program program);

}  // namespace hyde
