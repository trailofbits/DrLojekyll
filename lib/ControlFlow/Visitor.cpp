// Copyright 2020, Trail of Bits. All rights reserved.

#include "Program.h"

namespace hyde {

#define MAKE_VISITOR(cls) \
  void cls ## Impl::Accept(ProgramVisitor &visitor) { \
    cls val(this); \
    visitor.Visit(val); \
  } \
  void ProgramVisitor::Visit(cls) {}

ProgramVisitor::~ProgramVisitor(void) {}

MAKE_VISITOR(ProgramCallRegion)
MAKE_VISITOR(ProgramReturnRegion)
MAKE_VISITOR(ProgramTestAndSetRegion)
MAKE_VISITOR(ProgramGenerateRegion)
MAKE_VISITOR(ProgramInductionRegion)
MAKE_VISITOR(ProgramModeSwitchRegion)
MAKE_VISITOR(ProgramLetBindingRegion)
MAKE_VISITOR(ProgramParallelRegion)
MAKE_VISITOR(ProgramProcedure)
MAKE_VISITOR(ProgramPublishRegion)
MAKE_VISITOR(ProgramSeriesRegion)
MAKE_VISITOR(ProgramVectorAppendRegion)
MAKE_VISITOR(ProgramVectorClearRegion)
MAKE_VISITOR(ProgramVectorLoopRegion)
MAKE_VISITOR(ProgramVectorSwapRegion)
MAKE_VISITOR(ProgramVectorUniqueRegion)
MAKE_VISITOR(ProgramChangeTupleRegion)
MAKE_VISITOR(ProgramChangeRecordRegion)
MAKE_VISITOR(ProgramCheckTupleRegion)
MAKE_VISITOR(ProgramCheckRecordRegion)
MAKE_VISITOR(ProgramTableJoinRegion)
MAKE_VISITOR(ProgramTableProductRegion)
MAKE_VISITOR(ProgramTableScanRegion)
MAKE_VISITOR(ProgramTupleCompareRegion)
MAKE_VISITOR(ProgramWorkerIdRegion)

#undef MAKE_VISITOR

}  // namespace hyde
