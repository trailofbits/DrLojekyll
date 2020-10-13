#include "CodeGen.h"

namespace hyde {

class CPPCodeGenVisitor : public ProgramVisitor {
 public:
  CPPCodeGenVisitor(OutputStream &os) : os(os) {}
  ~CPPCodeGenVisitor(void) {
    os.Flush();
  }

  // void Visit(const DataColumn &val) override {
  // }

  // void Visit(const DataIndex &val) override {
  // }

  // void Visit(const DataTable &val) override {
  // }

  // void Visit(const DataVariable &val) override {
  // }

  // void Visit(const DataVector &val) override {
  // }

  // void Visit(const ProgramCallRegion &val) override {
  // }

  // void Visit(const ProgramExistenceAssertionRegion &val) override {
  // }

  // void Visit(const ProgramExistenceCheckRegion &val) override {
  // }

  // void Visit(const ProgramGenerateRegion &val) override {
  // }

  // void Visit(const ProgramInductionRegion &val) override {
  // }

  // void Visit(const ProgramLetBindingRegion &val) override {
  // }

  // void Visit(const ProgramParallelRegion &val) override {
  // }

  // void Visit(const ProgramProcedure &val) override {
  // }

  // void Visit(const ProgramPublishRegion &val) override {
  // }

  // void Visit(const ProgramSeriesRegion &val) override {
  // }

  // void Visit(const ProgramVectorAppendRegion &val) override {
  // }

  // void Visit(const ProgramVectorClearRegion &val) override {
  // }

  // void Visit(const ProgramVectorLoopRegion &val) override {
  // }

  // void Visit(const ProgramVectorUniqueRegion &val) override {
  // }

  // void Visit(const ProgramTableInsertRegion &val) override {
  // }

  // void Visit(const ProgramTableJoinRegion &val) override {
  // }

  // void Visit(const ProgramTableProductRegion &val) override {
  // }

  // void Visit(const ProgramTupleCompareRegion &val) override {
  // }

  void Visit(const Program &val) override {
    os << "Program\n";
  }

 private:
  OutputStream &os;
};

void GenerateCode(const Program &program, OutputStream &os)
{
  CPPCodeGenVisitor visitor(os);
  program.Accept(visitor);
}

}  // namespace hyde
