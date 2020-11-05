#include "CodeGen.h"

namespace hyde {

class CPPCodeGenVisitor : public ProgramVisitor {
 public:
  CPPCodeGenVisitor(OutputStream &os) : os(os) {}
  ~CPPCodeGenVisitor(void) {
    os.Flush();
  }

  void Visit(DataColumn &val) override {
    os << "DataColumn\n";
  }

  void Visit(DataIndex &val) override {
    os << "DataIndex\n";
  }

  void Visit(DataTable &val) override {
    os << "DataTable\n";
  }

  void Visit(DataVariable &val) override {
    os << "DataVariable\n";
  }

  void Visit(DataVector &val) override {
    os << "DataVector\n";
  }

  void Visit(ProgramCallRegion &val) override {
    os << "ProgramCallRegion\n";
  }

  void Visit(ProgramExistenceAssertionRegion &val) override {
    os << "ProgramExistenceAssertionRegion\n";
  }

  void Visit(ProgramExistenceCheckRegion &val) override {
    os << "ProgramExistenceCheckRegion\n";
  }

  void Visit(ProgramGenerateRegion &val) override {
    os << "ProgramGenerateRegion\n";
  }

  void Visit(ProgramInductionRegion &val) override {
    os << "ProgramInductionRegion\n";
  }

  void Visit(ProgramLetBindingRegion &val) override {
    os << "ProgramLetBindingRegion\n";
  }

  void Visit(ProgramParallelRegion &val) override {
    os << "ProgramParallelRegion\n";
  }

  void Visit(ProgramProcedure &val) override {
    os << "ProgramProcedure\n";
  }

  void Visit(ProgramPublishRegion &val) override {
    os << "ProgramPublishRegion\n";
  }

  void Visit(ProgramSeriesRegion &val) override {
    os << "ProgramSeriesRegion\n";
  }

  void Visit(ProgramVectorAppendRegion &val) override {
    os << "ProgramVectorAppendRegion\n";
  }

  void Visit(ProgramVectorClearRegion &val) override {
    os << "ProgramVectorClearRegion\n";
  }

  void Visit(ProgramVectorLoopRegion &val) override {
    os << "ProgramVectorLoopRegion\n";
  }

  void Visit(ProgramVectorUniqueRegion &val) override {
    os << "ProgramVectorUniqueRegion\n";
  }

  void Visit(ProgramTableInsertRegion &val) override {
    os << "ProgramTableInsertRegion\n";
  }

  void Visit(ProgramTableJoinRegion &val) override {
    os << "ProgramTableJoinRegion\n";
  }

  void Visit(ProgramTableProductRegion &val) override {
    os << "ProgramTableProductRegion\n";
  }

  void Visit(ProgramTupleCompareRegion &val) override {
    os << "ProgramTupleCompareRegion\n";
  }

  void Visit(Program &val) override {
    os << "Program\n";
  }

 private:
  OutputStream &os;
};

void GenerateCode(Program &program, OutputStream &os) {
  CPPCodeGenVisitor visitor(os);
  program.Accept(visitor);
}

}  // namespace hyde
