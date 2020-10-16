#include "CodeGen.h"

namespace hyde {

class CPPCodeGenVisitor : public ProgramVisitor {
 public:
  CPPCodeGenVisitor(OutputStream &os) : os(os) {}
  ~CPPCodeGenVisitor(void) {
    os.Flush();
  }

  void Visit(const DataColumn &val) override {
    os << "DataColumn\n";
  }

  void Visit(const DataIndex &val) override {
    os << "DataIndex\n";
  }

  void Visit(const DataTable &val) override {
    os << "DataTable\n";
  }

  void Visit(const DataVariable &val) override {
    os << "DataVariable\n";
  }

  void Visit(const DataVector &val) override {
    os << "DataVector\n";
  }

  void Visit(const ProgramCallRegion &val) override {
    os << "ProgramCallRegion\n";
  }

  void Visit(const ProgramExistenceAssertionRegion &val) override {
    os << "ProgramExistenceAssertionRegion\n";
  }

  void Visit(const ProgramExistenceCheckRegion &val) override {
    os << "ProgramExistenceCheckRegion\n";
  }

  void Visit(const ProgramGenerateRegion &val) override {
    os << "ProgramGenerateRegion\n";
  }

  void Visit(const ProgramInductionRegion &val) override {
    os << "ProgramInductionRegion\n";
  }

  void Visit(const ProgramLetBindingRegion &val) override {
    os << "ProgramLetBindingRegion\n";
  }

  void Visit(const ProgramParallelRegion &val) override {
    os << "ProgramParallelRegion\n";
  }

  void Visit(const ProgramProcedure &val) override {
    os << "ProgramProcedure\n";
  }

  void Visit(const ProgramPublishRegion &val) override {
    os << "ProgramPublishRegion\n";
  }

  void Visit(const ProgramSeriesRegion &val) override {
    os << "ProgramSeriesRegion\n";
  }

  void Visit(const ProgramVectorAppendRegion &val) override {
    os << "ProgramVectorAppendRegion\n";
  }

  void Visit(const ProgramVectorClearRegion &val) override {
    os << "ProgramVectorClearRegion\n";
  }

  void Visit(const ProgramVectorLoopRegion &val) override {
    os << "ProgramVectorLoopRegion\n";
  }

  void Visit(const ProgramVectorUniqueRegion &val) override {
    os << "ProgramVectorUniqueRegion\n";
  }

  void Visit(const ProgramTableInsertRegion &val) override {
    os << "ProgramTableInsertRegion\n";
  }

  void Visit(const ProgramTableJoinRegion &val) override {
    os << "ProgramTableJoinRegion\n";
  }

  void Visit(const ProgramTableProductRegion &val) override {
    os << "ProgramTableProductRegion\n";
  }

  void Visit(const ProgramTupleCompareRegion &val) override {
    os << "ProgramTupleCompareRegion\n";
  }

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
