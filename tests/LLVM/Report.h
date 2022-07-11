// Copyright 2022, Trail of Bits. All rights reserved.

namespace llvm {
class Argument;
class Function;
class Instruction;
class Module;
class Use;
class User;
}  // namespace llvm

class LLVMReport {
 public:
  void call_2(llvm::Function *F_from, llvm::Function *F_to, bool added);
};
