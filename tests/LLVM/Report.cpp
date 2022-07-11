// Copyright 2022, Trail of Bits. All rights reserved.

#include "Report.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wunused-variable"
#include <llvm/IR/Function.h>
#include <llvm/IR/Instruction.h>
#include <llvm/IR/Instructions.h>
#include <llvm/IR/Module.h>
#pragma GCC diagnostic pop

#include <iostream>

void LLVMReport::call_2(llvm::Function *F_from, llvm::Function *F_to,
                        bool added) {
  std::cout << "Call from " << F_from->getName().str()
            << " to " << F_to->getName().str()
            << std::endl;
}
