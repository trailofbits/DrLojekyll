// Copyright 2022, Trail of Bits. All rights reserved.

#include "Interface.h"

#include <cstdlib>
#include <vector>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wunused-variable"
#include <llvm/IRReader/IRReader.h>
#include <llvm/Support/SourceMgr.h>
#include <llvm/Support/Error.h>
#pragma GCC diagnostic pop

class LLVMInterfaceImpl {
 public:
  llvm::LLVMContext context;
  std::vector<std::unique_ptr<llvm::Module>> modules;
};

LLVMInterface::~LLVMInterface(void) {}

LLVMInterface::LLVMInterface(void)
    : impl(new LLVMInterfaceImpl) {
  impl->context.enableOpaquePointers();
}

// Returns `true` if `B_from` is a predecessor of `B_to`.
bool LLVMInterface::block_successor_bb(llvm::BasicBlock * B_from,
                                       llvm::BasicBlock * B_to) {
  for (llvm::BasicBlock *B_found : llvm::successors(B_from)) {
    if (B_found == B_to) {
      return true;
    }
  }
  return false;
}

// Find the successors of `I_from`. This could be inter-block, if `I_from` is
// a terminator, or intra-block, if it's an internal instruction in a basic
// block.
gap::generator<llvm::Instruction *> LLVMInterface::instruction_successor_bf(
    llvm::Instruction *I_from) {
  if (llvm::Instruction *I_succ = I_from->getNextNode()) {
    co_yield I_succ;

  } else {
    for (llvm::BasicBlock *B_to : llvm::successors(I_from->getParent())) {
      for (llvm::Instruction &I_to : *B_to) {
        co_yield &I_to;
        break;
      }
    }
  }
}

// Find the predecessors of `I_to`. This could be inter-block, if `I_to` is
// the first instruction in a block, or intra-block, if it's an internal
// instruction in a basic block.
gap::generator<llvm::Instruction *> LLVMInterface::instruction_successor_fb(
      llvm::Instruction * I_to) {
  if (llvm::Instruction *I_from = I_to->getPrevNode()) {
    co_yield I_from;

  } else {
    for (llvm::BasicBlock *B_from : llvm::predecessors(I_to->getParent())) {
      if (auto B_from_term = B_from->getTerminator()) {
        co_yield B_from_term;
      }
    }
  }
}

bool LLVMInterface::instruction_successor_bb(llvm::Instruction *I_from,
                                             llvm::Instruction *I_to) {
  llvm::BasicBlock *B_from = I_from->getParent();
  llvm::BasicBlock *B_to = I_to->getParent();
  if (B_from == B_to) {
    return I_from->getNextNode() == I_to;
  } else if (I_from->getNextNode() || I_to->getPrevNode()) {
    return false;
  } else {
    return block_successor_bb(B_from, B_to);
  }
}

// Load a module from a file.
llvm::Module *LLVMInterface::load_module_bf(
    const std::filesystem::path &file_name) {
  llvm::SMDiagnostic err;
  auto module = llvm::parseIRFile(file_name.generic_string(), err,
                                  impl->context);
  if (!module) {
    return nullptr;
  }

  llvm::Error ec = module->materializeAll();  // Just in case.
  if (ec) {
    return nullptr;
  }

  impl->modules.emplace_back(std::move(module));
  return impl->modules.back().get();
}
