// Copyright 2022, Trail of Bits. All rights reserved.

#include <filesystem>
#include <gap/core/generator.hpp>
#include <memory>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wunused-variable"
#include <llvm/IR/Function.h>
#include <llvm/IR/Instruction.h>
#include <llvm/IR/Instructions.h>
#include <llvm/IR/Module.h>
#pragma GCC diagnostic pop

class LLVMInterfaceImpl;

class LLVMInterface {
 private:
  std::unique_ptr<LLVMInterfaceImpl> impl;

 public:
  ~LLVMInterface(void);
  LLVMInterface(void);

  llvm::Module *load_module_bf(const std::filesystem::path &file_name);

  inline static gap::generator<llvm::Function *> module_function_bf(
      llvm::Module * M) {
    for (llvm::Function &F : M->functions()) {
      co_yield &F;
    }
  }

  inline static llvm::Module *module_function_fb(llvm::Function *F) {
    return F->getParent();
  }

  inline static bool module_function_bb(llvm::Module *M, llvm::Function *F) {
    return F->getParent() == M;
  }

  inline static gap::generator<llvm::GlobalVariable *> module_variable_bf(
      llvm::Module * M) {
    for (llvm::GlobalVariable &V : M->globals()) {
      co_yield &V;
    }
  }

  inline static llvm::Module *module_variable_fb(llvm::GlobalVariable *V) {
    return V->getParent();
  }

  inline static bool module_variable_bb(llvm::Module *M,
                                        llvm::GlobalVariable *V) {
    return V->getParent() == M;
  }

  inline static gap::generator<llvm::BasicBlock *>
  function_block_bf(llvm::Function *F) {
    for (llvm::BasicBlock &B : *F) {
      co_yield &B;
    }
  }

  inline static llvm::Function *function_block_fb(llvm::BasicBlock *B) {
    return B->getParent();
  }

  inline static bool function_block_bb(llvm::Function *F, llvm::BasicBlock *B) {
    return B->getParent() == F;
  }

  inline static gap::generator<llvm::Argument *>
  function_argument_bf(llvm::Function *F) {
    for (llvm::Argument &A : F->args()) {
      co_yield &A;
    }
  }

  inline static llvm::Function *function_argument_fb(llvm::Argument *A) {
    return A->getParent();
  }

  inline static bool function_argument_bb(llvm::Function *F, llvm::Argument *A) {
    return A->getParent() == F;
  }

  inline static gap::generator<llvm::Use *> function_use_bf(llvm::Function *F) {
    for (llvm::Use &U : F->uses()) {
      co_yield &U;
    }
  }

  inline static llvm::Function *function_use_fb(llvm::Use *U) {
    return llvm::dyn_cast<llvm::Function>(U->get());
  }

  inline static bool function_use_bb(llvm::Function *F, llvm::Use *U) {
    return U->get() == F;
  }

  inline static bool argument_use_bb(llvm::Argument *A, llvm::Use *U) {
    return U->get() == A;
  }

  inline static gap::generator<llvm::Use *> argument_use_bf(llvm::Argument *A) {
    for (llvm::Use &U : A->uses()) {
      co_yield &U;
    }
  }

  inline static llvm::Argument *argument_use_fb(llvm::Use *U) {
    return llvm::dyn_cast<llvm::Argument>(U->get());
  }

  inline static bool instruction_use_bb(llvm::Instruction *I, llvm::Use *U) {
    return U->get() == I;
  }

  inline static gap::generator<llvm::Use *> instruction_use_bf(
      llvm::Instruction *I) {
    for (llvm::Use &U : I->uses()) {
      co_yield &U;
    }
  }

  inline static llvm::Instruction *instruction_use_fb(llvm::Use *U) {
    return llvm::dyn_cast<llvm::Instruction>(U->get());
  }

  inline static bool variable_use_bb(llvm::GlobalVariable *V, llvm::Use *U) {
    return U->get() == V;
  }

  inline static gap::generator<llvm::Use *> variable_use_bf(
      llvm::GlobalVariable *V) {
    for (llvm::Use &U : V->uses()) {
      co_yield &U;
    }
  }

  inline static llvm::GlobalVariable *variable_use_fb(llvm::Use *U) {
    return llvm::dyn_cast<llvm::GlobalVariable>(U->get());
  }

  inline static bool instruction_user_bb(llvm::Instruction *I, llvm::Use *U) {
    return U->getUser() == I;
  }

  inline static gap::generator<llvm::Use *> instruction_user_bf(
      llvm::Instruction *I) {
    for (llvm::Use &U : I->operands()) {
      co_yield &U;
    }
  }

  inline static llvm::Instruction *instruction_user_fb(llvm::Use *U) {
    return llvm::dyn_cast<llvm::Instruction>(U->getUser());
  }

  inline static gap::generator<llvm::Instruction *>
  function_instruction_bf(llvm::Function *F) {
    for (llvm::BasicBlock &B : *F) {
      for (llvm::Instruction &I : B) {
        co_yield &I;
      }
    }
  }

  inline static unsigned instruction_opcode_bf(llvm::Instruction *I) {
    return I->getOpcode();
  }


  inline static bool instruction_opcode_bb(llvm::Instruction *I,
                                           unsigned O) {
    return I->getOpcode() == O;
  }

  inline static llvm::Function *function_instruction_fb(llvm::Instruction *I) {
    return I->getFunction();
  }

  inline static bool function_instruction_bb(llvm::Function *F,
                                             llvm::Instruction *I) {
    return F == I->getFunction();
  }

  inline static gap::generator<llvm::Instruction *>
  block_instruction_bf(llvm::BasicBlock *B) {
    for (llvm::Instruction &I : *B) {
      co_yield &I;
    }
  }

  inline static llvm::BasicBlock *block_instruction_fb(llvm::Instruction *I) {
    return I->getParent();
  }

  inline static bool block_instruction_bb(llvm::BasicBlock *B,
                                          llvm::Instruction *I) {
    return I->getParent() == B;
  }

  inline static llvm::Instruction *block_terminator_bf(llvm::BasicBlock *B) {
    return B->getTerminator();
  }

  inline static llvm::BasicBlock *block_terminator_fb(llvm::Instruction *I) {
    if (I->isTerminator()) {
      return I->getParent();
    } else {
      return nullptr;
    }
  }

  inline static bool block_terminator_bb(llvm::BasicBlock *B,
                                         llvm::Instruction *I) {
    return B->getTerminator() == I;
  }

  inline static gap::generator<llvm::BasicBlock *> block_successor_bf(
      llvm::BasicBlock *B_from) {
    for (llvm::BasicBlock *B_to : llvm::successors(B_from)) {
      co_yield B_to;
    }
  }

  inline static gap::generator<llvm::BasicBlock *> block_successor_fb(
      llvm::BasicBlock * B_to) {
    for (llvm::BasicBlock *B_from : llvm::predecessors(B_to)) {
      co_yield B_from;
    }
  }

  static bool block_successor_bb(llvm::BasicBlock * B_from,
                                 llvm::BasicBlock * B_to);

  static gap::generator<llvm::Instruction *> instruction_successor_bf(
      llvm::Instruction * I_from);

  static gap::generator<llvm::Instruction *> instruction_successor_fb(
      llvm::Instruction * I_to);

  static bool instruction_successor_bb(llvm::Instruction *I_from,
                                       llvm::Instruction *I_to);


  static bool instruction_dominators_bb(llvm::Instruction *I_dominator,
                                        llvm::Instruction *I_dominated);
};
