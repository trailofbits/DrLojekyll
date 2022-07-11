// Copyright 2022, Trail of Bits. All rights reserved.

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wunused-variable"
#include <cstdlib>
#include <drlojekyll/Runtime/StdRuntime.h>
#include <unordered_map>

#include <llvm/IRReader/IRReader.h>
#include <llvm/Support/SourceMgr.h>
#include <llvm/Support/Error.h>

#include <gap/core/coroutine.hpp>
#include <gap/core/generator.hpp>
#include "llvm.db.h"  // Auto-generated.
#pragma GCC diagnostic pop

#include "Interface.h"
#include "Report.h"

using DatabaseStorage = hyde::rt::StdStorage;
using DatabaseFunctors = LLVMInterface;
using DatabaseLog = LLVMReport;
using Database = llvm::Database<DatabaseStorage, DatabaseLog, DatabaseFunctors>;

template <typename... Args>
using Vector = hyde::rt::Vector<DatabaseStorage, Args...>;

int main(int argc, char *argv[]) {
  DatabaseFunctors functors;
  DatabaseLog log;
  DatabaseStorage storage;
  Database db(storage, log, functors);

  Vector<std::filesystem::path> files(storage, 0);

  for (auto i = 1; i < argc; ++i) {
    std::filesystem::path p(argv[i]);
    files.Add(std::move(p));
  }

  db.add_from_from_file_1(std::move(files));

  return EXIT_SUCCESS;
}
