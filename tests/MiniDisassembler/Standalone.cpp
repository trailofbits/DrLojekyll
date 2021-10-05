// Copyright 2021, Trail of Bits. All rights reserved.

#include <gtest/gtest.h>

#include <drlojekyll/Runtime/Server/Std/Runtime.h>
#include <algorithm>
#include <vector>
#include <iomanip>
#include <iostream>
#include <set>
#include <cstdio>
#include <cinttypes>

#include "database.db.h"  // Auto-generated.

template <typename DB>
void dump(DB &db) {
  std::cout << "Dump:\n";

  for (auto func_ea = 0; func_ea < 50; func_ea++) {
    db.function_instructions_bf(func_ea, [] (uint64_t func_ea_, uint64_t inst_ea) {
      std::cout << "  FuncEA=" << func_ea_ << " InstEA=" << inst_ea << "\n";
      return true;
    });
  }

  std::cout << "\n";
}

template <typename DB>
size_t NumFunctionInstructions(DB &db, uint64_t func_ea) {
  std::vector<uint64_t> eas;
  db.function_instructions_bf(func_ea, [&eas] (uint64_t, uint64_t inst_ea) {
    eas.push_back(inst_ea);
    return true;
  });
  std::sort(eas.begin(), eas.end());
  auto it = std::unique(eas.begin(), eas.end());
  eas.erase(it, eas.end());
  return eas.size();
}

using DatabaseStorage = hyde::rt::StdStorage;
using DatabaseFunctors = database::DatabaseFunctors;
using DatabaseLog = database::DatabaseLog;
using Database = database::Database<DatabaseStorage, DatabaseLog, DatabaseFunctors>;

template <typename... Args>
using Vector = hyde::rt::Vector<DatabaseStorage, Args...>;

// A simple Google Test example
TEST(MiniDisassembler, DifferentialUpdatesWork) {

  DatabaseFunctors functors;
  DatabaseLog log;
  DatabaseStorage storage;
  Database db(storage, log, functors);

  // Start with a few instructions, with no control-flow between them.
  Vector<uint64_t> instructions(storage, 0);
  instructions.Add(10);
  instructions.Add(11);
  instructions.Add(12);
  instructions.Add(13);
  instructions.Add(14);
  instructions.Add(15);
  db.instruction_1(std::move(instructions));

  dump(db);
  ASSERT_EQ(NumFunctionInstructions(db, 9), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 10), 1u);
  ASSERT_EQ(NumFunctionInstructions(db, 11), 1u);
  ASSERT_EQ(NumFunctionInstructions(db, 12), 1u);
  ASSERT_EQ(NumFunctionInstructions(db, 13), 1u);
  ASSERT_EQ(NumFunctionInstructions(db, 14), 1u);
  ASSERT_EQ(NumFunctionInstructions(db, 15), 1u);

  // Now we add the fall-through edges, and 10 is the only instruction with
  // no predecessor, so its the function head.
  Vector<uint64_t, uint64_t, database::EdgeType> transfers(storage, 0);

  transfers.Add(10, 11, database::EdgeType::FALL_THROUGH);
  transfers.Add(11, 12, database::EdgeType::FALL_THROUGH);
  transfers.Add(12, 13, database::EdgeType::FALL_THROUGH);
  transfers.Add(13, 14, database::EdgeType::FALL_THROUGH);
  transfers.Add(14, 15, database::EdgeType::FALL_THROUGH);
  db.raw_transfer_3(std::move(transfers));

  dump(db);
  ASSERT_EQ(NumFunctionInstructions(db, 9), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 10), 6u);
  ASSERT_EQ(NumFunctionInstructions(db, 11), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 12), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 13), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 14), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 15), 0u);

  // Now add the instruction 9. It will show up as a function head, because
  // it has no predecessors. The rest will stay the same because there is
  // no changes to control-flow.
  Vector<uint64_t> instructions2(storage, 0);
  instructions2.Add(9);
  db.instruction_1(std::move(instructions2));

  dump(db);
  ASSERT_EQ(NumFunctionInstructions(db, 9), 1u);
  ASSERT_EQ(NumFunctionInstructions(db, 10), 6u);
  ASSERT_EQ(NumFunctionInstructions(db, 11), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 12), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 13), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 14), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 15), 0u);

  // Now add a fall-through between 9 and 10. 10 now has a successor, so it's
  // not a function head anymore, so all of the function instructions transfer
  // over to function 9.
  Vector<uint64_t, uint64_t, database::EdgeType> transfers2(storage, 0);
  transfers2.Add(9, 10, database::EdgeType::FALL_THROUGH);
  db.raw_transfer_3(std::move(transfers2));

  dump(db);
  ASSERT_EQ(NumFunctionInstructions(db, 9), 7u);
  ASSERT_EQ(NumFunctionInstructions(db, 10), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 11), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 12), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 13), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 14), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 15), 0u);

  // Now add a function call between 10 and 14. That makes 14 look like
  // a function head, and so now that 14 is a function head, it's no longer
  // part of function 9.
  Vector<uint64_t, uint64_t, database::EdgeType> transfers3(storage, 0);
  transfers3.Add(10, 14, database::EdgeType::CALL);
  db.raw_transfer_3(std::move(transfers3));

  dump(db);
  ASSERT_EQ(NumFunctionInstructions(db, 9), 5u);
  ASSERT_EQ(NumFunctionInstructions(db, 10), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 11), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 12), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 13), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 14), 2u);
  ASSERT_EQ(NumFunctionInstructions(db, 15), 0u);
}

