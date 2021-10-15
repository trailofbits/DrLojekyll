// Copyright 2021, Trail of Bits. All rights reserved.

#include <gtest/gtest.h>

#include "database.client.h"  // Auto-generated.

template <typename DB>
void dump(
    DB &db,
    ::hyde::rt::ClientResultStream<database::DatalogClientMessage> &updates,
    bool wait = true) {
  if (wait) {

    // Pluck one update off of what the server published.
    std::cerr << "Awaiting convergence\n";
    for (auto res : updates) {
      std::cerr << "Converged\n\n";
      break;
    }
  }

  std::cout << "Dump:\n";

  for (auto func_ea = 0; func_ea < 50; func_ea++) {
    for (auto inst : db.function_instructions_bf(func_ea)) {
      std::cout << "  FuncEA=" << inst->FuncEA() << " InstEA=" << inst->InstEA()
                << "\n";
    }
  }

  std::cout << "\n";
}

template <typename DB>
size_t NumFunctionInstructions(DB &db, uint64_t func_ea) {
  std::vector<uint64_t> eas;
  for (auto inst : db.function_instructions_bf(func_ea)) {
    eas.push_back(inst->InstEA());
  }

  std::sort(eas.begin(), eas.end());
  auto it = std::unique(eas.begin(), eas.end());
  eas.erase(it, eas.end());
  return eas.size();
}

// A simple Google Test example
TEST(MiniDisassembler, ServerConnectionWorks) {

  grpc::ChannelArguments args;
  args.SetMaxSendMessageSize(std::numeric_limits<int>::max());
  args.SetMaxReceiveMessageSize(std::numeric_limits<int>::max());


  auto send_channel = grpc::CreateCustomChannel(
      "localhost:50051", grpc::InsecureChannelCredentials(), args);

  auto recv_channel = grpc::CreateCustomChannel(
      "localhost:50051", grpc::InsecureChannelCredentials(), args);

  auto query_channel = grpc::CreateCustomChannel(
      "localhost:50051", grpc::InsecureChannelCredentials(), args);


  // Make two separate channels; we expect very different access patterns for
  // incoming updates and for general usage.

  ASSERT_NE(recv_channel.get(), send_channel.get());

  database::DatalogClient db(std::move(send_channel), std::move(recv_channel),
                             std::move(query_channel));

  auto updates = db.Subscribe("MiniDisassemblerTest");

  database::DatalogMessageBuilder builder;

  // Start with a few instructions, with no control-flow between them.
  builder.instruction_1(10);
  builder.instruction_1(11);
  builder.instruction_1(12);
  builder.instruction_1(13);
  builder.instruction_1(14);
  builder.instruction_1(15);
  db.Publish(builder);

  dump(db, updates);
  assert(!db.function_b(9));
  assert(db.function_b(10));
  assert(db.function_b(11));
  assert(db.function_b(12));
  assert(db.function_b(13));
  assert(db.function_b(14));
  assert(db.function_b(15));
  ASSERT_EQ(NumFunctionInstructions(db, 9), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 10), 1u);
  ASSERT_EQ(NumFunctionInstructions(db, 11), 1u);
  ASSERT_EQ(NumFunctionInstructions(db, 12), 1u);
  ASSERT_EQ(NumFunctionInstructions(db, 13), 1u);
  ASSERT_EQ(NumFunctionInstructions(db, 14), 1u);
  ASSERT_EQ(NumFunctionInstructions(db, 15), 1u);

  // Now we add the fall-through edges, and 10 is the only instruction with
  // no predecessor, so its the function head.
  builder.raw_transfer_3(10, 11, database::EdgeType::FALL_THROUGH);
  builder.raw_transfer_3(11, 12, database::EdgeType::FALL_THROUGH);
  builder.raw_transfer_3(12, 13, database::EdgeType::FALL_THROUGH);
  builder.raw_transfer_3(13, 14, database::EdgeType::FALL_THROUGH);
  builder.raw_transfer_3(14, 15, database::EdgeType::FALL_THROUGH);
  db.Publish(builder);

  dump(db, updates);
  assert(!db.function_b(9));
  assert(db.function_b(10));
  assert(!db.function_b(11));
  assert(!db.function_b(12));
  assert(!db.function_b(13));
  assert(!db.function_b(14));
  assert(!db.function_b(15));
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
  builder.instruction_1(9);
  db.Publish(builder);

  dump(db, updates);
  assert(db.function_b(9));
  assert(db.function_b(10));
  assert(!db.function_b(11));
  assert(!db.function_b(12));
  assert(!db.function_b(13));
  assert(!db.function_b(14));
  assert(!db.function_b(15));
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
  builder.raw_transfer_3(9, 10, database::EdgeType::FALL_THROUGH);
  db.Publish(builder);

  dump(db, updates);
  assert(db.function_b(9));
  assert(!db.function_b(10));
  assert(!db.function_b(11));
  assert(!db.function_b(12));
  assert(!db.function_b(13));
  assert(!db.function_b(14));
  assert(!db.function_b(15));
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
  builder.raw_transfer_3(10, 14, database::EdgeType::CALL);
  db.Publish(builder);

  dump(db, updates);
  assert(db.function_b(9));
  assert(!db.function_b(10));
  assert(!db.function_b(11));
  assert(!db.function_b(12));
  assert(!db.function_b(13));
  assert(db.function_b(14));
  assert(!db.function_b(15));
  ASSERT_EQ(NumFunctionInstructions(db, 9), 5u);
  ASSERT_EQ(NumFunctionInstructions(db, 10), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 11), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 12), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 13), 0u);
  ASSERT_EQ(NumFunctionInstructions(db, 14), 2u);
  ASSERT_EQ(NumFunctionInstructions(db, 15), 0u);
}
