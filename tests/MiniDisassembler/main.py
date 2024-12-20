
import flatbuffers
import grpc
import database
import time

if __name__ == "__main__":
  channel = grpc.insecure_channel('localhost:50051')

  builder = database.InputMessageBuilder()
  builder.produce_instruction_1(10)
  builder.produce_instruction_1(11)
  builder.produce_instruction_1(12)
  builder.produce_instruction_1(13)
  builder.produce_instruction_1(14)
  builder.produce_instruction_1(15)

  db = database.Datalog("test", channel)
  db.produce(builder)

  time.sleep(1)
  for func in db.function_instructions_bf(10):
    print(func)


  # db = Database(DatabaseLog(), DatabaseFunctors())

  # # Start with a few instructions, with no control-flow between them.
  # db.instruction_1([10, 11, 12, 13, 14, 15])
  # dump(db)


  # # At first, every instruction looks like a function head, because we have no
  # # transfers. Note: instruction 9 doesn't exist yet.

  # assert len(set(db.function_instructions_bf(9))) == 0
  # assert len(set(db.function_instructions_bf(10))) == 1
  # assert len(set(db.function_instructions_bf(11))) == 1
  # assert len(set(db.function_instructions_bf(12))) == 1
  # assert len(set(db.function_instructions_bf(13))) == 1
  # assert len(set(db.function_instructions_bf(14))) == 1
  # assert len(set(db.function_instructions_bf(15))) == 1



  # # Now we add the fall-through edges, and 10 is the only instruction with
  # # no predecessor, so its the function head.


  # db.raw_transfer_3([(10, 11, FALL_THROUGH),
  #                    (11, 12, FALL_THROUGH),
  #                    (12, 13, FALL_THROUGH),
  #                    (13, 14, FALL_THROUGH),
  #                    (14, 15, FALL_THROUGH)])
  # dump(db)


  # assert len(set(db.function_instructions_bf(9))) == 0
  # assert len(set(db.function_instructions_bf(10))) == 6
  # assert len(set(db.function_instructions_bf(11))) == 0
  # assert len(set(db.function_instructions_bf(12))) == 0
  # assert len(set(db.function_instructions_bf(13))) == 0
  # assert len(set(db.function_instructions_bf(14))) == 0
  # assert len(set(db.function_instructions_bf(15))) == 0


  # db.instruction_1([9])
  # dump(db)

  # assert len(set(db.function_instructions_bf(9))) == 1
  # assert len(set(db.function_instructions_bf(10))) == 6
  # assert len(set(db.function_instructions_bf(11))) == 0
  # assert len(set(db.function_instructions_bf(12))) == 0
  # assert len(set(db.function_instructions_bf(13))) == 0
  # assert len(set(db.function_instructions_bf(14))) == 0
  # assert len(set(db.function_instructions_bf(15))) == 0


  # db.raw_transfer_3([(9, 10, FALL_THROUGH)])
  # dump(db)

  # assert len(set(db.function_instructions_bf(9))) == 7
  # assert len(set(db.function_instructions_bf(10))) == 0
  # assert len(set(db.function_instructions_bf(11))) == 0
  # assert len(set(db.function_instructions_bf(12))) == 0
  # assert len(set(db.function_instructions_bf(13))) == 0
  # assert len(set(db.function_instructions_bf(14))) == 0
  # assert len(set(db.function_instructions_bf(15))) == 0


  # db.raw_transfer_3([(10, 14, CALL)])
  # dump(db)

  # assert len(set(db.function_instructions_bf(9))) == 5
  # assert len(set(db.function_instructions_bf(10))) == 0
  # assert len(set(db.function_instructions_bf(11))) == 0
  # assert len(set(db.function_instructions_bf(12))) == 0
  # assert len(set(db.function_instructions_bf(13))) == 0
  # assert len(set(db.function_instructions_bf(14))) == 2
  # assert len(set(db.function_instructions_bf(15))) == 0