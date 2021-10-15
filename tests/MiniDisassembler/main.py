
import flatbuffers
import grpc
import database
import time

from database.EdgeType import EdgeType



def dump(db: database.Datalog, updateGen):  
  # block until convervge
  for buff in updateGen:
    print(buff)
    break
  
  for func_ea in range(0,50):
    for insn in db.function_instructions_bf(func_ea):
      print("FuncEA="+ str(insn.FuncEA()) + " " + "InstEA=" + str(insn.InstEA()))

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
  updateGen = db.consume()
  db.produce(builder)
 # db.produce(builder)


  dump(db,updateGen)
  assert len(set(db.function_instructions_bf(9))) == 0
  assert len(set(db.function_instructions_bf(10))) == 1
  assert len(set(db.function_instructions_bf(11))) == 1
  assert len(set(db.function_instructions_bf(12))) == 1
  assert len(set(db.function_instructions_bf(13))) == 1
  assert len(set(db.function_instructions_bf(14))) == 1
  assert len(set(db.function_instructions_bf(15))) == 1

  # # Now we add the fall-through edges, and 10 is the only instruction with
  # # no predecessor, so its the function head.

  builder = database.InputMessageBuilder()
  builder.produce_raw_transfer_3(10, 11, EdgeType.FALL_THROUGH)
  builder.produce_raw_transfer_3(11, 12, EdgeType.FALL_THROUGH)
  builder.produce_raw_transfer_3(12, 13, EdgeType.FALL_THROUGH)
  builder.produce_raw_transfer_3(13, 14, EdgeType.FALL_THROUGH)
  builder.produce_raw_transfer_3(14, 15, EdgeType.FALL_THROUGH)

  db.produce(builder)
  dump(db, updateGen)


  assert len(set(db.function_instructions_bf(9))) == 0
  assert len(set(db.function_instructions_bf(10))) == 6
  assert len(set(db.function_instructions_bf(11))) == 0
  assert len(set(db.function_instructions_bf(12))) == 0
  assert len(set(db.function_instructions_bf(13))) == 0
  assert len(set(db.function_instructions_bf(14))) == 0
  assert len(set(db.function_instructions_bf(15))) == 0


  builder = database.InputMessageBuilder()  
  builder.produce_instruction_1(9)
  db.produce(builder)
  dump(db, updateGen)

  assert len(set(db.function_instructions_bf(9))) == 1
  assert len(set(db.function_instructions_bf(10))) == 6
  assert len(set(db.function_instructions_bf(11))) == 0
  assert len(set(db.function_instructions_bf(12))) == 0
  assert len(set(db.function_instructions_bf(13))) == 0
  assert len(set(db.function_instructions_bf(14))) == 0
  assert len(set(db.function_instructions_bf(15))) == 0


  builder = database.InputMessageBuilder()
  builder.produce_raw_transfer_3(9,10, EdgeType.FALL_THROUGH)
  db.produce(builder)

  dump(db, updateGen)

  assert len(set(db.function_instructions_bf(9))) == 7
  assert len(set(db.function_instructions_bf(10))) == 0
  assert len(set(db.function_instructions_bf(11))) == 0
  assert len(set(db.function_instructions_bf(12))) == 0
  assert len(set(db.function_instructions_bf(13))) == 0
  assert len(set(db.function_instructions_bf(14))) == 0
  assert len(set(db.function_instructions_bf(15))) == 0
  
  builder = database.InputMessageBuilder()
  builder.produce_raw_transfer_3(10,14, EdgeType.CALL)
  db.produce(builder)

  dump(db, updateGen)

  assert len(set(db.function_instructions_bf(9))) == 5
  assert len(set(db.function_instructions_bf(10))) == 0
  assert len(set(db.function_instructions_bf(11))) == 0
  assert len(set(db.function_instructions_bf(12))) == 0
  assert len(set(db.function_instructions_bf(13))) == 0
  assert len(set(db.function_instructions_bf(14))) == 2
  assert len(set(db.function_instructions_bf(15))) == 0