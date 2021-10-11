// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#include "Serialize.h"

namespace grpc {
namespace hyde {


Status SliceSerializer::Serialize(const Slice &msg,
                                  ByteBuffer *buffer,
                                  bool *own_buffer) {
  *buffer = ByteBuffer(&msg, 1);
  *own_buffer = true;
  return grpc::Status::OK;
}

Status SliceSerializer::Deserialize(ByteBuffer *buf, Slice *msg) {
  if (!buf->TrySingleSlice(msg).ok()) {
    if (!buf->DumpToSingleSlice(msg).ok()) {
      return ::grpc::Status(::grpc::StatusCode::INTERNAL, "No payload");
    }
  }
  return ::grpc::Status::OK;
}

}  // namespace hyde
}  // namespace grpc
