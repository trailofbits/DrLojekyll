// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <grpcpp/grpcpp.h>

namespace grpc {
namespace hyde {

class SliceSerializer {
 public:
  static Status Serialize(const grpc_slice &msg,
                          grpc_byte_buffer **buffer,
                          bool *own_buffer);

  static Status Deserialize(ByteBuffer *buf, grpc_slice *msg);
};

}  // namespace hyde

template<>
class SerializationTraits<grpc_slice, void>
    : public ::grpc::hyde::SliceSerializer {};

}  // namespace grpc
