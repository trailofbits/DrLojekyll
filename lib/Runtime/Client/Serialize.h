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


  static Status Serialize(const Slice &msg,
                          grpc_byte_buffer **buffer,
                          bool *own_buffer);

  static Status Deserialize(ByteBuffer *buf, Slice *msg);
};

}  // namespace hyde

template<>
class SerializationTraits<grpc_slice, void>
    : public ::grpc::hyde::SliceSerializer {};

template<>
class SerializationTraits<Slice, void>
    : public ::grpc::hyde::SliceSerializer {};

}  // namespace grpc
