// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <grpcpp/grpcpp.h>

namespace grpc {
namespace hyde {

class SliceSerializer {
 public:
  static Status Serialize(const Slice &msg,
                          ByteBuffer *buf,
                          bool *own_buffer);

  static Status Deserialize(ByteBuffer *buf, Slice *msg);
};

}  // namespace hyde

template<>
class SerializationTraits<Slice, void>
    : public ::grpc::hyde::SliceSerializer {};

}  // namespace grpc
