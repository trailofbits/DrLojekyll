// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <drlojekyll/Runtime/Client.h>

#include <chrono>
#include <grpcpp/grpcpp.h>
#include <grpcpp/support/async_stream.h>
#include <list>
#include <mutex>
#include <new>
#include <type_traits>

#include "Serialize.h"

namespace hyde {
namespace rt {

class ClientResultStreamImpl
    : public std::enable_shared_from_this<ClientResultStreamImpl> {
 public:
  ~ClientResultStreamImpl(void);

  ClientResultStreamImpl(
      std::shared_ptr<grpc::Channel> channel_,
      const grpc::internal::RpcMethod &method,
      const grpc::Slice &request);

  // Hold onto the connection to make sure we don't lose it.
  std::shared_ptr<grpc::Channel> channel;

  // Context for our RPC.
  grpc::ClientContext context;

  // Synchronous stream reader.
  std::mutex read_lock;
  std::unique_ptr<grpc::ClientReader<grpc::Slice>> reader;

  // Get the next thing.
  bool Next(std::shared_ptr<uint8_t> *out, size_t align, size_t min_size);
};

}  // namespace rt
}  // namespace hyde
