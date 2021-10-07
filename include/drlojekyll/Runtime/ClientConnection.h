// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <chrono>
#include <cstdint>
#include <grpcpp/grpcpp.h>
#include <string>
#include <memory>

#include "ClientResult.h"

namespace hyde {
namespace rt {

class ClientConnectionImpl;

// Specialize this over a gRPC-auto-generated class.
class ClientConnection {
 private:
  template <typename>
  friend class ClientResultStream;

  ClientConnection(void) = delete;

 protected:

  std::shared_ptr<ClientConnectionImpl> impl;

  // Try to pull data in from active streams.
  void PumpActiveStreams(void) const;

  // Send data to the backend.
  bool Publish(const grpc::internal::RpcMethod &method,
               const grpc::Slice &data) const;

  // Invoke an RPC that returns a single value.
  void Call(const grpc::internal::RpcMethod &method,
            const grpc::Slice &data,
            grpc::Slice &output_data) const;

  template <typename T>
  inline ClientResult<T> CallResult(const grpc::internal::RpcMethod &method,
                                     const grpc::Slice &data) const {
    ClientResult<T> ret;
    Call(method, data, ret.message);
    return ret;
  }

 public:
  ~ClientConnection(void);

  ClientConnection(std::shared_ptr<grpc::Channel> channel_);
  ClientConnection(const ClientConnection &) = default;
  ClientConnection(ClientConnection &&) noexcept= default;

  ClientConnection &operator=(const ClientConnection &) = default;
  ClientConnection &operator=(ClientConnection &&) noexcept = default;
};

}  // namespace rt
}  // namespace hyde
