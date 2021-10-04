// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <chrono>
#include <cstdint>
#include <grpcpp/grpcpp.h>
#include <string>
#include <memory>

#include "Result.h"

namespace hyde {
namespace rt {

class BackendConnectionImpl;

// Specialize this over a gRPC-auto-generated class.
class BackendConnection {
 private:
  template <typename>
  friend class BackendResultStream;

  BackendConnection(void) = delete;

 protected:

  std::shared_ptr<BackendConnectionImpl> impl;

  // Try to pull data in from active streams.
  void PumpActiveStreams(void) const;

  // Send data to the backend.
  bool Publish(const grpc::internal::RpcMethod &method,
               const grpc_slice &data) const;

  // Invoke an RPC that returns a single value.
  void Call(const grpc::internal::RpcMethod &method,
            const grpc_slice &data, grpc_slice &output_data) const;

  template <typename T>
  inline BackendResult<T> CallResult(const grpc::internal::RpcMethod &method,
                                     const grpc_slice &data) const {
    BackendResult<T> ret;
    Call(method, data, ret.message);
    return ret;
  }

 public:
  ~BackendConnection(void);

  BackendConnection(std::shared_ptr<grpc::Channel> channel_);
  BackendConnection(const BackendConnection &) = default;
  BackendConnection(BackendConnection &&) noexcept= default;

  BackendConnection &operator=(const BackendConnection &) = default;
  BackendConnection &operator=(BackendConnection &&) noexcept = default;
};

}  // namespace rt
}  // namespace hyde
