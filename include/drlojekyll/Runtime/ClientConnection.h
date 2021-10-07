// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <chrono>
#include <cstdint>
#include <flatbuffers/flatbuffers.h>
#include <grpcpp/grpcpp.h>
#include <string>
#include <memory>

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
  std::shared_ptr<uint8_t> Call(const grpc::internal::RpcMethod &method,
            const grpc::Slice &data,
            size_t min_size, size_t align) const;

  template <typename T>
  inline std::shared_ptr<T> CallResult(const grpc::internal::RpcMethod &method,
                                       const grpc::Slice &data) const {
    auto ret = Call(method, data, sizeof(T), alignof(T));
    std::shared_ptr<T> new_ret(ret, flatbuffers::GetMutableRoot<T>(ret.get()));
    return new_ret;
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
