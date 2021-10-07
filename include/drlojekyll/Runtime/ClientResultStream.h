// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <cassert>
#include <memory>
#include <grpcpp/grpcpp.h>
#include <flatbuffers/flatbuffers.h>

namespace hyde {
namespace rt {

class ClientConnectionImpl;
class ClientResultStreamImpl;

namespace internal {

// Create a request.
std::shared_ptr<ClientResultStreamImpl> RequestStream(
    const std::shared_ptr<ClientConnectionImpl> &conn,
    const grpc::internal::RpcMethod &method,
    const grpc::Slice &request);

// Try to get the next value from the request.
bool NextOpaque(ClientResultStreamImpl &impl,
                const std::shared_ptr<uint8_t> &,
                size_t align, size_t min_size);

}  // namespace internal

class ClientResultStreamEndIterator {};

// Forward iterator that consumes things from the stream as it goes.
template <typename Response>
class ClientResultStreamIterator {
 private:
  std::shared_ptr<ClientResultStreamImpl> impl;
  std::shared_ptr<uint8_t> message;

  ClientResultStreamIterator(
      const ClientResultStreamIterator<Response> &) = delete;
  ClientResultStreamIterator<Response> &operator=(
      const ClientResultStreamIterator<Response> &) = delete;

 public:

  // Implicit construction from an opaque stream.
  ClientResultStreamIterator(
      const std::shared_ptr<ClientResultStreamImpl> &impl_) {
    if (internal::NextOpaque(*impl_, message, alignof(Response),
                             sizeof(Response))) {
      impl = impl_;
    }
  }

  inline std::shared_ptr<Response> operator*(void) const noexcept {
    std::shared_ptr<Response> ret(
        message, flatbuffers::GetMutableRoot<Response>(message.get()));
    return ret;
  }

  inline ClientResultStreamIterator<Response> &operator++(void) noexcept {
    if (!internal::NextOpaque(*impl, message, alignof(Response),
                              sizeof(Response))) {
      impl.reset();
    }
    return *this;
  }

  inline bool operator==(ClientResultStreamEndIterator that) const noexcept {
    return !impl;
  }

  inline bool operator!=(ClientResultStreamEndIterator that) const noexcept {
    return !!impl;
  }
};

// An typed interface to an asynchronous gRPC stream.
template <typename Response>
class ClientResultStream {
 private:
  std::shared_ptr<ClientResultStreamImpl> impl;

 public:
  inline explicit ClientResultStream(
      const std::shared_ptr<ClientConnectionImpl> &conn,
      const grpc::internal::RpcMethod &method,
      const grpc::Slice &request)
      : impl(internal::RequestStream(conn, method, request)) {}

  inline ClientResultStreamIterator<Response> begin(void) const noexcept {
    return impl;
  }

  inline ClientResultStreamEndIterator end(void) const noexcept {
    return {};
  }
};

}  // namespace rt
}  // namespace hyde
