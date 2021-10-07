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
bool NextOpaque(ClientResultStreamImpl &impl, std::shared_ptr<uint8_t> &,
                size_t align, size_t min_size);

}  // namespace internal

class ClientResultStreamEndIterator {};

// Forward iterator that consumes things from the stream as it goes.
template <typename Response>
class ClientResultStreamIterator {
 private:
  std::shared_ptr<ClientResultStreamImpl> impl;
  std::shared_ptr<Response> message;

  ClientResultStreamIterator(
      const ClientResultStreamIterator<Response> &) = delete;
  ClientResultStreamIterator<Response> &operator=(
      const ClientResultStreamIterator<Response> &) = delete;

 public:

  // Implicit construction from an opaque stream.
  ClientResultStreamIterator(
      const std::shared_ptr<ClientResultStreamImpl> &impl_) {

    std::shared_ptr<uint8_t> data;
    if (internal::NextOpaque(*impl_, data, alignof(Response),
                             sizeof(Response))) {
      const auto message_ptr = flatbuffers::GetMutableRoot<Response>(data.get());
#if __cplusplus > 201703L
      std::shared_ptr<Response> ret(std:move(data), message_ptr);
#else
      std::shared_ptr<Response> ret(data, message_ptr);
#endif
      ret.swap(message);
      impl = impl_;
    }
  }

  inline const std::shared_ptr<Response> &operator*(void) const noexcept {
    return message;
  }

  inline ClientResultStreamIterator<Response> &operator++(void) noexcept {
    std::shared_ptr<uint8_t> data;
    if (internal::NextOpaque(*impl, data, alignof(Response),
                              sizeof(Response))) {
      const auto message_ptr = flatbuffers::GetMutableRoot<Response>(data.get());
#if __cplusplus > 201703L
      std::shared_ptr<Response> ret(std:move(data), message_ptr);
#else
      std::shared_ptr<Response> ret(data, message_ptr);
#endif
      ret.swap(message);
    } else {
      impl.reset();
      message.reset();
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
