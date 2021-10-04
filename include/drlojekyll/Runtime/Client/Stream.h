// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <cassert>
#include <memory>
#include <grpcpp/grpcpp.h>
#include <flatbuffers/flatbuffers.h>

namespace hyde {
namespace rt {

class BackendConnectionImpl;
class BackendResultStreamImpl;

namespace internal {

// Create a request.
std::shared_ptr<BackendResultStreamImpl> RequestStream(
    const std::shared_ptr<BackendConnectionImpl> &conn,
    const grpc::internal::RpcMethod &method,
    grpc_slice request);

// Try to get the next value from the request.
bool NextOpaque(BackendResultStreamImpl &impl, const grpc_slice &);

}  // namespace internal

class BackendResultStreamEndIterator {};

// Forward iterator that consumes things from the stream as it goes.
template <typename Response>
class BackendResultStreamIterator {
 private:
  std::shared_ptr<BackendResultStreamImpl> impl;
  grpc_slice message;

  BackendResultStreamIterator(
      const BackendResultStreamIterator<Response> &) = delete;
  BackendResultStreamIterator<Response> &operator=(
      const BackendResultStreamIterator<Response> &) = delete;

 public:

  BackendResultStreamIterator(void)
      : message(grpc_empty_slice()) {}

  BackendResultStreamIterator(
      BackendResultStreamIterator<Response> &&that) noexcept
      : impl(std::move(that.impl)),
        message(std::move(that.message)) {
    that.message = grpc_empty_slice();
  }

  BackendResultStreamIterator<Response> &operator=(
      const BackendResultStreamIterator<Response> &&that) noexcept {

    impl = std::move(that.impl);
    message = std::move(that.message);
    that.message = grpc_empty_slice();
    return *this;
  }

  ~BackendResultStreamIterator(void) {
    grpc_slice_unref(message);
    message = grpc_empty_slice();
  }

  // Implicit construction from an opaque stream.
  BackendResultStreamIterator(
      const std::shared_ptr<BackendResultStreamImpl> &impl_)
    : message(grpc_empty_slice()) {
    if (internal::NextOpaque(*impl_, message)) {
      assert(flatbuffers::Verifier(
          GRPC_SLICE_START_PTR(message),
          GRPC_SLICE_LENGTH(message)).VerifyBuffer<Response>(nullptr));
      impl = impl_;
    }
  }

  inline const Response &operator*(void) const noexcept {
    return *(flatbuffers::GetRoot<Response>(
        GRPC_SLICE_START_PTR(message)));
  }

  inline const Response *operator->(void) const noexcept {
    return flatbuffers::GetRoot<Response>(
        GRPC_SLICE_START_PTR(message));
  }

  inline BackendResultStreamIterator<Response> &operator++(void) noexcept {
    if (!internal::NextOpaque(*impl, message)) {
      impl.reset();
    } else {
      assert(flatbuffers::Verifier(
          GRPC_SLICE_START_PTR(message),
          GRPC_SLICE_LENGTH(message)).VerifyBuffer<Response>(nullptr));
    }
    return *this;
  }

  inline bool operator==(BackendResultStreamEndIterator that) const noexcept {
    return !impl;
  }

  inline bool operator!=(BackendResultStreamEndIterator that) const noexcept {
    return !!impl;
  }
};

// An typed interface to an asynchronous gRPC stream.
template <typename Response>
class BackendResultStream {
 private:
  std::shared_ptr<BackendResultStreamImpl> impl;

 public:
  inline explicit BackendResultStream(
      const std::shared_ptr<BackendConnectionImpl> &conn,
      const grpc::internal::RpcMethod &method,
      const grpc_slice &request)
      : impl(internal::RequestStream(conn, method, request)) {}

  inline BackendResultStreamIterator<Response> begin(void) const noexcept {
    return impl;
  }

  inline BackendResultStreamEndIterator end(void) const noexcept {
    return {};
  }
};

}  // namespace rt
}  // namespace hyde
