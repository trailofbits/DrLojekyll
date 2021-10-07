// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <cassert>
#include <memory>
#include <grpcpp/grpcpp.h>
#include <flatbuffers/flatbuffers.h>

namespace hyde {
namespace rt {

class ClientConnection;

template <typename T>
class ClientResult {
 private:
  friend class ClientConnection;

  template <typename>
  friend class ClientResultStreamIterator;

  grpc::Slice message;

  inline ClientResult(grpc::Slice message_)
      : message(std::move(message_)) {}

 public:
  ~ClientResult(void) = default;
  ClientResult(void) = default;
  ClientResult(const ClientResult<T> &) = default;
  ClientResult(ClientResult<T> &&) noexcept = default;
  ClientResult<T> &operator=(const ClientResult<T> &) = default;
  ClientResult &operator=(ClientResult<T> &&) noexcept = default;

  inline operator const T *(void) const noexcept {
    return message.size() ?
           flatbuffers::GetRoot<T>(message.begin()) :
           nullptr;
  }

  inline const T *operator->(void) const noexcept {
    return flatbuffers::GetRoot<T>(message.begin());
  }

  inline const T &operator*(void) const noexcept {
    return *flatbuffers::GetRoot<T>(message.begin());
  }
};

}  // namespace rt
}  // namespace hyde
