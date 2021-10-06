// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <cassert>
#include <memory>
#include <grpcpp/grpcpp.h>
#include <flatbuffers/flatbuffers.h>

namespace hyde {
namespace rt {

class BackendConnection;

template <typename T>
class BackendResult {
 private:
  friend class BackendConnection;

  template <typename>
  friend class BackendResultStreamIterator;

  grpc::Slice message;

  inline BackendResult(grpc::Slice message_)
      : message(std::move(message_)) {}

 public:
  ~BackendResult(void) = default;
  BackendResult(void) = default;
  BackendResult(const BackendResult<T> &) = default;
  BackendResult(BackendResult<T> &&) noexcept = default;
  BackendResult<T> &operator=(const BackendResult<T> &) = default;
  BackendResult &operator=(BackendResult<T> &&) noexcept = default;

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
