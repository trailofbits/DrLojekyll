// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <cassert>
#include <memory>
#include <grpcpp/grpcpp.h>
#include <flatbuffers/flatbuffers.h>

namespace hyde {
namespace rt {

template <typename T>
class BackendResult {
 private:
  template <typename>
  friend class BackendResultStreamIterator;

  grpc_slice message;

  inline BackendResult(grpc_slice message_)
      : message(message_) {
    grpc_slice_ref(message);
  }

 public:
  inline ~BackendResult(void) {
    grpc_slice_unref(message);
  }

  BackendResult(void)
      : message(grpc_empty_slice()) {}

  BackendResult(BackendResult<T> &&that) noexcept
      : message(that.message) {
    that.message = grpc_empty_slice();
  }

  BackendResult(const BackendResult<T> &that)
      : message(that.message) {
    grpc_slice_ref(message);
  }

  inline BackendResult<T> &operator=(BackendResult<T> &&that) noexcept {
    message = std::move(that.message);
    that.message = grpc_empty_slice();
    return *this;
  }

  inline BackendResult<T> &operator=(const BackendResult<T> &that) {
    BackendResult<T> copy(that.message);
    return this->operator=(std::move(copy));
  }

  inline operator bool(void) const noexcept {
    return GRPC_SLICE_LENGTH(message);
  }

  inline operator T *(void) noexcept {
    return GRPC_SLICE_LENGTH(message) ?
           flatbuffers::GetMutableRoot<T>(GRPC_SLICE_START_PTR(message)) :
           nullptr;
  }

  inline operator const T *(void) noexcept {
    return GRPC_SLICE_LENGTH(message) ?
           flatbuffers::GetRoot<T>(GRPC_SLICE_START_PTR(message)) :
           nullptr;
  }

  inline T *operator->(void) noexcept {
    return flatbuffers::GetMutableRoot<T>(GRPC_SLICE_START_PTR(message));
  }

  inline const T *operator->(void) const noexcept {
    return flatbuffers::GetRoot<T>(GRPC_SLICE_START_PTR(message));
  }

  inline T &operator*(void) noexcept {
    return *flatbuffers::GetMutableRoot<T>(GRPC_SLICE_START_PTR(message));
  }

  inline const T &operator*(void) const noexcept {
    return *flatbuffers::GetRoot<T>(GRPC_SLICE_START_PTR(message));
  }
};

}  // namespace rt
}  // namespace hyde
