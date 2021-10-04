// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <drlojekyll/Runtime/Client/Connection.h>

#include <mutex>

namespace hyde {
namespace rt {

class BackendResultStreamImpl;

class BackendConnectionImpl : public std::enable_shared_from_this<BackendConnectionImpl> {
 private:
  BackendConnectionImpl(void) = delete;
  BackendConnectionImpl(const BackendConnectionImpl &) = delete;
  BackendConnectionImpl(BackendConnectionImpl &&) noexcept = delete;

 public:
  const std::shared_ptr<grpc::Channel> channel;

  // Cache of active recent streams.
  std::mutex pending_streams_lock;

  // Linked list of asynchronous streams that we can pump for results.
  BackendResultStreamImpl *pending_streams{nullptr};

  inline BackendConnectionImpl(std::shared_ptr<grpc::Channel> channel_)
      : channel(std::move(channel_)) {}
};

}  // namespace rt
}  // namespace hyde
