// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <mutex>

#include <drlojekyll/Runtime/ClientConnection.h>

namespace hyde {
namespace rt {

class ClientResultStreamImpl;

class ClientConnectionImpl
    : public std::enable_shared_from_this<ClientConnectionImpl> {
 private:
  ClientConnectionImpl(void) = delete;
  ClientConnectionImpl(const ClientConnectionImpl &) = delete;
  ClientConnectionImpl(ClientConnectionImpl &&) noexcept = delete;

 public:
  const std::shared_ptr<grpc::Channel> channel;

  // Cache of active recent streams.
  std::mutex pending_streams_lock;

  // Linked list of asynchronous streams that we can pump for results.
  ClientResultStreamImpl *pending_streams{nullptr};

  inline ClientConnectionImpl(std::shared_ptr<grpc::Channel> channel_)
      : channel(std::move(channel_)) {}
};

}  // namespace rt
}  // namespace hyde
