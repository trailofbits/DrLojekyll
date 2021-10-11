// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <chrono>
#include <grpcpp/grpcpp.h>
#include <grpcpp/support/async_stream.h>
#include <list>
#include <mutex>
#include <new>
#include <type_traits>

#include <drlojekyll/Runtime/ClientResultStream.h>
#include "Serialize.h"

namespace hyde {
namespace rt {

enum class RequestTag : uintptr_t { kStartCall, kReadInitialMetadata, kRead, kFinish };

static const auto kStartCallTag =
    reinterpret_cast<void *>(RequestTag::kStartCall);
static const auto kReadInitialMetadata =
    reinterpret_cast<void *>(RequestTag::kReadInitialMetadata);
static const auto kReadTag =
    reinterpret_cast<void *>(RequestTag::kRead);
static const auto kFinishTag =
    reinterpret_cast<void *>(RequestTag::kFinish);

class ClientConnection;

class ClientResultStreamImpl
    : public std::enable_shared_from_this<ClientResultStreamImpl> {
 public:
  ~ClientResultStreamImpl(void);

  ClientResultStreamImpl(const ClientConnection &connection,
                         const grpc::internal::RpcMethod &method,
                         const grpc::Slice &request);

  // Hold onto the connection to make sure we don't lose it.
  std::shared_ptr<grpc::Channel> channel;

  // Context for our RPC.
  grpc::ClientContext context;

  // Synchronous stream reader.
  std::mutex read_lock;
  std::unique_ptr<grpc::ClientReader<grpc::Slice>> reader;

  // Get the next thing.
  bool Next(std::shared_ptr<uint8_t> *out, size_t align, size_t min_size);
};

}  // namespace rt
}  // namespace hyde
