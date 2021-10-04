// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <drlojekyll/Runtime/Client/Stream.h>

#include <chrono>
#include <grpcpp/grpcpp.h>
#include <grpcpp/support/async_stream.h>
#include <list>
#include <mutex>

#include "Serialize.h"

namespace hyde {
namespace rt {

enum class RequestTag : uintptr_t { kStartCall, kRead, kFinish };

static const auto kStartCallTag =
    reinterpret_cast<void *>(RequestTag::kStartCall);
static const auto kReadTag =
    reinterpret_cast<void *>(RequestTag::kRead);
static const auto kFinishTag =
    reinterpret_cast<void *>(RequestTag::kFinish);

class BackendConnectionImpl;

class BackendResultStreamImpl
    : public std::enable_shared_from_this<BackendResultStreamImpl> {
 public:
  ~BackendResultStreamImpl(void);

  BackendResultStreamImpl(const std::shared_ptr<BackendConnectionImpl> &conn,
                          const grpc::internal::RpcMethod &method,
                          const grpc_slice &request);

  // If this stream is cached, then this points at the lock of the cache.
  // This is an aliasing shared pointer, whose refcount is the connection
  // itself.
  std::shared_ptr<std::mutex> cache_lock;

  // Links into the cache to unlink ourselves.
  BackendResultStreamImpl **prev_link{nullptr};
  BackendResultStreamImpl *next{nullptr};

  grpc::ClientContext context;
  grpc::CompletionQueue completion_queue;

  grpc_slice pending_response;

  std::unique_ptr<grpc::ClientAsyncReader<grpc_slice>> reader;
  bool sent_finished{false};
  bool is_finished{false};
  bool sent_shut_down{false};
  bool is_shut_down{false};

  grpc::Status status;
  std::list<grpc_slice> queued_responses;

  // Unlink this stream from the cache.
  void Unlink(void);

  // Try to pull data in from this stream.
  bool Pump(std::chrono::system_clock::time_point deadline, bool *timed_out);

  // Get the next thing.
  bool Next(grpc_slice *out_);
};

}  // namespace rt
}  // namespace hyde