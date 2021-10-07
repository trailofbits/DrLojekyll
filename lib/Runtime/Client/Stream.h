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

enum class RequestTag : uintptr_t { kStartCall, kRead, kFinish };

static const auto kStartCallTag =
    reinterpret_cast<void *>(RequestTag::kStartCall);
static const auto kReadTag =
    reinterpret_cast<void *>(RequestTag::kRead);
static const auto kFinishTag =
    reinterpret_cast<void *>(RequestTag::kFinish);

class ClientConnectionImpl;

class ClientResultStreamImpl
    : public std::enable_shared_from_this<ClientResultStreamImpl> {
 public:
  ~ClientResultStreamImpl(void);

  ClientResultStreamImpl(const std::shared_ptr<ClientConnectionImpl> &conn,
                          const grpc::internal::RpcMethod &method,
                          const grpc::Slice &request);

  void TearDown(bool shut_down=true);

  // If this stream is cached, then this points at the lock of the cache.
  // This is an aliasing shared pointer, whose refcount is the connection
  // itself.
  std::shared_ptr<std::mutex> cache_lock;

  // Links into the cache to unlink ourselves.
  ClientResultStreamImpl **prev_link{nullptr};
  ClientResultStreamImpl *next{nullptr};

  std::aligned_storage_t<sizeof(grpc::ClientContext),
                         alignof(grpc::ClientContext)> context_storage;
  grpc::ClientContext *context;
  std::aligned_storage_t<sizeof(grpc::CompletionQueue),
                         alignof(grpc::CompletionQueue)> completion_queue_storage;
  grpc::CompletionQueue *completion_queue;

  grpc::Slice pending_response;

  std::unique_ptr<grpc::ClientAsyncReader<grpc::Slice>> reader;
  bool sent_finished{false};
  bool is_finished{false};
  bool sent_shut_down{false};
  bool is_shut_down{false};

  grpc::Status status;
  std::list<grpc::Slice> queued_responses;

  // Unlink this stream from the cache.
  void Unlink(void);

  // Try to pull data in from this stream.
  bool Pump(std::chrono::system_clock::time_point deadline, bool *timed_out);

  // Get the next thing.
  bool Next(grpc::Slice *out_);
};

}  // namespace rt
}  // namespace hyde
