// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#include "Stream.h"

#include <atomic>
#include <grpcpp/impl/codegen/async_stream.h>
#include <grpcpp/impl/codegen/status.h>
#include <grpcpp/impl/codegen/status_code_enum.h>

#include "Connection.h"

//#include <iostream>

namespace hyde {
namespace rt {

ClientResultStreamImpl::ClientResultStreamImpl(
    const std::shared_ptr<ClientConnectionImpl> &conn,
    const grpc::internal::RpcMethod &method,
    const grpc::Slice &request)
    : cache_lock(conn, &(conn->pending_streams_lock)),
      context(new (&context_storage) grpc::ClientContext),
      completion_queue(new (&completion_queue_storage) grpc::CompletionQueue),
      reader(grpc::internal::ClientAsyncReaderFactory<grpc::Slice>::Create<grpc::Slice>(
          conn->channel.get(),
          completion_queue,
          method,
          context,
          request,
          true,
          reinterpret_cast<void *>(RequestTag::kStartCall))) {

  // std::cerr << "A " << reinterpret_cast<const void *>(this) << " making stream\n";

  // Link it into the cache.
  std::unique_lock<std::mutex> locker(conn->pending_streams_lock);
  if (conn->pending_streams) {
    conn->pending_streams->prev_link = &next;
    next = conn->pending_streams;
  }

  prev_link = &(conn->pending_streams);
  conn->pending_streams = this;
}

ClientResultStreamImpl::~ClientResultStreamImpl(void) {
  if (!is_finished && !sent_finished && reader) {
    sent_finished = true;
    reader->Finish(&status, kFinishTag);
    // std::cerr << "B " << reinterpret_cast<const void *>(this) << " finishing stream\n";
  }

  TearDown();
}

// Unlink this stream from the cache.
void ClientResultStreamImpl::Unlink(void) {
  if (prev_link) {
    {
      std::unique_lock<std::mutex> locker(*cache_lock);
      if (next) {
        next->prev_link = prev_link;
      }
      *prev_link = next;
      cache_lock.reset();
    }

    prev_link = nullptr;
    next = nullptr;
  }
}

void ClientResultStreamImpl::TearDown(bool shut_down) {
  Unlink();
  if (completion_queue) {
    if (shut_down) {
      sent_shut_down = true;
      completion_queue->Shutdown();
    }

    is_finished = true;
    completion_queue->~CompletionQueue();
    completion_queue = nullptr;
  }

  reader.reset();
}

bool ClientResultStreamImpl::Pump(
    std::chrono::system_clock::time_point deadline,
    bool *timed_out) {

  if (is_shut_down || !reader) {
    return false;
  }

  void *tag = nullptr;
  bool succeeded = false;

  switch (completion_queue->AsyncNext(&tag, &succeeded, deadline)) {

    // Shutting down this stream.
    case grpc::CompletionQueue::SHUTDOWN:
      assert(sent_shut_down);
      TearDown(false);
      is_finished = true;
      is_shut_down = true;
      // std::cerr << "D " << reinterpret_cast<const void *>(this) << " shut down\n";
      return false;

    // We timed out, that's OK, because we're just trying to pull stuff off
    // of the completion queue while we're being asked to do something else.
    case grpc::CompletionQueue::TIMEOUT:
      // std::cerr << "E " << reinterpret_cast<const void *>(this) << " timeout\n";
      *timed_out = true;
      return false;

    // We've got an event, check what type it is.
    case grpc::CompletionQueue::GOT_EVENT: {
      switch (static_cast<RequestTag>(reinterpret_cast<uintptr_t>(tag))) {
        case RequestTag::kStartCall:
          if (!succeeded) {
            sent_finished = true;
            reader->Finish(&status, kFinishTag);
            // std::cerr << "F " << reinterpret_cast<const void *>(this) << " finishing\n";
            return false;

          // Schedule us to read `pending_response`.
          } else {
            reader->Read(&pending_response, kReadTag);
            return true;
          }

        case RequestTag::kRead:
          if (succeeded) {
            queued_responses.emplace_back(std::move(pending_response));

            // Schedule the next one to be read.
            reader->Read(&pending_response, kReadTag);
            return true;

          } else {
            sent_finished = true;
            reader->Finish(&status, kFinishTag);
            // std::cerr << "G " << reinterpret_cast<const void *>(this) << " finishing\n";
            return false;
          }

        case RequestTag::kFinish:
          TearDown();
          // std::cerr << "H " << reinterpret_cast<const void *>(this) << " finished\n";
          return false;
      }
    }
  }

  __builtin_unreachable();
  return false;
}

// Get the next thing.
bool ClientResultStreamImpl::Next(grpc::Slice *out) {

  // We've got some queued responses.
  if (!queued_responses.empty()) {
    // std::cerr << "H " << reinterpret_cast<const void *>(this) << " unqueueing\n";
    *out = std::move(queued_responses.front());
    queued_responses.pop_front();
    return true;
  }

  // We're done.
  if (!completion_queue) {
    return false;
  }

  // Go try to read.
  for (;;) {

    void *tag = nullptr;
    bool succeeded = false;
    if (!completion_queue->Next(&tag, &succeeded)) {
      TearDown();
      // std::cerr << "I " << reinterpret_cast<const void *>(this) << " finished\n";
      return false;
    }

    // We've got an event, check what type it is.
    switch (static_cast<RequestTag>(reinterpret_cast<intptr_t>(tag))) {
      case RequestTag::kStartCall:
        if (!succeeded) {
          sent_finished = true;
          reader->Finish(&status, kFinishTag);
          // std::cerr << "J " << reinterpret_cast<const void *>(this) << " sent finish\n";
          return false;

        } else {
          reader->Read(&pending_response, kReadTag);
          continue;
        }

      case RequestTag::kRead:
        if (succeeded) {

          // Take the refcount from `pending_response`.
          *out = std::move(pending_response);

          // Schedule the next one to be read.
          reader->Read(&pending_response, kReadTag);
          return true;

        } else {
          sent_finished = true;
          reader->Finish(&status, kFinishTag);
          // std::cerr << "K " << reinterpret_cast<const void *>(this) << " sent finish\n";
          return false;
        }

      case RequestTag::kFinish:
        TearDown();
        // std::cerr << "L " << reinterpret_cast<const void *>(this) << " finished\n";
        return false;
    }
  }

  __builtin_unreachable();
  return false;
}

namespace internal {

std::shared_ptr<ClientResultStreamImpl> RequestStream(
    const std::shared_ptr<ClientConnectionImpl> &conn,
    const grpc::internal::RpcMethod &method,
    const grpc::Slice &request) {
  return std::make_shared<ClientResultStreamImpl>(conn, method, request);
}

bool NextOpaque(ClientResultStreamImpl &impl, const grpc::Slice &out) {
  return impl.Next(const_cast<grpc::Slice *>(&out));
}

}  // namespace internal
}  // namespace rt
}  // namespace hyde
