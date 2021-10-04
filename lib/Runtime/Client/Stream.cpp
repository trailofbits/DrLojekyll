// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#include "Stream.h"

#include <atomic>
#include <grpcpp/impl/codegen/async_stream.h>
#include <grpcpp/impl/codegen/status.h>
#include <grpcpp/impl/codegen/status_code_enum.h>

#include "Connection.h"

[[gnu::noinline]]
extern "C" void pumped(void) {}

namespace hyde {
namespace rt {

BackendResultStreamImpl::BackendResultStreamImpl(
    const std::shared_ptr<BackendConnectionImpl> &conn,
    const grpc::internal::RpcMethod &method,
    const grpc_slice &request)
    : cache_lock(conn, &(conn->pending_streams_lock)),
      pending_response(grpc_empty_slice()),
      reader(grpc::internal::ClientAsyncReaderFactory<grpc_slice>::Create<grpc_slice>(
          conn->channel.get(),
          &completion_queue,
          method,
          &context,
          request,
          true,
          reinterpret_cast<void *>(RequestTag::kStartCall))) {

  // Link it into the cache.
  std::unique_lock<std::mutex> locker(conn->pending_streams_lock);
  if (conn->pending_streams) {
    conn->pending_streams->prev_link = &next;
    next = conn->pending_streams;
  }

  prev_link = &(conn->pending_streams);
  conn->pending_streams = this;
}

BackendResultStreamImpl::~BackendResultStreamImpl(void) {
  Unlink();

  if (!is_finished && !sent_finished && reader) {
    sent_finished = true;
    reader->Finish(&status, kFinishTag);
  }

  if (!is_shut_down && !sent_shut_down && reader) {
    completion_queue.Shutdown();
  }

  reader.reset();

  grpc_slice_unref(pending_response);
  for (auto &unused_response : queued_responses) {
    grpc_slice_unref(unused_response);
  }
  queued_responses.clear();
}

// Unlink this stream from the cache.
void BackendResultStreamImpl::Unlink(void) {
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

bool BackendResultStreamImpl::Pump(
    std::chrono::system_clock::time_point deadline,
    bool *timed_out) {

  if (is_shut_down || !reader) {
    return false;
  }

  void *tag = nullptr;
  bool succeeded = false;

  switch (completion_queue.AsyncNext(&tag, &succeeded, deadline)) {

    // Shutting down this stream.
    case grpc::CompletionQueue::SHUTDOWN:
      assert(sent_shut_down);
      Unlink();
      reader.reset();
      is_finished = true;
      is_shut_down = true;
      return false;

    // We timed out, that's OK, because we're just trying to pull stuff off
    // of the completion queue while we're being asked to do something else.
    case grpc::CompletionQueue::TIMEOUT:
      *timed_out = true;
      return false;

    // We've got an event, check what type it is.
    case grpc::CompletionQueue::GOT_EVENT: {
      switch (static_cast<RequestTag>(reinterpret_cast<uintptr_t>(tag))) {
        case RequestTag::kStartCall:
          if (!succeeded) {
            Unlink();

            grpc_slice_unref(pending_response);
            pending_response = grpc_empty_slice();

            sent_finished = true;
            reader->Finish(&status, kFinishTag);
            return false;

          // Schedule us to read `pending_response`.
          } else {
            reader->Read(&pending_response, kReadTag);
            return true;
          }

        case RequestTag::kRead:
          if (succeeded) {
            queued_responses.emplace_back(std::move(pending_response));
            pending_response = grpc_empty_slice();

            // Schedule the next one to be read.
            reader->Read(&pending_response, kReadTag);
            return true;

          } else {
            Unlink();

            grpc_slice_unref(pending_response);
            pending_response = grpc_empty_slice();

            sent_finished = true;
            reader->Finish(&status, kFinishTag);
            return false;
          }

        case RequestTag::kFinish:
          Unlink();

          grpc_slice_unref(pending_response);
          pending_response = grpc_empty_slice();

          is_finished = true;
          sent_shut_down = true;
          completion_queue.Shutdown();
          return false;
      }
    }
  }

  __builtin_unreachable();
  return false;
}

// Get the next thing.
bool BackendResultStreamImpl::Next(grpc_slice *out) {

  grpc_slice_unref(*out);

  // We've got some queued responses.
  if (!queued_responses.empty()) {
    *out = std::move(queued_responses.front());
    queued_responses.pop_front();
    return true;
  }

  *out = grpc_empty_slice();

  // We're done.
  if (is_finished || is_shut_down || !reader) {
    return false;
  }

  // Go try to read.
  for (;;) {

    void *tag = nullptr;
    bool succeeded = false;
    if (!completion_queue.Next(&tag, &succeeded)) {
      Unlink();

      reader.reset();
      is_finished = true;
      is_shut_down = true;
      return false;
    }

    // We've got an event, check what type it is.
    switch (static_cast<RequestTag>(reinterpret_cast<intptr_t>(tag))) {
      case RequestTag::kStartCall:
        if (!succeeded) {
          sent_finished = true;
          reader->Finish(&status, kFinishTag);
          return false;

        } else {
          reader->Read(&pending_response, kReadTag);
          continue;
        }

      case RequestTag::kRead:
        if (succeeded) {

          // Take the refcount from `pending_response`.
          *out = pending_response;

          // Schedule the next one to be read.
          pending_response = grpc_empty_slice();
          reader->Read(&pending_response, kReadTag);
          return true;

        } else {
          grpc_slice_unref(pending_response);
          pending_response = grpc_empty_slice();

          sent_finished = true;
          reader->Finish(&status, kFinishTag);
          return false;
        }

      case RequestTag::kFinish:
        Unlink();

        grpc_slice_unref(pending_response);
        pending_response = grpc_empty_slice();

        is_finished = true;
        sent_shut_down = true;
        completion_queue.Shutdown();
        return false;
    }
  }

  __builtin_unreachable();
  return false;
}

namespace internal {

std::shared_ptr<BackendResultStreamImpl> RequestStream(
    const std::shared_ptr<BackendConnectionImpl> &conn,
    const grpc::internal::RpcMethod &method,
    grpc_slice request) {
  return std::make_shared<BackendResultStreamImpl>(conn, method, request);
}

bool NextOpaque(BackendResultStreamImpl &impl, const grpc_slice &out) {
  return impl.Next(const_cast<grpc_slice *>(&out));
}

}  // namespace internal
}  // namespace rt
}  // namespace hyde
