// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#include "Stream.h"

#include <atomic>
#include <new>

#include <grpcpp/impl/codegen/sync_stream.h>
#include <grpcpp/impl/codegen/status.h>
#include <grpcpp/impl/codegen/status_code_enum.h>

#include "Connection.h"

#include <iostream>

namespace hyde {
namespace rt {

ClientResultStreamImpl::ClientResultStreamImpl(
    const std::shared_ptr<ClientConnectionImpl> &conn,
    const grpc::internal::RpcMethod &method,
    const grpc::Slice &request)
    : //cache_lock(conn, &(conn->pending_streams_lock)),
//      context(new (&context_storage) grpc::ClientContext),
//      completion_queue(new (&completion_queue_storage) grpc::CompletionQueue),
      context(),
      reader(grpc::internal::ClientReaderFactory<grpc::Slice>::Create<grpc::Slice>(
          conn->channel.get(),
          method,
          &context,
          request)) {

  // std::cerr << "A " << reinterpret_cast<const void *>(this) << " making stream\n";

//  // Link it into the cache.
//  std::unique_lock<std::mutex> locker(conn->pending_streams_lock);
//  if (conn->pending_streams) {
//    conn->pending_streams->prev_link = &next;
//    next = conn->pending_streams;
//  }
//
//  prev_link = &(conn->pending_streams);
//  conn->pending_streams = this;
}

ClientResultStreamImpl::~ClientResultStreamImpl(void) {
//  TearDown();
}

// Unlink this stream from the cache.
void ClientResultStreamImpl::Unlink(void) {
  // std::cerr << "X " << reinterpret_cast<const void *>(this) << " unlink\n";
//
//  std::unique_lock<std::mutex> locker(*cache_lock);
//  if (!prev_link) {
//    return;
//  }
//
//  if (next) {
//    next->prev_link = prev_link;
//  }
//
//  *prev_link = next;
//  cache_lock.reset();
//
//  prev_link = nullptr;
//  next = nullptr;
}

void ClientResultStreamImpl::TearDown(void) {
//  Unlink();

//  if (!completion_queue) {
//    return;
//  }
//
//  // std::cerr << "deleting completion queue\n";
//
//  if (!sent_finished) {
//    // std::cerr << "B " << reinterpret_cast<const void *>(this) << " finishing stream\n";
//    sent_finished = true;
//    reader->Finish(&status, kFinishTag);
//  }
//
////  if (!is_finished) {
////    void *tag = nullptr;
////    bool succeeded = false;
////    // std::cerr << "F " << reinterpret_cast<const void *>(this) << " draining queue\n";
////    while (completion_queue->Next(&tag, &succeeded)) {
////      // std::cerr << "? " << reinterpret_cast<const void *>(this) << " got tag " << tag << "\n";
////      if (tag == kFinishTag) {
////        // std::cerr << "T " << reinterpret_cast<const void *>(this) << " finished stream\n";
////        is_finished = true;
////      }
////    }
////
////    sent_shut_down = true;
////  }
//
//  if (!sent_shut_down) {
//    sent_shut_down = true;
//    completion_queue->Shutdown();
//  }
//
//  // std::cerr << "Y " << reinterpret_cast<const void *>(this) << " tear down: " << status.error_details() << "\n" << status.error_message() << '\n';

//  reader.reset();
//
//  is_finished = true;
//  completion_queue->~CompletionQueue();
//  completion_queue = nullptr;
//
//  context->~ClientContext();
//  context = nullptr;
}

bool ClientResultStreamImpl::Pump(
    std::chrono::system_clock::time_point deadline,
    bool *timed_out) {
//
//  if (!completion_queue) {
//    return false;
//  }
//
//  void *tag = nullptr;
//  bool succeeded = false;
//
//  switch (completion_queue->AsyncNext(&tag, &succeeded, deadline)) {
//
//    // Shutting down this stream.
//    case grpc::CompletionQueue::SHUTDOWN:
//      assert(sent_shut_down);
//      sent_finished = true;
//      is_finished = true;
//      sent_shut_down = true;
//      TearDown();
//      // std::cerr << "D " << reinterpret_cast<const void *>(this) << " shut down\n";
//      return false;
//
//    // We timed out, that's OK, because we're just trying to pull stuff off
//    // of the completion queue while we're being asked to do something else.
//    case grpc::CompletionQueue::TIMEOUT:
//      // std::cerr << "E " << reinterpret_cast<const void *>(this) << " timeout\n";
//      *timed_out = true;
//      return false;
//
//    // We've got an event, check what type it is.
//    case grpc::CompletionQueue::GOT_EVENT: {
//      switch (static_cast<RequestTag>(reinterpret_cast<uintptr_t>(tag))) {
//        case RequestTag::kStartCall:
//          if (!succeeded) {
//            sent_finished = true;
//            reader->Finish(&status, kFinishTag);
//            // std::cerr << "F " << reinterpret_cast<const void *>(this) << " finishing\n";
//            return false;
//
//          // Schedule us to read `pending_response`.
//          } else {
//            reader->ReadInitialMetadata(kReadInitialMetadata);
//            // std::cerr << "1 " << reinterpret_cast<const void *>(this) << " sent read initial metadata\n";
//            return false;
//          }
//
//        case RequestTag::kReadInitialMetadata:
//          if (!succeeded) {
//            sent_finished = true;
//            reader->Finish(&status, kFinishTag);
//            // std::cerr << "9 " << reinterpret_cast<const void *>(this) << " finishing\n";
//            return false;
//
//          } else {
//            // std::cerr << "2 " << reinterpret_cast<const void *>(this) << " read metadata, sending read\n";
//            reader->Read(&pending_response, kReadTag);
//            return false;
//          }
//
//        case RequestTag::kRead:
//          if (succeeded) {
//            queued_responses.emplace_back(std::move(pending_response));
//
//            // Schedule the next one to be read.
//            reader->Read(&pending_response, kReadTag);
//            return true;
//
//          } else {
//            sent_finished = true;
//            reader->Finish(&status, kFinishTag);
//            // std::cerr << "G " << reinterpret_cast<const void *>(this) << " finishing\n";
//            return false;
//          }
//
//        case RequestTag::kFinish:
//          is_finished = true;
//          TearDown();
//          // std::cerr << "H " << reinterpret_cast<const void *>(this) << " finished\n";
//          return false;
//      }
//    }
//  }
//
//  __builtin_unreachable();
  return false;
}

void ClientResultStreamImpl::Allocate(std::shared_ptr<uint8_t> *out,
                                      size_t align, size_t min_size,
                                      grpc::Slice slice) {
  auto size = std::max<size_t>(slice.size(), min_size);
  std::shared_ptr<uint8_t> new_out(
      new (std::align_val_t{align}) uint8_t[size],
      [](uint8_t *p ){ delete [] p; });

  memcpy(new_out.get(), slice.begin(), slice.size());
  out->swap(new_out);
}

// Get the next thing.
bool ClientResultStreamImpl::Next(std::shared_ptr<uint8_t> *out,
                                  size_t align, size_t min_size) {
  std::unique_lock<std::mutex> read_locker(read_lock);
  if (!reader) {
    return false;
  }

  grpc::Slice slice;
  if (reader->Read(&slice)) {
    Allocate(out, align, min_size, std::move(slice));
    return true;
  } else {
    auto status = reader->Finish();
    std::cerr << "status: " << status.error_message() << '\n';
    reader.reset();
    return false;
  }

//  // We've got some queued responses.
//  if (!queued_responses.empty()) {
//    // std::cerr << "I " << reinterpret_cast<const void *>(this) << " unqueueing\n";
//    Allocate(out, align, min_size, std::move(queued_responses.front()));
//    queued_responses.pop_front();
//    return true;
//  }
//
//  // We're done.
//  if (!completion_queue) {
//    return false;
//  }
//
//  // Go try to read.
//  for (;;) {
//
//    void *tag = nullptr;
//    bool succeeded = false;
//    if (!completion_queue->Next(&tag, &succeeded)) {
//      sent_finished = true;
//      is_finished = true;
//      sent_shut_down = true;
//      // std::cerr << "J " << reinterpret_cast<const void *>(this) << " finished\n";
//      TearDown();
//      return false;
//    }
//
//    // We've got an event, check what type it is.
//    switch (static_cast<RequestTag>(reinterpret_cast<intptr_t>(tag))) {
//      case RequestTag::kStartCall:
//        if (!succeeded) {
//          sent_finished = true;
//          reader->Finish(&status, kFinishTag);
//          // std::cerr << "K " << reinterpret_cast<const void *>(this) << " sent finish\n";
//          return false;
//
//        } else {
//          reader->ReadInitialMetadata(kReadInitialMetadata);
//          // std::cerr << "1 " << reinterpret_cast<const void *>(this) << " sent read initial metadata\n";
//          continue;
//        }
//
//      case RequestTag::kReadInitialMetadata:
//        if (!succeeded) {
//          sent_finished = true;
//          reader->Finish(&status, kFinishTag);
//          // std::cerr << "4 " << reinterpret_cast<const void *>(this) << " sent finish\n";
//          return false;
//
//        } else {
//          // std::cerr << "L " << reinterpret_cast<const void *>(this) << " sent read\n";
//          reader->Read(&pending_response, kReadTag);
//          continue;
//        }
//
//      case RequestTag::kRead:
//        if (succeeded) {
//          Allocate(out, align, min_size, std::move(pending_response));
//
//          // std::cerr << "M " << reinterpret_cast<const void *>(this) << " read\n";
//
//          // Schedule the next one to be read.
//          reader->Read(&pending_response, kReadTag);
//          return true;
//
//        } else {
//          sent_finished = true;
//          reader->Finish(&status, kFinishTag);
//          // std::cerr << "N " << reinterpret_cast<const void *>(this) << " sent finish slice size = " << pending_response.size() << '\n';
//          return false;
//        }
//
//      case RequestTag::kFinish:
//        assert(sent_finished);
//        is_finished = true;
//        // std::cerr << "O " << reinterpret_cast<const void *>(this) << " finished\n";
//        TearDown();
//        return false;
//    }
//  }
//
//  __builtin_unreachable();
//  return false;
}

namespace internal {

std::shared_ptr<ClientResultStreamImpl> RequestStream(
    const std::shared_ptr<ClientConnectionImpl> &conn,
    const grpc::internal::RpcMethod &method,
    const grpc::Slice &request) {
  return std::make_shared<ClientResultStreamImpl>(conn, method, request);
}

bool NextOpaque(ClientResultStreamImpl &impl,
                std::shared_ptr<uint8_t> &out,
                size_t align, size_t min_size) {
  return impl.Next(&out, align, min_size);
}

}  // namespace internal
}  // namespace rt
}  // namespace hyde
