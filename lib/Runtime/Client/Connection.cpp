// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#include "Connection.h"

#include <grpcpp/grpcpp.h>
#include <grpcpp/impl/grpc_library.h>
#include <grpcpp/impl/codegen/client_unary_call.h>

#include "Serialize.h"
#include "Stream.h"

namespace hyde {
namespace rt {
namespace {

//static const auto kOneMillisecond = std::chrono::milliseconds(1);

[[gnu::used]] const ::grpc::internal::GrpcLibraryInitializer kInitGRPC;

}  // namespace

ClientConnection::ClientConnection(std::shared_ptr<grpc::Channel> channel_)
    : impl(std::make_shared<ClientConnectionImpl>(std::move(channel_))) {}

void ClientConnection::PumpActiveStreams(void) const {
//  const auto deadline = std::chrono::system_clock::now() + kOneMillisecond;
//
//  std::unique_lock<std::mutex> locker(impl->pending_streams_lock);
//  std::cerr << "Pumping\n";
//  bool timed_out = false;
//  auto made_progress = true;
//  auto has_non_empty = false;
//  for (auto stream = impl->pending_streams; stream && !timed_out;
//       stream = stream->next) {
//    if (stream->queued_responses.empty()) {
//      if (stream->Pump(deadline, &timed_out)) {
//        made_progress = true;
//      }
//    } else  {
//      has_non_empty = true;
//    }
//  }
//
//  // If we didn't get anything, pump the them all.
//  if (!made_progress && has_non_empty) {
//    for (auto stream = impl->pending_streams; stream && !timed_out;
//         stream = stream->next) {
//      if (stream->Pump(deadline, &timed_out)) {
//        made_progress = true;
//      }
//    }
//  }
//  std::cerr << "Done pumping\n";
}

ClientConnection::~ClientConnection(void) {
  std::unique_lock<std::mutex> locker(impl->pending_streams_lock);
  assert(!impl->pending_streams);
}

// Send data to the backend.
bool ClientConnection::Publish(const grpc::internal::RpcMethod &method,
                                const grpc::Slice &data) const {
  grpc::ClientContext context;
  grpc::Slice ret_data;
  grpc::Status status =
      ::grpc::internal::BlockingUnaryCall<grpc::Slice, grpc::Slice>(
          impl->channel.get(), method, &context, data, &ret_data);
  return status.error_code() == grpc::StatusCode::OK;
}

// Invoke an RPC that returns a single value.
std::shared_ptr<uint8_t> ClientConnection::Call(
    const grpc::internal::RpcMethod &method,
    const grpc::Slice &data, size_t min_size, size_t align) const {
  grpc::ClientContext context;
  grpc::Slice slice;
  grpc::Status status =
      ::grpc::internal::BlockingUnaryCall<grpc::Slice, grpc::Slice>(
          impl->channel.get(), method, &context, data, &slice);

  auto size = std::max<size_t>(slice.size(), min_size);
  std::shared_ptr<uint8_t> out(
      new (std::align_val_t{align}) uint8_t[size],
      [](uint8_t *p ){ delete [] p; });

  memcpy(out.get(), slice.begin(), slice.size());
  return out;
}

}  // namespace rt
}  // namespace hyde
