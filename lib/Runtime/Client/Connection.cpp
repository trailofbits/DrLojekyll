// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#include "Connection.h"

#include <grpcpp/grpcpp.h>
#include <grpcpp/impl/codegen/client_unary_call.h>

#include "Serialize.h"
#include "Stream.h"

namespace hyde {
namespace rt {
namespace {

static const auto kOneMillisecond = std::chrono::milliseconds(1);

}  // namespace

BackendConnection::BackendConnection(std::shared_ptr<grpc::Channel> channel_)
    : impl(std::make_shared<BackendConnectionImpl>(std::move(channel_))) {}

void BackendConnection::PumpActiveStreams(void) const {
  const auto deadline = std::chrono::system_clock::now() + kOneMillisecond;

  std::unique_lock<std::mutex> locker(impl->pending_streams_lock);

  bool timed_out = false;
  for (auto made_progress = true; made_progress && !timed_out; ) {
    made_progress = false;
    for (auto stream = impl->pending_streams; stream; stream = stream->next) {
      if (stream->Pump(deadline, &timed_out)) {
        made_progress = true;
      }
    }
  }
}

BackendConnection::~BackendConnection(void) {
  std::unique_lock<std::mutex> locker(impl->pending_streams_lock);
  assert(!impl->pending_streams);
}

// Send data to the backend.
bool BackendConnection::Publish(const grpc::internal::RpcMethod &method,
                                const grpc_slice &data) const {
  grpc::ClientContext context;
  grpc_slice ret_data = grpc_empty_slice();
  grpc::Status status =
      ::grpc::internal::BlockingUnaryCall<grpc_slice, grpc_slice>(
          impl->channel.get(), method, &context, data, &ret_data);
  grpc_slice_unref(ret_data);
  return status.error_code() == grpc::StatusCode::OK;
}

// Invoke an RPC that returns a single value.
void BackendConnection::Call(
    const grpc::internal::RpcMethod &method,
    const grpc_slice &data, grpc_slice &ret_data) const {
  grpc::ClientContext context;
  grpc::Status status =
      ::grpc::internal::BlockingUnaryCall<grpc_slice, grpc_slice>(
          impl->channel.get(), method, &context, data, &ret_data);

  if (status.error_code() != grpc::StatusCode::OK) {
    grpc_slice_unref(ret_data);
    ret_data = grpc_empty_slice();
  }
}

}  // namespace rt
}  // namespace hyde
