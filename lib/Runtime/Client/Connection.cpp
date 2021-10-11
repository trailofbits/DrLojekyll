// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#include <drlojekyll/Runtime/ClientConnection.h>

#include <grpcpp/grpcpp.h>
#include <grpcpp/impl/grpc_library.h>
#include <grpcpp/impl/codegen/client_unary_call.h>

#include "Serialize.h"
#include "Stream.h"

namespace hyde {
namespace rt {
namespace {

[[gnu::used]] const ::grpc::internal::GrpcLibraryInitializer kInitGRPC;

}  // namespace

ClientConnection::ClientConnection(std::shared_ptr<grpc::Channel> channel_)
    : channel(std::move(channel_)) {}

ClientConnection::~ClientConnection(void) {}

// Send data to the backend.
bool ClientConnection::Publish(const grpc::internal::RpcMethod &method,
                                const grpc::Slice &data) const {
  grpc::ClientContext context;
  grpc::Slice ret_data;
  grpc::Status status =
      ::grpc::internal::BlockingUnaryCall<grpc::Slice, grpc::Slice>(
          channel.get(), method, &context, data, &ret_data);
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
          channel.get(), method, &context, data, &slice);

  auto size = std::max<size_t>(slice.size(), min_size);
  std::shared_ptr<uint8_t> out(
      new (std::align_val_t{align}) uint8_t[size],
      [](uint8_t *p ){ delete [] p; });

  memcpy(out.get(), slice.begin(), slice.size());
  return out;
}

}  // namespace rt
}  // namespace hyde
