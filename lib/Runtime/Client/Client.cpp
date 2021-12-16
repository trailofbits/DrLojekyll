// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#include "Client.h"

#include <atomic>
#include <new>

#include <grpcpp/impl/codegen/sync_stream.h>
#include <grpcpp/impl/codegen/status.h>
#include <grpcpp/impl/codegen/status_code_enum.h>
#include <grpcpp/impl/codegen/client_unary_call.h>

#include <iostream>

namespace hyde {
namespace rt {

ClientResultStreamImpl::ClientResultStreamImpl(
    std::shared_ptr<grpc::Channel> channel_,
    const grpc::internal::RpcMethod &method,
    const grpc::Slice &request)
    : channel(std::move(channel_)),
      context() {

//  context.set_compression_algorithm(GRPC_COMPRESS_STREAM_GZIP);
  context.set_wait_for_ready(true);
  reader.reset(
      grpc::internal::ClientReaderFactory<grpc::Slice>::Create<grpc::Slice>(
        channel.get(),
        method,
        &context,
        request));

  reader->WaitForInitialMetadata();
}

ClientResultStreamImpl::~ClientResultStreamImpl(void) {
  if (reader) {
    std::cerr << "trying to finish\n";

    context.TryCancel();
  }
}

// Get the next thing.
bool ClientResultStreamImpl::Next(std::shared_ptr<uint8_t> *out,
                                  size_t align, size_t min_size) {
  grpc::Slice slice;
  auto read = false;

  for (auto retry = 8; !read && retry--; ) {
    std::unique_lock<std::mutex> read_locker(read_lock);
    if (!reader) {
      return false;
    }
    read = reader->Read(&slice);
  }

  if (read) {
    auto size = std::max<size_t>(slice.size(), min_size);
    std::shared_ptr<uint8_t> new_out(
        new (std::align_val_t{align}) uint8_t[size],
        [](uint8_t *p ){ delete [] p; });

    memcpy(new_out.get(), slice.begin(), slice.size());
    out->swap(new_out);
    return true;

  } else {
    reader.reset();
    return false;
  }
}

namespace internal {

std::shared_ptr<ClientResultStreamImpl> RequestStream(
    std::shared_ptr<grpc::Channel> channel_,
    const grpc::internal::RpcMethod &method,
    const grpc::Slice &request) {
  return std::make_shared<ClientResultStreamImpl>(
      std::move(channel_), method, request);
}

bool NextOpaque(ClientResultStreamImpl &impl,
                std::shared_ptr<uint8_t> &out,
                size_t align, size_t min_size) {
  return impl.Next(&out, align, min_size);
}

// Invoke an RPC that returns a single value.
std::shared_ptr<uint8_t> Call(
    grpc::Channel *channel, const grpc::internal::RpcMethod &method,
    const grpc::Slice &data, size_t min_size, size_t align) {

  grpc::ClientContext context;

  // Make sure that the trailing metadata array count is zero.
  grpc::Slice slice;
  grpc::Status status =
      ::grpc::internal::BlockingUnaryCall<grpc::Slice, grpc::Slice>(
          channel, method, &context, data, &slice);
  if (status.ok()) {
    auto size = std::max<size_t>(slice.size(), min_size);
    std::shared_ptr<uint8_t> out(
        new (std::align_val_t{align}) uint8_t[size],
        [](uint8_t *p ){ delete [] p; });

    memcpy(out.get(), slice.begin(), slice.size());
    return out;
  } else {
    return {};
  }
}

// Kill a stream.
void Kill(ClientResultStreamImpl *stream) {
  if (stream) {
    stream->context.TryCancel();
  }
}

}  // namespace internal

// Send data to the backend.
bool Publish(grpc::Channel *channel, const grpc::internal::RpcMethod &method,
             const grpc::Slice &data) {
  grpc::ClientContext context;
  grpc::Slice ret_data;
  grpc::Status status =
      ::grpc::internal::BlockingUnaryCall<grpc::Slice, grpc::Slice>(
          channel, method, &context, data, &ret_data);
  return status.error_code() == grpc::StatusCode::OK;
}

}  // namespace rt
}  // namespace hyde
