// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#include "Stream.h"

#include <atomic>
#include <new>

#include <grpcpp/impl/codegen/sync_stream.h>
#include <grpcpp/impl/codegen/status.h>
#include <grpcpp/impl/codegen/status_code_enum.h>

#include <drlojekyll/Runtime/ClientConnection.h>

#include <iostream>

namespace hyde {
namespace rt {

ClientResultStreamImpl::ClientResultStreamImpl(
    const ClientConnection &connection,
    const grpc::internal::RpcMethod &method,
    const grpc::Slice &request)
    : channel(connection.channel),
      context() {

  context.set_compression_algorithm(GRPC_COMPRESS_STREAM_GZIP);
  context.set_wait_for_ready(true);
  context.set_idempotent(true);
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
    context.TryCancel();
  }
}

// Get the next thing.
bool ClientResultStreamImpl::Next(std::shared_ptr<uint8_t> *out,
                                  size_t align, size_t min_size) {
  grpc::Slice slice;
  auto read = false;
  {
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
    std::cerr << "trying to finish\n";
    auto status = reader->Finish();
    std::cerr << "status: " << status.error_message() << '\n';
    reader.reset();
    return false;
  }
}

namespace internal {

std::shared_ptr<ClientResultStreamImpl> RequestStream(
    const ClientConnection &conn,
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
