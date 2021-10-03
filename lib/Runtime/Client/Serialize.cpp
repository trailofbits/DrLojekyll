// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#include "Serialize.h"

namespace grpc {
namespace hyde {

Status SliceSerializer::Serialize(const grpc_slice &msg,
                                  grpc_byte_buffer **buffer,
                                  bool *own_buffer) {
  grpc_slice *slice = const_cast<grpc_slice *>(&msg);
  *buffer = grpc_raw_byte_buffer_create(slice, 1);
  *own_buffer = true;
  return Status::OK;
}

Status SliceSerializer::Deserialize(ByteBuffer *buf, grpc_slice *msg) {
  grpc_byte_buffer *buffer = *reinterpret_cast<grpc_byte_buffer **>(buf);
  if (!buffer) {
    return Status(StatusCode::INTERNAL, "No payload");
  }

  // Check if this is a single uncompressed slice. If it is, then we can
  // reference the `grpc_slice` directly.
  if ((buffer->type == GRPC_BB_RAW) &&
      (buffer->data.raw.compression == GRPC_COMPRESS_NONE) &&
      (buffer->data.raw.slice_buffer.count == 1)) {

    *msg = buffer->data.raw.slice_buffer.slices[0];
    grpc_slice_ref(*msg);

    // *msg = grpc_slice_buffer_take_first(&(buffer->data.raw.slice_buffer));

  // Otherwise, we need to use `grpc_byte_buffer_reader_readall` to read
  // `buffer` into a single contiguous `grpc_slice`. The gRPC reader gives
  // us back a new slice with the refcount already incremented.
  } else {

    grpc_byte_buffer_reader reader;
    grpc_byte_buffer_reader_init(&reader, buffer);
    // NOTE(pag): Don't increment refcount; it's already been incremented.
    *msg = grpc_byte_buffer_reader_readall(&reader);
    grpc_byte_buffer_reader_destroy(&reader);
  }
  grpc_byte_buffer_destroy(buffer);
  return ::grpc::Status::OK;
}

}  // namespace hyde
}  // namespace grpc
