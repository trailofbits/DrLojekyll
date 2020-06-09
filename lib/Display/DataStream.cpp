// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include "DataStream.h"

namespace hyde {
namespace display {

DataStream::~DataStream(void) {}

StringViewStream::StringViewStream(const std::string_view data_)
    : data(data_) {}

StringViewStream::~StringViewStream(void) {}

bool StringViewStream::ReadData(std::string_view *data_out) {
  if (done) {
    if (data_out) {
      *data_out = std::string_view();
    }
    return false;
  } else {
    if (data_out) {
      *data_out = data;
    }
    done = true;
    return true;
  }
}

bool StringViewStream::TryGetErrorMessage(std::ostream *) const {
  return false;
}

}  // namespace display
}  // namespace hyde
