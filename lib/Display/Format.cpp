// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include <drlojekyll/Display/Format.h>

namespace hyde {

OutputStream::~OutputStream(void) {
  os.flush();
}

OutputStream &OutputStream::operator<<(DisplayRange range) {
  std::string_view data;
  (void)display_manager.TryReadData(range, &data);
  os << data;
  return *this;
}

}  // namespace hyde
