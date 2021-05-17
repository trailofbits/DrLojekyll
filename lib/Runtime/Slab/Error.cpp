// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#include "Error.h"

#ifdef _WIN32
#  include <errhandlingapi.h>
#endif

namespace hyde {
namespace rt {

void ClearLastError(void) {
#ifdef _WIN32
  ::SetLastError(0);
#else
  errno = 0;
#endif
}

std::error_code GetLastError(void) {
#ifdef _WIN32
  return std::error_code(::GetLastError(), std::system_category());
#else
  return std::error_code(errno, std::generic_category());
#endif
}

}  // namespace rt
}  // namespace hyde
