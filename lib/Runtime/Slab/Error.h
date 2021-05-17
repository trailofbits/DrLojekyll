// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <system_error>

namespace hyde {
namespace rt {

void ClearLastError(void);

std::error_code GetLastError(void);

}  // namespace rt
}  // namespace hyde
