// Copyright 2019, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/DataFlow/Query.h>

namespace hyde {

class OutputStream;

OutputStream &operator<<(OutputStream &os, Query query);

}  // namespace hyde
