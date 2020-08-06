// Copyright 2019, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/ControlFlow/Program.h>

namespace hyde {

class OutputStream;

OutputStream &operator<<(OutputStream &os, Program program);

}  // namespace hyde
