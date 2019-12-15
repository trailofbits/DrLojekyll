// Copyright 2019, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/Lex/Token.h>

namespace hyde {

class OutputStream;

OutputStream &operator<<(OutputStream &os, Token tok);

}  // namespace hyde
