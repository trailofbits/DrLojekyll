// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <cstddef>

namespace hyde {
namespace rt {

enum : size_t {
  k1MiB = 1ull << 20ull,
  k2MiB = k1MiB * 2ull,
  kSlabSize = k2MiB
};

class Slab;

}  // namespace rt
}  // namespace hyde
