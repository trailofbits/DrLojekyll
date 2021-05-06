// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <drlojekyll/Runtime/SlabStorage.h>

#include <memory>

#include "SlabManager.h"

namespace hyde {
namespace rt {

class SlabStorage {
 public:
  inline SlabStorage(unsigned num_workers)
      : manager(num_workers) {}

  SlabManager manager;
};

}  // namespace rt
}  // namespace hyde
