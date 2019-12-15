// Copyright 2019, Trail of Bits. All rights reserved.

#include <drlojekyll/Util/Node.h>

namespace hyde {

void *NodeTraverser::Next(void *node, intptr_t offset) {
  return *reinterpret_cast<void **>(reinterpret_cast<intptr_t>(node) + offset);
}

}  // namespace hyde
