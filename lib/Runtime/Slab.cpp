// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#include "Slab.h"

#include <cstdio>
#include <cstdlib>
#include <mutex>

#include "SlabManager.h"

namespace hyde {
namespace rt {
namespace {

static std::mutex gAtomicAccessLock;

}  // namespace

void LockSlab(void *, uint32_t) noexcept {
  gAtomicAccessLock.lock();
}

void UnlockSlab(void *, uint32_t) noexcept {
  gAtomicAccessLock.unlock();
}

Slab::Slab(bool is_persistent) {
  header.u.s.is_persistent = uint64_t(is_persistent) & 1ull;
  header.u.s.has_next = 0;
}

void Slab::operator delete(void *ptr) noexcept {
  free(ptr);
}

void *Slab::operator new(size_t, SlabManager &manager,
                         bool is_persistent) noexcept {

  // If we're not using a file-backed
  if (!is_persistent || manager.fd == -1) {
    return manager.AllocateEphemeralSlab();
  } else {
    return manager.AllocatePersistentSlab();
  }
}

}  // namespace rt
}  // namespace hyde
