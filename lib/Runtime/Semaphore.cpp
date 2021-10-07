// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#include <drlojekyll/Runtime/Semaphore.h>

#include <blockingconcurrentqueue.h>

namespace hyde {
namespace rt {

class SemaphoreImpl : public moodycamel::LightweightSemaphore {};

Semaphore::Semaphore(void)
    : impl(std::make_unique<SemaphoreImpl>()) {}

Semaphore::~Semaphore(void) {}

void Semaphore::Signal(void) {
  impl->signal();
}

bool Semaphore::Wait(void) {
  return impl->wait();
}

void Semaphore::Signal(ssize_t count) {
  impl->signal(count);
}

bool Semaphore::Wait(ssize_t max) {
  return impl->waitMany(max);
}

}  // namespace rt
}  // namespace hyde
