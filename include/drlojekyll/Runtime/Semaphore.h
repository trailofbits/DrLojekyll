// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

#include <memory>
#include <cstddef>

namespace hyde {
namespace rt {

class SemaphoreImpl;
class Semaphore {
 private:
  std::unique_ptr<SemaphoreImpl> impl;

 public:
  Semaphore(void);
  ~Semaphore(void);
  void Signal(void);
  bool Wait(void);
  void Signal(ssize_t count);
  bool Wait(ssize_t max);
};

}  // namespace rt
}  // namespace hyde
