// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/Runtime/Runtime.h>

#include <cinttypes>
#include <cstdio>
#include <cstdlib>

namespace hyde {
namespace rt {

//namespace {
//
//static uint64_t NumWorkersMask(uint64_t num_workers) noexcept {
//  switch (num_workers) {
//    case 0:
//    case 1:
//      return 0;
//    case 2:
//    case 4:
//    case 8:
//    case 16:
//    case 32:
//    case 64:
//    case 128:
//    case 256:
//    case 512:
//      return num_workers ^ (num_workers - 1ull);
//    default:
//      fprintf(stderr, "Invalid number of workers: %" PRIu64, num_workers);
//      abort();
//  }
//}
//
//}  // namespace
//
//ProgramBase::~ProgramBase(void) {}
//
//ProgramBase::ProgramBase(unsigned worker_id_, unsigned num_workers_)
//    : __worker_id(worker_id_),
//      __num_workers(num_workers_),
//      __num_workers_mask(NumWorkersMask(num_workers_)) {}

}  // namespace rt
}  // namespace hyde
