// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#include "SlabStorage.h"

#include <string>

#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "Error.h"

namespace hyde {
namespace rt {
namespace {

}  // namespace

#ifndef MAP_UNINITIALIZED
# define MAP_UNINITIALIZED 0
#endif

#ifndef MAP_FILE
# define MAP_FILE 0
#endif

#if defined(MAP_ANON) && !defined(MAP_ANONYMOUS)
# define MAP_ANONYMOUS MAP_ANON
#endif

Result<SlabStorePtr, std::error_code> CreateSlabStorage(
    SlabStoreKind kind, SlabStoreSize size, unsigned num_workers) {

  ClearLastError();

  // Go and
  auto base = mmap(nullptr, static_cast<size_t>(size), PROT_NONE,
                   MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE |
                   MAP_UNINITIALIZED, -1, 0);
  if (MAP_FAILED == base) {
    return GetLastError();
  }

  int fd = -1;
  uint64_t file_size = 0u;

  // If this is going to be a file-backed storage, then go and open or
  // create the file.
  if (std::holds_alternative<FileBackedSlabStore>(kind)) {
    auto path_str = std::get<FileBackedSlabStore>(kind).string();

    fd = open(path_str.c_str(), O_RDWR | O_CREAT, 0666);
    if (fd == -1) {
      return GetLastError();
    }

    struct stat info {};
    if (-1 == fstat(fd, &info)) {
      return GetLastError();
    }

    if (0 > info.st_size) {
      return std::make_error_code(std::errc::bad_file_descriptor);
    }

    // The file size should be a multiple of the slab size.
    file_size = static_cast<uint64_t>(info.st_size);
    if ((file_size & ~(sizeof(Slab) - 1u)) != file_size) {
      return std::make_error_code(std::errc::file_too_large);
    }
  }

  // Map the file to be beginning of our stuff.
  if (-1 != fd && file_size) {
    auto file_base = mmap(base, file_size, PROT_READ | PROT_WRITE,
                          MAP_SHARED | MAP_FILE, fd, 0);
    if (file_base != base) {
      return GetLastError();
    }
  }

  return std::make_unique<SlabStorage>(
      num_workers, fd, file_size, base, static_cast<uint64_t>(size));
}

SlabStorage::~SlabStorage(void) {
  {
    std::unique_lock<std::mutex> locker(maybe_free_slabs_lock);
    maybe_free_slabs.clear();
  }
  {
    std::unique_lock<std::mutex> locker(all_slabs_lock);
    all_slabs.clear();
  }
}

SlabStorage::SlabStorage(unsigned num_workers_, int fd_, uint64_t file_size_,
                         void *base_, uint64_t max_size_)
    : num_workers(num_workers_),
      fd(fd_),
      base_file_size(file_size_),
      current_file_size(file_size_),
      base(reinterpret_cast<uint8_t *>(base_)),
      max_size(max_size_),
      has_free_slab_heads(false) {
  all_slabs.reserve(4096u);
  maybe_free_slabs.reserve(4096u);
}

void ShutDownSlabStorage(SlabStorage *ptr) {
  delete ptr;
}

SlabStats GarbageCollect(SlabStorage &storage) {

  SlabStats stats;

  auto count_num_used = [&] (Slab *slab) {
    for (; slab; slab = slab->header.u.next) {
      stats.num_open_slabs += 1u;
    }
  };

  auto count_num_free = [&] (Slab *slab) {
    for (; slab; slab = slab->header.u.next) {
      if (slab->IsReferenced()) {
        count_num_used(slab);
        break;
      } else {
        stats.num_free_slabs += 1u;
      }
    }
  };

  std::scoped_lock locker(storage.all_slabs_lock,
                          storage.maybe_free_slabs_lock);

  stats.num_allocated_slabs = storage.all_slabs.size();
  for (auto slab : storage.maybe_free_slabs) {
    count_num_free(slab);
  }

  return stats;
}

}  // namespace rt
}  // namespace hyde
