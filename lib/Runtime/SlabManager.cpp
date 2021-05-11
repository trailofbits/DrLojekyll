// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#include "SlabManager.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <string>

#include "Error.h"

namespace hyde {
namespace rt {
namespace {}  // namespace

#ifndef MAP_UNINITIALIZED
#  define MAP_UNINITIALIZED 0
#endif

#ifndef MAP_FILE
#  define MAP_FILE 0
#endif

#if defined(MAP_ANON) && !defined(MAP_ANONYMOUS)
#  define MAP_ANONYMOUS MAP_ANON
#endif

Result<SlabManagerPtr, std::error_code> CreateSlabManager(
    SlabStoreKind kind, SlabStoreSize size_, unsigned num_workers) {

  ClearLastError();

  size_t real_size = static_cast<size_t>(size_);
  auto real_base = mmap(
      nullptr, real_size, PROT_NONE,
      MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE | MAP_UNINITIALIZED, -1, 0);
  if (MAP_FAILED == real_base) {
    return GetLastError();
  }

  // Make sure the `mmap`ed address is `Slab`-aligned.
  auto base = real_base;
  auto size = real_size;
  auto base_addr = reinterpret_cast<uintptr_t>(base);
  while (base_addr & (sizeof(Slab) - 1ull)) {
    base_addr += 4096u;
    size -= 4096u;
    base = reinterpret_cast<void *>(base_addr);
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
                          MAP_SHARED | MAP_FILE | MAP_FIXED, fd, 0);
    if (file_base != base) {
      return GetLastError();
    }
  }

  return std::make_unique<SlabManager>(num_workers, fd, file_size, real_base,
                                       real_size, base, size);
}

SlabManager::~SlabManager(void) {
  {
    std::unique_lock<std::mutex> locker(maybe_free_slabs_lock);
    maybe_free_slabs.clear();
  }
  {
    std::unique_lock<std::mutex> locker(all_slabs_lock);
    all_slabs.clear();
  }
  if (-1 != fd) {
    std::unique_lock<std::mutex> locker(file_size_lock);
    msync(base, file_size, MS_SYNC);
    fsync(fd);
    munmap(real_base, real_max_size);
    close(fd);
  }
}

SlabManager::SlabManager(unsigned num_workers_, int fd_, uint64_t file_size_,
                         void *real_base_, uint64_t real_max_size_, void *base_,
                         uint64_t max_size_)
    : num_workers(num_workers_),
      fd(fd_),
      base_file_size(file_size_),
      real_base(real_base_),
      real_max_size(real_max_size_),
      base(reinterpret_cast<Slab *>(base_)),
      max_size(max_size_),
      file_size(file_size_),
      has_free_slab_heads(false) {
  all_slabs.reserve(4096u);
  maybe_free_slabs.reserve(4096u);
}

void ShutDownSlabManager(SlabManager *ptr) {
  delete ptr;
}

SlabStats GarbageCollect(SlabManager &storage) {

  SlabStats stats;

  auto count_num_used = [&](Slab *slab) {
    for (; slab; slab = slab->Next()) {
      stats.num_open_slabs += 1u;
    }
  };

  auto count_num_free = [&](Slab *slab) {
    for (; slab; slab = slab->Next()) {
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

// Allocate an ephemeral slab.
void *SlabManager::AllocateEphemeralSlab(void) {
  Slab *ret_slab = nullptr;
  if (has_free_slab_heads.load(std::memory_order_acquire)) {

    std::unique_lock<std::mutex> locker(maybe_free_slabs_lock);

    unsigned to_remove = 0u;
    for (size_t max_i = maybe_free_slabs.size(), i = 0; i < max_i; ++i) {

      auto &found_slab = maybe_free_slabs[i];

      // This slot has a null entry, schedule it for removal.
      if (!found_slab) {
        ++to_remove;
        std::swap(found_slab, maybe_free_slabs[max_i - to_remove]);
        continue;
      }

      // This slab is still referenced.
      if (found_slab->IsReferenced()) {
        continue;
      }

      ret_slab = found_slab;
      found_slab = ret_slab->Next();

      // `ret_slab->header.next` is null, schedule it for removal.
      if (!found_slab) {
        ++to_remove;
        std::swap(found_slab, maybe_free_slabs[max_i - to_remove]);
      }
      break;
    }

    // Clean up our list.
    if (to_remove) {
      while (!maybe_free_slabs.back()) {
        maybe_free_slabs.pop_back();
      }
      has_free_slab_heads.store(!maybe_free_slabs.empty(),
                                std::memory_order_release);
    }
  }

  // We have nothing in a free list, so go and allocate some new memory.
  if (!ret_slab) {
    void *ptr = nullptr;
    if (posix_memalign(&ptr, sizeof(Slab), sizeof(Slab))) {
      perror("Failed to perform 2 MiB aligned allocation");
      abort();
    }

    ret_slab = reinterpret_cast<Slab *>(ptr);
    std::unique_lock<std::mutex> locker(all_slabs_lock);
    all_slabs.emplace_back(ret_slab);
  }

  return ret_slab;
}

// Allocate a persistent slab.
void *SlabManager::AllocatePersistentSlab(void) {
  uint64_t old_size = 0;
  {
    std::unique_lock<std::mutex> locker(file_size_lock);
    old_size = file_size;
    file_size += sizeof(Slab);
    if (ftruncate(fd, static_cast<off_t>(file_size))) {
      perror("Unable to extend backing file");
      abort();
    }
  }

  auto ret = mmap(&(base[old_size / sizeof(Slab)]), sizeof(Slab),
                  PROT_READ | PROT_WRITE, MAP_FIXED | MAP_SHARED | MAP_FILE, fd,
                  static_cast<off_t>(old_size));
  if (MAP_FAILED == ret) {
    perror("Failed to map Slab to file");
    abort();
  }

  return ret;
}

}  // namespace rt
}  // namespace hyde
