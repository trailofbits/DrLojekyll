// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <system_error>

#include <drlojekyll/Runtime/Result.h>

namespace hyde {
namespace rt {

class SlabManager;

void ShutDownSlabManager(SlabManager *);

}  // namespace rt
}  // namespace hyde
namespace std {
template <>
struct default_delete<::hyde::rt::SlabManager> {
  inline void operator()(::hyde::rt::SlabManager *ptr) const noexcept {
    ::hyde::rt::ShutDownSlabManager(ptr);
  }
};
}  // namespace std
namespace hyde {
namespace rt {

struct InMemorySlabStore {};

struct FileBackedSlabStore : public std::filesystem::path {
  using std::filesystem::path::path;
};

using SlabStoreKind = std::variant<InMemorySlabStore, FileBackedSlabStore>;
using SlabManagerPtr = std::unique_ptr<SlabManager>;

enum class SlabStoreSize : uint64_t {
  kTiny = 1ull * (1ull << 30u),  // 1 GiB
  kSmall = 4ull * (1ull << 30u),  // 4 GiB
  kMedium = 16ull * (1ull << 30u),  // 16 GiB
  kLarge = 512ull * (1ull << 30u),  // 512 GiB
  kExtraLarge = 1ull * (1ull << 40u),  // 1 TiB
  kHuge = 4ull * (1ull << 40u),  // 4 TiB
};

// Create a new slab storage engine.
Result<SlabManagerPtr, std::error_code>
CreateSlabManager(SlabStoreKind kind, SlabStoreSize size,
                  unsigned num_workers = 1u);

struct SlabStats {
  size_t num_allocated_slabs{0};
  size_t num_free_slabs{0};
  size_t num_open_slabs{0};
};

// Perform garbage collection. Mostly useful for testing purposes.
SlabStats GarbageCollect(SlabManager &storage);

}  // namespace rt
}  // namespace hyde
