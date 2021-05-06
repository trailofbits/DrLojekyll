// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#include "UnitTests.h"

#include <drlojekyll/Runtime/SlabStorage.h>
#include <drlojekyll/Runtime/SlabVector.h>

TEST(SlabRuntime, SlabVectorOfU8sWorks) {
  auto storage = hyde::rt::CreateSlabStorage();
  hyde::rt::TypedSlabVector<uint8_t> vec(*storage, 0u);
  vec.Add<uint8_t>(0xAA);
  vec.Add<uint8_t>(0x11);
  vec.Add<uint8_t>(0xBB);

  auto index = 0u;
  for (auto [val_ref] : vec) {
    uint8_t val = val_ref;
    switch (index++) {
      case 0: RC_ASSERT(val == 0xAAu); break;
      case 1: RC_ASSERT(val == 0x11u); break;
      case 2: RC_ASSERT(val == 0xBBu); break;
      default: break;
    }
  }

  RC_ASSERT(index == 3u);
}

TEST(SlabRuntime, SlabVectorOfU16sWorks) {
  auto storage = hyde::rt::CreateSlabStorage();
  hyde::rt::TypedSlabVector<uint16_t> vec(*storage, 0u);
  vec.Add<uint16_t>(0xAA11);
  vec.Add<uint16_t>(0xBB22);
  vec.Add<uint16_t>(0xCC33);

  auto index = 0u;
  for (auto [val_ref] : vec) {
    uint16_t val = val_ref;
    switch (index++) {
      case 0: RC_ASSERT(val == 0xAA11u); break;
      case 1: RC_ASSERT(val == 0xBB22u); break;
      case 2: RC_ASSERT(val == 0xCC33u); break;
      default: break;
    }
  }

  RC_ASSERT(index == 3u);
}

TEST(SlabRuntime, SlabVectorOfU32sWorks) {
  auto storage = hyde::rt::CreateSlabStorage();
  hyde::rt::TypedSlabVector<uint32_t> vec(*storage, 0u);
  vec.Add<uint32_t>(0xAABBCCDDu);
  vec.Add<uint32_t>(0x11223344u);
  vec.Add<uint32_t>(0xaa11bb22u);

  auto index = 0u;
  for (auto [val_ref] : vec) {
    uint32_t val = val_ref;
    switch (index++) {
      case 0: RC_ASSERT(val == 0xAABBCCDDu); break;
      case 1: RC_ASSERT(val == 0x11223344u); break;
      case 2: RC_ASSERT(val == 0xaa11bb22u); break;
      default: break;
    }
  }

  RC_ASSERT(index == 3u);
}

TEST(SlabRuntime, SlabVectorOfU64sWorks) {
  auto storage = hyde::rt::CreateSlabStorage();
  hyde::rt::TypedSlabVector<uint64_t> vec(*storage, 0u);
  vec.Add<uint64_t>(0xAABBCCDDull);
  vec.Add<uint64_t>(0x1122334400000000ull);
  vec.Add<uint64_t>(0xaa001100bb002200ull);

  auto index = 0u;
  for (auto [val_ref] : vec) {
    uint64_t val = val_ref;
    switch (index++) {
      case 0: RC_ASSERT(val == 0xAABBCCDDull); break;
      case 1: RC_ASSERT(val == 0x1122334400000000ull); break;
      case 2: RC_ASSERT(val == 0xaa001100bb002200ull); break;
      default: break;
    }
  }

  RC_ASSERT(index == 3u);
}

TEST(SlabRuntime, SlabVectorOfF32sWorks) {
  auto storage = hyde::rt::CreateSlabStorage();
  hyde::rt::TypedSlabVector<float> vec(*storage, 0u);
  vec.Add<float>(1.0f);
  vec.Add<float>(99.0f);
  vec.Add<float>(0.0001f);

  auto index = 0u;
  for (auto [val_ref] : vec) {
    float val = val_ref;
    switch (index++) {
      case 0: RC_ASSERT(val == 1.0f); break;
      case 1: RC_ASSERT(val == 99.0f); break;
      case 2: RC_ASSERT(val == 0.0001f); break;
      default: break;
    }
  }

  RC_ASSERT(index == 3u);
}

TEST(SlabRuntime, SlabVectorOfF64sWorks) {
  auto storage = hyde::rt::CreateSlabStorage();
  hyde::rt::TypedSlabVector<double> vec(*storage, 0u);
  vec.Add<double>(1.0);
  vec.Add<double>(99.0);
  vec.Add<double>(0.0001);

  auto index = 0u;
  for (auto [val_ref] : vec) {
    double val = val_ref;
    switch (index++) {
      case 0: RC_ASSERT(val == 1.0); break;
      case 1: RC_ASSERT(val == 99.0); break;
      case 2: RC_ASSERT(val == 0.0001); break;
      default: break;
    }
  }

  RC_ASSERT(index == 3u);
}


TEST(SlabRuntime, SlabVectorOfPairsWorks) {
  auto storage = hyde::rt::CreateSlabStorage();
  hyde::rt::TypedSlabVector<double, int> vec(*storage, 0u);
  vec.Add<double, int>(1.0, 11111);
  vec.Add<double, int>(99.0, 22222);
  vec.Add<double, int>(0.0001, 33333);

  auto index = 0u;
  for (auto [v1_ref, v2_ref] : vec) {
    double v1 = v1_ref;
    int v2 = v2_ref;
    switch (index++) {
      case 0:
        RC_ASSERT(v1 == 1.0);
        RC_ASSERT(v2 == 11111);
        break;
      case 1:
        RC_ASSERT(v1 == 99.0);
        RC_ASSERT(v2 == 22222);
        break;
      case 2:
        RC_ASSERT(v1 == 0.0001);
        RC_ASSERT(v2 == 33333);
        break;
      default: break;
    }
  }

  RC_ASSERT(index == 3u);
}

TEST(SlabRuntime, SlabVectorOfTriplesOfPairsWorks) {
  auto storage = hyde::rt::CreateSlabStorage();
  hyde::rt::TypedSlabVector<std::pair<int, bool>,
                            std::array<char, 2>,
                            std::tuple<float, double>> vec(*storage, 0u);
  vec.Add<std::pair<int, bool>,
          std::array<char, 2>,
          std::tuple<float, double>>({1, false}, {'a', 'b'}, {1.5f, 99.05});

  auto it = vec.begin();
  auto tuple = *it;

  std::pair<int, bool> a = std::get<0>(tuple);
  std::array<char, 2> b = std::get<1>(tuple);
  std::tuple<float, double> c = std::get<2>(tuple);

  auto a_0 = std::get<0>(a);
  auto a_1 = std::get<1>(a);

  auto b_0 = std::get<0>(b);
  auto b_1 = std::get<1>(b);

  auto c_0 = std::get<0>(c);
  auto c_1 = std::get<1>(c);

  // std::cerr << "a_0=" << a_0 << " a_1=" << a_1 << std::endl;

  RC_ASSERT(a_0 == 1);
  RC_ASSERT(a_1 == false);

  // std::cerr << "b_0=" << b_0 << " b_1=" << b_1 << std::endl;

  RC_ASSERT(b_0 == 'a');
  RC_ASSERT(b_1 == 'b');

  // std::cerr << "c_0=" << c_0 << " c_1=" << c_1 << std::endl;

  RC_ASSERT(c_0 == 1.5f);
  RC_ASSERT(c_1 == 99.05);
}

TEST(SlabRuntime, SlabVectorOfStringsWorks) {
  auto storage = hyde::rt::CreateSlabStorage();
  hyde::rt::TypedSlabVector<std::string> vec(*storage, 0u);
  std::string x;
  for (auto i = 0; i < ::hyde::rt::kSlabSize; ++i) {
    x.push_back("abcdefghijklmnopqrstuvwxyz0123456789"[i % 36]);
  }
  vec.Add(x);
  vec.Add(x);

  for (auto [str] : vec) {

    // This tests that we can do a serialization-based comparison.
    auto eq = str == x;
    RC_ASSERT(eq);
  }
}

TEST(SlabRuntime, AllMemoryIsFreed) {
  auto storage = hyde::rt::CreateSlabStorage();

  // Making a new storage provider does not allocate any slabs.
  auto stats = hyde::rt::GarbageCollect(*storage);
  RC_ASSERT(stats.num_allocated_slabs == 0u);
  RC_ASSERT(stats.num_free_slabs == 0u);
  RC_ASSERT(stats.num_open_slabs == 0u);

  {
    hyde::rt::TypedSlabVector<int> vec(*storage, 0u);

    // Making an empty vector does not create any slabs.
    stats = hyde::rt::GarbageCollect(*storage);
    RC_ASSERT(stats.num_allocated_slabs == 0u);
    RC_ASSERT(stats.num_free_slabs == 0u);
    RC_ASSERT(stats.num_open_slabs == 0u);

    vec.Add(1);

    // Adding something uses one slab, and without the vector going out of
    // scope, there are no free or semi-used slabs.
    stats = hyde::rt::GarbageCollect(*storage);
    RC_ASSERT(stats.num_allocated_slabs == 1u);
    RC_ASSERT(stats.num_free_slabs == 0u);
    RC_ASSERT(stats.num_open_slabs == 0u);
  }

  // The vector going out of scope free the one allocated slab.
  stats = hyde::rt::GarbageCollect(*storage);
  RC_ASSERT(stats.num_allocated_slabs == 1u);
  RC_ASSERT(stats.num_free_slabs == 1u);
  RC_ASSERT(stats.num_open_slabs == 0u);
}


TEST(SlabRuntime, AllMemoryIsFreedWithScopedIteration) {
  auto storage = hyde::rt::CreateSlabStorage();

  // Making a new storage provider does not allocate any slabs.
  auto stats = hyde::rt::GarbageCollect(*storage);
  RC_ASSERT(stats.num_allocated_slabs == 0u);
  RC_ASSERT(stats.num_free_slabs == 0u);
  RC_ASSERT(stats.num_open_slabs == 0u);

  {
    hyde::rt::TypedSlabVector<std::string> vec(*storage, 0u);

    // Making an empty vector does not create any slabs.
    stats = hyde::rt::GarbageCollect(*storage);
    RC_ASSERT(stats.num_allocated_slabs == 0u);
    RC_ASSERT(stats.num_free_slabs == 0u);
    RC_ASSERT(stats.num_open_slabs == 0u);

    vec.Add("hello");

    // Adding something uses one slab, and without the vector going out of
    // scope, there are no free or semi-used slabs.
    stats = hyde::rt::GarbageCollect(*storage);
    RC_ASSERT(stats.num_allocated_slabs == 1u);
    RC_ASSERT(stats.num_free_slabs == 0u);
    RC_ASSERT(stats.num_open_slabs == 0u);

    auto num_iters = 0u;
    for (auto [ref] : vec) {
      stats = hyde::rt::GarbageCollect(*storage);
      RC_ASSERT(stats.num_allocated_slabs == 1u);
      RC_ASSERT(stats.num_free_slabs == 0u);
      RC_ASSERT(stats.num_open_slabs == 0u);

      (void) ref;
      ++num_iters;
    }

    RC_ASSERT(num_iters == 1u);
  }

  // The vector going out of scope free the one allocated slab.
  stats = hyde::rt::GarbageCollect(*storage);
  RC_ASSERT(stats.num_allocated_slabs == 1u);
  RC_ASSERT(stats.num_free_slabs == 1u);
  RC_ASSERT(stats.num_open_slabs == 0u);
}

TEST(SlabRuntime, AllMemoryIsFreedRefEscapesByCopy) {
  auto storage = hyde::rt::CreateSlabStorage();

  // Making a new storage provider does not allocate any slabs.
  auto stats = hyde::rt::GarbageCollect(*storage);
  RC_ASSERT(stats.num_allocated_slabs == 0u);
  RC_ASSERT(stats.num_free_slabs == 0u);
  RC_ASSERT(stats.num_open_slabs == 0u);

  hyde::rt::TypedSlabReference<std::string> escaped_ref;

  {
    hyde::rt::TypedSlabVector<std::string> vec(*storage, 0u);

    // Making an empty vector does not create any slabs.
    stats = hyde::rt::GarbageCollect(*storage);
    RC_ASSERT(stats.num_allocated_slabs == 0u);
    RC_ASSERT(stats.num_free_slabs == 0u);
    RC_ASSERT(stats.num_open_slabs == 0u);

    vec.Add("hello");

    // Adding something uses one slab, and without the vector going out of
    // scope, there are no free or semi-used slabs.
    stats = hyde::rt::GarbageCollect(*storage);
    RC_ASSERT(stats.num_allocated_slabs == 1u);
    RC_ASSERT(stats.num_free_slabs == 0u);
    RC_ASSERT(stats.num_open_slabs == 0u);

    auto num_iters = 0u;
    for (auto [ref] : vec) {
      RC_ASSERT(ref == "hello");

      stats = hyde::rt::GarbageCollect(*storage);
      RC_ASSERT(stats.num_allocated_slabs == 1u);
      RC_ASSERT(stats.num_free_slabs == 0u);
      RC_ASSERT(stats.num_open_slabs == 0u);

      escaped_ref = ref;
      ++num_iters;
    }

    RC_ASSERT(num_iters == 1u);
  }

  // The reference to the string keeps a reference to the backing slab open.
  stats = hyde::rt::GarbageCollect(*storage);
  RC_ASSERT(stats.num_allocated_slabs == 1u);
  RC_ASSERT(stats.num_free_slabs == 0u);
  RC_ASSERT(stats.num_open_slabs == 1u);


  RC_ASSERT(escaped_ref == "hello");
  std::string str_val = escaped_ref;
  RC_ASSERT(str_val == "hello");

  escaped_ref.Clear();

  // Clearing the escaped reference frees up all pages.
  stats = hyde::rt::GarbageCollect(*storage);
  RC_ASSERT(stats.num_allocated_slabs == 1u);
  RC_ASSERT(stats.num_free_slabs == 1u);
  RC_ASSERT(stats.num_open_slabs == 0u);
}


TEST(SlabRuntime, AllMemoryIsFreedRefEscapesByMove) {
  auto storage = hyde::rt::CreateSlabStorage();

  // Making a new storage provider does not allocate any slabs.
  auto stats = hyde::rt::GarbageCollect(*storage);
  RC_ASSERT(stats.num_allocated_slabs == 0u);
  RC_ASSERT(stats.num_free_slabs == 0u);
  RC_ASSERT(stats.num_open_slabs == 0u);

  hyde::rt::TypedSlabReference<std::string> escaped_ref;

  {
    hyde::rt::TypedSlabVector<std::string> vec(*storage, 0u);

    // Making an empty vector does not create any slabs.
    stats = hyde::rt::GarbageCollect(*storage);
    RC_ASSERT(stats.num_allocated_slabs == 0u);
    RC_ASSERT(stats.num_free_slabs == 0u);
    RC_ASSERT(stats.num_open_slabs == 0u);

    vec.Add("hello");

    // Adding something uses one slab, and without the vector going out of
    // scope, there are no free or semi-used slabs.
    stats = hyde::rt::GarbageCollect(*storage);
    RC_ASSERT(stats.num_allocated_slabs == 1u);
    RC_ASSERT(stats.num_free_slabs == 0u);
    RC_ASSERT(stats.num_open_slabs == 0u);

    auto num_iters = 0u;
    for (auto [ref] : vec) {
      RC_ASSERT(ref == "hello");

      stats = hyde::rt::GarbageCollect(*storage);
      RC_ASSERT(stats.num_allocated_slabs == 1u);
      RC_ASSERT(stats.num_free_slabs == 0u);
      RC_ASSERT(stats.num_open_slabs == 0u);

      escaped_ref = std::move(ref);
      ++num_iters;
    }

    RC_ASSERT(num_iters == 1u);
  }

  // The reference to the string keeps a reference to the backing slab open.
  stats = hyde::rt::GarbageCollect(*storage);
  RC_ASSERT(stats.num_allocated_slabs == 1u);
  RC_ASSERT(stats.num_free_slabs == 0u);
  RC_ASSERT(stats.num_open_slabs == 1u);


  RC_ASSERT(escaped_ref == "hello");
  std::string str_val = escaped_ref;
  RC_ASSERT(str_val == "hello");

  escaped_ref.Clear();

  // Clearing the escaped reference frees up all pages.
  stats = hyde::rt::GarbageCollect(*storage);
  RC_ASSERT(stats.num_allocated_slabs == 1u);
  RC_ASSERT(stats.num_free_slabs == 1u);
  RC_ASSERT(stats.num_open_slabs == 0u);
}
