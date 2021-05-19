//// Copyright 2021, Trail of Bits, Inc. All rights reserved.
//
//#include <drlojekyll/Runtime/SlabVector.h>
//#include <drlojekyll/Runtime/SlabManager.h>
//#include <drlojekyll/Runtime/SlabStorage.h>
//#include <drlojekyll/Runtime/SlabTable.h>
//
//#include "UnitTests.h"
//
//TEST(SlabRuntime, TinyInMemorySlabStore) {
//  auto maybe_storage = hyde::rt::CreateInMemorySlabManager(
//      hyde::rt::SlabStoreSize::kTiny);
//  RC_ASSERT(maybe_storage.Succeeded());
//}
//
//TEST(SlabRuntime, SmallInMemorySlabStore) {
//  auto maybe_storage = hyde::rt::CreateInMemorySlabManager(
//      hyde::rt::SlabStoreSize::kSmall);
//  RC_ASSERT(maybe_storage.Succeeded());
//}
//
//TEST(SlabRuntime, MediumInMemorySlabStore) {
//  auto maybe_storage = hyde::rt::CreateInMemorySlabManager(
//      hyde::rt::SlabStoreSize::kMedium);
//  RC_ASSERT(maybe_storage.Succeeded());
//}
//
//TEST(SlabRuntime, LargeInMemorySlabStore) {
//  auto maybe_storage = hyde::rt::CreateInMemorySlabManager(
//      hyde::rt::SlabStoreSize::kLarge);
//  RC_ASSERT(maybe_storage.Succeeded());
//}
//
//TEST(SlabRuntime, ExtraLargeInMemorySlabStore) {
//  auto maybe_storage = hyde::rt::CreateInMemorySlabManager(
//      hyde::rt::SlabStoreSize::kExtraLarge);
//  RC_ASSERT(maybe_storage.Succeeded());
//}
//
//TEST(SlabRuntime, HugeInMemorySlabStore) {
//  auto maybe_storage = hyde::rt::CreateInMemorySlabManager(
//      hyde::rt::SlabStoreSize::kHuge);
//  RC_ASSERT(maybe_storage.Succeeded());
//}
//
//TEST(SlabRuntime, SlabVectorOfU8sWorks) {
//  auto maybe_storage = hyde::rt::CreateInMemorySlabManager(
//      hyde::rt::SlabStoreSize::kTiny);
//  RC_ASSERT(maybe_storage.Succeeded());
//
//  auto storage = maybe_storage.TakeValue();
//  hyde::rt::TypedSlabVector<uint8_t> vec(*storage, 0u);
//  vec.Add<uint8_t>(0xAA);
//  vec.Add<uint8_t>(0x11);
//  vec.Add<uint8_t>(0xBB);
//
//  auto index = 0u;
//  for (auto [val_ref] : vec) {
//    uint8_t val = val_ref;
//    switch (index++) {
//      case 0: RC_ASSERT(val == 0xAAu); break;
//      case 1: RC_ASSERT(val == 0x11u); break;
//      case 2: RC_ASSERT(val == 0xBBu); break;
//      default: break;
//    }
//  }
//
//  RC_ASSERT(index == 3u);
//}
//
////TEST(SlabRuntime, PersistentSlabVectorOfU8sWorks) {
////  auto path = std::filesystem::temp_directory_path() /
////              "PersistentSlabVectorOfU8sWorks.tmp";
////
//////  std::cerr << "Path is: " << path.string();
////
////  auto maybe_storage = hyde::rt::CreateSlabStorage(
////      hyde::rt::FileBackedSlabStore{path},
////      hyde::rt::SlabStoreSize::kTiny);
////  RC_ASSERT(maybe_storage.Succeeded());
////
////  auto storage = maybe_storage.TakeValue();
////  hyde::rt::PersistentTypedSlabVector<uint8_t> vec(*storage, 0u);
////  vec.Add<uint8_t>(0xAA);
////  vec.Add<uint8_t>(0x11);
////  vec.Add<uint8_t>(0xBB);
////
////  auto index = 0u;
////  for (auto [val_ref] : vec) {
////    uint8_t val = val_ref;
////    switch (index++) {
////      case 0: RC_ASSERT(val == 0xAAu); break;
////      case 1: RC_ASSERT(val == 0x11u); break;
////      case 2: RC_ASSERT(val == 0xBBu); break;
////      default: break;
////    }
////  }
////
////  RC_ASSERT(index == 3u);
////}
//
//TEST(SlabRuntime, SlabVectorOfU16sWorks) {
//  auto maybe_storage = hyde::rt::CreateInMemorySlabManager(
//      hyde::rt::SlabStoreSize::kTiny);
//  RC_ASSERT(maybe_storage.Succeeded());
//
//  auto storage = maybe_storage.TakeValue();
//  hyde::rt::TypedSlabVector<uint16_t> vec(*storage, 0u);
//  vec.Add<uint16_t>(0xAA11);
//  vec.Add<uint16_t>(0xBB22);
//  vec.Add<uint16_t>(0xCC33);
//
//  auto index = 0u;
//  for (auto [val_ref] : vec) {
//    uint16_t val = val_ref;
//    switch (index++) {
//      case 0: RC_ASSERT(val == 0xAA11u); break;
//      case 1: RC_ASSERT(val == 0xBB22u); break;
//      case 2: RC_ASSERT(val == 0xCC33u); break;
//      default: break;
//    }
//  }
//
//  RC_ASSERT(index == 3u);
//}
//
//TEST(SlabRuntime, SlabVectorOfU32sWorks) {
//  auto maybe_storage = hyde::rt::CreateInMemorySlabManager(
//      hyde::rt::SlabStoreSize::kTiny);
//  RC_ASSERT(maybe_storage.Succeeded());
//
//  auto storage = maybe_storage.TakeValue();
//  hyde::rt::TypedSlabVector<uint32_t> vec(*storage, 0u);
//  vec.Add<uint32_t>(0xAABBCCDDu);
//  vec.Add<uint32_t>(0x11223344u);
//  vec.Add<uint32_t>(0xaa11bb22u);
//
//  auto index = 0u;
//  for (auto [val_ref] : vec) {
//    uint32_t val = val_ref;
//    switch (index++) {
//      case 0: RC_ASSERT(val == 0xAABBCCDDu); break;
//      case 1: RC_ASSERT(val == 0x11223344u); break;
//      case 2: RC_ASSERT(val == 0xaa11bb22u); break;
//      default: break;
//    }
//  }
//
//  RC_ASSERT(index == 3u);
//}
//
//TEST(SlabRuntime, SlabVectorOfU64sWorks) {
//  auto maybe_storage = hyde::rt::CreateInMemorySlabManager(
//      hyde::rt::SlabStoreSize::kTiny);
//  RC_ASSERT(maybe_storage.Succeeded());
//
//  auto storage = maybe_storage.TakeValue();
//  hyde::rt::TypedSlabVector<uint64_t> vec(*storage, 0u);
//  vec.Add<uint64_t>(0xAABBCCDDull);
//  vec.Add<uint64_t>(0x1122334400000000ull);
//  vec.Add<uint64_t>(0xaa001100bb002200ull);
//
//  auto index = 0u;
//  for (auto [val_ref] : vec) {
//    uint64_t val = val_ref;
//    switch (index++) {
//      case 0: RC_ASSERT(val == 0xAABBCCDDull); break;
//      case 1: RC_ASSERT(val == 0x1122334400000000ull); break;
//      case 2: RC_ASSERT(val == 0xaa001100bb002200ull); break;
//      default: break;
//    }
//  }
//
//  RC_ASSERT(index == 3u);
//}
//
//TEST(SlabRuntime, SlabVectorOfF32sWorks) {
//  auto maybe_storage = hyde::rt::CreateInMemorySlabManager(
//      hyde::rt::SlabStoreSize::kTiny);
//  RC_ASSERT(maybe_storage.Succeeded());
//
//  auto storage = maybe_storage.TakeValue();
//  hyde::rt::TypedSlabVector<float> vec(*storage, 0u);
//  vec.Add<float>(1.0f);
//  vec.Add<float>(99.0f);
//  vec.Add<float>(0.0001f);
//
//  auto index = 0u;
//  for (auto [val_ref] : vec) {
//    float val = val_ref;
//    switch (index++) {
//      case 0: RC_ASSERT(val == 1.0f); break;
//      case 1: RC_ASSERT(val == 99.0f); break;
//      case 2: RC_ASSERT(val == 0.0001f); break;
//      default: break;
//    }
//  }
//
//  RC_ASSERT(index == 3u);
//}
//
//TEST(SlabRuntime, SlabVectorOfF64sWorks) {
//  auto maybe_storage = hyde::rt::CreateInMemorySlabManager(
//      hyde::rt::SlabStoreSize::kTiny);
//  RC_ASSERT(maybe_storage.Succeeded());
//
//  auto storage = maybe_storage.TakeValue();
//  hyde::rt::TypedSlabVector<double> vec(*storage, 0u);
//  vec.Add<double>(1.0);
//  vec.Add<double>(99.0);
//  vec.Add<double>(0.0001);
//
//  auto index = 0u;
//  for (auto [val_ref] : vec) {
//    double val = val_ref;
//    switch (index++) {
//      case 0: RC_ASSERT(val == 1.0); break;
//      case 1: RC_ASSERT(val == 99.0); break;
//      case 2: RC_ASSERT(val == 0.0001); break;
//      default: break;
//    }
//  }
//
//  RC_ASSERT(index == 3u);
//}
//
//
//TEST(SlabRuntime, SlabVectorOfPairsWorks) {
//  auto maybe_storage = hyde::rt::CreateInMemorySlabManager(
//      hyde::rt::SlabStoreSize::kTiny);
//  RC_ASSERT(maybe_storage.Succeeded());
//
//  auto storage = maybe_storage.TakeValue();
//  hyde::rt::TypedSlabVector<double, int> vec(*storage, 0u);
//  vec.Add<double, int>(1.0, 11111);
//  vec.Add<double, int>(99.0, 22222);
//  vec.Add<double, int>(0.0001, 33333);
//
//  auto index = 0u;
//  for (auto [v1_ref, v2_ref] : vec) {
//    double v1 = v1_ref;
//    int v2 = v2_ref;
//    switch (index++) {
//      case 0:
//        RC_ASSERT(v1 == 1.0);
//        RC_ASSERT(v2 == 11111);
//        break;
//      case 1:
//        RC_ASSERT(v1 == 99.0);
//        RC_ASSERT(v2 == 22222);
//        break;
//      case 2:
//        RC_ASSERT(v1 == 0.0001);
//        RC_ASSERT(v2 == 33333);
//        break;
//      default: break;
//    }
//  }
//
//  RC_ASSERT(index == 3u);
//}
//
//TEST(SlabRuntime, SlabVectorOfTriplesOfPairsWorks) {
//  auto maybe_storage = hyde::rt::CreateInMemorySlabManager(
//      hyde::rt::SlabStoreSize::kTiny);
//  RC_ASSERT(maybe_storage.Succeeded());
//
//  auto storage = maybe_storage.TakeValue();
//  hyde::rt::TypedSlabVector<std::pair<int, bool>, std::array<char, 2>,
//                            std::tuple<float, double>>
//      vec(*storage, 0u);
//  vec.Add<std::pair<int, bool>, std::array<char, 2>, std::tuple<float, double>>(
//      {1, false}, {'a', 'b'}, {1.5f, 99.05});
//
//  auto it = vec.begin();
//  auto tuple = *it;
//
//  std::pair<int, bool> a = std::get<0>(tuple);
//  std::array<char, 2> b = std::get<1>(tuple);
//  std::tuple<float, double> c = std::get<2>(tuple);
//
//  auto a_0 = std::get<0>(a);
//  auto a_1 = std::get<1>(a);
//
//  auto b_0 = std::get<0>(b);
//  auto b_1 = std::get<1>(b);
//
//  auto c_0 = std::get<0>(c);
//  auto c_1 = std::get<1>(c);
//
//  // std::cerr << "a_0=" << a_0 << " a_1=" << a_1 << std::endl;
//
//  RC_ASSERT(a_0 == 1);
//  RC_ASSERT(a_1 == false);
//
//  // std::cerr << "b_0=" << b_0 << " b_1=" << b_1 << std::endl;
//
//  RC_ASSERT(b_0 == 'a');
//  RC_ASSERT(b_1 == 'b');
//
//  // std::cerr << "c_0=" << c_0 << " c_1=" << c_1 << std::endl;
//
//  RC_ASSERT(c_0 == 1.5f);
//  RC_ASSERT(c_1 == 99.05);
//}
//
//TEST(SlabRuntime, SlabVectorOfStringsWorks) {
//  auto maybe_storage = hyde::rt::CreateInMemorySlabManager(
//      hyde::rt::SlabStoreSize::kTiny);
//  RC_ASSERT(maybe_storage.Succeeded());
//
//  auto storage = maybe_storage.TakeValue();
//  hyde::rt::TypedSlabVector<std::string> vec(*storage, 0u);
//  std::string x;
//  for (auto i = 0u; i < ::hyde::rt::kSlabSize; ++i) {
//    x.push_back("abcdefghijklmnopqrstuvwxyz0123456789"[i % 36]);
//  }
//  vec.Add(x);
//  vec.Add(x);
//
//  for (auto [str] : vec) {
//
//    // This tests that we can do a serialization-based comparison.
//    auto eq = str == x;
//    RC_ASSERT(eq);
//  }
//}
//
//TEST(SlabRuntime, AllMemoryIsFreed) {
//  auto maybe_storage = hyde::rt::CreateInMemorySlabManager(
//      hyde::rt::SlabStoreSize::kTiny);
//  RC_ASSERT(maybe_storage.Succeeded());
//
//  auto storage = maybe_storage.TakeValue();
//
//  // Making a new storage provider does not allocate any slabs.
//  auto stats = hyde::rt::GarbageCollect(*storage);
//  RC_ASSERT(stats.num_allocated_slabs == 0u);
//  RC_ASSERT(stats.num_free_slabs == 0u);
//  RC_ASSERT(stats.num_open_slabs == 0u);
//
//  {
//    hyde::rt::TypedSlabVector<int> vec(*storage, 0u);
//
//    // Making an empty vector does not create any slabs.
//    stats = hyde::rt::GarbageCollect(*storage);
//    RC_ASSERT(stats.num_allocated_slabs == 0u);
//    RC_ASSERT(stats.num_free_slabs == 0u);
//    RC_ASSERT(stats.num_open_slabs == 0u);
//
//    vec.Add(1);
//
//    // Adding something uses one slab, and without the vector going out of
//    // scope, there are no free or semi-used slabs.
//    stats = hyde::rt::GarbageCollect(*storage);
//    RC_ASSERT(stats.num_allocated_slabs == 1u);
//    RC_ASSERT(stats.num_free_slabs == 0u);
//    RC_ASSERT(stats.num_open_slabs == 0u);
//  }
//
//  // The vector going out of scope free the one allocated slab.
//  stats = hyde::rt::GarbageCollect(*storage);
//  RC_ASSERT(stats.num_allocated_slabs == 1u);
//  RC_ASSERT(stats.num_free_slabs == 1u);
//  RC_ASSERT(stats.num_open_slabs == 0u);
//}
//
//
//TEST(SlabRuntime, AllMemoryIsFreedWithScopedIteration) {
//  auto maybe_storage = hyde::rt::CreateInMemorySlabManager(
//      hyde::rt::SlabStoreSize::kTiny);
//  RC_ASSERT(maybe_storage.Succeeded());
//
//  auto storage = maybe_storage.TakeValue();
//
//  // Making a new storage provider does not allocate any slabs.
//  auto stats = hyde::rt::GarbageCollect(*storage);
//  RC_ASSERT(stats.num_allocated_slabs == 0u);
//  RC_ASSERT(stats.num_free_slabs == 0u);
//  RC_ASSERT(stats.num_open_slabs == 0u);
//
//  {
//    hyde::rt::TypedSlabVector<std::string> vec(*storage, 0u);
//
//    // Making an empty vector does not create any slabs.
//    stats = hyde::rt::GarbageCollect(*storage);
//    RC_ASSERT(stats.num_allocated_slabs == 0u);
//    RC_ASSERT(stats.num_free_slabs == 0u);
//    RC_ASSERT(stats.num_open_slabs == 0u);
//
//    vec.Add("hello");
//
//    // Adding something uses one slab, and without the vector going out of
//    // scope, there are no free or semi-used slabs.
//    stats = hyde::rt::GarbageCollect(*storage);
//    RC_ASSERT(stats.num_allocated_slabs == 1u);
//    RC_ASSERT(stats.num_free_slabs == 0u);
//    RC_ASSERT(stats.num_open_slabs == 0u);
//
//    auto num_iters = 0u;
//    for (auto [ref] : vec) {
//      stats = hyde::rt::GarbageCollect(*storage);
//      RC_ASSERT(stats.num_allocated_slabs == 1u);
//      RC_ASSERT(stats.num_free_slabs == 0u);
//      RC_ASSERT(stats.num_open_slabs == 0u);
//
//      (void) ref;
//      ++num_iters;
//    }
//
//    RC_ASSERT(num_iters == 1u);
//  }
//
//  // The vector going out of scope free the one allocated slab.
//  stats = hyde::rt::GarbageCollect(*storage);
//  RC_ASSERT(stats.num_allocated_slabs == 1u);
//  RC_ASSERT(stats.num_free_slabs == 1u);
//  RC_ASSERT(stats.num_open_slabs == 0u);
//}
//
//TEST(SlabRuntime, AllMemoryIsFreedRefEscapesByCopy) {
//  auto maybe_storage = hyde::rt::CreateInMemorySlabManager(
//      hyde::rt::SlabStoreSize::kTiny);
//  RC_ASSERT(maybe_storage.Succeeded());
//
//  auto storage = maybe_storage.TakeValue();
//
//  // Making a new storage provider does not allocate any slabs.
//  auto stats = hyde::rt::GarbageCollect(*storage);
//  RC_ASSERT(stats.num_allocated_slabs == 0u);
//  RC_ASSERT(stats.num_free_slabs == 0u);
//  RC_ASSERT(stats.num_open_slabs == 0u);
//
//  hyde::rt::TypedSlabReference<std::string> escaped_ref;
//
//  {
//    hyde::rt::TypedSlabVector<std::string> vec(*storage, 0u);
//
//    // Making an empty vector does not create any slabs.
//    stats = hyde::rt::GarbageCollect(*storage);
//    RC_ASSERT(stats.num_allocated_slabs == 0u);
//    RC_ASSERT(stats.num_free_slabs == 0u);
//    RC_ASSERT(stats.num_open_slabs == 0u);
//
//    vec.Add("hello");
//
//    // Adding something uses one slab, and without the vector going out of
//    // scope, there are no free or semi-used slabs.
//    stats = hyde::rt::GarbageCollect(*storage);
//    RC_ASSERT(stats.num_allocated_slabs == 1u);
//    RC_ASSERT(stats.num_free_slabs == 0u);
//    RC_ASSERT(stats.num_open_slabs == 0u);
//
//    auto num_iters = 0u;
//    for (auto [ref] : vec) {
//      RC_ASSERT(ref == "hello");
//
//      stats = hyde::rt::GarbageCollect(*storage);
//      RC_ASSERT(stats.num_allocated_slabs == 1u);
//      RC_ASSERT(stats.num_free_slabs == 0u);
//      RC_ASSERT(stats.num_open_slabs == 0u);
//
//      escaped_ref = ref;
//      ++num_iters;
//    }
//
//    RC_ASSERT(num_iters == 1u);
//  }
//
//  // The reference to the string keeps a reference to the backing slab open.
//  stats = hyde::rt::GarbageCollect(*storage);
//  RC_ASSERT(stats.num_allocated_slabs == 1u);
//  RC_ASSERT(stats.num_free_slabs == 0u);
//  RC_ASSERT(stats.num_open_slabs == 1u);
//
//
//  RC_ASSERT(escaped_ref == "hello");
//  std::string str_val = escaped_ref;
//  RC_ASSERT(str_val == "hello");
//
//  escaped_ref.Clear();
//
//  // Clearing the escaped reference frees up all pages.
//  stats = hyde::rt::GarbageCollect(*storage);
//  RC_ASSERT(stats.num_allocated_slabs == 1u);
//  RC_ASSERT(stats.num_free_slabs == 1u);
//  RC_ASSERT(stats.num_open_slabs == 0u);
//}
//
//
//TEST(SlabRuntime, AllMemoryIsFreedRefEscapesByMove) {
//  auto maybe_storage = hyde::rt::CreateInMemorySlabManager(
//      hyde::rt::SlabStoreSize::kTiny);
//  RC_ASSERT(maybe_storage.Succeeded());
//
//  auto storage = maybe_storage.TakeValue();
//
//  // Making a new storage provider does not allocate any slabs.
//  auto stats = hyde::rt::GarbageCollect(*storage);
//  RC_ASSERT(stats.num_allocated_slabs == 0u);
//  RC_ASSERT(stats.num_free_slabs == 0u);
//  RC_ASSERT(stats.num_open_slabs == 0u);
//
//  hyde::rt::TypedSlabReference<std::string> escaped_ref;
//
//  {
//    hyde::rt::TypedSlabVector<std::string> vec(*storage, 0u);
//
//    // Making an empty vector does not create any slabs.
//    stats = hyde::rt::GarbageCollect(*storage);
//    RC_ASSERT(stats.num_allocated_slabs == 0u);
//    RC_ASSERT(stats.num_free_slabs == 0u);
//    RC_ASSERT(stats.num_open_slabs == 0u);
//
//    vec.Add("hello");
//
//    // Adding something uses one slab, and without the vector going out of
//    // scope, there are no free or semi-used slabs.
//    stats = hyde::rt::GarbageCollect(*storage);
//    RC_ASSERT(stats.num_allocated_slabs == 1u);
//    RC_ASSERT(stats.num_free_slabs == 0u);
//    RC_ASSERT(stats.num_open_slabs == 0u);
//
//    auto num_iters = 0u;
//    for (auto [ref] : vec) {
//      RC_ASSERT(ref == "hello");
//
//      stats = hyde::rt::GarbageCollect(*storage);
//      RC_ASSERT(stats.num_allocated_slabs == 1u);
//      RC_ASSERT(stats.num_free_slabs == 0u);
//      RC_ASSERT(stats.num_open_slabs == 0u);
//
//      escaped_ref = std::move(ref);
//      ++num_iters;
//    }
//
//    RC_ASSERT(num_iters == 1u);
//  }
//
//  // The reference to the string keeps a reference to the backing slab open.
//  stats = hyde::rt::GarbageCollect(*storage);
//  RC_ASSERT(stats.num_allocated_slabs == 1u);
//  RC_ASSERT(stats.num_free_slabs == 0u);
//  RC_ASSERT(stats.num_open_slabs == 1u);
//
//
//  RC_ASSERT(escaped_ref == "hello");
//  std::string str_val = escaped_ref;
//  RC_ASSERT(str_val == "hello");
//
//  escaped_ref.Clear();
//
//  // Clearing the escaped reference frees up all pages.
//  stats = hyde::rt::GarbageCollect(*storage);
//  RC_ASSERT(stats.num_allocated_slabs == 1u);
//  RC_ASSERT(stats.num_free_slabs == 1u);
//  RC_ASSERT(stats.num_open_slabs == 0u);
//}
//
//TEST(SlabRuntime, PointerToAddressableTest) {
//  using namespace hyde::rt;
//
//  auto maybe_storage =
//      CreateInMemorySlabManager(SlabStoreSize::kTiny);
//  RC_ASSERT(maybe_storage.Succeeded());
//
//  auto storage = maybe_storage.TakeValue();
//  TypedSlabVector<Addressable<int>> vec1(*storage, 0u);
//  TypedSlabVector<int *> vec2(*storage, 0u);
//  vec1.Add(111);
//  vec1.Add(222);
//  vec1.Add(333);
//
//  auto num_iters = 0;
//  for (auto [int_ref] : vec1) {
//    vec2.Add(&int_ref);
//    ++num_iters;
//  }
//
//  RC_ASSERT(num_iters == 3);
//
//  num_iters = 0;
//  for (auto [int_ptr] : vec2) {
//    auto int_ref = *int_ptr;
//    int int_val = int_ref;
//
//    switch (num_iters) {
//      case 0: RC_ASSERT(int_val == 111); break;
//      case 1: RC_ASSERT(int_val == 222); break;
//      case 2: RC_ASSERT(int_val == 333); break;
//    }
//
//    ++num_iters;
//  }
//
//  RC_ASSERT(num_iters == 3);
//}
//
//TEST(SlabRuntime, PointerToMutableTest) {
//  using namespace hyde::rt;
//
//  auto maybe_storage =
//      CreateInMemorySlabManager(SlabStoreSize::kTiny);
//  RC_ASSERT(maybe_storage.Succeeded());
//
//  auto storage = maybe_storage.TakeValue();
//  TypedSlabVector<Mutable<int>> vec1(*storage, 0u);
//  TypedSlabVector<int *> vec2(*storage, 0u);
//  vec1.Add(111);
//  vec1.Add(222);
//  vec1.Add(333);
//
//  auto num_iters = 0;
//  for (auto [int_ref] : vec1) {
//    vec2.Add(&int_ref);
//    ++num_iters;
//  }
//
//  RC_ASSERT(num_iters == 3);
//
//  num_iters = 0;
//  for (auto [int_ptr] : vec2) {
//    auto int_ref = *int_ptr;
//    int int_val = int_ref;
//
//    switch (num_iters) {
//      case 0: RC_ASSERT(int_val == 111); break;
//      case 1: RC_ASSERT(int_val == 222); break;
//      case 2: RC_ASSERT(int_val == 333); break;
//    }
//
//    ++num_iters;
//  }
//
//  RC_ASSERT(num_iters == 3);
//
//  // Go through and mutate the underlying data.
//  for (auto [int_ref] : vec1) {
//    int_ref = int(int_ref) * 2;
//  }
//
//  // Now go back and read the mutated data.
//  num_iters = 0;
//  for (auto [int_ptr] : vec2) {
//    auto int_ref = *int_ptr;
//    int int_val = int_ref;
//
//    switch (num_iters) {
//      case 0: RC_ASSERT(int_val == 222); break;
//      case 1: RC_ASSERT(int_val == 444); break;
//      case 2: RC_ASSERT(int_val == 666); break;
//    }
//
//    ++num_iters;
//  }
//
//  RC_ASSERT(num_iters == 3);
//}
//
//namespace hyde::rt {
//
//template <>
//struct TableDescriptor<1> {
//  using ColumnIds = IdList<2, 3>;
//  using IndexIds = IdList<4, 5>;
//  static constexpr unsigned kNumColumns = 2;
//};
//
//template <>
//struct ColumnDescriptor<2> {
//  static constexpr bool kIsNamed = false;
//  static constexpr unsigned kId = 2;
//  static constexpr unsigned kTableId = 1;
//  static constexpr unsigned kOffset = 0;
//  using Type = std::string;
//};
//
//template <>
//struct ColumnDescriptor<3> {
//  static constexpr bool kIsNamed = false;
//  static constexpr unsigned kId = 3;
//  static constexpr unsigned kTableId = 1;
//  static constexpr unsigned kOffset = 1;
//  using Type = std::vector<int>;
//};
//
//template <>
//struct IndexDescriptor<4> {
//  static constexpr unsigned kTableId = 1;
//  using KeyColumnIds = IdList<2>;
//  using ValueColumnIds = IdList<3>;
//  using Columns = TypeList<KeyColumn<2>, ValueColumn<3>>;
//};
//
//template <>
//struct IndexDescriptor<5> {
//  static constexpr unsigned kTableId = 1;
//  using KeyColumnIds = IdList<3>;
//  using ValueColumnIds = IdList<2>;
//  using Columns = TypeList<ValueColumn<2>, KeyColumn<3>>;
//};
//
//}  // namespace hyde::rt
//
//TEST(SlabRuntime, TableTest) {
//  using namespace hyde::rt;
//
//  auto maybe_storage =
//      CreateInMemorySlabManager(SlabStoreSize::kTiny);
//  RC_ASSERT(maybe_storage.Succeeded());
//
//  SlabStorage storage(maybe_storage.TakeValue());
//
//  TypedSlabVector<std::string> strings(storage, 0u);
//  TypedSlabVector<std::vector<int>> numbers(storage, 0u);
//
//  strings.Add("hello");
//  strings.Add("world");
//
//  std::vector<int> nums;
//  numbers.Add(nums);
//
//  nums.push_back(1);
//  numbers.Add(nums);
//
//  nums.push_back(2);
//  numbers.Add(nums);
//
//  SlabTable<1> table(storage);
//
//  // Empty table, check for absence.
//  auto num_iters = 0;
//  for (auto [s] : strings) {
//    for (auto [n] : numbers) {
//      RC_ASSERT(table.GetState(s, n) == TupleState::kAbsent);
//      ++num_iters;
//    }
//  }
//
//  RC_ASSERT(num_iters == 6);
//
//  // Add the tuples.
//  num_iters = 0;
//  for (auto [s] : strings) {
//    for (auto [n] : numbers) {
//      RC_ASSERT(table.TryChangeStateFromAbsentOrUnknownToPresent(s, n));
//      ++num_iters;
//    }
//  }
//
//  RC_ASSERT(num_iters == 6);
//
//  // Does it seem like the tuples were added?
//  RC_ASSERT(table.Size() == (2u * 3u));
//
//  // Double check that the tuples were added.
//  num_iters = 0;
//  for (auto [s] : strings) {
//    for (auto [n] : numbers) {
//      RC_ASSERT(table.GetState(s, n) == TupleState::kPresent);
//      ++num_iters;
//    }
//  }
//
//  RC_ASSERT(num_iters == 6);
//
//  // Go read the indices
//  num_iters = 0;
//  for (auto [s] : strings) {
//    for (auto [s1, n] : table.Scan(s, IndexTag<4>{})) {
//      ++num_iters;
//    }
//  }
//
//  RC_ASSERT(num_iters == 6);
//
//  num_iters = 0;
//  for (auto [n] : numbers) {
//    for (auto [s, n1] : table.Scan(n, IndexTag<5>{})) {
//      ++num_iters;
//    }
//  }
//
//  RC_ASSERT(num_iters == 6);
//}
