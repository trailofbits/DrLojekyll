// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/Util.h>

#include <algorithm>
#include <set>

namespace hyde {

class EqualitySet::Impl {
 public:
  std::set<std::pair<uintptr_t, uintptr_t>> equalities;
};

EqualitySet::EqualitySet(void) : impl(new Impl) {}

EqualitySet::~EqualitySet(void) {}

void EqualitySet::Insert(const void *a_, const void *b_) noexcept {
  if (a_ != b_) {
    const auto a = reinterpret_cast<uintptr_t>(a_);
    const auto b = reinterpret_cast<uintptr_t>(b_);
    impl->equalities.emplace(std::min(a, b), std::max(a, b));
  }
}

void EqualitySet::Remove(const void *a_, const void *b_) noexcept {
  if (a_ != b_) {
    const auto a = reinterpret_cast<uintptr_t>(a_);
    const auto b = reinterpret_cast<uintptr_t>(b_);
    impl->equalities.erase(std::make_pair(std::min(a, b), std::max(a, b)));
  }
}

bool EqualitySet::Contains(const void *a_, const void *b_) const noexcept {
  if (a_ == b_) {
    return true;
  }
  const auto a = reinterpret_cast<uintptr_t>(a_);
  const auto b = reinterpret_cast<uintptr_t>(b_);
  const auto res = std::make_pair(std::min(a, b), std::max(a, b));
  return impl->equalities.count(res) == 1;
}

void EqualitySet::Clear(void) noexcept {
  impl->equalities.clear();
}

}  // namespace hyde
