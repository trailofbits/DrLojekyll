// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include <drlojekyll/Lex/StringPool.h>

#include <string>

namespace hyde {

class StringPool::Impl {
 public:
  Impl(void) {
    pool.reserve(8192);
    pool.push_back('\0');
    pool.push_back('_');
  }

  std::string pool;
};

StringPool::~StringPool(void) {}

StringPool::StringPool(void)
    : impl(std::make_shared<Impl>()) {}

// Intern a string into the pool, returning its offset in the pool.
unsigned StringPool::InternString(std::string_view data, bool force) const {
  if (force) {
    auto pos = impl->pool.size();
    impl->pool.insert(impl->pool.end(), data.begin(), data.end());
    impl->pool.push_back('\0');
    return static_cast<unsigned>(pos);

  } else {
    size_t pos = 1;
    while (pos != std::string::npos) {
      pos = impl->pool.find(data, pos);
      if (std::string::npos == pos ||
          (pos + data.size()) >= impl->pool.size()) {
        break;

      // Make sure we match on a trailing NUL.
      } else if (!impl->pool[pos+data.size()]) {
        return static_cast<unsigned>(pos);

      } else {
        pos += 1;
      }
    }

    pos = impl->pool.size();
    impl->pool.insert(impl->pool.end(), data.begin(), data.end());
    impl->pool.push_back('\0');
    return static_cast<unsigned>(pos);
  }
}

// Read out some string.
bool StringPool::TryReadString(
    unsigned index, unsigned len, std::string_view *data_out) const {
  if (!index || impl->pool.size() < (index + len)) {
    return false;
  } else if (data_out) {
    *data_out = impl->pool;
    *data_out = data_out->substr(index, len);
    return true;
  } else {
    return true;
  }
}

}  // namespace hyde
