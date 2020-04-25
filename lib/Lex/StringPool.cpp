// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include <drlojekyll/Lex/StringPool.h>

#include <string>
#include <vector>

namespace hyde {

class StringPool::Impl {
 public:
  Impl(void) {
    pool.reserve(8192);
    pool.push_back('\0');
    pool.push_back('_');
  }

  std::string pool;
  std::vector<std::string> code_blocks;
};

StringPool::~StringPool(void) {}

StringPool::StringPool(void)
    : impl(std::make_shared<Impl>()) {}

// Intern a code block into the pool, returning its ID.
unsigned StringPool::InternCode(std::string_view code) const {
  auto id = static_cast<unsigned>(impl->code_blocks.size() + 1);
  impl->code_blocks.emplace_back(code);
  return id;
}

// Read out some code block given its ID.
bool StringPool::TryReadCode(unsigned id_, std::string_view *code_out) const {
  if (!id_) {
    return false;
  }
  const auto id = id_ - 1u;
  if (id >= impl->code_blocks.size()) {
    return false;
  }

  *code_out = impl->code_blocks[id];
  return true;
}

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

// Read out some string given its index and length.
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
