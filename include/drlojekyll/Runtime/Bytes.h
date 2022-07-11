// Copyright 2021, Trail of Bits. All rights reserved.

#pragma once

#include <cassert>
#include <climits>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "Endian.h"

namespace hyde {
namespace rt {

template <typename T, typename Traits, typename Alloc>
static std::vector<uint8_t> BytesFromString(
    const std::basic_string<T, Traits, Alloc> &str) {
  std::vector<uint8_t> ret;
  ret.reserve(str.size() * sizeof(T));
  for (auto c : str) {
    if constexpr (sizeof(T) == 1u) {
      ret.push_back(static_cast<uint8_t>(c));

    // Little endian.
    } else if HYDE_RT_CONSTEXPR_ENDIAN (HYDE_RT_LITTLE_ENDIAN) {
      if constexpr (sizeof(T) == 2u) {
        ret.push_back(static_cast<uint8_t>(static_cast<uint16_t>(c) >> 0));
        ret.push_back(static_cast<uint8_t>(static_cast<uint16_t>(c) >> 8));
      } else if constexpr (sizeof(T) == 4u) {
        ret.push_back(static_cast<uint8_t>(static_cast<uint32_t>(c) >> 0));
        ret.push_back(static_cast<uint8_t>(static_cast<uint32_t>(c) >> 8));
        ret.push_back(static_cast<uint8_t>(static_cast<uint32_t>(c) >> 16));
        ret.push_back(static_cast<uint8_t>(static_cast<uint32_t>(c) >> 24));
      } else {
        static_assert(sizeof(T) == 1u || sizeof(T) == 2u || sizeof(T) == 4u);
      }

    // Big endian.
    } else {
      if constexpr (sizeof(T) == 2u) {
        ret.push_back(static_cast<uint8_t>(static_cast<uint16_t>(c) >> 8));
        ret.push_back(static_cast<uint8_t>(static_cast<uint16_t>(c) >> 0));
      } else if constexpr (sizeof(T) == 4u) {
        ret.push_back(static_cast<uint8_t>(static_cast<uint32_t>(c) >> 24));
        ret.push_back(static_cast<uint8_t>(static_cast<uint32_t>(c) >> 16));
        ret.push_back(static_cast<uint8_t>(static_cast<uint32_t>(c) >> 8));
        ret.push_back(static_cast<uint8_t>(static_cast<uint32_t>(c) >> 0));
      } else {
        static_assert(sizeof(T) == 1u || sizeof(T) == 2u || sizeof(T) == 4u);
      }
    }
  }
  return ret;
}

template <typename T>
static std::vector<uint8_t> BytesFromString(std::basic_string_view<T> str) {
  std::vector<uint8_t> ret;
  ret.reserve(str.size() * sizeof(T));
  for (auto c : str) {
    if constexpr (sizeof(T) == 1u) {
      ret.push_back(static_cast<uint8_t>(c));

    // Little endian.
    } else if HYDE_RT_CONSTEXPR_ENDIAN (HYDE_RT_LITTLE_ENDIAN) {
      if constexpr (sizeof(T) == 2u) {
        ret.push_back(static_cast<uint8_t>(static_cast<uint16_t>(c) >> 0));
        ret.push_back(static_cast<uint8_t>(static_cast<uint16_t>(c) >> 8));
      } else if constexpr (sizeof(T) == 4u) {
        ret.push_back(static_cast<uint8_t>(static_cast<uint32_t>(c) >> 0));
        ret.push_back(static_cast<uint8_t>(static_cast<uint32_t>(c) >> 8));
        ret.push_back(static_cast<uint8_t>(static_cast<uint32_t>(c) >> 16));
        ret.push_back(static_cast<uint8_t>(static_cast<uint32_t>(c) >> 24));
      } else {
        static_assert(sizeof(T) == 1u || sizeof(T) == 2u || sizeof(T) == 4u);
      }

    // Big endian.
    } else {
      if constexpr (sizeof(T) == 2u) {
        ret.push_back(static_cast<uint8_t>(static_cast<uint16_t>(c) >> 8));
        ret.push_back(static_cast<uint8_t>(static_cast<uint16_t>(c) >> 0));
      } else if constexpr (sizeof(T) == 4u) {
        ret.push_back(static_cast<uint8_t>(static_cast<uint32_t>(c) >> 24));
        ret.push_back(static_cast<uint8_t>(static_cast<uint32_t>(c) >> 16));
        ret.push_back(static_cast<uint8_t>(static_cast<uint32_t>(c) >> 8));
        ret.push_back(static_cast<uint8_t>(static_cast<uint32_t>(c) >> 0));
      } else {
        static_assert(sizeof(T) == 1u || sizeof(T) == 2u || sizeof(T) == 4u);
      }
    }
  }
  return ret;
}

struct Bytes final : public std::vector<uint8_t> {
 public:
  Bytes(void) {}
  Bytes &operator=(const Bytes &that) {
    this->std::vector<uint8_t>::operator =(that);
    return *this;
  }

  Bytes &operator=(Bytes &&that) {
    this->std::vector<uint8_t>::operator =(
        std::forward<std::vector<uint8_t>>(that));
    return *this;
  }

  Bytes &operator=(const std::vector<uint8_t> &that) {
    this->std::vector<uint8_t>::operator =(that);
    return *this;
  }

  Bytes &operator=(std::vector<uint8_t> &&that) {
    this->std::vector<uint8_t>::operator =(
        std::forward<std::vector<uint8_t>>(that));
    return *this;
  }

  inline Bytes(const std::vector<uint8_t> &val)
      : std::vector<uint8_t>(val) {}

  inline Bytes(const Bytes &val)
      : std::vector<uint8_t>(val) {}

  inline Bytes(Bytes &&val)
      : std::vector<uint8_t>(std::forward<std::vector<uint8_t>>(val)) {}

  inline Bytes(std::vector<uint8_t> &&val)
      : std::vector<uint8_t>(std::forward<std::vector<uint8_t>>(val)) {}

  inline Bytes(const uint8_t *begin_, const uint8_t *end_)
      : std::vector<uint8_t>(begin_, end_) {}

  template <typename T, typename Traits, typename Alloc>
  inline Bytes(const std::basic_string<T, Traits, Alloc> &str)
      : std::vector<uint8_t>(BytesFromString<T, Traits, Alloc>(str)) {}

  template <typename T>
  inline Bytes(const std::basic_string_view<T> &str)
      : std::vector<uint8_t>(BytesFromString<T>(str)) {}

  inline Bytes(const char *data)
      : std::vector<uint8_t>(BytesFromString<char>(std::string_view(data))) {}

  std::string ToString(void) const {
    std::string data;
    if (empty()) {
      return data;
    } else {
      data.insert(data.end(), reinterpret_cast<const char *>(&(front())),
                  &(reinterpret_cast<const char *>(&(back()))[1]));
    }
    return data;
  }
};


}  // namespace rt
}  // namespace hyde
