// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <flatbuffers/flatbuffers.h>

namespace hyde {
namespace rt {

template <typename ElemType, typename ParamType>
struct FBType;

template <typename ElemType>
struct FBType<ElemType, ElemType> {
  static inline ElemType Intern(
      flatbuffers::FlatBufferBuilder &_fbb, const ElemType &val) {
    return val;
  }
};

template <typename ElemType>
struct FBType<ElemType, const ElemType &> {
  static inline ElemType Intern(
      flatbuffers::FlatBufferBuilder &_fbb, const ElemType &val) {
    return val;
  }
};

#define DR_MAKE_CASTED_FB_TYPE(to_type, from_type) \
    template <> \
    struct FBType<to_type, from_type> { \
      static inline to_type Intern(flatbuffers::FlatBufferBuilder &_fbb, \
                                   const from_type &val) { \
        return static_cast<to_type>(val); \
      } \
    };

DR_MAKE_CASTED_FB_TYPE(uint8_t, int)
DR_MAKE_CASTED_FB_TYPE(uint16_t, int)
DR_MAKE_CASTED_FB_TYPE(uint32_t, int)
DR_MAKE_CASTED_FB_TYPE(uint64_t, int)
DR_MAKE_CASTED_FB_TYPE(int8_t, int)
DR_MAKE_CASTED_FB_TYPE(int16_t, int)
DR_MAKE_CASTED_FB_TYPE(int64_t, int)

#undef DR_MAKE_CASTED_FB_TYPE

template <>
struct FBType<flatbuffers::Offset<flatbuffers::String>,
              std::string> {
  static inline flatbuffers::Offset<flatbuffers::String> Intern(
      flatbuffers::FlatBufferBuilder &_fbb,
      const std::string &arg) noexcept {
    return _fbb.CreateString(arg);
  }
};

template <>
struct FBType<flatbuffers::Offset<flatbuffers::String>,
              std::string_view> {
  static inline flatbuffers::Offset<flatbuffers::String> Intern(
      flatbuffers::FlatBufferBuilder &_fbb,
      const std::string_view &arg) noexcept {
#ifdef FLATBUFFERS_HAS_STRING_VIEW
    return _fbb.CreateString(arg);
#else
    return _fbb.CreateString(arg.data(), arg.size())
#endif
  }
};

template <>
struct FBType<flatbuffers::Offset<flatbuffers::String>,
              std::vector<char>> {
  static inline flatbuffers::Offset<flatbuffers::String> Intern(
      flatbuffers::FlatBufferBuilder &_fbb,
      const std::vector<char> &arg) noexcept {
    return _fbb.CreateString(arg.data(), arg.size());
  }
};

template <>
struct FBType<flatbuffers::Offset<flatbuffers::String>,
              std::vector<int8_t>> {
  static inline flatbuffers::Offset<flatbuffers::String> Intern(
      flatbuffers::FlatBufferBuilder &_fbb,
      const std::vector<int8_t> &arg) noexcept {
    return _fbb.CreateString(reinterpret_cast<const char *>(arg.data()),
                             arg.size());
  }
};

template <>
struct FBType<flatbuffers::Offset<flatbuffers::String>,
              std::vector<uint8_t>> {
  static inline flatbuffers::Offset<flatbuffers::String> Intern(
      flatbuffers::FlatBufferBuilder &_fbb,
      const std::vector<uint8_t> &arg) noexcept {
    return _fbb.CreateString(reinterpret_cast<const char *>(arg.data()),
                             arg.size());
  }
};

template <>
struct FBType<flatbuffers::Offset<flatbuffers::Vector<char>>,
              std::string> {
  static inline flatbuffers::Offset<flatbuffers::Vector<char>> Intern(
      flatbuffers::FlatBufferBuilder &_fbb,
      const std::string &arg) noexcept {
    return _fbb.CreateVector(arg.data(), arg.size());
  }
};

template <>
struct FBType<flatbuffers::Offset<flatbuffers::Vector<char>>,
              std::string_view> {
  static inline flatbuffers::Offset<flatbuffers::Vector<char>> Intern(
      flatbuffers::FlatBufferBuilder &_fbb,
      const std::string_view &arg) noexcept {
    return _fbb.CreateVector(arg.data(), arg.size());
  }
};

template <>
struct FBType<flatbuffers::Offset<flatbuffers::Vector<char>>,
              std::vector<char>> {
  static inline flatbuffers::Offset<flatbuffers::Vector<char>> Intern(
      flatbuffers::FlatBufferBuilder &_fbb,
      const std::vector<char> &arg) noexcept {
    return _fbb.CreateVector(arg.data(), arg.size());
  }
};

template <>
struct FBType<flatbuffers::Offset<flatbuffers::Vector<char>>,
              std::vector<int8_t>> {
  static inline flatbuffers::Offset<flatbuffers::Vector<char>> Intern(
      flatbuffers::FlatBufferBuilder &_fbb,
      const std::vector<int8_t> &arg) noexcept {
    return _fbb.CreateVector(reinterpret_cast<const char *>(arg.data()),
                             arg.size());
  }
};

template <>
struct FBType<flatbuffers::Offset<flatbuffers::Vector<char>>,
              std::vector<uint8_t>> {
  static inline flatbuffers::Offset<flatbuffers::Vector<char>> Intern(
      flatbuffers::FlatBufferBuilder &_fbb,
      const std::vector<uint8_t> &arg) noexcept {
    return _fbb.CreateVector(reinterpret_cast<const char *>(arg.data()),
                             arg.size());
  }
};

template <>
struct FBType<flatbuffers::Offset<flatbuffers::Vector<int8_t>>,
              std::string> {
  static inline flatbuffers::Offset<flatbuffers::Vector<int8_t>> Intern(
      flatbuffers::FlatBufferBuilder &_fbb,
      const std::string &arg) noexcept {
    return _fbb.CreateVector(reinterpret_cast<const int8_t *>(arg.data()),
                             arg.size());
  }
};

template <>
struct FBType<flatbuffers::Offset<flatbuffers::Vector<int8_t>>,
              std::string_view> {
  static inline flatbuffers::Offset<flatbuffers::Vector<int8_t>> Intern(
      flatbuffers::FlatBufferBuilder &_fbb,
      const std::string_view &arg) noexcept {
    return _fbb.CreateVector(reinterpret_cast<const int8_t *>(arg.data()),
                             arg.size());
  }
};

template <>
struct FBType<flatbuffers::Offset<flatbuffers::Vector<int8_t>>,
              std::vector<char>> {
  static inline flatbuffers::Offset<flatbuffers::Vector<int8_t>> Intern(
      flatbuffers::FlatBufferBuilder &_fbb,
      const std::vector<char> &arg) noexcept {
    return _fbb.CreateVector(reinterpret_cast<const int8_t *>(arg.data()),
                             arg.size());
  }
};

template <>
struct FBType<flatbuffers::Offset<flatbuffers::Vector<int8_t>>,
              std::vector<int8_t>> {
  static inline flatbuffers::Offset<flatbuffers::Vector<int8_t>> Intern(
      flatbuffers::FlatBufferBuilder &_fbb,
      const std::vector<int8_t> &arg) noexcept {
    return _fbb.CreateVector(reinterpret_cast<const int8_t *>(arg.data()),
                             arg.size());
  }
};

template <>
struct FBType<flatbuffers::Offset<flatbuffers::Vector<int8_t>>,
              std::vector<uint8_t>> {
  static inline flatbuffers::Offset<flatbuffers::Vector<int8_t>> Intern(
      flatbuffers::FlatBufferBuilder &_fbb,
      const std::vector<uint8_t> &arg) noexcept {
    return _fbb.CreateVector(reinterpret_cast<const int8_t *>(arg.data()),
                             arg.size());
  }
};

template <>
struct FBType<flatbuffers::Offset<flatbuffers::Vector<uint8_t>>,
              std::string> {
  static inline flatbuffers::Offset<flatbuffers::Vector<uint8_t>> Intern(
      flatbuffers::FlatBufferBuilder &_fbb,
      const std::string &arg) noexcept {
    return _fbb.CreateVector(reinterpret_cast<const uint8_t *>(arg.data()),
                             arg.size());
  }
};

template <>
struct FBType<flatbuffers::Offset<flatbuffers::Vector<uint8_t>>,
              std::string_view> {
  static inline flatbuffers::Offset<flatbuffers::Vector<uint8_t>> Intern(
      flatbuffers::FlatBufferBuilder &_fbb,
      const std::string_view &arg) noexcept {
    return _fbb.CreateVector(reinterpret_cast<const uint8_t *>(arg.data()),
                             arg.size());
  }
};

template <>
struct FBType<flatbuffers::Offset<flatbuffers::Vector<uint8_t>>,
              std::vector<char>> {
  static inline flatbuffers::Offset<flatbuffers::Vector<uint8_t>> Intern(
      flatbuffers::FlatBufferBuilder &_fbb,
      const std::vector<char> &arg) noexcept {
    return _fbb.CreateVector(reinterpret_cast<const uint8_t *>(arg.data()),
                             arg.size());
  }
};

template <>
struct FBType<flatbuffers::Offset<flatbuffers::Vector<uint8_t>>,
              std::vector<int8_t>> {
  static inline flatbuffers::Offset<flatbuffers::Vector<uint8_t>> Intern(
      flatbuffers::FlatBufferBuilder &_fbb,
      const std::vector<int8_t> &arg) noexcept {
    return _fbb.CreateVector(reinterpret_cast<const uint8_t *>(arg.data()),
                             arg.size());
  }
};

template <>
struct FBType<flatbuffers::Offset<flatbuffers::Vector<uint8_t>>,
              std::vector<uint8_t>> {
  static inline flatbuffers::Offset<flatbuffers::Vector<uint8_t>> Intern(
      flatbuffers::FlatBufferBuilder &_fbb,
      const std::vector<uint8_t> &arg) noexcept {
    return _fbb.CreateVector(reinterpret_cast<const uint8_t *>(arg.data()),
                             arg.size());
  }
};

template <typename T>
class CreateFB {
 public:
  using Traits = typename T::Traits;

  template <typename... Elems, typename... Args>
  inline static flatbuffers::Offset<T> Impl(
      flatbuffers::Offset<T> (*create)(
          flatbuffers::FlatBufferBuilder &, Elems...),
      flatbuffers::FlatBufferBuilder &_fbb,
      const Args&... args) noexcept {

    return create(_fbb, FBType<Elems, Args>::Intern(_fbb, args)...);
  }

  template <typename... Args>
  inline static flatbuffers::Offset<T> Create(
      flatbuffers::FlatBufferBuilder &_fbb,
      flatbuffers::Offset<T> arg) noexcept {
    return arg;
  }

  template <typename... Args>
  inline static flatbuffers::Offset<T> Create(
      flatbuffers::FlatBufferBuilder &_fbb, const Args&... args) noexcept {
    return Impl(Traits::Create, _fbb, args...);
  }
};

template <typename T>
class FBCast {
 public:
  inline static T From(T &&val) noexcept {
    return std::forward<T>(val);
  }
};

template <>
class FBCast<std::string> {
 public:
  using T = std::string;
  using ET = char;

  inline static T From(T &&val) noexcept {
    return std::forward<T>(val);
  }

  inline static T From(const flatbuffers::String *str) noexcept {
    return T(reinterpret_cast<const ET *>(str->data()), str->size());
  }

  inline static T From(const flatbuffers::Vector<char> *str) noexcept {
    return T(reinterpret_cast<const ET *>(str->data()), str->size());
  }

  inline static T From(const flatbuffers::Vector<int8_t> *str) noexcept {
    return T(reinterpret_cast<const ET *>(str->data()),
             str->size());
  }

  inline static T From(const flatbuffers::Vector<uint8_t> *str) noexcept {
    return T(reinterpret_cast<const ET *>(str->data()),
             str->size());
  }
};

template <>
class FBCast<std::vector<char>> {
 public:
  using T = std::vector<char>;
  using ET = char;

  inline static T From(T &&val) noexcept {
    return std::forward<T>(val);
  }

  inline static T From(const flatbuffers::String *str) noexcept {
    auto iter = reinterpret_cast<const ET *>(str->data());
    return T(iter, iter + str->size());
  }

  inline static T From(const flatbuffers::Vector<char> *str) noexcept {
    auto iter = reinterpret_cast<const ET *>(str->data());
    return T(iter, iter + str->size());
  }

  inline static T From(const flatbuffers::Vector<int8_t> *str) noexcept {
    auto iter = reinterpret_cast<const ET *>(str->data());
    return T(iter, iter + str->size());
  }

  inline static T From(const flatbuffers::Vector<uint8_t> *str) noexcept {
    auto iter = reinterpret_cast<const ET *>(str->data());
    return T(iter, iter + str->size());
  }
};

template <>
class FBCast<std::vector<int8_t>> {
 public:
  using T = std::vector<int8_t>;
  using ET = int8_t;

  inline static T From(T &&val) noexcept {
    return std::forward<T>(val);
  }

  inline static T From(const flatbuffers::String *str) noexcept {
    auto iter = reinterpret_cast<const ET *>(str->data());
    return T(iter, iter + str->size());
  }

  inline static T From(const flatbuffers::Vector<char> *str) noexcept {
    auto iter = reinterpret_cast<const ET *>(str->data());
    return T(iter, iter + str->size());
  }

  inline static T From(const flatbuffers::Vector<int8_t> *str) noexcept {
    auto iter = reinterpret_cast<const ET *>(str->data());
    return T(iter, iter + str->size());
  }

  inline static T From(const flatbuffers::Vector<uint8_t> *str) noexcept {
    auto iter = reinterpret_cast<const ET *>(str->data());
    return T(iter, iter + str->size());
  }
};

template <>
class FBCast<std::vector<uint8_t>> {
 public:
  using T = std::vector<uint8_t>;
  using ET = uint8_t;

  inline static T From(T &&val) noexcept {
    return std::forward<T>(val);
  }

  inline static T From(const flatbuffers::String *str) noexcept {
    auto iter = reinterpret_cast<const ET *>(str->data());
    return T(iter, iter + str->size());
  }

  inline static T From(const flatbuffers::Vector<char> *str) noexcept {
    auto iter = reinterpret_cast<const ET *>(str->data());
    return T(iter, iter + str->size());
  }

  inline static T From(const flatbuffers::Vector<int8_t> *str) noexcept {
    auto iter = reinterpret_cast<const ET *>(str->data());
    return T(iter, iter + str->size());
  }

  inline static T From(const flatbuffers::Vector<uint8_t> *str) noexcept {
    auto iter = reinterpret_cast<const ET *>(str->data());
    return T(iter, iter + str->size());
  }
};

}  // namespace rt
}  // namespace hyde
