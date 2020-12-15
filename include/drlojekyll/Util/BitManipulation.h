// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

#include <cstdint>

namespace hyde {

/// Rotate `val` to the right `rot` positions.
inline static uint64_t RotateRight64(uint64_t val, unsigned rot) {

// NOTE: if we ever move to C++20, there are builtin rotation functions in the
//       standard library, which we should use instead.
#ifdef __has_builtin
#  if !__has_builtin(__builtin_rotateright64)
#    define HYDE_NEEDS_ROR64 1
#  else
#    define HYDE_NEEDS_ROR64 0
#  endif
#elif !defined(__clang__)
#  define HYDE_NEEDS_ROR64 1
#endif

#if HYDE_NEEDS_ROR64
  if (!rot)
    return val;
  return (val >> rot) | (val << (64u - (rot % 64u)));
#else
  return __builtin_rotateright64(val, rot);
#endif
}

/// Rotate `val` to the right `rot` positions.
inline static uint32_t RotateRight32(uint32_t val, unsigned rot) {

// NOTE: if we ever move to C++20, there are builtin rotation functions in the
//       standard library, which we should use instead.
#ifdef __has_builtin
#  if !__has_builtin(__builtin_rotateright32)
#    define HYDE_NEEDS_ROR32 1
#  else
#    define HYDE_NEEDS_ROR32 0
#  endif
#elif !defined(__clang__)
#  define HYDE_NEEDS_ROR32 1
#endif

#if HYDE_NEEDS_ROR64
  if (!rot)
    return val;
  return (val >> rot) | (val << (32u - (rot % 32u)));
#else
  return __builtin_rotateright32(val, rot);
#endif
}

}  // namespace hyde
