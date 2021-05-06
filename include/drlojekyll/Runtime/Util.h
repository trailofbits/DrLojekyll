// Copyright 2021, Trail of Bits, Inc. All rights reserved.

#pragma once

#include <cassert>

namespace hyde {
namespace rt {

#ifdef NDEBUG
# define HYDE_RT_LIKELY(...) __builtin_expect((__VA_ARGS__), 1)
# define HYDE_RT_UNLIKELY(...) __builtin_expect((__VA_ARGS__), 0)
# define HYDE_RT_INLINE inline
# define HYDE_RT_ALWAYS_INLINE [[gnu::always_inline]] HYDE_RT_INLINE
# define HYDE_RT_FLATTEN [[gnu::flatten]]
#else
# define HYDE_RT_LIKELY(...) __VA_ARGS__
# define HYDE_RT_UNLIKELY(...) __VA_ARGS__
# define HYDE_RT_INLINE
# define HYDE_RT_ALWAYS_INLINE HYDE_RT_INLINE
# define HYDE_RT_FLATTEN
#endif

}  // namespace rt
}  // namespace hyde
