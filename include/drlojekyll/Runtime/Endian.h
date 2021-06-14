// Copyright 2021, Trail of Bits. All rights reserved.

#pragma once

#if defined(__has_include) && __has_include(<endian.h>)
#  include <endian.h>
#endif

#if defined(__has_include) && __has_include(<sys/param.h>)
#  include <sys/param.h>
#endif

#if (defined(__BYTE_ORDER__) && defined(__ORDER_LITTLE_ENDIAN__) && \
     __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__) || \
    (defined(__LITTLE_ENDIAN__) && __LITTLE_ENDIAN__)
#  define HYDE_RT_CONSTEXPR_ENDIAN constexpr
#  define HYDE_RT_LITTLE_ENDIAN 1
#  define HYDE_RT_BIG_ENDIAN 0
#elif (defined(__BYTE_ORDER__) && defined(__ORDER_BIG_ENDIAN__) && \
       __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__) || \
       (defined(__BIG_ENDIAN__) && __BIG_ENDIAN__)
#  define HYDE_RT_CONSTEXPR_ENDIAN constexpr
#  define HYDE_RT_LITTLE_ENDIAN 0
#  define HYDE_RT_BIG_ENDIAN 1
#else
#  define HYDE_RT_CONSTEXPR_ENDIAN
#  define HYDE_RT_LITTLE_ENDIAN 0
#  define HYDE_RT_BIG_ENDIAN (!*(unsigned char *)&(unsigned short){1})
namespace hyde {
namespace rt {
static const unsigned short kEndian = 0xFFEE;
}  // namespace rt
}  // namespace hyde
#  define HYDE_RT_LITTLE_ENDIAN ((*reinterpret_cast<const unsigned char *>(&::hyde::rt::kEndian)) == 0xEE)
#  define HYDE_RT_BIG_ENDIAN ((*reinterpret_cast<const unsigned char *>(&::hyde::rt::kEndian)) == 0xFF)
#endif
