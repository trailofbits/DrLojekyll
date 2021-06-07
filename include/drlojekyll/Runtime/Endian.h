// Copyright 2021, Trail of Bits. All rights reserved.

#pragma once

#if (defined(__BYTE_ORDER__) && __BYTE_ORDER__ == 1234) || \
    (defined(__LITTLE_ENDIAN__) && __LITTLE_ENDIAN__)
#  define HYDE_RT_LITTLE_ENDIAN 1
#  define HYDE_RT_BIG_ENDIAN 0
#elif (defined(__BYTE_ORDER__) && __BYTE_ORDER__ == 4321) || \
       (defined(__BIG_ENDIAN__) && __BIG_ENDIAN__)
#  define HYDE_RT_LITTLE_ENDIAN 0
#  define HYDE_RT_BIG_ENDIAN 1
#else
#  error "Unrecognized byte order"
#endif
