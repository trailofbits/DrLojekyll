// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

#ifndef __cplusplus
#error "__cplusplus not defined"
#endif

#if __cplusplus < 201703L
#if defined(__clang__)
#define HYDE_FALLTHROUGH [[clang::fallthrough]]
#elif defined(__GNUG__)
#define HYDE_FALLTHROUGH [[gnu::fallthrough]]
#else
#define HYDE_FALLTHROUGH __attribute__((fallthrough))
#endif
#else
#define HYDE_FALLTHROUGH [[fallthrough]]
#endif
