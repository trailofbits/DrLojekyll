# Copyright 2021, Trail of Bits, Inc. All rights reserved.

fetch_git_dependency(
  NAME
    xxHash
  REPOSITORY
    "https://github.com/Cyan4973/xxHash.git"
  TAG
    "v0.8.0"
  SOURCE_SUBDIR
    cmake_unofficial
  CMAKE_ARGS
    -DXXHASH_BUILD_XXHSUM=OFF
    -DXXHASH_BUILD_ENABLE_INLINE_API=OFF
    -DXXHASH_BUNDLED_MODE=OFF
    -DBUILD_SHARED_LIBS=OFF)

fetch_git_dependency(
  NAME
    gflags
  REPOSITORY
    "https://github.com/gflags/gflags.git"
  TAG
    "v2.2.2"
  CMAKE_ARGS
    -DGFLAGS_INSTALL_SHARED_LIBS:BOOL=OFF
    -DGFLAGS_INSTALL_STATIC_LIBS:BOOL=ON
    -DGFLAGS_INSTALL_HEADERS:BOOL=ON
    -DGFLAGS_BUILD_SHARED_LIBS:BOOL=ON
    -DGFLAGS_BUILD_STATIC_LIBS:BOOL=ON
    -DGFLAGS_BUILD_TESTING:BOOL=OFF
    -DGFLAGS_BUILD_gflags_LIB:BOOL=ON
    -DGFLAGS_BUILD_gflags_nothreads_LIB:BOOL=OFF)

fetch_git_dependency(
  NAME
    gtest
  REPOSITORY
    "https://github.com/google/googletest.git"
  TAG
    "release-1.11.0"
  CMAKE_ARGS
    -DBUILD_GMOCK:BOOL=OFF
    -Dgtest_build_tests:BOOL=OFF
    -Dgtest_build_samples:BOOL=OFF)


fetch_git_dependency(
  NAME
    rapidcheck
  REPOSITORY
    "https://github.com/emil-e/rapidcheck.git")

fetch_git_dependency(
  NAME
    reproc
  REPOSITORY
    "https://github.com/DaanDeMeyer/reproc.git"
  TAG
    "v14.2.3")

fetch_git_dependency(
  NAME
    openssl
  REPOSITORY
    "https://github.com/openssl/openssl.git"
  TAG
    "openssl-3.0.0"
  APPLY_PATCH
    "${CMAKE_CURRENT_LIST_DIR}/patches/openssl.patch"
  CMAKE_ARGS
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON)

fetch_git_dependency(
  NAME
    zlib
  REPOSITORY
    "https://github.com/madler/zlib.git"
  TAG
    "v1.2.11")

fetch_git_dependency(
  NAME
    c-ares
  REPOSITORY
    "https://github.com/c-ares/c-ares.git"
  TAG
    "cares-1_17_2"
  CMAKE_ARGS
    -DCARES_STATIC=ON
    -DCARES_SHARED=OFF
    -DCARES_STATIC_PIC=ON
    -DCARES_BUILD_TESTS=OFF
    -DCARES_BUILD_TOOLS=OFF)

#if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
#  set(GPR_TLS_FIXES -DCMAKE_C_FLAGS="-DGRPC_BAZEL_BUILD=1" -DCMAKE_CXX_FLAGS="-DGRPC_BAZEL_BUILD=1")
#endif()

fetch_git_dependency(
  NAME
    grpc
  REPOSITORY
    "https://github.com/grpc/grpc.git"
  TAG
    "v1.22.0"
  DEPENDS
    openssl
  CMAKE_ARGS
    -DgRPC_BUILD_TESTS=OFF
    -DgRPC_BACKWARDS_COMPATIBILITY_MODE=OFF
    -DgRPC_ZLIB_PROVIDER=package
    -DgRPC_PROTOBUF_PROVIDER=module
    -DgRPC_GFLAGS_PROVIDER=package
    -DgRPC_CARES_PROVIDER=package
    -DgRPC_SSL_PROVIDER=package
    -DgRPC_INSTALL=TRUE
    -DOPENSSL_ROOT_DIR=${CMAKE_CURRENT_BINARY_DIR}/install
    -DOPENSSL_USE_STATIC_LIBS=TRUE)

fetch_git_dependency(
  NAME
    flatbuffers
  REPOSITORY
    "https://github.com/google/flatbuffers.git"
  TAG
    "v2.0.0"
  CMAKE_ARGS
    -DFLATBUFFERS_BUILD_TESTS=OFF)
