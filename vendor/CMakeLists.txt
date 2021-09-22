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
    lz4
  REPOSITORY
    "https://github.com/lz4/lz4.git"
  TAG
    "v1.9.3"
  SOURCE_SUBDIR
    build/cmake
  CMAKE_ARGS
    -DLZ4_BUILD_CLI=OFF
    -DLZ4_BUILD_LEGACY_LZ4C=OFF)

fetch_git_dependency(
  NAME
    zlib
  REPOSITORY
    "https://github.com/madler/zlib.git"
  TAG
    "v1.2.11")

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
    glog
  REPOSITORY
    "https://github.com/google/glog.git"
  TAG
    "v0.5.0"
  CMAKE_ARGS
    -DBUILD_SHARED_LIBS:BOOL=OFF
    -DWITH_GFLAGS:BOOL=ON
    -DWITH_GTEST:BOOL=OFF
    -DWITH_THREADS:BOOL=ON
    -DWITH_TLS:BOOL=ON
    -DBUILD_TESTING:BOOL=OFF)

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
    protobuf
  REPOSITORY
    "https://github.com/protocolbuffers/protobuf.git"
  TAG
    "v3.18.0"
  SOURCE_SUBDIR
    cmake
  CMAKE_ARGS
    -DPROTOBUF_ROOT="${CMAKE_CURRENT_BINARY_DIR}/protobuf/source"
    -DBUILD_SHARED_LIBS=OFF
    -Dprotobuf_BUILD_SHARED_LIBS=OFF
    -Dprotobuf_BUILD_TESTS=OFF
    -Dprotobuf_WITH_ZLIB=ON)

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

if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  set(GPR_TLS_FIXES -DCMAKE_C_FLAGS="-DGRPC_BAZEL_BUILD=1" -DCMAKE_CXX_FLAGS="-DGRPC_BAZEL_BUILD=1")
endif()

fetch_git_dependency(
  NAME
    grpc
  REPOSITORY
    "https://github.com/grpc/grpc.git"
  TAG
    "v1.22.0"
  CMAKE_ARGS
    -DgRPC_BUILD_TESTS=OFF
    -DgRPC_BACKWARDS_COMPATIBILITY_MODE=OFF
    -DgRPC_ZLIB_PROVIDER=package
    -DgRPC_PROTOBUF_PROVIDER=package
    -DgRPC_GFLAGS_PROVIDER=package
    -DgRPC_CARES_PROVIDER=package
    -DgRPC_SSL_PROVIDER=package
    -DgRPC_INSTALL=ON
    ${GPR_TLS_FIXES}
    -DOPENSSL_ROOT_DIR="${CMAKE_CURRENT_BINARY_DIR}/install"
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

