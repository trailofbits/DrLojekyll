ARG ARCH=amd64
ARG UBUNTU_VERSION=18.04
ARG DISTRO_BASE=ubuntu${UBUNTU_VERSION}
ARG BUILD_BASE=ubuntu:${UBUNTU_VERSION}

ARG INSTALL_DIR=/opt/trailofbits/drlojekyll


# Run-time dependencies go here
FROM ${BUILD_BASE} as base
ARG INSTALL_DIR

RUN apt-get update && \
    apt-get upgrade -y && \
    rm -rf /var/lib/apt/lists/*


# Build-time dependencies go here
FROM base as deps
ARG INSTALL_DIR

RUN apt-get update && \
    apt-get install -y wget gnupg && \
    wget -qO - https://apt.kitware.com/keys/kitware-archive-latest.asc | apt-key add - && \
    echo "deb https://apt.kitware.com/ubuntu/ bionic main" >>/etc/apt/sources.list && \
    apt-get update -y && \
    apt-get upgrade -y && \
    apt-get install -y \
      cmake \
      curl \
      tar \
      zip \
      unzip \
      git \
      gcc-8 \
      g++-8 \
      ninja-build

WORKDIR /DrLojekyll

COPY scripts/download_vcpkg.sh scripts/download_vcpkg.sh
COPY vcpkg_commit.txt vcpkg_commit.txt
ENV CC=gcc-8 \
    CXX=g++-8

# vcpkg installation
COPY vcpkg.txt vcpkg.txt
RUN ./scripts/download_vcpkg.sh

# Source code build
FROM deps as build
ARG INSTALL_DIR

COPY . ./

RUN cmake -G Ninja \
      -B build \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_C_COMPILER=gcc-8 \
      -DCMAKE_CXX_COMPILER=g++-8 \
      -DWARNINGS_AS_ERRORS=1 \
      -DCMAKE_INSTALL_PREFIX="${INSTALL_DIR}" \
      -DCMAKE_TOOLCHAIN_FILE=vcpkg/scripts/buildsystems/vcpkg.cmake \
      . && \
    cmake --build build && \
    cmake --build build --target install


# Minimal distribution image with only DrLojekyll and run-time dependencies
FROM base as dist
ARG INSTALL_DIR

WORKDIR /drlog/local
COPY --from=build "${INSTALL_DIR}" "${INSTALL_DIR}"
ENV PATH="${INSTALL_DIR}/bin:${PATH}"
ENTRYPOINT ["drlojekyll"]


# Test library installation copying
# Needs to be "FROM deps" in order to actually build the test project with gcc
FROM deps as test_lib
ARG INSTALL_DIR

WORKDIR /test_lib
COPY . .
COPY --from=dist "${INSTALL_DIR}" "${INSTALL_DIR}"
RUN cd tests/external_build && \
    cmake \
      -B build \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_C_COMPILER=gcc-8 \
      -DCMAKE_CXX_COMPILER=g++-8 \
      -DWARNINGS_AS_ERRORS=1 \
      -DCMAKE_PREFIX_PATH="${INSTALL_DIR}" \
      -DCMAKE_TOOLCHAIN_FILE="/DrLojekyll/vcpkg/scripts/buildsystems/vcpkg.cmake" \
      . && \
    cmake --build build


# Make this last so that it's also default
FROM dist
