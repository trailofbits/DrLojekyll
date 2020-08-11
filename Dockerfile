FROM ubuntu:20.04

RUN apt-get update -y && \
    apt-get install -y wget gnupg && \
    wget -qO - https://apt.kitware.com/keys/kitware-archive-latest.asc | apt-key add - && \
    echo "deb https://apt.kitware.com/ubuntu/ focal main" >>/etc/apt/sources.list

RUN apt-get update -y && \
    apt-get upgrade -y && \
    apt-get install -y \
      cmake \
      gcc \
      g++ \
      ninja-build

WORKDIR /DrLojekyll

COPY ./ /DrLojekyll

RUN cmake -G Ninja \
      -B build \
      -DCMAKE_BUILD_TYPE=Debug \
      -DCMAKE_C_COMPILER=gcc \
      -DCMAKE_CXX_COMPILER=g++ \
      -DWARNINGS_AS_ERRORS=1 \
      . && \
    cmake --build build -j "$(nproc)"
