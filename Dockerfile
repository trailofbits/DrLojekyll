FROM ubuntu:bionic

RUN apt-get update -y && \
	apt-get install -y \
	wget \ 
	gnupg 

# Install latest version of cmake 
RUN wget -qO - https://apt.kitware.com/keys/kitware-archive-latest.asc | apt-key add - \
 && echo "deb https://apt.kitware.com/ubuntu/ bionic main" >>/etc/apt/sources.list

#Install stdlibc++
RUN apt-get update -y && apt install -y \
	cmake \
	clang-9 \
	llvm-9 \
	libc++-dev \
	libc++abi-dev \
	ninja-build 

COPY . /DrLojekyll

WORKDIR /DrLojekyll 

RUN rm -rf build && mkdir -p build 

WORKDIR /DrLojekyll/build

RUN cmake -G Ninja -DCMAKE_C_COMPILER=clang-9 -DCMAKE_CXX_COMPILER=clang++-9 -DCMAKE_CXX_FLAGS="-std=c++11 -stdlib=libc++" .. && ninja 

 
