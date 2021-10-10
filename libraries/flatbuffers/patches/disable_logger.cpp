// Copyright 2020, Trail of Bits, Inc. All rights reserved.

#include <string>
#include <iostream>

namespace flatbuffers {

void LogCompilerWarn(const std::string &message) {
  if (false) {
    std::cerr << "FLATC WARN: " << message << std::endl;
  }
}

void LogCompilerError(const std::string &message) {
  if (false) {
    std::cerr << "FLATC ERROR: " << message << std::endl;
  }
}

}  // namespace flatbuffers
