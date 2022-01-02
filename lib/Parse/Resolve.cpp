// Copyright 2020, Trail of Bits. All rights reserved.

#include "Parser.h"

namespace hyde {

std::error_code
ParserImpl::ResolvePath(const std::filesystem::path &path,
                        const std::vector<std::filesystem::path> &search_dirs,
                        std::filesystem::path &out_resolved_path) {

  std::error_code ec;

  if (path.is_absolute()) {
    out_resolved_path = std::filesystem::canonical(path, ec);
    return ec;
  }

  for (const auto &search_dir : search_dirs) {
    std::filesystem::path joined_path(search_dir / path);
    std::filesystem::canonical(joined_path, ec);
    if (ec) {
      continue;
    }

    out_resolved_path = std::filesystem::canonical(joined_path, ec);
    break;
  }

  return ec;
}

}  // namespace hyde
