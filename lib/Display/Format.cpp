// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include <drlojekyll/Display/Format.h>

namespace hyde {

OutputStream::~OutputStream(void) {
  os.flush();
}


OutputStream &OutputStream::DisplayNameOr(DisplayPosition pos,
                                          std::string_view backup) {
  if (pos.IsValid()) {
    auto display_name = display_manager.DisplayName(pos);
    if (!display_name.empty()) {
      os << display_name;
    } else {
      os << backup;
    }
  } else {
    os << backup;
  }
  return *this;
}

OutputStream &OutputStream::LineNumberOr(DisplayPosition pos,
                                         std::string_view backup) {
  if (pos.IsValid() && pos.Line() < ~0U) {
    os << pos.Line();
  } else {
    os << backup;
  }
  return *this;
}

OutputStream &OutputStream::ColumnNumberOr(DisplayPosition pos,
                                           std::string_view backup) {
  if (pos.IsValid() && pos.Column() < ~0u) {
    os << pos.Column();
  } else {
    os << backup;
  }
  return *this;
}

OutputStream &OutputStream::operator<<(DisplayRange range) {
  std::string_view data;
  (void) display_manager.TryReadData(range, &data);
  os << data;
  return *this;
}

}  // namespace hyde
