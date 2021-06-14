// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include <drlojekyll/Display/DisplayManager.h>

#include <ostream>
#include <string>

#pragma once

namespace hyde {

// Wrapper around a `std::ostream` that lets us stream out `Token`s and
// `DisplayRange`s.
class OutputStream {
 public:
  ~OutputStream(void);

  inline OutputStream(const DisplayManager &display_manager_, std::ostream &os_)
      : display_manager(display_manager_),
        os(os_) {
    indent.reserve(16);
  }

  OutputStream &DisplayNameOr(DisplayPosition pos, std::string_view);
  OutputStream &LineNumberOr(DisplayPosition pos, std::string_view);
  OutputStream &ColumnNumberOr(DisplayPosition pos, std::string_view);

  OutputStream &operator<<(DisplayRange range);

  inline OutputStream &operator<<(OutputStream &that) {
    return that;
  }

  template <typename T>
  OutputStream &operator<<(T val) {
    os << val;
    return *this;
  }

  inline void SetRenameLocals(bool state) {
    rename_locals = state;
  }

  inline bool RenameLocals(void) const {
    return rename_locals;
  }

  inline void Flush(void) {
    os.flush();
  }

  inline void PushIndent(void) {
    indent.insert(indent.end(), indent_size, ' ');
  }

  inline void SetIndentSize(unsigned new_size) {
    indent_size = new_size;
  }

  inline void PopIndent(void) {
    indent.resize(indent.size() - indent_size);
  }

  inline const std::string &Indent(void) const {
    return indent;
  }

  const DisplayManager &display_manager;

 private:
  std::ostream &os;
  bool rename_locals{false};
  std::string indent;
  unsigned indent_size{2u};
};

}  // namespace hyde
