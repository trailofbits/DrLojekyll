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
    indent.push_back(' ');
    indent.push_back(' ');
  }

  inline void PopIndent(void) {
    indent.resize(indent.size() - 2u);
  }

  inline const std::string &Indent(void) const {
    return indent;
  }

 private:
  const DisplayManager &display_manager;
  std::ostream &os;
  bool rename_locals{false};
  std::string indent;
};

}  // namespace hyde
