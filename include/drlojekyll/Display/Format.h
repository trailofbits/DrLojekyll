// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include <drlojekyll/Display/DisplayManager.h>

#include <ostream>

#pragma once

namespace hyde {

// Wrapper around a `std::ostream` that lets us stream out `Token`s and
// `DisplayRange`s.
class OutputStream {
 public:
  ~OutputStream(void);

  inline OutputStream(const DisplayManager &display_manager_, std::ostream &os_)
      : display_manager(display_manager_),
        os(os_) {}

  OutputStream &operator<<(DisplayRange range);

  inline OutputStream &operator<<(OutputStream &that) {
    return that;
  }

  template <typename T>
  OutputStream &operator<<(T val) {
    os << val;
    return *this;
  }

  inline void SetKeepImports(bool state) {
    include_imports = state;
  }

  inline void SetRenameLocals(bool state) {
    rename_locals = state;
  }

  inline bool KeepImports(void) const {
    return include_imports;
  }

  inline bool RenameLocals(void) const {
    return rename_locals;
  }

  inline void Flush(void) {
    os.flush();
  }

 private:
  const DisplayManager &display_manager;
  std::ostream &os;
  bool include_imports{true};
  bool rename_locals{false};
};

}  // namespace hyde
