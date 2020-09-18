// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

#include <cstdint>
#include <iosfwd>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>

namespace hyde {

class DisplayImpl;
class DisplayPosition;
class DisplayRange;

// A Display is an abstraction over a Dr. Lojekyll source input.
//
// A Display allows you to render portions of source input for a user, given a
// configuration.  This is particularly useful for error reporting.
//
// Internally, a Display could reside entirely in memory, or it could be backed
// by a file.
class Display {
 public:
  ~Display(void) {}
  Display(const Display &) = default;
  Display(Display &&) noexcept = default;

  // Return the ID of this display.
  unsigned Id(void) const;

  // Tries to read a character from this display, given its position. Returns
  // `true` if successful and updates `*ch_out`.
  bool TryReadChar(DisplayPosition position, char *ch_out) const;

  // Tries to read a range of characters from the display. Returns `true` if
  // successful and updates `*data_out`.
  //
  // NOTE(pag): `*data_out` is valid for the lifetime of the `DisplayManager`
  //            from which this `Display` was created.
  bool TryReadData(DisplayRange range, std::string_view *data_out) const;

 private:
  friend class DisplayManager;
  friend class DisplayReader;

  inline explicit Display(DisplayImpl *impl_) : impl(impl_) {}

  Display(void) = delete;

  // Owned by the `DisplayManager` that created this `Display`.
  DisplayImpl *impl{nullptr};
};

// Configuration about how input is displayed. This is used both for parsing
// and warning/error reporting.
class DisplayConfiguration {
 public:
  // Name of the display. This could be a file path.
  std::string name;

  // Number of spaces in a tab.
  unsigned num_spaces_in_tab{2};

  // Whether or not the input source aligns tabs to specific columns (true)
  // or just flat out indents them by `tab_len`.
  bool use_tab_stops{true};
};

// Manages one or more displays. In most cases, these are just files.
class DisplayManager {
 public:
  ~DisplayManager(void);

  DisplayManager(void);
  DisplayManager(const DisplayManager &) = default;
  DisplayManager(DisplayManager &&) noexcept = default;

  // Return the name of a display, given a position.
  std::string_view DisplayName(DisplayPosition position) const;

  // Open a buffer as a display.
  //
  // NOTE(pag): `data` must remain a valid reference for the lifetime of the
  //            `DisplayManager`.
  Display OpenBuffer(std::string_view data,
                     const DisplayConfiguration &config) const;

  // Open a file, specified by its path. This will read the entire contents
  // of the file into a buffer.
  Display OpenPath(std::string_view path,
                   const DisplayConfiguration &config) const;

  // Open an input stream.
  //
  // NOTE(pag): `is` must remain a valid reference for the lifetime of the
  //            `DisplayManager`.
  Display OpenStream(std::istream &is,
                     const DisplayConfiguration &config) const;

  // Tries to read a character from a display, given its position. Returns
  // `true` if successful and updates `*ch_out`.
  bool TryReadChar(DisplayPosition position, char *ch_out) const;

  // Tries to read a range of characters from a display. Returns `true` if
  // successful and updates `*data_out`.
  //
  // NOTE(pag): `*data_out` is valid for the lifetime of this `DisplayManager`.
  bool TryReadData(DisplayRange range, std::string_view *data_out) const;

  // Try to displace `position` by `num_bytes`. If successful, modifies
  // `position` in place, and returns `true`, otherwise returns `false`.
  bool TryDisplacePosition(DisplayPosition &position, int num_bytes) const;

 private:
  friend class Lexer;

  class Impl;

  std::shared_ptr<Impl> impl;
};

// A DisplayPosition represents a location within a Display.
class DisplayPosition {
 public:
  DisplayPosition(void) = default;

  // Return the display ID, or `~0U` (32 0s, followed by 32 1s) if invalid.
  uint64_t DisplayId(void) const;

  // Index of the character at this position, or `~0U` (32 0s, followed by
  // 32 1s) if invalid.
  uint64_t Index(void) const;

  // Return the line number (starting at `1`) from the display referenced
  // by this position, or `~0U` (32 0s, followed by 32 1s) if invalid.
  uint64_t Line(void) const;

  // Return the column number (starting at `1`) from the display referenced
  // by this position, or `~0U` (32 0s, followed by 32 1s) if invalid.
  uint64_t Column(void) const;

  // Returns `true` if the display position is valid.
  bool IsValid(void) const;

  // Returns `true` if the display position has data.
  bool HasData(void) const;

  // Tries to compute the distance between two positions.
  bool TryComputeDistanceTo(DisplayPosition that, int *num_bytes,
                            int *num_lines, int *num_cols) const;

  inline bool IsInvalid(void) const {
    return !IsValid();
  }

  inline bool operator==(DisplayPosition that) const {
    return opaque_data == that.opaque_data;
  }

  inline bool operator!=(DisplayPosition that) const {
    return opaque_data != that.opaque_data;
  }

 private:
  friend class Display;
  friend class DisplayImpl;
  friend class DisplayManager;
  friend class DisplayRange;
  friend class DisplayReader;
  friend class Token;

  explicit DisplayPosition(uint64_t display_id, uint64_t index, uint64_t line,
                           uint64_t column);

  // Private constructor for use by tokens and such.
  inline explicit DisplayPosition(uint64_t opaque_data_)
      : opaque_data(opaque_data_) {}

  uint64_t opaque_data{0};
};

// A DisplayRange represents an exclusive range between two DisplayPosition values.
class DisplayRange {
 public:
  DisplayRange(void) = default;

  bool IsValid(void) const;

  inline bool IsInvalid(void) const {
    return !IsValid();
  }

  inline explicit DisplayRange(DisplayPosition from_, DisplayPosition to_)
      : from(from_),
        to(to_) {}

  inline DisplayPosition From(void) const {
    return from;
  }

  inline DisplayPosition To(void) const {
    return to;
  }

  // Tries to compute the distance between two positions.
  inline bool TryComputeDistance(int *num_bytes, int *num_lines,
                                 int *num_cols) const {
    return from.TryComputeDistanceTo(to, num_bytes, num_lines, num_cols);
  }

 private:
  friend class Display;
  friend class DisplayManager;

  DisplayPosition from;
  DisplayPosition to;
};

// Used to read characters from a display.
class DisplayReader {
 public:
  ~DisplayReader(void);

  explicit DisplayReader(const Display &display_);

  // Tries to read a character from the display. If successful, returns `true`
  // and updates `*ch_out`.
  bool TryReadChar(char *ch_out);

  // Unreads the last read character.
  void UnreadChar(void);

  // Returns the current display position.
  DisplayPosition CurrentPosition(void) const;

  // Returns `true` if there was an error, and if `os` is non-NULL, outputs
  // the error message to the `os` stream.
  bool TryGetErrorMessage(std::ostream *os) const;

 private:
  DisplayReader(void) = delete;

  DisplayImpl *display;
  uint64_t index{0};
  uint64_t line{1};
  uint64_t column{1};
};

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
