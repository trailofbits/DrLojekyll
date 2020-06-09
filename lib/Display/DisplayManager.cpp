// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include <drlojekyll/Display/DisplayManager.h>

#include <cerrno>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

#include <drlojekyll/Display/DisplayConfiguration.h>

#include "DataStream.h"
#include "Display.h"
#include "DisplayPosition.h"

namespace hyde {
namespace {

// Stream that never reads data, and reports an error on failing to read from
// the file path.
class BadFilePathStream final : public display::DataStream {
 public:
  virtual ~BadFilePathStream(void) = default;

  explicit BadFilePathStream(const std::string_view path_,
                             std::error_code ec_)
      : path(path_),
        ec(ec_) {}

  bool ReadData(std::string_view *out_data) override {
    if (out_data) {
      *out_data = std::string_view();
    }
    return false;
  }

  bool TryGetErrorMessage(std::ostream *os) const override {
    if (os) {
      (*os) << "Could not open file '" << path << "' for reading: "
            << ec.message();
    }
    return true;
  }

 private:
  const std::string path;
  const std::error_code ec;
};

// Stream that reads data from a file.
class FilePathStream final : public display::DataStream {
 public:
  virtual ~FilePathStream(void) = default;

  explicit FilePathStream(const std::string_view path_,
                          std::ifstream &&fs_)
      : path(path_),
        fs(std::forward<std::ifstream>(fs_)) {}

  // Read data into `data_out`.
  bool ReadData(std::string_view *data_out) override {
    if (done) {
      if (data_out) {
        *data_out = std::string_view();
      }
      return false;
    }

    data.clear();
    while (true) {
      char ch = '\0';
      errno = 0;
      if (fs.get(ch)) {
        data.push_back(ch);

      } else if (fs.eof()) {
        done = true;
        break;

      } else {
        ec = std::error_code(errno, std::system_category());
        if (!ec) {
          ec = std::make_error_code(std::errc::io_error);
        }
        done = true;
        break;
      }
    }

    if (data_out) {
      *data_out = data;
    }

    return !data.empty();
  }

  bool TryGetErrorMessage(std::ostream *os) const override {
    if (!ec) {
      return false;
    }
    if (os) {
      (*os) << "Error reading from file '" << path << "': "
            << ec.message();
    }
    return true;
  }

 private:
  const std::string_view path;
  bool done{false};
  std::ifstream fs;
  std::error_code ec;
  std::string data;
};

// Stream that reads data from a file.
class UserSuppliedStream final : public display::DataStream {
 public:
  virtual ~UserSuppliedStream(void) = default;

  explicit UserSuppliedStream( const std::string &name_, std::istream &is_)
      : name(name_),
        is(is_) {
    data.reserve(4096);
  }

  // Read data into `data_out`.
  bool ReadData(std::string_view *data_out) override {
    if (done) {
      if (data_out) {
        *data_out = std::string_view();
      }
      return false;
    }

    data.clear();
    while (true) {
      char ch = '\0';
      errno = 0;
      if (is.get(ch)) {
        data.push_back(ch);

      } else if (is.eof()) {
        done = true;
        break;

      } else {
        done = true;
        error = true;
        break;
      }
    }

    if (data_out) {
      *data_out = data;
    }

    return !data.empty();
  }

  bool TryGetErrorMessage(std::ostream *os) const override {
    if (!error) {
      return false;
    }
    if (os) {
      (*os) << "Error reading from stream '" << name << "'";
    }
    return true;
  }

 private:
  const std::string name;
  bool done{false};
  bool error{false};
  std::istream &is;
  std::string data;
};

}  // namespace

// Manages ownership of one or more displays.
class DisplayManager::Impl {
 public:
  Impl(void);

  DisplayImpl *stdin_display{nullptr};
  std::vector<std::unique_ptr<DisplayImpl>> displays;
  std::unordered_map<std::string, DisplayImpl *> path_to_display;
};

DisplayManager::Impl::Impl(void) {
  displays.emplace_back();  // Ensure that display ID 0 is never used.
}

DisplayManager::~DisplayManager(void) {}

DisplayManager::DisplayManager(void)
    : impl(std::make_shared<DisplayManager::Impl>()) {}

// Return the name of a display, given a position.
std::string_view DisplayManager::DisplayName(DisplayPosition position) const {
  if (position.IsValid()) {
    display::PositionInterpreter interpreter = {position.opaque_data};
    const auto display_id = interpreter.position.display_id;
    if (display_id && display_id < impl->displays.size()) {
      return impl->displays[interpreter.position.display_id]->Name();
    }
  }
  return std::string_view();
}

// Open a buffer as a display. This will copy the underlying data.
Display DisplayManager::OpenBuffer(std::string_view data,
                                   const DisplayConfiguration &config) const {
  auto id = static_cast<unsigned>(impl->displays.size());
  auto stream = new display::StringViewStream(data);
  auto display = new DisplayImpl(id, config, stream);
  impl->displays.emplace_back(display);
  return Display(display);
}

// Open a file, specified by its path. This will read the entire contents
// of the file into a buffer.
Display DisplayManager::OpenPath(std::string_view path_,
                                 const DisplayConfiguration &config) const {
  std::string path(path_);
  if (path == "-" || path == "/dev/stdin") {
    return OpenStream(std::cin, config);
  }

  auto &display = impl->path_to_display[path];
  if (display) {
    return Display(display);
  }

  display::DataStream *stream = nullptr;
  errno = 0;
  std::ifstream fs(path);
  const auto err = errno;
  if (!fs) {
    std::error_code ec(err, std::system_category());
    stream = new BadFilePathStream(path, ec);
  } else {
    stream = new FilePathStream(path, std::move(fs));
  }

  auto id = static_cast<unsigned>(impl->displays.size());
  display = new DisplayImpl(id, config, stream);
  impl->displays.emplace_back(display);
  return Display(display);
}

// Open an input/output stream.
Display DisplayManager::OpenStream(
    std::istream &is, const DisplayConfiguration &config) const {
  if (&is == &(std::cin) && impl->stdin_display) {
    return Display(impl->stdin_display);
  }

  auto id = static_cast<unsigned>(impl->displays.size());
  auto stream = new UserSuppliedStream(config.name, is);
  auto display = new DisplayImpl(id, config, stream);
  impl->displays.emplace_back(display);

  if (&is == &(std::cin)) {
    impl->stdin_display = display;
  }

  return Display(display);
}

// Tries to read a character from a display, given its position. Returns
// `true` if successful and updates `*ch_out`.
bool DisplayManager::TryReadChar(DisplayPosition position, char *ch_out) const {
  if (position.IsInvalid()) {
    return false;
  }

  display::PositionInterpreter interpreter = {};
  interpreter.flat = position.opaque_data;
  const auto display_id = interpreter.position.display_id;
  if (!display_id || display_id >= impl->displays.size()) {
    return false;
  }

  return impl->displays[display_id]->TryReadChar(
      interpreter.position.index, ch_out);
}

// Tries to read a range of characters from a display. Returns `true` if
// successful and updates `*data_out`.
//
// NOTE(pag): `*data_out` is valid for the lifetime of this `DisplayManager`
bool DisplayManager::TryReadData(DisplayRange range,
                                 std::string_view *data_out) const {
  display::PositionInterpreter interpreter = {};
  interpreter.flat = range.from.opaque_data;
  const auto display_id = interpreter.position.display_id;
  if (!display_id || display_id >= impl->displays.size()) {
    return false;
  }

  return Display(impl->displays[display_id].get()).TryReadData(
      range, data_out);
}

// Try to displace `position` by `num_bytes`. If successful, modifies
// `position` in place, and returns `true`, otherwise returns `false`.
bool DisplayManager::TryDisplacePosition(
    DisplayPosition &position, int num_bytes) const {
  if (!position.IsValid()) {
    return false;
  }

  display::PositionInterpreter interpreter = {position.opaque_data};
  const auto display_id = interpreter.position.display_id;
  if (!display_id || display_id >= impl->displays.size()) {
    return false;
  }

  DisplayImpl * const display = impl->displays[display_id].get();
  const auto index = static_cast<unsigned>(interpreter.position.index);

  if (!num_bytes) {
    return true;

  // Jump forward.
  } else if (0 < num_bytes) {
    const auto disp = static_cast<unsigned>(num_bytes);
    if (display->TryGetPosition(index + disp, &position)) {
      return true;
    }

    // `index + disp - 1` is the last character in the display.
    if (display->TryGetPosition(index + disp - 1, &position)) {
      interpreter.flat = position.opaque_data;
      position = DisplayPosition(
          display_id, interpreter.position.index + 1,
          interpreter.position.line, interpreter.position.column + 1);
      return true;
    }

    return false;

  // Jump backward.
  } else {
    const auto disp = static_cast<unsigned>(static_cast<int64_t>(-num_bytes));
    if (disp == index) {
      interpreter.position.index = 0;
      interpreter.position.line = 1;
      interpreter.position.column = 1;
      position.opaque_data = interpreter.flat;
      return true;

    } else if (disp > index) {
      return false;

    } else {
      return display->TryGetPosition(index - disp, &position);
    }
  }
}

}  // namespace hyde
