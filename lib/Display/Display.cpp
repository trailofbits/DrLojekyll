// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include "Display.h"

#include <sstream>

#include "DataStream.h"
#include "DisplayPosition.h"

namespace hyde {
namespace {

static constexpr size_t kWayPointIndex = 256;

// Stream that never reads data, and reports an error.
class ErrorStream final : public display::DataStream {
 public:
  virtual ~ErrorStream(void) = default;

  explicit ErrorStream(std::string &&error_message_)
      : error_message(std::forward<std::string>(error_message_)) {}

  bool ReadData(std::string_view *data_out) override {
    if (data_out) {
      *data_out = std::string_view();
    }
    return false;
  }

  bool TryGetErrorMessage(std::ostream *os) const override {
    if (os) {
      (*os) << error_message;
    }
    return true;
  }

 private:
  const std::string error_message;
};

}  // namespace

DisplayImpl::DisplayImpl(unsigned id_, const DisplayConfiguration &config_,
                         display::DataStream *stream_)
    : id(id_),
      config(config_),
      stream(stream_) {
  if (!stream) {
    std::stringstream ss;
    ss << "Empty stream '" << config.name << "'";
    stream.reset(new ErrorStream(ss.str()));
  }
  data.reserve(4096);
}

DisplayImpl::~DisplayImpl(void) {}

// Tries to read the character at index `index`. This will optionally call
// out to the stream to get more data.
bool DisplayImpl::TryReadChar(uint64_t index, char *ch_out) {
  std::string_view data_read;

  auto try_add_waypoint = [this](void) {
    if (!(data.size() % kWayPointIndex)) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
      display::PositionInterpreter interpreter = {};
      interpreter.position.index = data.size();
      interpreter.position.line = next_line;
      interpreter.position.column = next_column;
      interpreter.position.display_id = id;
#pragma GCC diagnostic pop
      const DisplayPosition waypoint(interpreter.flat);
      waypoints.emplace_back(waypoint);
    }
  };

  do {

    // Go through and import any read characters. This will perform tab
    // expansion according to `config`, and if a bad character is found, it
    // will swap out `stream` with something that can report the position.
    for (auto ch : data_read) {
      switch (ch) {
        case '\r': continue;

        case '\n':
          try_add_waypoint();
          data.push_back(ch);
          next_line += 1;
          next_column = 1;
          continue;

        // Perform tab expansion.
        case '\t': {
          const auto curr_column = next_column;
          next_column += config.num_spaces_in_tab;

          // TODO(pag): Unfortunately these apply inside strings too :-/
          if (config.use_tab_stops) {
            next_column = (next_column / config.num_spaces_in_tab) *
                          config.num_spaces_in_tab;
          }

          for (auto i = 0U; i < (next_column - curr_column); ++i) {
            try_add_waypoint();
            data.push_back(' ');
          }

          continue;
        }

        default:
          if (ch < ' ' || ch > '~') {
            std::stringstream ss;
            ss << "Invalid character in stream '" << config.name << "' at line "
               << next_line << " and column " << next_column;
            stream.reset(new ErrorStream(ss.str()));
            goto done;
          } else {
            try_add_waypoint();
            data.push_back(ch);
            next_column += 1;
          }
          continue;
      }
    }

  done:
    if (index < data.size()) {
      if (ch_out) {
        *ch_out = data[index];
      }
      return true;
    }

  } while (stream->ReadData(&data_read));

  // Swap out the old stream, ideally closing file descriptors that may be
  // left open.
  auto new_stream = new display::StringViewStream(data);
  new_stream->MarkAsDone();
  stream.reset(new_stream);

  return false;
}

// Tries to get the position of the character at index `index`.
bool DisplayImpl::TryGetPosition(uint64_t index, DisplayPosition *pos_out) {
  if (!TryReadChar(index, nullptr)) {
    return false;
  }

  if (pos_out) {
    auto i = index / kWayPointIndex;
    const DisplayPosition waypoint = waypoints[i];
    if (!waypoint.IsValid()) {
      return false;
    }
    auto line = waypoint.Line();
    auto column = waypoint.Column();
    for (i = waypoint.Index(); i < data.size() && i < index; ++i) {
      if ('\n' == data[i]) {
        line += 1;
        column = 1;
      } else {
        column += 1;
      }
    }

    if (i == index) {
      *pos_out = DisplayPosition(id, index, line, column);
      return true;
    } else {
      return false;
    }
  }

  return true;
}

// Return the ID of this display.
unsigned Display::Id(void) const {
  return impl->id;
}

// Tries to read a character from this display, given its position. Returns
// `true` if successful and updates `*ch_out`.
bool Display::TryReadChar(DisplayPosition position, char *ch_out) const {
  if (position.IsInvalid()) {
    return false;
  }

  display::PositionInterpreter interpreter = {};
  interpreter.flat = position.opaque_data;
  if (interpreter.position.display_id != impl->id) {
    return false;
  }

  return impl->TryReadChar(interpreter.position.index, ch_out);
}

// Tries to read a range of characters from the display. Returns `true` if
// successful and updates `*data_out`.
bool Display::TryReadData(DisplayRange range,
                          std::string_view *data_out) const {
  if (range.IsInvalid()) {
    return false;
  }

  display::PositionInterpreter interpreter_from = {};
  interpreter_from.flat = range.from.opaque_data;

  display::PositionInterpreter interpreter_to = {};
  interpreter_to.flat = range.to.opaque_data;

  if (interpreter_from.position.display_id != impl->id) {
    return false;
  }

  const auto from_index =
      static_cast<unsigned>(interpreter_from.position.index);
  const auto to_index = static_cast<unsigned>(interpreter_to.position.index);

  // NOTE(pag): The range is an exclusive range.
  if (!impl->TryReadChar(to_index - 1, nullptr)) {
    return false;
  }

  if (data_out) {
    std::string_view view = impl->data;
    *data_out = view.substr(from_index, to_index - from_index);
  }

  return true;
}

}  // namespace hyde