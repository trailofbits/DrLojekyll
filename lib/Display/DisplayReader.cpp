// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include <drlojekyll/Display.h>

#include <cassert>

#include "DataStream.h"
#include "Display.h"
#include "DisplayPosition.h"

namespace hyde {

DisplayReader::~DisplayReader(void) {}

DisplayReader::DisplayReader(const Display &display_)
    : display(display_.impl) {}

// Tries to read a character from the display. If successful, returns `true`
// and updates `*ch_out`.
bool DisplayReader::TryReadChar(char *ch_out) {
  char ch = '\0';
  if (index < display->data.size()) {
    ch = display->data[index];
    index += 1;

  } else if (display->TryReadChar(index, &ch)) {
    index += 1;

  } else {
    return false;
  }

  if ('\n' == ch) {
    line += 1;
    column = 1;
  } else {
    column += 1;
  }

  if (ch_out) {
    *ch_out = ch;
  }

  return true;
}

// Returns `true` if there was an error, and if `os` is non-NULL, outputs
// the error message to the `os` stream.
bool DisplayReader::TryGetErrorMessage(std::ostream *os) const {
  return display->stream->TryGetErrorMessage(os);
}

// Unreads the last read character.
void DisplayReader::UnreadChar(void) {
  if (index > 0) {
    index -= 1;

    // If we unread across a line boundary, then we need to count the number
    // of characters on the previous line.
    if (display->data[index] == '\n') {
      column = 1;
      assert(1 < line);
      line -= 1;

      for (auto j = 0u; j < index; ++j) {
        if (display->data[index - j - 1] == '\n') {
          break;
        } else {
          column += 1;
        }
      }
    } else {
      column -= 1;
    }
  }
}

// Returns the current display position.
DisplayPosition DisplayReader::CurrentPosition(void) const {
  return DisplayPosition(display->id, index, line, column);
}

}  // namespace hyde
