// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include "Error.h"

#include <string>

#include <drlojekyll/Display/DisplayManager.h>
#include <drlojekyll/Display/DisplayPosition.h>

#include <drlojekyll/Lex/Token.h>

namespace hyde {
namespace {

static bool HasNewLines(std::string_view range) {
  for (auto ch : range) {
    if (ch == '\n') {
      return true;
    }
  }
  return false;
}

static std::string_view TruncateToOneLine(std::string_view range) {
  size_t i = 0;
  for (auto ch : range) {
    if (ch == '\n') {
      if (!i) {
        return TruncateToOneLine(range.substr(1));
      } else {
        return range.substr(0, i);
      }
    } else {
      ++i;
    }
  }
  return range;
}

static bool IsEmpty(std::string_view range) {
  for (auto ch : range) {
    if (ch != ' ' && ch != '\n') {
      return false;
    }
  }
  return true;
}

// Generate a carat line that highlights the error.
static void OutputCaratLine(std::stringstream &ss, std::string_view ref_line,
                            uint64_t start_offset, uint64_t size) {
  if ((start_offset + size) > ref_line.size()) {
    return;
  }

  ss << '\n';

  ss << "    ";

  for (size_t i = 0; i < start_offset; ++i) {
    ss << ' ';
  }

  if (!size) {
    ss << '^';
  } else {
    for (size_t i = 0; i < size; ++i) {
      ss << '~';
    }
  }
}

static void BeginColor(std::ostream &os, Color color) {
  switch (color) {
    case Color::kNone:
      return;
    case Color::kRed:
      os << "\033[31m";
      break;
    case Color::kGreen:
      os << "\033[92m";
      break;
    case Color::kYellow:
      os << "\033[93m";
      break;
    case Color::kBlue:
      os << "\033[94m";
      break;
    case Color::kPurple:
      os << "\033[95m";
      break;
    case Color::kBlack:
      os << "\033[30m";
      break;
    case Color::kWhite:
      os << "\033[97m";
      break;
  }
}

static void EndColor(std::ostream &os, Color color) {
  switch (color) {
    case Color::kNone:
      return;
    default:
      os << "\033[39m";
      break;
  }
}

static void BeginBackgroundColor(std::ostream &os, Color color) {
  switch (color) {
    case Color::kNone:
      return;
    case Color::kRed:
      os << "\033[41m";
      break;
    case Color::kGreen:
      os << "\033[42m";
      break;
    case Color::kYellow:
      os << "\033[43m";
      break;
    case Color::kBlue:
      os << "\033[44m";
      break;
    case Color::kPurple:
      os << "\033[45m";
      break;
    case Color::kBlack:
      os << "\033[40m";
      break;
    case Color::kWhite:
      os << "\033[107m";
      break;
  }
}

static void EndBackgroundColor(std::ostream &os, Color color) {
  switch (color) {
    case Color::kNone:
      return;
    default:
      os << "\033[49m";
      break;
  }
}

static void OutputColored(std::ostream &os, Color color, std::string_view str) {
  BeginColor(os, color);
  os << str;
  EndColor(os, color);
}

static void OutputColored(std::ostream &os, Color color, const char *str) {
  BeginColor(os, color);
  os << str;
  EndColor(os, color);
}

static void OutputColored(std::ostream &os, Color color, uint64_t dec_num) {
  BeginColor(os, color);
  os << dec_num;
  EndColor(os, color);
}

}  // namespace

const ErrorColorScheme Error::kDefaultColorScheme = {
  Color::kBlack,  // `background_color`.
  Color::kWhite,  // `file_path_color`.
  Color::kWhite,  // `line_color`.
  Color::kWhite,  // `column_color`.
  Color::kRed,  // `error_category_color`.
  Color::kGreen,  // `note_category_color`.
  Color::kWhite,  // `message_color`.
  Color::kYellow,  // `source_line_color`.
  Color::kGreen,  // `carat_line_color`.
  Color::kWhite  // `text_color`.
};

// Stream in a token.
const ErrorStream &ErrorStream::operator<<(const Token &token) const {
  if (dm) {
    std::string_view token_data;
    if (dm->TryReadData(token.SpellingRange(), &token_data)) {
      (*os) << token_data;
    } else {
      (*os) << "!!!";
    }
  } else {
    (*os) << "???";
  }
  return *this;
}


Error::~Error(void) {}

// A basic error message with no file/location information.
Error::Error(void)
    : impl(std::make_shared<ErrorImpl>()) {}

// An error message related to a line:column offset.
Error::Error(const DisplayManager &dm, const DisplayPosition &pos)
    : Error() {
  impl->display_manager = &dm;
  impl->path = dm.DisplayName(pos);
  impl->line = pos.Line();
  impl->column = pos.Column();
}

// An error message related to a highlighted range of tokens.
Error::Error(const DisplayManager &dm, const DisplayRange &range)
    : Error(dm, range.From()) {
  dm.TryReadData(range, &(impl->highlight_range));
  impl->highlight_range = TruncateToOneLine(impl->highlight_range);
}

// An error message related to a highlighted range of tokens, with one
// character in particular being referenced.
Error::Error(const DisplayManager &dm, const DisplayRange &range_,
             const DisplayPosition &pos_in_range)
   : Error(dm, pos_in_range) {
  auto range = range_;

  int line_diff = 0;
  int col_diff = 0;
  int num_bytes = 0;
  auto has_dist = range.From().TryComputeDistanceTo(
      pos_in_range, &num_bytes, &line_diff, &col_diff);
  dm.TryReadData(range, &(impl->highlight_range));

  // Try to find the line containing `sub_range`.
  if (HasNewLines(impl->highlight_range)) {
    for (auto i = 0; i < num_bytes; ++i) {
      auto x = static_cast<unsigned>(num_bytes - i - 1);
      if (x > impl->highlight_range.size()) {
        break;
      } else if (impl->highlight_range[x] != '\n') {
        continue;
      }

      auto new_start = pos_in_range;
      if (dm.TryDisplacePosition(new_start, -i)) {
        range = DisplayRange(new_start, range.To());
        has_dist = range.From().TryComputeDistanceTo(
            pos_in_range, &num_bytes, &line_diff, &col_diff);
        dm.TryReadData(range, &(impl->highlight_range));
      }

      break;
    }
  }

  if (HasNewLines(impl->highlight_range)) {
    if (has_dist) {

      if (line_diff) {
        DisplayRange new_range(pos_in_range, range.To());
        dm.TryReadData(new_range, &(impl->highlight_range));
        impl->highlight_range = TruncateToOneLine(impl->highlight_range);

      } else if (0 > col_diff) {
        DisplayRange new_range(pos_in_range, range.From());
        dm.TryReadData(new_range, &(impl->highlight_range));
        impl->highlight_range = TruncateToOneLine(impl->highlight_range);

      } else {
        impl->highlight_range = TruncateToOneLine(impl->highlight_range);
        impl->carat = static_cast<size_t>(col_diff);
      }

    } else {
      impl->highlight_range = std::string_view();
      impl->line = range.From().Line();
      impl->column = ~0u;
    }

  } else if (has_dist) {
    if (0 > col_diff || line_diff) {
      impl->highlight_range = std::string_view();

    } else {
      impl->carat = static_cast<size_t>(col_diff);
    }

  } else {
    impl->highlight_range = std::string_view();
  }
}

// An error message related to a highlighted range of tokens, with a sub-range
// in particular being referenced.
Error::Error(const DisplayManager &dm, const DisplayRange &range,
             const DisplayRange &sub_range)
   : Error(dm, range, sub_range.From()) {
  int line_diff = 0;
  int col_diff = 0;
  if (std::string::npos != impl->carat &&
      sub_range.TryComputeDistance(nullptr, &line_diff, &col_diff)) {
    if (!line_diff && 0 < col_diff) {
      impl->underline_length = static_cast<unsigned>(col_diff);
    }
  }
}

// Attach an empty to the the error message.
::hyde::Note Error::Note(void) const {
  Error note;
  note.impl->next.swap(impl->next);
  impl->next.swap(note.impl);
  return ::hyde::Note(note.impl.get());
}

// Attach a note to the original error.
::hyde::Note Error::Note(const DisplayManager &dm,
                         const DisplayPosition &pos) const {
  Error note(dm, pos);
  note.impl->next.swap(impl->next);
  impl->next.swap(note.impl);
  return ::hyde::Note(impl->next.get());
}

// An note related to a highlighted range of tokens.
::hyde::Note Error::Note(const DisplayManager &dm,
                         const DisplayRange &range) const {
  Error note(dm, range);
  note.impl->next.swap(impl->next);
  impl->next.swap(note.impl);
  return ::hyde::Note(impl->next.get());
}

// A note related to a highlighted range of tokens, with one
// character in particular being referenced.
::hyde::Note Error::Note(const DisplayManager &dm, const DisplayRange &range,
                         const DisplayPosition &pos_in_range) const {
  Error note(dm, range, pos_in_range);
  note.impl->next.swap(impl->next);
  impl->next.swap(note.impl);
  return ::hyde::Note(impl->next.get());
}

// An error message related to a highlighted range of tokens, with a sub-range
// in particular being referenced.
::hyde::Note Error::Note(const DisplayManager &dm, const DisplayRange &range,
                         const DisplayRange &sub_range) const {
  Error note(dm, range, sub_range);
  note.impl->next.swap(impl->next);
  impl->next.swap(note.impl);
  return ::hyde::Note(impl->next.get());
}

// Render the formatted error to a stream, along with any attached notes.
void Error::Render(std::ostream &os,
                   const ErrorColorScheme &color_scheme) const {
  auto category_color = color_scheme.error_category_color;
  auto category_name = "error: ";
  std::stringstream ss;

  for (auto curr = impl.get(); curr; curr = curr->next.get()) {
    BeginBackgroundColor(ss, color_scheme.background_color);
    if (!curr->path.empty()) {
      OutputColored(ss, color_scheme.file_path_color, curr->path);
      if (curr->line < ~0u) {
        OutputColored(ss, color_scheme.text_color, ":");
        OutputColored(ss, color_scheme.line_color, curr->line);
        if (curr->column < ~0u) {
          OutputColored(ss, color_scheme.text_color, ":");
          OutputColored(ss, color_scheme.column_color, curr->column);
        }
      }
      ss << ' ';
    }
    OutputColored(ss, category_color, category_name);
    OutputColored(ss, color_scheme.message_color, curr->message.str());

    auto highlight_range = TruncateToOneLine(curr->highlight_range);
    if (!IsEmpty(highlight_range)) {
      ss << "\n    ";
      OutputColored(ss, color_scheme.source_line_color, highlight_range);
      if (std::string::npos != curr->carat) {
        BeginColor(ss, color_scheme.carat_line_color);
        OutputCaratLine(ss, highlight_range,
                        static_cast<unsigned>(curr->carat),
                        curr->underline_length);
        EndColor(ss, color_scheme.carat_line_color);
      }
    }
    EndBackgroundColor(ss, color_scheme.background_color);
    ss << "\n\n";

    category_color = color_scheme.note_category_color;
    category_name = "note: ";
  }
  os << ss.str();
}

ErrorStream Note::Stream(void) const {
  return ErrorStream(&(impl->message), impl->display_manager);
}

ErrorStream Error::Stream(void) const {
  return ErrorStream(&(impl->message), impl->display_manager);
}

}  // namespace hyde
