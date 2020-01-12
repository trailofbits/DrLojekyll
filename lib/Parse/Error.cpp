// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include "Error.h"

#include <iomanip>
#include <cassert>
#include <drlojekyll/Display/DisplayManager.h>
#include <drlojekyll/Display/DisplayPosition.h>

#include <drlojekyll/Lex/Token.h>

namespace hyde {
namespace {

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

static DisplayPosition NextByte(const DisplayManager &dm, DisplayPosition pos) {
  if (dm.TryDisplacePosition(pos, 1)) {
    return pos;
  } else {
    return DisplayPosition();
  }
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
  Color::kWhite,  // `highlight_color`.
  Color::kRed,  // `highlight_background_color`.
  Color::kWhite  // `text_color`.
};


const ErrorStream &ErrorStream::operator<<(const DisplayRange &range) const {
  if (dm) {
    std::string_view data;
    if (dm->TryReadData(range, &data)) {
      (*os) << data;
    }
  }
  return *this;
}

// Stream in a token.
const ErrorStream &ErrorStream::operator<<(const Token &token) const {
  if (dm) {
    std::string_view token_data;
    if (dm->TryReadData(token.SpellingRange(), &token_data)) {
      (*os) << token_data;
    }
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
  std::string_view char_range;
  if (dm.TryReadData(range, &char_range)) {
    impl->source = char_range;
    impl->source.push_back(' ');
    impl->source.push_back(' ');
  }
}

// An error message related to a highlighted range of tokens, with one
// character in particular being referenced.
Error::Error(const DisplayManager &dm, const DisplayRange &range_,
             const DisplayPosition &pos_in_range)
   : Error(dm, DisplayRange(pos_in_range, NextByte(dm, pos_in_range))) {}

// An error message related to a highlighted range of tokens, with a sub-range
// in particular being referenced, where the error itself is at
// `pos_in_range`.
Error::Error(const DisplayManager &dm, const DisplayRange &range,
             const DisplayRange &sub_range, const DisplayPosition &pos_in_range)
   : Error(dm, range, sub_range) {

  if (pos_in_range.IsValid()) {
    impl->line = pos_in_range.Line();
    impl->column = pos_in_range.Column();
  }
}

// An error message related to a highlighted range of tokens, with a sub-range
// in particular being referenced.
Error::Error(const DisplayManager &dm, const DisplayRange &range,
             const DisplayRange &sub_range)
   : Error(dm, sub_range.From()) {

  int num_bytes = 0;
  std::string_view char_range;
  if (dm.TryReadData(range, &char_range)) {
    impl->source = char_range;
    impl->source.push_back(' ');
    impl->source.push_back(' ');
  } else {
    return;
  }

  impl->hightlight_line = range.From().Line();
  impl->is_error.clear();
  impl->is_error.resize(impl->source.size(), false);
  auto has_dist = range.From().TryComputeDistanceTo(
      sub_range.From(), &num_bytes, nullptr, nullptr);

  if (!has_dist || 0 > num_bytes ||
      static_cast<size_t>(num_bytes) >= impl->is_error.size()) {
    return;
  }

  int range_num_bytes = 0;
  if (!sub_range.TryComputeDistance(
      &range_num_bytes, nullptr, nullptr)) {
    return;
  }

  if (0 >= range_num_bytes) {
    return;
  }

  for (auto i = num_bytes;
       (i < (num_bytes + range_num_bytes) &&
        static_cast<size_t>(i) < impl->is_error.size());
       ++i) {
    impl->is_error[static_cast<size_t>(i)] = true;
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

    if (impl->hightlight_line) {
      auto print_line = true;
      auto line_num = impl->hightlight_line;
      auto i = 0u;

      for (; i < curr->source.size(); ++i) {
        const auto ch = curr->source[i];
        if (print_line) {
          ss << '\n';
          BeginColor(ss, color_scheme.line_color);
          ss << std::setfill(' ') << std::setw(8)
             << line_num << " | ";
          print_line = false;
          ++line_num;
          EndColor(ss, color_scheme.line_color);
          BeginColor(ss, color_scheme.source_line_color);
        }

        if ('\n' == ch) {
          if (i < curr->is_error.size() && curr->is_error[i]) {
            EndColor(ss, color_scheme.source_line_color);
            EndBackgroundColor(ss, color_scheme.background_color);
            BeginBackgroundColor(ss, color_scheme.highlight_background_color);
            BeginColor(ss, color_scheme.highlight_color);
            ss << ' ';
            EndColor(ss, color_scheme.highlight_color);
            EndBackgroundColor(ss, color_scheme.highlight_background_color);
            BeginBackgroundColor(ss, color_scheme.background_color);
            BeginColor(ss, color_scheme.source_line_color);
          }

          print_line = true;
          EndColor(ss, color_scheme.source_line_color);

        } else {
          if (i < curr->is_error.size() && curr->is_error[i]) {
            EndColor(ss, color_scheme.source_line_color);
            EndBackgroundColor(ss, color_scheme.background_color);
            BeginBackgroundColor(ss, color_scheme.highlight_background_color);
            BeginColor(ss, color_scheme.highlight_color);
            ss << ch;
            EndColor(ss, color_scheme.highlight_color);
            EndBackgroundColor(ss, color_scheme.highlight_background_color);
            BeginBackgroundColor(ss, color_scheme.background_color);
            BeginColor(ss, color_scheme.source_line_color);
          } else {
            ss << ch;
          }
        }
      }

      if (!print_line) {
        EndColor(ss, color_scheme.source_line_color);
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
