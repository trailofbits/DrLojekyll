// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include "Error.h"

#include <drlojekyll/Display/DisplayPosition.h>
#include <drlojekyll/Lex/Token.h>
#include <drlojekyll/Parse/Parse.h>

#include <cassert>
#include <iomanip>

namespace hyde {
namespace {

static void BeginColor(std::ostream &os, Color color) {
  switch (color) {
    case Color::kNone: return;
    case Color::kRed: os << "\033[31m"; break;
    case Color::kGreen: os << "\033[92m"; break;
    case Color::kGrey: os << "\033[90m"; break;
    case Color::kYellow: os << "\033[93m"; break;
    case Color::kBlue: os << "\033[94m"; break;
    case Color::kPurple: os << "\033[95m"; break;
    case Color::kBlack: os << "\033[30m"; break;
    case Color::kWhite: os << "\033[97m"; break;
  }
}

static void EndColor(std::ostream &os, Color color) {
  switch (color) {
    case Color::kNone: return;
    default: os << "\033[39m"; break;
  }
}

static void BeginBackgroundColor(std::ostream &os, Color color) {
  switch (color) {
    case Color::kNone: return;
    case Color::kRed: os << "\033[41m"; break;
    case Color::kGreen: os << "\033[42m"; break;
    case Color::kGrey: os << "\033[100m"; break;
    case Color::kYellow: os << "\033[43m"; break;
    case Color::kBlue: os << "\033[44m"; break;
    case Color::kPurple: os << "\033[45m"; break;
    case Color::kBlack: os << "\033[40m"; break;
    case Color::kWhite: os << "\033[107m"; break;
  }
}

static void EndBackgroundColor(std::ostream &os, Color color) {
  switch (color) {
    case Color::kNone: return;
    default: os << "\033[49m"; break;
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
    Color::kGrey,  // `disabled_source_line_color`.
    Color::kWhite,  // `error_source_line_color`.
    Color::kRed,  // `error_background_color`.
    Color::kWhite,  // `note_source_line_color`.
    Color::kBlue,  // `note_background_color`.
    Color::kWhite  // `text_color`.
};

const ErrorStream &ErrorStream::operator<<(const DisplayRange &range) const {
  std::string_view data;
  if (dm.TryReadData(range, &data)) {
    (*os) << data;
  }
  return *this;
}

// Stream in a token.
const ErrorStream &ErrorStream::operator<<(const Token &token) const {
  std::string_view token_data;
  if (dm.TryReadData(token.SpellingRange(), &token_data)) {
    (*os) << token_data;
  }
  return *this;
}

const ErrorStream &
ErrorStream::operator<<(const ParsedVariable &var) const {
  auto token = var.Name();
  std::string_view token_data;

  // NOTE(pag): We might synthesize variables that map to the range of non-
  //            variable things, so just double check that the thing looks like
  //            a variable.
  if (dm.TryReadData(token.SpellingRange(), &token_data) &&
      !token_data.empty() &&
      (token_data[0] == '_' ||
       (std::isalpha(token_data[0]) && std::isupper(token_data[0])))) {
    (*os) << token_data;
  } else {
    (*os) << "_MissingVar";
  }
  return *this;
}

const ErrorStream &
ErrorStream::operator<<(const std::optional<ParsedVariable> &maybe_var) const {
  if (maybe_var.has_value()) {
    return (*this) << (*maybe_var);
  } else {
    (*os) << "_MissingVar";
    return *this;
  }
}

// An error with no position information.
Error::Error(const DisplayManager &dm)
    : impl(std::make_shared<ErrorImpl>(dm)) {}

// An error message related to a line:column offset.
Error::Error(const DisplayManager &dm, const DisplayPosition &pos) : Error(dm) {
  impl->path = dm.DisplayName(pos);
  impl->line = pos.Line();
  impl->column = pos.Column();
}

// An error message related to a highlighted range of tokens.
Error::Error(const DisplayManager &dm, const DisplayRange &range)
    : Error(dm, range.From()) {

  std::string_view char_range;

  // If the start of this source range isn't on the beginning of its line then
  // go find the beginning of the line and try to add that code to `pre_source`.
  if (auto from = range.From(); from.IsValid() && 1 < from.Column()) {
    auto next_from = from;

    while (dm.TryDisplacePosition(next_from, -1) &&
           next_from.Line() == from.Line()) {
      from = next_from;
    }

    auto pre_range = DisplayRange(from, range.From());
    if (dm.TryReadData(pre_range, &char_range)) {
      impl->pre_source = char_range;
    }
  }

  // Try to read in the source code of the main range of code to get.
  if (dm.TryReadData(range, &char_range)) {
    impl->hightlight_line = range.From().Line();
    impl->source = char_range;
  }

  // If there is stuff after the line on the last line, then go get that.
  if (auto to = range.To(); to.IsValid()) {
    auto next_to = to;
    auto stop = false;
    while (!stop && dm.TryDisplacePosition(next_to, 1)) {
      stop = next_to.Line() > to.Line();
      to = next_to;
    }
    auto post_range = DisplayRange(range.To(), to);
    if (dm.TryReadData(post_range, &char_range)) {
      impl->post_source_start = impl->source.size();
      impl->post_source_len = char_range.size();
      impl->source.insert(impl->source.end(), char_range.begin(),
                          char_range.end());
    }
  }

  if (!impl->post_source_len) {
    impl->source.push_back(' ');
    impl->source.push_back(' ');
  }
}

// An error message related to a highlighted range of tokens, with one
// character in particular being referenced.
Error::Error(const DisplayManager &dm, const DisplayRange &range_,
             const DisplayPosition &pos_in_range)
    : Error(dm, range_, DisplayRange(pos_in_range, NextByte(dm, pos_in_range)),
            pos_in_range) {}

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
    : Error(dm, range) {

  if (auto from = sub_range.From(); from.IsValid()) {
    impl->line = from.Line();
    impl->column = from.Column();
  }

  int64_t num_bytes = 0;

  impl->hightlight_line = range.From().Line();
  impl->is_error.clear();
  impl->is_error.resize(impl->source.size(), false);
  auto has_dist = range.From().TryComputeDistanceTo(
      sub_range.From(), &num_bytes, nullptr, nullptr);

  if (!has_dist || 0 > num_bytes ||
      static_cast<size_t>(num_bytes) >= impl->is_error.size()) {
    return;
  }

  int64_t range_num_bytes = 0;
  if (!sub_range.TryComputeDistance(&range_num_bytes, nullptr, nullptr)) {
    return;
  }

  if (0 >= range_num_bytes) {
    return;
  }

  for (auto i = num_bytes; (i < (num_bytes + range_num_bytes) &&
                            static_cast<size_t>(i) < impl->is_error.size());
       ++i) {
    impl->is_error[static_cast<size_t>(i)] = true;
  }
}

// Attach an empty to the the error message.
::hyde::Note Error::Note(void) const {
  Error note(impl->display_manager);
  note.impl->next.swap(impl->next);
  impl->next.swap(note.impl);
  return ::hyde::Note(note.impl.get());
}

// Attach a note to the original error.
::hyde::Note Error::Note(const DisplayPosition &pos) const {
  Error note(impl->display_manager, pos);
  note.impl->next.swap(impl->next);
  impl->next.swap(note.impl);
  return ::hyde::Note(impl->next.get());
}

// An note related to a highlighted range of tokens.
::hyde::Note Error::Note(const DisplayRange &range) const {
  Error note(impl->display_manager, range);
  note.impl->next.swap(impl->next);
  impl->next.swap(note.impl);
  return ::hyde::Note(impl->next.get());
}

// A note related to a highlighted range of tokens, with one
// character in particular being referenced.
::hyde::Note Error::Note(const DisplayRange &range,
                         const DisplayPosition &pos_in_range) const {
  Error note(impl->display_manager, range, pos_in_range);
  note.impl->next.swap(impl->next);
  impl->next.swap(note.impl);
  return ::hyde::Note(impl->next.get());
}

// An error message related to a highlighted range of tokens, with a sub-range
// in particular being referenced.
::hyde::Note Error::Note(const DisplayRange &range,
                         const DisplayRange &sub_range) const {
  Error note(impl->display_manager, range, sub_range);
  note.impl->next.swap(impl->next);
  impl->next.swap(note.impl);
  return ::hyde::Note(impl->next.get());
}

// An error message related to a highlighted range of tokens, with a sub-range
// in particular being referenced.
::hyde::Note Error::Note(const DisplayRange &range,
                         const DisplayRange &sub_range,
                         const DisplayPosition &pos_in_sub_range) const {
  Error note(impl->display_manager, range, sub_range, pos_in_sub_range);
  note.impl->next.swap(impl->next);
  impl->next.swap(note.impl);
  return ::hyde::Note(impl->next.get());
}

// Render the formatted error to a stream, along with any attached notes.
void Error::Render(std::ostream &os,
                   const ErrorColorScheme &color_scheme) const {
  auto category_color = color_scheme.error_category_color;
  auto category_name = "error: ";
  auto highlight_color = color_scheme.error_source_line_color;
  auto highlight_bgcolor = color_scheme.error_background_color;
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

    if (curr->hightlight_line) {
      auto print_line = true;
      auto line_num = curr->hightlight_line;
      auto i = 0u;
      auto source_line_color = color_scheme.source_line_color;

      for (; i < curr->source.size(); ++i) {

        // If we're on the same line as the last line of the highlighted range,
        // then switch over to the disabled source line color.
        if (impl->post_source_len && i >= impl->post_source_start &&
            source_line_color != color_scheme.disabled_source_line_color) {
          EndColor(ss, source_line_color);
          source_line_color = color_scheme.disabled_source_line_color;
          BeginColor(ss, source_line_color);
        }

        if (print_line) {
          ss << '\n';
          BeginColor(ss, color_scheme.line_color);
          assert(line_num != 4294967295u);
          ss << std::setfill(' ') << std::setw(8) << line_num << " | ";
          print_line = false;
          ++line_num;

          EndColor(ss, color_scheme.line_color);

          // On the first line, print the stuff before the highlighted range
          // that is on the same line.
          if (!i) {
            BeginColor(ss, color_scheme.disabled_source_line_color);
            for (auto ch : curr->pre_source) {
              ss << ch;
            }
            EndColor(ss, color_scheme.disabled_source_line_color);
          }

          BeginColor(ss, source_line_color);
        }

        const auto ch = curr->source[i];
        if ('\n' == ch) {
          if (i < curr->is_error.size() && curr->is_error[i]) {
            EndColor(ss, source_line_color);
            EndBackgroundColor(ss, color_scheme.background_color);
            BeginBackgroundColor(ss, highlight_bgcolor);
            BeginColor(ss, highlight_color);
            ss << ' ';
            EndColor(ss, highlight_color);
            EndBackgroundColor(ss, highlight_bgcolor);
            BeginBackgroundColor(ss, color_scheme.background_color);
            BeginColor(ss, color_scheme.source_line_color);
          }

          print_line = true;
          EndColor(ss, source_line_color);

        } else {
          if (i < curr->is_error.size() && curr->is_error[i]) {
            EndColor(ss, source_line_color);
            EndBackgroundColor(ss, color_scheme.background_color);
            BeginBackgroundColor(ss, highlight_bgcolor);
            BeginColor(ss, highlight_color);
            ss << ch;
            EndColor(ss, highlight_color);
            EndBackgroundColor(ss, highlight_bgcolor);
            BeginBackgroundColor(ss, color_scheme.background_color);
            BeginColor(ss, source_line_color);
          } else {
            ss << ch;
          }
        }
      }

      if (!print_line) {
        EndColor(ss, source_line_color);
      }
    }

    EndBackgroundColor(ss, color_scheme.background_color);
    ss << "\n\n";

    category_color = color_scheme.note_category_color;
    highlight_color = color_scheme.note_source_line_color;
    highlight_bgcolor = color_scheme.note_background_color;
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
