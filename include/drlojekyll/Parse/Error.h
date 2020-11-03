// Copyright 2019, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/Display/DisplayPosition.h>

#include <memory>
#include <ostream>
#include <type_traits>
#include <utility>
#include <variant>

namespace hyde {
namespace parse {
template <typename>
class ParsedNode;
}  // namespace parse

class DisplayManager;

enum class Color : unsigned char {
  kNone,
  kRed,
  kGreen,
  kGrey,
  kYellow,
  kBlue,
  kPurple,
  kBlack,
  kWhite
};

// Color scheme for printing errors out to a terminal emulator.
struct ErrorColorScheme {
  Color background_color;
  Color file_path_color;
  Color line_color;
  Color column_color;
  Color error_category_color;
  Color note_category_color;
  Color message_color;
  Color source_line_color;
  Color disabled_source_line_color;
  Color error_source_line_color;
  Color error_background_color;
  Color note_source_line_color;
  Color note_background_color;
  Color text_color;
};

class Error;
class ErrorImpl;
class Note;
class Token;
class DisplayRange;

// Used to stream in error information. This is a thing wrapper around a
// `std::ostream`, with support to taking in tokens and getting their spellings
// from a `DisplayManager`.
class ErrorStream {
 public:
  ErrorStream(const ErrorStream &) = default;
  ErrorStream(ErrorStream &&) noexcept = default;

  // Stream in a token.
  const ErrorStream &operator<<(const Token &token) const;

  const ErrorStream &operator<<(const DisplayRange &range) const;

  template <typename T1, typename... Rest>
  const ErrorStream &operator<<(std::variant<T1, Rest...> options) const {
    std::visit(
        [=](auto &&arg) {
          DisplayRange range = arg.SpellingRange();
          (*this) << range;
        },
        options);
    return *this;
  }

  struct node_tag {};
  struct not_node_tag {};

  static inline constexpr node_tag *kNode = nullptr;
  static inline constexpr not_node_tag *kNotNode = nullptr;

  template <typename T, typename std::enable_if<
                            std::is_convertible_v<T *, parse::ParsedNode<T> *>,
                            node_tag *>::type = kNode>
  inline const ErrorStream &operator<<(T node) const {
    (*this) << node.SpellingRange();
    return *this;
  }

  template <typename T, typename std::enable_if<
                            !std::is_convertible_v<T *, parse::ParsedNode<T> *>,
                            not_node_tag *>::type = kNotNode>
  inline const ErrorStream &operator<<(T data) const {
    (*os) << data;
    return *this;
  }

 private:
  friend class Error;
  friend class Note;

  inline explicit ErrorStream(std::ostream *os_, const DisplayManager &dm_)
      : os(os_),
        dm(dm_) {}

  std::ostream *const os;
  const DisplayManager &dm;
};

// A note is an addendum to an error that adds additional context. It is fully
// owned by its corresponding error.
class Note {
 public:
  template <typename T>
  inline ErrorStream operator<<(T val) const {
    return Stream() << val;
  }

 private:
  friend class Error;

  inline Note(ErrorImpl *impl_) : impl(impl_) {}

  ErrorStream Stream(void) const;

  ErrorImpl *const impl;
};

// Represents an error that was discovered during parsing or semantic analysis.
class Error {
 public:
  // An error with no position information.
  explicit Error(const DisplayManager &dm);

  // An error message related to a line:column offset.
  Error(const DisplayManager &dm, const DisplayPosition &pos);

  // An error message related to a highlighted range of tokens.
  Error(const DisplayManager &dm, const DisplayRange &range);

  // An error message related to a highlighted range of tokens, with one
  // character in particular being referenced.
  Error(const DisplayManager &dm, const DisplayRange &range,
        const DisplayPosition &pos_in_range);

  // An error message related to a highlighted range of tokens, with a sub-range
  // in particular being referenced.
  Error(const DisplayManager &dm, const DisplayRange &range,
        const DisplayRange &sub_range);

  // An error message related to a highlighted range of tokens, with a sub-range
  // in particular being referenced, where the error itself is at
  // `pos_in_range`.
  Error(const DisplayManager &dm, const DisplayRange &range,
        const DisplayRange &sub_range, const DisplayPosition &pos_in_range);

  template <typename T>
  inline ErrorStream operator<<(T val) const {
    return Stream() << val;
  }

  // Default color scheme for logging.
  static const ErrorColorScheme kDefaultColorScheme;

  // Render the formatted error to a stream, along with any attached notes.
  void Render(std::ostream &os,
              const ErrorColorScheme &color_scheme = kDefaultColorScheme) const;

  // Attach an empty to the the error message.
  ::hyde::Note Note(void) const;

  // Attach a note to the original error.
  ::hyde::Note Note(const DisplayPosition &pos) const;

  // An note related to a highlighted range of tokens.
  ::hyde::Note Note(const DisplayRange &range) const;

  // A note related to a highlighted range of tokens, with one
  // character in particular being referenced.
  ::hyde::Note Note(const DisplayRange &range,
                    const DisplayPosition &pos_in_range) const;

  // An error message related to a highlighted range of tokens, with a sub-range
  // in particular being referenced.
  ::hyde::Note Note(const DisplayRange &range,
                    const DisplayRange &sub_range) const;

 private:
  ErrorStream Stream(void) const;

  std::shared_ptr<ErrorImpl> impl;
};

}  // namespace hyde
