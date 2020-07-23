// Copyright 2019, Trail of Bits. All rights reserved.

#pragma once

#include <string>

namespace hyde {

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

}  // namespace hyde
