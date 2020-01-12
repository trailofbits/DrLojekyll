// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

#include <iosfwd>

#include <drlojekyll/Parse/Parse.h>
#include <drlojekyll/Util/Node.h>

namespace hyde {

class DisplayManager;
class ParsedModule;

namespace bam {

template <typename T>
class CodeFragment {
 public:
  inline CodeFragment(Node<T> *impl_)
      : impl(impl_) {}

  inline bool operator==(CodeFragment<T> that) const {
    return impl == that.impl;
  }

  inline bool operator!=(CodeFragment<T> that) const {
    return impl == that.impl;
  }

  inline bool operator<(CodeFragment<T> that) const {
    return impl < that.impl;
  }

  uintptr_t UniqueId(void) const noexcept {
    return reinterpret_cast<uintptr_t>(impl);
  }

 protected:
  Node<T> *impl;
};

}  // namespace bam

class FunctionParameter;
class FunctionArgument;

// A generic value.
class Value : public bam::CodeFragment<Value> {
 public:
  TypeLoc Type(void) const noexcept;
  DisplayRange SpellingRange(void) const noexcept;
  NodeRange<Value> Uses(void) const;

  bool IsParameter(void) const noexcept;
  bool IsArgument(void) const noexcept;

  bool ReplaceAllUses(const Value &that) const noexcept;
  bool ReplaceAllUses(const FunctionParameter &that) const noexcept;
  bool ReplaceAllUses(const FunctionArgument &that) const noexcept;
};

class FunctionParameter : public bam::CodeFragment<FunctionParameter> {
 public:
};

class FunctionArgument : public bam::CodeFragment<FunctionArgument> {
 public:
};

class Function : public bam::CodeFragment<Function> {
 public:
};

class FunctionCall : public bam::CodeFragment<FunctionCall> {
 public:
};

// Generates BAM-like code following the push method of pipelined bottom-up
// execution of datalog.
void CodeGenBAM(
    const DisplayManager &display_manager, const ParsedModule &root_module,
    std::ostream &cxx_os);

}  // namespace hyde
