// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/Util/DefUse.h>
#include <drlojekyll/Util/Node.h>

#include <memory>
#include <optional>

namespace hyde {

class ErrorLog;
class ProgramImpl;
class Query;

namespace program {

template <typename T>
class ProgramNode {
 public:
  inline ProgramNode(Node<T> *impl_) : impl(impl_) {}

  inline bool operator==(ProgramNode<T> that) const {
    return impl == that.impl;
  }

  inline bool operator!=(ProgramNode<T> that) const {
    return impl == that.impl;
  }

  inline bool operator<(ProgramNode<T> that) const {
    return impl < that.impl;
  }

  uintptr_t UniqueId(void) const noexcept {
    return reinterpret_cast<uintptr_t>(impl);
  }

 protected:
  friend class ::hyde::ProgramImpl;

  Node<T> *impl;
};

}  // namespace program

enum TableKind {
  kPersistent,
  kVector
};

enum class VariableRole {
  kParameter, kLocal, kFree, kGlobalBoolean, kConditionRefCount
};

class Program {
 public:
  // Build a program from a query.
  static std::optional<Program> Build(const Query &query, const ErrorLog &log);

  ~Program(void);

  Program(const Program &) = default;
  Program(Program &&) noexcept = default;
  Program &operator=(const Program &) = default;
  Program &operator=(Program &&) noexcept = default;

 private:
  Program(std::shared_ptr<ProgramImpl> impl_);

  std::shared_ptr<ProgramImpl> impl;
};

}  // namespace hyde
