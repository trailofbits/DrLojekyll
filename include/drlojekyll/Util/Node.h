// Copyright 2019, Trail of Bits. All rights reserved.

#pragma once

#include <cstdint>
#include <type_traits>

namespace hyde {

template <typename T>
class Node;

template <typename T>
class NodeIterator;

template <typename T>
class NodeRange;

// Used for traversing nodes that are arranged in a list. Class based so that
// the use of `Next` and `Prev` are privileged.
class NodeTraverser {
 private:
  template <typename T>
  friend class NodeIterator;

  static void *Next(void *, intptr_t);
};

// Iterator over a chain of nodes.
template <typename T>
class NodeIterator {
 public:
  NodeIterator(const NodeIterator<T> &) noexcept = default;
  NodeIterator(NodeIterator<T> &&) noexcept = default;

  NodeIterator<T> &operator=(const NodeIterator<T> &) noexcept = default;
  NodeIterator<T> &operator=(NodeIterator<T> &&) noexcept = default;

  T operator*(void) const {
    return T(impl);
  }

  T operator->(void) const = delete;

  bool operator==(NodeIterator<T> that) const {
    return impl == that.impl;
  }

  bool operator!=(NodeIterator<T> that) const {
    return impl != that.impl;
  }

  inline NodeIterator<T> &operator++(void) {
    impl = reinterpret_cast<Node<T> *>(NodeTraverser::Next(impl, offset));
    return *this;
  }

  inline NodeIterator<T> operator++(int) const {
    auto ret = *this;
    impl = reinterpret_cast<Node<T> *>(NodeTraverser::Next(impl, offset));
    return ret;
  }

 private:
  friend class NodeRange<T>;

  inline explicit NodeIterator(Node<T> *impl_, intptr_t offset_)
      : impl(impl_), offset(offset_) {}

  NodeIterator(void) = default;

  Node<T> *impl{nullptr};
  intptr_t offset{0};
};

template <typename T>
class NodeRange {
 public:
  NodeRange(void) = default;

  inline explicit NodeRange(Node<T> *impl_, intptr_t offset_)
      : impl(impl_), offset(offset_) {}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Winvalid-offsetof"
  template <typename U>
  inline explicit NodeRange(Node<U> *impl_)
      : impl(impl_)
      , offset(static_cast<intptr_t>(__builtin_offsetof(Node<U>, next))) {}
#pragma GCC diagnostic pop

  inline NodeIterator<T> begin(void) const {
    return NodeIterator<T>(impl, offset);
  }

  inline NodeIterator<T> end(void) const {
    return {};
  }

 private:
  template <typename U>
  friend class Node;

  Node<T> *impl{nullptr};
  intptr_t offset{0};
};

}  // namespace hyde
