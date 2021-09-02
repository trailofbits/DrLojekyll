// Copyright 2019, Trail of Bits. All rights reserved.

#pragma once

#include <cstdint>
#include <type_traits>

namespace hyde {

template <typename PublicT, typename PrivateT>
class Node {
 public:
  using PublicType = PublicT;
  using PrivateType = PrivateT;

  inline Node(PrivateT *impl_) : impl(impl_) {}

  inline bool operator==(const PublicT &that) const noexcept {
    return impl == that.impl;
  }

  inline bool operator!=(const PublicT &that) const noexcept {
    return impl != that.impl;
  }

  inline bool operator<(const PublicT &that) const noexcept {
    return impl < that.impl;
  }

  uintptr_t UniqueId(void) const noexcept {
    return reinterpret_cast<uintptr_t>(impl);
  }

  inline uint64_t Hash(void) const {
    return reinterpret_cast<uintptr_t>(impl);
  }

  PrivateT *impl;
};

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

  using PublicType = typename T::PublicType;
  using PrivateType = typename T::PrivateType;
  using NodeType = Node<PublicType, PrivateType>;

  static_assert(std::is_same_v<T, PublicType>);

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
    impl = reinterpret_cast<NodeType *>(NodeTraverser::Next(impl, offset));
    return *this;
  }

  inline NodeIterator<T> operator++(int) const {
    auto ret = *this;
    impl = reinterpret_cast<NodeType *>(NodeTraverser::Next(impl, offset));
    return ret;
  }

 private:
  friend class NodeRange<T>;

  inline explicit NodeIterator(NodeType *impl_, intptr_t offset_)
      : impl(impl_),
        offset(offset_) {}

  NodeIterator(void) = default;

  NodeType *impl{nullptr};
  intptr_t offset{0};
};

template <typename T>
class NodeRange {
 public:
  using PublicType = typename T::PublicType;
  using PrivateType = typename T::PrivateType;
  using NodeType = Node<PublicType, PrivateType>;

  static_assert(std::is_same_v<T, PublicType>);

  NodeRange(void) = default;

  inline explicit NodeRange(NodeType *impl_, intptr_t offset_)
      : impl(impl_),
        offset(offset_) {}

#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Winvalid-offsetof"
#endif
  template <typename UPub, typename UPriv>
  inline explicit NodeRange(Node<UPub, UPriv> *impl_)
      : impl(impl_),
        offset(static_cast<intptr_t>(
            __builtin_offsetof(Node<UPub, UPriv>, next))) {}

#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC diagnostic pop
#endif

  inline NodeIterator<T> begin(void) const {
    return NodeIterator<T>(impl, offset);
  }

  inline NodeIterator<T> end(void) const {
    return {};
  }

  inline bool empty(void) const noexcept {
    return impl == nullptr;
  }

 private:
  template <typename U>
  friend class Node;

  NodeType *impl{nullptr};
  intptr_t offset{0};
};

}  // namespace hyde
