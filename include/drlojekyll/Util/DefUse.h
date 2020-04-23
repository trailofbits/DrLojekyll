// Copyright 2019, Trail of Bits. All rights reserved.

#pragma once

#include <algorithm>
#include <cstdint>
#include <vector>

namespace hyde {

class User {
 public:
  virtual ~User(void);

  // NOTE(pag): It is forbidden to remove/replace uses within an `Update`.
  virtual void Update(uint64_t next_timestamp);

  static uint64_t gNextTimestamp;

 protected:
  uint64_t timestamp{0u};
};

template <typename T>
class Use;

template <typename T>
class UseRef;

template <typename T>
class Def;

template <typename T>
class UseList;

template <typename T>
class UseListIterator;

template <typename T>
class Use {
 public:
  T *get(void) const noexcept {
    return def_being_used;
  }

  T *operator->(void) const noexcept {
    return def_being_used;
  }

  T *operator*(void) const noexcept {
    return *def_being_used;
  }

 private:
  template <typename>
  friend class UseRef;

  template <typename>
  friend class UseList;

  template <typename>
  friend class UseListIterator;

  template <typename>
  friend class Def;

  User *user{nullptr};
  T *def_being_used{nullptr};
};

// An iterator over a use list.
template <typename T>
class UseListIterator {
 public:

  inline UseListIterator(const Use<T> * const *it_) noexcept
      : it(it_) {}

  inline UseListIterator<T> &operator++(void) noexcept {
    ++it;
    return *this;
  }

  inline UseListIterator<T> operator++(int) noexcept {
    return it++;
  }

  inline bool operator==(UseListIterator<T> that) const noexcept {
    return it == that.it;
  }

  inline bool operator!=(UseListIterator<T> that) const noexcept {
    return it != that.it;
  }

  inline bool operator<(UseListIterator<T> that) const noexcept {
    return it < that.it;
  }

  inline T *operator*(void) const noexcept {
    return (*it)->def_being_used;
  }

  inline T *operator[](size_t index) const noexcept {
    return it[index]->def_being_used;
  }

  inline unsigned operator-(const UseListIterator<T> &that) const noexcept {
    return static_cast<unsigned>(it - that.it);
  }

  inline UseListIterator<T> operator+(unsigned disp) const noexcept {
    return UseListIterator<T>(&(it[disp]));
  }

 private:
  const Use<T> * const *it;
};

// A list of uses.
template <typename T>
class UseList {
 public:
  UseList(UseList<T> &&that) noexcept
      : owner(that.owner),
        uses(std::move(that.uses)) {}

  UseList(User *owner_)
      : owner(owner_) {}

  ~UseList(void) {
    Clear();
  }

  inline T *operator[](size_t index) const {
    return uses[index]->def_being_used;
  }

  UseListIterator<T> begin(void) const {
    return uses.data();
  }

  UseListIterator<T> end(void) const {
    return &(uses.data()[uses.size()]);
  }

  void AddUse(Def<T> *def);

  unsigned Size(void) const noexcept {
    return static_cast<unsigned>(uses.size());
  }

  bool Empty(void) const noexcept {
    return uses.empty();
  }

  void Sort(void) noexcept {
    std::sort(uses.begin(), uses.end(),
              [] (Use<T> *a, Use<T> *b) {
                return a->get() < b->get();
              });
  }

  void ClearWithoutErasure(void) {
    uses.clear();
  }

  void Swap(UseList<T> &that) {
    assert(owner == that.owner);
    uses.swap(that.uses);

    if (owner && that.owner) {
      owner->Update(User::gNextTimestamp++);
      if (owner != that.owner) {
        that.owner->Update(User::gNextTimestamp++);
      }
    } else if (owner) {
      owner->Update(User::gNextTimestamp++);
    } else if (that.owner) {
      that.owner->Update(User::gNextTimestamp++);
    }
  }

  void Clear(void);

 private:
  User * const owner;
  std::vector<Use<T> *> uses;
};

template <typename T>
class DefList;

// An iterator over a definition list.
template <typename T>
class DefListIterator {
 public:

  inline DefListIterator(const std::unique_ptr<T> *it_) noexcept
      : it(it_) {}

  inline DefListIterator<T> &operator++(void) noexcept {
    ++it;
    return *this;
  }

  inline DefListIterator<T> operator++(int) noexcept {
    return it++;
  }

  inline bool operator==(DefListIterator<T> that) const noexcept {
    return it == that.it;
  }

  inline bool operator!=(DefListIterator<T> that) const noexcept {
    return it != that.it;
  }

  inline bool operator<(DefListIterator<T> that) const noexcept {
    return it < that.it;
  }

  inline T *operator*(void) const noexcept {
    return it->get();
  }

  inline T *operator[](size_t index) const noexcept {
    return it[index].get();
  }

  inline unsigned operator-(const DefListIterator<T> &that) const noexcept {
    return static_cast<unsigned>(it - that.it);
  }

  inline DefListIterator<T> operator-(unsigned sub) const noexcept {
    return DefListIterator<T>(&(it[-static_cast<int>(sub)]));
  }

  inline DefListIterator<T> operator+(unsigned add) const noexcept {
    return DefListIterator<T>(&(it[add]));
  }

 private:
  const std::unique_ptr<T> *it;
};

// CRTP class for definitions of `T`. `Use<T>::def` should point to a `T`, which
// is derived from `Def<T>`.
template <typename T>
class Def {
 public:
  explicit Def(T *self_)
      : self(self_) {}

  Use<T> *CreateUse(User *user) {
    const auto use = new Use<T>;
    user->Update(User::gNextTimestamp++);
    use->def_being_used = this->self;
    use->user = user;
    uses.emplace_back(use);
    return use;
  }

  void ReplaceAllUsesWith(T *that) {
    if (self == that->self || uses.empty()) {
      return;
    }

    auto i = that->uses.size();

    // First, move the uses into the target's use list.
    for (auto &use : uses) {
      if (const auto use_val = use.release()) {
        use_val->def_being_used = that->self;
        that->uses.emplace_back(use_val);
      }
    }

    uses.clear();

    // Now, go through all those moved uses (in the target's use list), and
    // tell all the users that there's been an update.
    //
    // NOTE(pag): We assume that `Update` does not end up triggering any
    //            use removals.
    const auto max_i = that->uses.size();
    const auto time = User::gNextTimestamp++;
    for (; i < max_i; ++i) {
      if (auto use_val = that->uses[i].get()) {
        use_val->user->Update(time);
      }
    }
  }

  T *operator->(void) const noexcept {
    return self;
  }

  T &operator*(void) const noexcept {
    return *self;
  }

  bool IsUsed(void) const noexcept {
    return !uses.empty();
  }

  unsigned NumUses(void) const noexcept {
    return static_cast<unsigned>(uses.size());
  }

  template <typename CB>
  inline void ForEachUse(CB cb) {
    for (const auto &use : uses) {
      if (use) {
        cb(use->user, use->def_being_used);
      }
    }
  }

 private:
  template <typename>
  friend class Use;

  template <typename>
  friend class UseRef;

  template <typename>
  friend class UseList;

  template <typename>
  friend class DefList;

  static bool CompareUsePointers1(const std::unique_ptr<Use<T>> &a,
                                  const std::unique_ptr<Use<T>> &b) {
    return a.get() < b.get();
  }


  static bool CompareUsePointers2(const std::unique_ptr<Use<T>> &a,
                                  Use<T> *b) {
    return a.get() < b;
  }

  // Erase a use from the uses list.
  void EraseUse(Use<T> *to_remove) {
    if (!to_remove) {
      return;
    }

    auto it = std::remove_if(uses.begin(), uses.end(),
                             [=] (const std::unique_ptr<Use<T>> &a) {
                               return a.get() == to_remove;
                             });
    uses.erase(it, uses.end());
  }

  // Points to this definition, just in case of multiple inheritance.
  T * const self;

  // All uses of this definition.
  std::vector<std::unique_ptr<Use<T>>> uses;
};

template <typename T>
void UseList<T>::Clear(void) {
  if (uses.empty()) {
    return;
  }

  std::vector<Use<T> *> old_uses;
  uses.swap(old_uses);

  for (auto use : old_uses) {
    if (use) {
      use->def_being_used->EraseUse(use);
    }
  }
}

template <typename T>
void UseList<T>::AddUse(Def<T> *def) {
  const auto use = def->CreateUse(owner);
  uses.push_back(use);
}

template <typename T>
class UseRef {
 public:
  UseRef(Use<T> *use_)
      : use(use_) {}

  UseRef(void) = default;

  ~UseRef(void) {
    if (use) {
      const auto use_copy = use;
      use = nullptr;
      use_copy->def_being_used->EraseUse(use_copy);
    }
  }

  T *operator->(void) const noexcept {
    return use->def_being_used;
  }

  T &operator*(void) const noexcept {
    return *(use->def_being_used);
  }

  T *get(void) const noexcept {
    return use ? use->def_being_used : nullptr;
  }

  operator bool(void) const noexcept {
    return !!use;
  }

  void ClearWithoutErasure(void) {
    use = nullptr;
  }

 private:
  UseRef(const UseRef<T> &) = delete;
  UseRef(UseRef<T> &&) noexcept = delete;
  UseRef<T> &operator=(const UseRef<T> &) = delete;
  UseRef<T> &operator=(UseRef<T> &&) noexcept = delete;

  Use<T> *use{nullptr};
};

template <typename T>
class DefList {
 public:
  DefList(void) = default;

  DefList(User *owner_)
      : owner(owner_) {}

  template <typename... Args>
  T *Create(Args&&... args) {
    auto new_def = new T(std::forward<Args>(args)...);
    defs.emplace_back(new_def);
    return new_def;
  }

  DefListIterator<T> begin(void) const noexcept {
    return defs.data();
  }

  DefListIterator<T> end(void) const noexcept {
    return &(defs.data()[defs.size()]);
  }

  T *operator[](size_t index) const noexcept {
    return defs[index]->self;
  }

  unsigned Size(void) const noexcept {
    return static_cast<unsigned>(defs.size());
  }

  bool Empty(void) const noexcept {
    return defs.empty();
  }

  void Swap(DefList<T> &that) {
    defs.swap(that.defs);
    if (owner && that.owner) {
      owner->Update(User::gNextTimestamp++);
      if (owner != that.owner) {
        that.owner->Update(User::gNextTimestamp++);
      }
    } else if (owner) {
      owner->Update(User::gNextTimestamp++);

    } else if (that.owner) {
      that.owner->Update(User::gNextTimestamp++);
    }
  }

  template <typename C>
  void Sort(C cmp) {
    std::sort(
        defs.begin(), defs.end(),
        [&cmp] (const std::unique_ptr<T> &a, const std::unique_ptr<T> &b) {
          return cmp(a.get(), b.get());
        });
    if (owner) {
      owner->Update(User::gNextTimestamp++);
    }
  }

  size_t RemoveUnused(void) {
    const auto old_size = defs.size();
    auto it = std::remove_if(
        defs.begin(), defs.end(),
        [] (const std::unique_ptr<T> &v) {
          return !v->IsUsed();
        });
    defs.erase(it, defs.end());
    return defs.size() - old_size;
  }

  ~DefList(void) {
    if (defs.empty()) {
      return;
    }

    std::vector<std::unique_ptr<T>> old_defs;
    old_defs.swap(defs);
    old_defs.clear();
  }

 private:
  User *owner{nullptr};
  std::vector<std::unique_ptr<T>> defs;
};

template <typename T>
class Node;

template <typename T>
class UsedNodeIterator {
 public:
  inline UsedNodeIterator(UseListIterator<Node<T>> it_)
      : it(it_) {}

  inline UsedNodeIterator<T> &operator++(void) noexcept {
    ++it;
    return *this;
  }

  inline UsedNodeIterator<T> operator++(int) noexcept {
    return it++;
  }

  inline bool operator==(UsedNodeIterator<T> that) const noexcept {
    return it == that.it;
  }

  inline bool operator!=(UsedNodeIterator<T> that) const noexcept {
    return it != that.it;
  }

  inline T operator*(void) const noexcept {
    return T(*it);
  }

  inline T operator[](size_t index) const noexcept {
    return T(it[index]);
  }

  inline unsigned operator-(const UsedNodeIterator<T> &that) const noexcept {
    return it - that.it;
  }

 private:
  UseListIterator<Node<T>> it;
};

template <typename T>
class DefinedNodeIterator {
 public:
  inline DefinedNodeIterator(DefListIterator<Node<T>> it_)
      : it(it_) {}

  inline DefinedNodeIterator<T> &operator++(void) noexcept {
    ++it;
    return *this;
  }

  inline DefinedNodeIterator<T> operator++(int) noexcept {
    return it++;
  }

  inline bool operator==(DefinedNodeIterator<T> that) const noexcept {
    return it == that.it;
  }

  inline bool operator!=(DefinedNodeIterator<T> that) const noexcept {
    return it != that.it;
  }

  inline T operator*(void) const noexcept {
    return T(*it);
  }

  inline T operator[](size_t index) const noexcept {
    return T(it[index]);
  }

  inline unsigned operator-(const DefinedNodeIterator<T> &that) const noexcept {
    return it - that.it;
  }

 private:
  DefListIterator<Node<T>> it;
};

template <typename T>
struct UsedNodeRange {
 public:
  UsedNodeIterator<T> begin(void) const {
    return begin_;
  }
  UsedNodeIterator<T> end(void) const {
    return end_;
  }

  bool empty(void) const noexcept {
    return begin_ == end_;
  }

  unsigned size(void) const noexcept {
    return end_ - begin_;
  }

  inline T operator[](size_t index) const noexcept {
    return begin_[index];
  }

  const UsedNodeIterator<T> begin_;
  const UsedNodeIterator<T> end_;
};

template <typename T>
struct DefinedNodeRange {
 public:
  DefinedNodeIterator<T> begin(void) const {
    return begin_;
  }
  DefinedNodeIterator<T> end(void) const {
    return end_;
  }
  bool empty(void) const noexcept {
    return begin_ == end_;
  }

  unsigned size(void) const noexcept {
    return end_ - begin_;
  }

  inline T operator[](size_t index) const noexcept {
    return begin_[index];
  }
  const DefinedNodeIterator<T> begin_;
  const DefinedNodeIterator<T> end_;
};

}  // namespace hyde
namespace std {

template <typename T>
struct iterator_traits<::hyde::UseListIterator<T>> {
 public:
  typedef std::forward_iterator_tag iterator_category;
  typedef T *value_type;
  typedef unsigned difference_type;
  typedef void pointer;
  typedef void reference;
};

template <typename T>
struct iterator_traits<::hyde::DefListIterator<T>> {
 public:
  typedef std::forward_iterator_tag iterator_category;
  typedef T *value_type;
  typedef unsigned difference_type;
  typedef void pointer;
  typedef void reference;
};

template <typename T>
struct iterator_traits<::hyde::UsedNodeIterator<T>> {
 public:
  typedef std::forward_iterator_tag iterator_category;
  typedef T value_type;
  typedef unsigned difference_type;
  typedef void pointer;
  typedef T reference;
};

template <typename T>
struct iterator_traits<::hyde::DefinedNodeIterator<T>> {
 public:
  typedef std::forward_iterator_tag iterator_category;
  typedef T value_type;
  typedef unsigned difference_type;
  typedef void pointer;
  typedef T reference;
};

}  // namespace std
