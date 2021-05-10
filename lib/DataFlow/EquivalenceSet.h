
#include <optional>

#include "Query.h"

namespace hyde {

class EquivalenceSet {
 public:
  EquivalenceSet(unsigned id_, VIEW *view)
      : parent(this),
        id(id_),
        views_in_set(view) {
    views_in_set.AddUse(view);
  }

  EquivalenceSet *Find(void) {
    if (parent == this) {
      return this;
    } else {
      parent = parent->Find();
      return parent;
    }
  }

  static void ForceUnion(EquivalenceSet *a, EquivalenceSet *b) {
    a = a->Find();
    b = b->Find();
    if (a != b) {
      if (a->id > b->id) {
        MakeParent(a, b);
      } else {
        MakeParent(b, a);
      }
    }
  }

  static bool TryUnion(EquivalenceSet *a, EquivalenceSet *b) {
    a = a->Find();
    b = b->Find();
    if (a == b) {
      return true;

    } else if (a->induction_view == b->induction_view) {
      if (a->id > b->id) {
        MakeParent(a, b);
        return true;
      } else {
        MakeParent(b, a);
        return true;
      }

    } else if (a->induction_view && !b->induction_view) {
      b->induction_view = a->induction_view;
      MakeParent(b, a);
      return true;

    } else if (!a->induction_view && b->induction_view) {
      a->induction_view = b->induction_view;
      MakeParent(a, b);
      return true;

    } else {
      return false;
    }
  }

  VIEW *InductionView(void) {
    return Find()->induction_view;
  }

  bool TrySetInductionGroup(VIEW *view) {
    auto self = Find();
    if (self->induction_view) {
      return false;
    }
    this->induction_view = view;
    self->induction_view = view;
    return true;
  }

  EquivalenceSet *parent;
  unsigned id;
  WeakUseList<VIEW> views_in_set;

 private:
  static void MakeParent(EquivalenceSet *old_set, EquivalenceSet *new_parent) {
    for (auto use : old_set->views_in_set) {
      new_parent->views_in_set.AddUse(use);
    }
    old_set->parent = new_parent;
  }

  VIEW *induction_view{nullptr};
};

}  // namespace hyde
