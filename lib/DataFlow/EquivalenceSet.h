
#include <optional>

#include "Query.h"

namespace hyde {

class EquivalenceSet {
 public:
  EquivalenceSet(unsigned id_) : parent(this), id(id_) {}

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
        a->parent = b;
      } else {
        b->parent = a;
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
        a->parent = b;
        return true;
      } else {
        b->parent = a;
        return true;
      }

    } else if (a->induction_view && !b->induction_view) {
      b->induction_view = a->induction_view;
      b->parent = a;
      return true;

    } else if (!a->induction_view && b->induction_view) {
      a->induction_view = b->induction_view;
      a->parent = b;
      return true;

    } else {
      return false;
    }
  }
  std::optional<unsigned> InductionGroup(void) {
    return *Find()->induction_group_id;
  }
  bool TrySetInductionGroup(VIEW *view) {
    auto self = Find();

    //if (self->induction_view
    //  *(self->induction_group_id) != induction_group_id_ &&
    //  *(self->induction_depth) != induction_depth_) {
    // return false;
    //}
    this->induction_view = view;
    self->induction_view = view;
    return true;
  }
  EquivalenceSet *parent;
  unsigned id;
  std::optional<unsigned> induction_group_id;

 private:
  VIEW *induction_view{nullptr};
};

}  // namespace hyde
