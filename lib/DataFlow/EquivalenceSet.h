
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

  static bool TryUnion(EquivalenceSet *a, EquivalenceSet *b) {
    a = a->Find();
    b = b->Find();
    if (a == b) {
      return true;
    } else if (!a->induction_group_id && !b->induction_group_id &&
               !a->induction_depth && !b->induction_depth) {
      if (a->id > b->id) {
        a->parent = b;
        return true;
      } else {
        b->parent = a;
        return true;
      }
    } else if (a->induction_group_id && !b->induction_group_id &&
               a->induction_depth && !b->induction_depth) {
      b->induction_group_id.emplace(*a->induction_group_id);
      b->induction_depth.emplace(*a->induction_depth);
      b->parent = a;
      return true;
    } else if (!a->induction_group_id && b->induction_group_id &&
               !a->induction_depth && b->induction_depth) {
      a->induction_group_id.emplace(*b->induction_group_id);
      a->induction_depth.emplace(*b->induction_depth);
      a->parent = b;
      return true;
    } else if (*a->induction_group_id == *b->induction_group_id &&
               *a->induction_depth == *b->induction_depth) {
      a->parent = b;
      return true;
    } else {
      return false;
    }
  }

  std::optional<unsigned> InductionGroup(void) {
    return *Find()->induction_group_id;
  }

  bool TrySetInductionGroup(unsigned induction_group_id_,
                            unsigned induction_depth_) {
    auto self = Find();
    if (self->induction_group_id && self->induction_depth &&
        *(self->induction_group_id) != induction_group_id_ &&
        *(self->induction_depth) != induction_depth_) {
      return false;
    }
    this->induction_group_id.emplace(induction_group_id_);
    self->induction_group_id.emplace(induction_group_id_);
    this->induction_depth.emplace(induction_depth_);
    self->induction_depth.emplace(induction_depth_);
    return true;
  }

  EquivalenceSet *parent;
  unsigned id;

 private:
  std::optional<unsigned> induction_group_id;
  std::optional<unsigned> induction_depth;
};

}  // namespace hyde
