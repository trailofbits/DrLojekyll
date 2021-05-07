// Copyright 2020, Trail of Bits. All rights reserved.

#include <drlojekyll/Util/DisjointSet.h>

#include <optional>

#include "Query.h"

namespace hyde {

template <typename... Args>
static void ReplaceInputsWithTuple(QueryImpl *impl, VIEW *user,
                                   Args... input_lists) {
  UseList<COL> *input_col_lists[] = {input_lists...};
  TUPLE *const tuple = impl->tuples.Create();
  unsigned col_index = 0u;
  for (auto input_col_list : input_col_lists) {
    if (input_col_list->Empty()) {
      continue;
    }

    UseList<COL> new_col_list(user);
    for (auto in_col : *input_col_list) {
      assert(in_col->IsConstant());
      COL *const out_col = tuple->columns.Create(
          in_col->var, in_col->type, tuple, in_col->id, col_index++);
      out_col->CopyConstantFrom(in_col);
      new_col_list.AddUse(out_col);
      tuple->input_columns.AddUse(in_col);
    }

    input_col_list->Swap(new_col_list);
  }
}

class DataModelSet : public DisjointSet {
 public:
  using DisjointSet::DisjointSet;

  DataModelSet(unsigned id_) : parent(this), id(id_) {}

  DataModelSet *Find(void) {
    if (parent == this) {
      return this;
    } else {
      parent = parent->Find();
      return parent;
    }
  }

  static bool TryUnion(DataModelSet *a, DataModelSet *b) {
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
   } else if (a->induction_group_id && !b->induction_group_id
       && a->induction_depth && !b->induction_depth) {
     b->induction_group_id.emplace(*a->induction_group_id);
     b->induction_depth.emplace(*a->induction_depth);
     b->parent = a;
     return true;
   } else if (!a->induction_group_id && b->induction_group_id
       && !a->induction_depth && b->induction_depth) {
     a->induction_group_id.emplace(*b->induction_group_id);
     a->induction_depth.emplace(*b->induction_depth);
     a->parent = b;
     return true;
   } else if (*a->induction_group_id == *b->induction_group_id
       && *a->induction_depth == *b->induction_depth) {
     a->parent = b;
     return true;
   } else {
     return false;
   }
  }

  std::optional<unsigned> InductionGroup(void) {
    return *Find()->induction_group_id;
  }

  bool TrySetInductionGroup(unsigned induction_group_id_, unsigned induction_depth_) {
   auto self = Find();
   if (self->induction_group_id && self->induction_depth
       && *(self->induction_group_id) != induction_group_id_
       && *(self->induction_depth) != induction_depth_) {
     return false;
   }
   this->induction_group_id.emplace(induction_group_id_);
   self->induction_group_id.emplace(induction_group_id_);
   this->induction_depth.emplace(induction_depth_);
   self->induction_depth.emplace(induction_depth_);
   return true;
  }

  DataModelSet *parent;
  unsigned id;

 private:
  std::optional<unsigned> induction_group_id;
  std::optional<unsigned> induction_depth;

};

}  // namespace hyde
