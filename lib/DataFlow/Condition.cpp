// Copyright 2020, Trail of Bits. All rights reserved.

#include "Build.h"

namespace hyde {

Node<QueryCondition>::~Node(void) {
  is_dead = true;

  for (auto setter : setters) {
    if (setter) {
      //assert(setter->sets_condition.get() == this);
      setter->sets_condition.Clear();
      setter->is_canonical = false;

      // If there's an "increment" `INSERT` associated with this condition, then
      // we want to make sure it looks unused as well.
      if (const auto insert = setter->AsInsert();
          insert && insert->relation && declaration &&
          insert->declaration == *declaration) {

        // Disconnect the insert from its relation, making it look unused, and
        // thus subject to elimination.
        insert->relation->inserts.RemoveIf(
            [=](VIEW *v) { return v == insert; });
        insert->relation.Clear();
      }
    }
  }
  setters.Clear();
}

// An anonymous, not-user-defined condition that is instead inferred based
// off of optmizations.
Node<QueryCondition>::Node(void)
    : Def<Node<QueryCondition>>(this),
      User(this),
      positive_users(this),
      negative_users(this),
      setters(this) {}

// An explicit, user-defined condition. Usually associated with there-exists
// checks or configuration options.
Node<QueryCondition>::Node(ParsedExport decl_)
    : Def<Node<QueryCondition>>(this),
      User(this),
      declaration(decl_),
      positive_users(this),
      negative_users(this),
      setters(this) {}


// Is this a trivial condition?
bool Node<QueryCondition>::IsTrivial(
    std::unordered_map<Node<QueryView> *, bool> &conditional_views) {
  if (in_trivial_check) {
    assert(false);  // Suggests a condition is dependent on itself.
    return true;
  }

  in_trivial_check = true;

  for (VIEW *setter : setters) {
    if (VIEW::IsConditional(setter, conditional_views)) {
      in_trivial_check = false;
      return false;
    }
  }

  in_trivial_check = true;
  return true;
}

// Is this a trivial condition?
bool Node<QueryCondition>::IsTrivial(void) {
  std::unordered_map<Node<QueryView> *, bool> conditional_views;
  return this->IsTrivial(conditional_views);
}

// Are the `positive_users` and `negative_users` lists consistent?
bool Node<QueryCondition>::UsersAreConsistent(void) const {
  auto consistent = true;
  for (auto view : positive_users) {
    if (view) {
      auto found = false;
      for (auto cond : view->positive_conditions) {
        if (cond == this) {
          found = true;
          break;
        }
      }
      consistent = consistent && found;
    }
  }

  for (auto view : negative_users) {
    if (view) {
      auto found = false;
      for (auto cond : view->negative_conditions) {
        if (cond == this) {
          found = true;
          break;
        }
      }
      consistent = consistent && found;
    }
  }

  return consistent;
}

// Are the setters of this condition consistent?
bool Node<QueryCondition>::SettersAreConsistent(void) const {
  for (VIEW *setter : setters) {
    if (setter->sets_condition.get() != this) {
      return false;
    }
  }

  bool ok = true;

  this->ForEachUse<VIEW>([&ok] (VIEW *setter, COND *self) {
    if (setter->is_dead ||
        setter->sets_condition.get() != self) {
      return;
    }

    for (auto v : self->setters) {
      if (v == setter) {
        return;
      }
    }

    ok = false;
  });

  return ok;
}

// Extract conditions from regular nodes and force them to belong to only
// tuple nodes. This simplifies things substantially for downstream users.
void QueryImpl::ExtractConditionsToTuples(void) {
  std::vector<VIEW *> conditional_views;

  const_cast<const QueryImpl *>(this)->ForEachView([&](VIEW *view) {
    if (view->sets_condition || !view->positive_conditions.Empty() ||
        !view->negative_conditions.Empty()) {
      conditional_views.push_back(view);
    }
  });

  for (auto view : conditional_views) {

    // Proxy the insert with a tuple that does the conditional stuff.
    if (auto insert = view->AsInsert(); insert) {
      TUPLE *pre_tuple = this->tuples.Create();

      auto col_index = 0u;
      for (auto in_col : insert->input_columns) {
        auto out_col = pre_tuple->columns.Create(
            in_col->var, in_col->type, pre_tuple, in_col->id, col_index++);
        pre_tuple->input_columns.AddUse(in_col);
        out_col->CopyConstantFrom(in_col);
      }

      insert->input_columns.Clear();
      for (auto col : pre_tuple->columns) {
        insert->input_columns.AddUse(col);
      }

      insert->CopyDifferentialAndGroupIdsTo(pre_tuple);
      insert->TransferSetConditionTo(pre_tuple);
      insert->CopyTestedConditionsTo(pre_tuple);
      insert->DropTestedConditions();

      assert(!insert->sets_condition);
      assert(insert->positive_conditions.Empty());
      assert(insert->negative_conditions.Empty());

      // Force kill it.
      if (insert->relation && !insert->relation->declaration.Arity()) {
        insert->PrepareToDelete();
      }

      if (pre_tuple->positive_conditions.Empty() &&
          pre_tuple->negative_conditions.Empty()) {
        continue;
      }

      view = pre_tuple;
    }

    // Given a view `cond V`, create `V -> cond TUPLE_b -> TUPLE_a`, such that
    // if `V` set any conditions, then `TUPLE_a` sets those conditions, and
    // the conditions tested in `V` are not tested in `TUPLE_b`.

    // Will take the set condition, if any.
    const auto had_condition = !!view->sets_condition;
    TUPLE *const tuple_a = view->GuardWithTuple(this, true);
    assert(tuple_a->positive_conditions.Empty());
    assert(tuple_a->negative_conditions.Empty());
    assert(!view->sets_condition);
    assert(!had_condition || tuple_a->sets_condition);
    (void) tuple_a;
    (void) had_condition;

    // Will take the tested conditions.
    if (!view->positive_conditions.Empty() ||
        !view->negative_conditions.Empty()) {
      TUPLE *const tuple_b = view->GuardWithTuple(this, true);
      assert(tuple_b->positive_conditions.Empty());
      assert(tuple_b->negative_conditions.Empty());
      view->CopyTestedConditionsTo(tuple_b);
      view->DropTestedConditions();
      assert(view->positive_conditions.Empty());
      assert(view->negative_conditions.Empty());
      assert(!tuple_b->positive_conditions.Empty() ||
             !tuple_b->negative_conditions.Empty());
    }
  }

#ifndef NDEBUG
  const_cast<const QueryImpl *>(this)->ForEachView([&](VIEW *view) {
    if (view->AsTuple()) {
      return;
    } else {
      assert(!view->sets_condition);
      assert(view->positive_conditions.Empty());
      assert(view->negative_conditions.Empty());
    }
  });
#endif
}

}  // namespace hyde
