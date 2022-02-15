// Copyright 2020, Trail of Bits. All rights reserved.

#include "Query.h"

namespace hyde {

QueryColumnImpl::~QueryColumnImpl(void) {}

QueryColumnImpl::QueryColumnImpl(
    std::optional<ParsedVariable> var_, TypeLoc type_,
    QueryViewImpl *view_, unsigned id_, unsigned index_)
    : Def<QueryColumnImpl>(this),
      var(var_),
      type(type_),
      view(view_),
      id(id_),
      index(index_) {
  assert(view != nullptr);
  assert(type.UnderlyingKind() != TypeKind::kInvalid);
}

QueryColumnImpl::QueryColumnImpl(
    ParsedVariable var_, QueryViewImpl *view_, unsigned id_, unsigned index_)
    : Def<QueryColumnImpl>(this),
      var(var_),
      type(var_.Type()),
      view(view_),
      id(id_),
      index(index_) {
  assert(view != nullptr);
  assert(type.UnderlyingKind() != TypeKind::kInvalid);
}

QueryColumnImpl::QueryColumnImpl(
    TypeLoc type_, QueryViewImpl *view_, unsigned id_, unsigned index_)
    : Def<QueryColumnImpl>(this),
      var(std::nullopt),
      type(type_),
      view(view_),
      id(id_),
      index(index_) {
  assert(view != nullptr);
  assert(type.UnderlyingKind() != TypeKind::kInvalid);
}

// Returns the real constant associated with this column if this column is
// a constant or constant reference. Otherwise it returns `nullptr`.
QueryColumnImpl *QueryColumnImpl::AsConstant(void) noexcept {
  if (referenced_constant) {
    return referenced_constant.get();
  }
  if (auto sel = view->AsSelect()) {
    if (sel->stream && sel->stream->AsConstant()) {
      return this;
    }
  }
  return nullptr;
}

// Try to resolve this column to a constant, and return it, otherwise returns
// `this`.
QueryColumnImpl *QueryColumnImpl::TryResolveToConstant(void) noexcept {
  if (auto const_col = referenced_constant.get(); const_col) {
    return const_col;
  } else {
    return this;
  }
}

// Returns `true` if will have a constant value at runtime.
bool QueryColumnImpl::IsConstantRef(void) const noexcept {
  return referenced_constant;
}

// Returns `true` if this column is a constant.
bool QueryColumnImpl::IsConstantOrConstantRef(void) const noexcept {
  if (referenced_constant) {
    return true;
  }
  if (auto sel = view->AsSelect()) {
    if (sel->stream && sel->stream->AsConstant()) {
      return true;
    }
  }
  return false;
}

// Returns `true` if this column is a constant.
bool QueryColumnImpl::IsConstant(void) const noexcept {
  if (auto sel = view->AsSelect()) {
    if (sel->stream && sel->stream->AsConstant()) {
      assert(!referenced_constant);
      return true;
    }
  }
  return false;
}

// Returns `true` if this column is a constant that is marked as being
// unique.
bool QueryColumnImpl::IsUniqueConstant(void) const noexcept {
  SELECT * const sel = view->AsSelect();
  if (!sel || !sel->stream) {
    return false;
  }

  CONST * const c = sel->stream->AsConstant();
  if (!c) {
    return false;
  }

  if (c->AsTag()) {
    return true;
  }

  if (!c->literal) {
    return false;
  }

  if (!c->literal->IsConstant() ||
      !c->literal->Type().IsForeign()) {
    return false;
  }

  auto fc = ParsedForeignConstant::From(c->literal.value());
  return fc.IsUnique();
}

// Returns `true` if this column is being used directly, or indirectly via
// a usage of the view (e.g. by a merge, a join, a condition, a negation, etc.)
//
// NOTE(pag): Even if the column doesn't look used, it might be used indirectly
//            via a merge, and thus we want to capture this.
bool QueryColumnImpl::IsUsed(void) const noexcept {
  if (this->Def<QueryColumnImpl>::IsUsed()) {
    return true;
  }

  return view->IsUsedDirectly();
}

// Return the index of this column inside of its view.
unsigned QueryColumnImpl::Index(void) noexcept {
  if (index >= view->columns.Size() || this != view->columns[index]) {
    auto i = 0u;
    for (auto col : view->columns) {
      if (col == this) {
        index = i;
        break;
      }
      ++i;
    }
  }
  return index;
}

uint64_t QueryColumnImpl::Hash(void) noexcept {
  if (!hash) {
    const auto view_hash = view->Hash();
    hash = view_hash ^
           RotateRight64(
               view_hash * 0xff51afd7ed558ccdull,
               (Index() + static_cast<unsigned>(type.Kind()) + 33u) % 64u);
  }
  return hash;
}

// Return a number that can be used to help sort this node. The idea here
// is that we often want to try to merge together two different instances
// of the same underlying node when we can.
uint64_t QueryColumnImpl::Sort(void) noexcept {
  return Hash();
}

void QueryColumnImpl::CopyConstantFrom(QueryColumnImpl *maybe_const_col) {
  if (auto const_col = maybe_const_col->AsConstant();
      const_col && !referenced_constant) {

    // We've done a kind of constant propagation, so mark the using views
    // as non-canonical.
    this->ForEachUse<VIEW>(
        [](VIEW *view, COL *) { view->is_canonical = false; });

    referenced_constant.Emplace(view, const_col);
  }
}

void QueryColumnImpl::ReplaceAllUsesWith(COL *that) {

  //  if (that->referenced_constant) {
  //    UseRef<COL> new_real_constant(that->referenced_constant->CreateUse(this->view));
  //    referenced_constant.Swap(new_real_constant);
  //
  //  } else if (that->IsConstant()) {
  //    UseRef<COL> new_real_constant(that->CreateUse(this->view));
  //    referenced_constant.Swap(new_real_constant);
  //
  //  } else {
  if (referenced_constant && !that->IsConstantOrConstantRef()) {
    that->CopyConstantFrom(referenced_constant.get());
  }
  this->Def<QueryColumnImpl>::ReplaceAllUsesWith(that);

  //  }
}

}  // namespace hyde
