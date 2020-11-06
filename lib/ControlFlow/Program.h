// Copyright 2020, Trail of Bits. All rights reserved.

#pragma once

#include <drlojekyll/ControlFlow/Program.h>
#include <drlojekyll/DataFlow/Query.h>
#include <drlojekyll/Parse/Parse.h>
#include <drlojekyll/Util/DefUse.h>
#include <drlojekyll/Util/DisjointSet.h>
#include <drlojekyll/Util/EqualitySet.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace std {

template <>
struct hash<std::pair<::hyde::QueryView, ::hyde::QueryView>> {
  using argument_type = std::pair<::hyde::QueryView, ::hyde::QueryView>;
  using result_type = uint64_t;
  inline uint64_t operator()(argument_type views) const noexcept {
    return (views.first.UniqueId() * views.second.Hash()) ^
           views.second.UniqueId();
  }
};

}  // namespace std
namespace hyde {

class ProgramInductionRegion;
class ProgramOperationRegion;

// A column within a table.
template <>
class Node<DataColumn> final : public Def<Node<DataColumn>>, public User {
 public:
  virtual ~Node(void);

  Node(unsigned id_, TypeKind type_, Node<DataTable> *table_);

  void Accept(ProgramVisitor &visitor);

  const unsigned id;
  const unsigned index;
  const TypeKind type;

  std::vector<Token> names;

  WeakUseRef<Node<DataTable>> table;
};

using TABLECOLUMN = Node<DataColumn>;

// Represents an index on some subset of columns in a table.
template <>
class Node<DataIndex> final : public Def<Node<DataIndex>>, public User {
 public:
  virtual ~Node(void);

  Node(unsigned id_, Node<DataTable> *table_, std::string column_spec_);

  void Accept(ProgramVisitor &visitor);

  const unsigned id;
  const std::string column_spec;

  UseList<TABLECOLUMN> columns;

  WeakUseRef<Node<DataTable>> table;
};

using TABLEINDEX = Node<DataIndex>;

// Represents a table of data.
//
// NOTE(pag): By default all tables already have a UNIQUE index on them.
template <>
class Node<DataTable> final : public Def<Node<DataTable>>, public User {
 public:
  virtual ~Node(void);

  inline Node(unsigned id_)
      : Def<Node<DataTable>>(this),
        User(this),
        id(id_),
        columns(this),
        indices(this) {}

  void Accept(ProgramVisitor &visitor);

  // Get or create a table in the program.
  static Node<DataTable> *GetOrCreate(ProgramImpl *impl, QueryView view);

  // Get or create an index on the table.
  TABLEINDEX *GetOrCreateIndex(ProgramImpl *impl, std::vector<unsigned> cols);

  const unsigned id;

  // List of defined columns.
  DefList<TABLECOLUMN> columns;

  // Indexes that should be created on this table. By default, all tables have
  // a UNIQUE index.
  DefList<TABLEINDEX> indices;
};

using TABLE = Node<DataTable>;

struct DataModel : public DisjointSet {
 public:
  TABLE *table{nullptr};
};

// A vector of tuples in the program.
template <>
class Node<DataVector> final : public Def<Node<DataVector>> {
 public:
  static constexpr unsigned kInputVectorId = 0u;

  template <typename ColList>
  Node(unsigned id_, VectorKind kind_, ColList &&cols)
      : Def<Node<DataVector>>(this),
        id(id_),
        kind(kind_) {

    for (QueryColumn col : cols) {
      col_types.push_back(col.Type().Kind());
    }
  }

  bool IsRead(void) const;

  void Accept(ProgramVisitor &visitor);

  const unsigned id;
  const VectorKind kind;
  std::vector<TypeKind> col_types;
};

using VECTOR = Node<DataVector>;

// A variable in the program. This could be a procedure parameter or a local
// variable.
template <>
class Node<DataVariable> final : public Def<Node<DataVariable>> {
 public:
  inline explicit Node(unsigned id_, VariableRole role_)
      : Def<Node<DataVariable>>(this),
        role(role_),
        id(id_) {}

  void Accept(ProgramVisitor &visitor);

  const VariableRole role;

  // Unique ID for this variable. Unrelated to column IDs.
  const unsigned id;

  // NOTE(pag): Only valid after optimization when building the control flow
  //            IR is complete.
  Node<ProgramRegion> *defining_region{nullptr};

  inline unsigned Sort(void) const noexcept {
    return id;
  }

  std::optional<QueryConstant> query_const;
  std::optional<QueryColumn> query_column;
  std::optional<QueryCondition> query_cond;
};

using VAR = Node<DataVariable>;

// A lexically scoped region in the program.
template <>
class Node<ProgramRegion> : public Def<Node<ProgramRegion>>, public User {
 public:
  virtual ~Node(void);
  explicit Node(Node<ProgramProcedure> *containing_procedure_);
  explicit Node(Node<ProgramRegion> *parent_);

  virtual void Accept(ProgramVisitor &visitor) = 0;

  virtual Node<ProgramProcedure> *AsProcedure(void) noexcept;
  virtual Node<ProgramOperationRegion> *AsOperation(void) noexcept;
  virtual Node<ProgramSeriesRegion> *AsSeries(void) noexcept;
  virtual Node<ProgramParallelRegion> *AsParallel(void) noexcept;
  virtual Node<ProgramInductionRegion> *AsInduction(void) noexcept;

  inline void ReplaceAllUsesWith(Node<ProgramRegion> *that) {
    this->Def<Node<ProgramRegion>>::ReplaceAllUsesWith(that);
    that->parent = this->parent;
    this->parent = nullptr;
  }

  // Gets or creates a local variable in the procedure.
  VAR *VariableFor(ProgramImpl *impl, QueryColumn col);
  VAR *VariableForRec(QueryColumn col);

  // Returns the lexical level of this node.
  unsigned Depth(void) const noexcept;

  // Returns `true` if this region is a no-op.
  virtual bool IsNoOp(void) const noexcept;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  virtual bool Equals(EqualitySet &eq,
                      Node<ProgramRegion> *that) const noexcept;

  // Return the nearest enclosing region that is itself enclosed by an
  // induction.
  Node<ProgramRegion> *NearestRegionEnclosedByInduction(void) noexcept;

  // Find an ancestor node that's both shared by `this` and `that`.
  Node<ProgramRegion> *FindCommonAncestor(Node<ProgramRegion> *that) noexcept;

  // Make sure that `this` will execute before `that`.
  void ExecuteBefore(ProgramImpl *program, Node<ProgramRegion> *that) noexcept;

  // Make sure that `this` will execute after `that`.
  void ExecuteAfter(ProgramImpl *program, Node<ProgramRegion> *that) noexcept;

  // Make sure that `this` will execute alongside `that`.
  void ExecuteAlongside(ProgramImpl *program,
                        Node<ProgramRegion> *that) noexcept;

  // Every child REGION of a procedure will have easy access to create new
  // variables.
  Node<ProgramProcedure> *const containing_procedure;
  Node<ProgramRegion> *parent{nullptr};

  // Maps `QueryColumn::Id()` values to variables. Used to provide lexical
  // scoping of variables.
  //
  // NOTE(pag): Only valid before optimization, during the building of the
  //            control flow IR.
  std::unordered_map<unsigned, VAR *> col_id_to_var;
};

using REGION = Node<ProgramRegion>;

enum class ProgramOperation {
  kInvalid,

  // Insert into a table. Can be interpreted as conditional (a runtime may
  // choose to check if the insert is new or not). If the insert succeeds, then
  // execution descends into `body`.
  kInsertIntoTable,

  // Check the state of a tuple from a table. This executes one of three
  // bodies: `body` if the tuple is present, `absent_body` if the tuple is
  // absent, and `unknown_body` if the tuple may have been deleted.
  kCheckStateInTable,

  // When dealing with MERGE/UNION nodes with an inductive cycle.
  kAppendInductionInputToVector,
  kLoopOverInductionInputVector,
  kClearInductionInputVector,

  // When dealing with a MERGE/UNION node that isn't part of an inductive
  // cycle.
  kAppendUnionInputToVector,
  kLoopOverUnionInputVector,
  kClearUnionInputVector,

  // Check if a row exists in a view. The tuple values are found in
  // `operands` and the table being used is `views[0]`.
  kCheckTupleIsPresentInTable,
  kCheckTupleIsNotPresentInTable,

  // Pairwise comparison between variables in two tuples.
  kCompareTuples,

  // Used by operations related to equi-joins over one or more tables.
  kJoinTables,
  kAppendJoinPivotsToVector,
  kSortAndUniquePivotVector,
  kClearJoinPivotVector,

  // Used to implement the cross-product of some tables.
  kCrossProduct,
  kAppendToProductInputVector,
  kSortAndUniqueProductInputVector,
  kClearProductInputVector,

  // Used to implement table/index scanning.
  kScanTable,
  kLoopOverScanVector,
  kClearScanVector,

  // Loop over a vector of inputs. The format of the vector is based off of
  // the variables in `variables`. The region `body` is executed for each
  // loop iteration.
  kLoopOverInputVector,

  // Call a filter functor, stored in `functor`, that is a functor where all
  // parameters are `bound`-attributed. These functors are interpreted as
  // predicates returning `true` or `false`. If `true` is returned, then
  // descend into `body`.
  kCallFilterFunctor,

  // Call a normal functor, stored in `functor`, where there is at least one
  // free parameter to the functor that it must generate. If anything is
  // generated, then descend into `body`.
  kCallFunctor,

  // Publish a message.
  kPublishMessage,

  // Creates a let binding, which assigns uses of variables to definitions of
  // variables. In practice, let bindings are eliminated during the process
  // of optimization.
  kLetBinding,

  // Used to test reference count variables associated with `QueryCondition`
  // nodes in the data flow.
  kTestAllNonZero,
  kTestAllZero,
  kIncrementAll,
  kDecrementAll,

  // Call another procedure.
  kCallProcedure,
  kCallProcedureCheckTrue,
  kCallProcedureCheckFalse,

  // Return from a procedure.
  kReturnTrueFromProcedure,
  kReturnFalseFromProcedure,

  // TODO: use in future.

  // Test/set a global boolean variable to `true`. The variable is
  // `variables[0]`. The usage is for cross products. If we've ever executed a
  // lazy cross-product, then we must always visit it.
  kTestGlobalVariableIsTrue,
  kSetGlobalVariableToTrue,

  // Merge two values into an updated value when using `mutable`-attributed
  // parameters.
  kCallMergeFunctor,
};

// A generic operation.
template <>
class Node<ProgramOperationRegion> : public Node<ProgramRegion> {
 public:
  virtual ~Node(void);
  explicit Node(REGION *parent_, ProgramOperation op_);

  virtual Node<ProgramCallRegion> *AsCall(void) noexcept;
  virtual Node<ProgramReturnRegion> *AsReturn(void) noexcept;
  virtual Node<ProgramExistenceCheckRegion> *AsExistenceCheck(void) noexcept;
  virtual Node<ProgramExistenceAssertionRegion> *
  AsExistenceAssertion(void) noexcept;
  virtual Node<ProgramGenerateRegion> *AsGenerate(void) noexcept;
  virtual Node<ProgramLetBindingRegion> *AsLetBinding(void) noexcept;
  virtual Node<ProgramPublishRegion> *AsPublish(void) noexcept;
  virtual Node<ProgramTransitionStateRegion> *AsTransitionState(void) noexcept;
  virtual Node<ProgramCheckStateRegion> *AsCheckState(void) noexcept;
  virtual Node<ProgramTableJoinRegion> *AsTableJoin(void) noexcept;
  virtual Node<ProgramTableProductRegion> *AsTableProduct(void) noexcept;
  virtual Node<ProgramTableScanRegion> *AsTableScan(void) noexcept;
  virtual Node<ProgramTupleCompareRegion> *AsTupleCompare(void) noexcept;
  virtual Node<ProgramVectorLoopRegion> *AsVectorLoop(void) noexcept;
  virtual Node<ProgramVectorAppendRegion> *AsVectorAppend(void) noexcept;
  virtual Node<ProgramVectorClearRegion> *AsVectorClear(void) noexcept;
  virtual Node<ProgramVectorUniqueRegion> *AsVectorUnique(void) noexcept;

  Node<ProgramOperationRegion> *AsOperation(void) noexcept override;

  const ProgramOperation op;

  // If this operation does something conditional then this is the body it
  // executes.
  UseRef<REGION> body;
};

using OP = Node<ProgramOperationRegion>;

// A let binding, i.e. an assignment of zero or more variables. Variables
// are assigned pairwise from `used_vars` into `defined_vars`.
template <>
class Node<ProgramLetBindingRegion> final
    : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);

  void Accept(ProgramVisitor &visitor) override;

  bool IsNoOp(void) const noexcept override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq,
              Node<ProgramRegion> *that) const noexcept override;

  inline Node(REGION *parent_)
      : Node<ProgramOperationRegion>(parent_, ProgramOperation::kLetBinding),
        defined_vars(this),
        used_vars(this) {}

  Node<ProgramLetBindingRegion> *AsLetBinding(void) noexcept override;

  // Local variables that are defined/used in the body of this procedure.
  DefList<VAR> defined_vars;
  UseList<VAR> used_vars;
};

using LET = Node<ProgramLetBindingRegion>;

// Loop over the vector `vector` and bind the extracted tuple elements into
// the variables specified in `defined_vars`.
template <>
class Node<ProgramVectorLoopRegion> final
    : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);

  void Accept(ProgramVisitor &visitor) override;

  inline Node(REGION *parent_, ProgramOperation op_)
      : Node<ProgramOperationRegion>(parent_, op_),
        defined_vars(this) {}

  bool IsNoOp(void) const noexcept override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq,
              Node<ProgramRegion> *that) const noexcept override;

  Node<ProgramVectorLoopRegion> *AsVectorLoop(void) noexcept override;

  // Local variables bound to the vector being looped.
  DefList<VAR> defined_vars;

  // Vector being looped.
  UseRef<VECTOR> vector;
};

using VECTORLOOP = Node<ProgramVectorLoopRegion>;

// Append a tuple into a vector. The elements in the tuple must match the
// element/column types of the vector.
template <>
class Node<ProgramVectorAppendRegion> final
    : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);

  void Accept(ProgramVisitor &visitor) override;

  inline Node(REGION *parent_, ProgramOperation op_)
      : Node<ProgramOperationRegion>(parent_, op_),
        tuple_vars(this) {}

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq,
              Node<ProgramRegion> *that) const noexcept override;

  Node<ProgramVectorAppendRegion> *AsVectorAppend(void) noexcept override;

  UseList<VAR> tuple_vars;
  UseRef<VECTOR> vector;
};

using VECTORAPPEND = Node<ProgramVectorAppendRegion>;

// Clear a vector.
template <>
class Node<ProgramVectorClearRegion> final
    : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);
  using Node<ProgramOperationRegion>::Node;

  void Accept(ProgramVisitor &visitor) override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq,
              Node<ProgramRegion> *that) const noexcept override;

  Node<ProgramVectorClearRegion> *AsVectorClear(void) noexcept override;

  UseRef<VECTOR> vector;
};

using VECTORCLEAR = Node<ProgramVectorClearRegion>;

// Sort and unique a vector.
template <>
class Node<ProgramVectorUniqueRegion> final
    : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);
  using Node<ProgramOperationRegion>::Node;

  void Accept(ProgramVisitor &visitor) override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq,
              Node<ProgramRegion> *that) const noexcept override;

  Node<ProgramVectorUniqueRegion> *AsVectorUnique(void) noexcept override;

  UseRef<VECTOR> vector;
};

using VECTORUNIQUE = Node<ProgramVectorUniqueRegion>;

// Set the state of a tuple in a view. In the simplest case, this behaves like
// a SQL `INSERT` statement: it says that some data exists in a relation. There
// are two other states that can be set: absent, which is like a `DELETE`, and
// unknown, which has no SQL equivalent, but it like a tentative `DELETE`. An
// unknown tuple is one which has been speculatively marked as deleted, and
// needs to be re-proven in order via alternate means in order for it to be
// used.
template <>
class Node<ProgramTransitionStateRegion> final
    : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);

  inline Node(Node<ProgramRegion> *parent_, TupleState from_state_,
              TupleState to_state_)
      : Node<ProgramOperationRegion>(parent_,
                                     ProgramOperation::kInsertIntoTable),
        col_values(this),
        from_state(from_state_),
        to_state(to_state_) {}
  
  void Accept(ProgramVisitor &visitor) override;

  Node<ProgramTransitionStateRegion> *AsTransitionState(void) noexcept override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq,
              Node<ProgramRegion> *that) const noexcept override;

  // Variables that make up the tuple.
  UseList<VAR> col_values;

  // View into which the tuple is being inserted.
  UseRef<TABLE> table;

  const TupleState from_state;
  const TupleState to_state;
};

using CHANGESTATE = Node<ProgramTransitionStateRegion>;

// Check the state of a tuple. This is sort of like asking if something exists,
// but has three conditionally executed children, based off of the state.
// One state is that the tuple os missing from a view. The second state is
// that the tuple is present in the view. The final state is that we are
// not sure if the tuple is present or absent, because it has been marked
// as a candidate for deletion, and thus we need to re-prove it.
template <>
class Node<ProgramCheckStateRegion> final
    : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);

  inline Node(Node<ProgramRegion> *parent_)
      : Node<ProgramOperationRegion>(parent_,
                                     ProgramOperation::kCheckStateInTable),
        col_values(this) {}

  void Accept(ProgramVisitor &visitor) override;

  bool IsNoOp(void) const noexcept override;

  Node<ProgramCheckStateRegion> *AsCheckState(void) noexcept override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq,
              Node<ProgramRegion> *that) const noexcept override;

  // Variables that make up the tuple.
  UseList<VAR> col_values;

  // View into which the tuple is being inserted.
  UseRef<TABLE> table;

  // Region that is conditionally executed if the tuple is not present.
  UseRef<REGION> absent_body;

  // Region that is conditionally executed if the tuple was deleted and hasn't
  // been re-checked.
  UseRef<REGION> unknown_body;
};

using CHECKSTATE = Node<ProgramCheckStateRegion>;

// Calls another IR procedure. All IR procedures return `true` or `false`. This
// return value can be tested, and if it is, a body can be conditionally
// executed based off of the result of that test.
template <>
class Node<ProgramCallRegion> final : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);

  Node(Node<ProgramRegion> *parent_, Node<ProgramProcedure> *called_proc_,
       ProgramOperation op_=ProgramOperation::kCallProcedure)
      : Node<ProgramOperationRegion>(parent_, op_),
        called_proc(called_proc_),
        arg_vars(this),
        arg_vecs(this) {}

  void Accept(ProgramVisitor &visitor) override;

  bool IsNoOp(void) const noexcept override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq,
              Node<ProgramRegion> *that) const noexcept override;

  Node<ProgramCallRegion> *AsCall(void) noexcept override;

  // Procedure being called.
  Node<ProgramProcedure> *const called_proc;

  // Variables passed as arguments.
  UseList<VAR> arg_vars;

  // Vectors passed as arguments.
  UseList<VECTOR> arg_vecs;
};

using CALL = Node<ProgramCallRegion>;

// Returns true/false from a procedure.
template <>
class Node<ProgramReturnRegion> final : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);

  Node(Node<ProgramRegion> *parent_, ProgramOperation op_)
      : Node<ProgramOperationRegion>(parent_, op_) {}

  void Accept(ProgramVisitor &visitor) override;
  bool IsNoOp(void) const noexcept override;
  bool Equals(EqualitySet &eq,
              Node<ProgramRegion> *that) const noexcept override;

  Node<ProgramReturnRegion> *AsReturn(void) noexcept override;
};

using RETURN = Node<ProgramReturnRegion>;

// Publishes a message to the pub/sub.
template <>
class Node<ProgramPublishRegion> final : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);

  Node(Node<ProgramRegion> *parent_, ParsedMessage message_)
      : Node<ProgramOperationRegion>(parent_,
                                     ProgramOperation::kPublishMessage),
        message(message_),
        arg_vars(this) {}

  void Accept(ProgramVisitor &visitor) override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq,
              Node<ProgramRegion> *that) const noexcept override;

  Node<ProgramPublishRegion> *AsPublish(void) noexcept override;

  // Message being published.
  const ParsedMessage message;

  // Variables passed as arguments.
  UseList<VAR> arg_vars;
};

using PUBLISH = Node<ProgramPublishRegion>;

// Represents a positive or negative existence check.
template <>
class Node<ProgramExistenceCheckRegion> final
    : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);

  inline Node(Node<ProgramRegion> *parent_, ProgramOperation op_)
      : Node<ProgramOperationRegion>(parent_, op_),
        cond_vars(this) {}

  void Accept(ProgramVisitor &visitor) override;

  bool IsNoOp(void) const noexcept override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq,
              Node<ProgramRegion> *that) const noexcept override;

  Node<ProgramExistenceCheckRegion> *AsExistenceCheck(void) noexcept override;

  // Variables associated with these existence checks.
  UseList<VAR> cond_vars;
};

using EXISTS = Node<ProgramExistenceCheckRegion>;

// Represents a positive or negative existence check.
template <>
class Node<ProgramExistenceAssertionRegion> final
    : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);

  inline Node(Node<ProgramRegion> *parent_, ProgramOperation op_)
      : Node<ProgramOperationRegion>(parent_, op_),
        cond_vars(this) {}

  void Accept(ProgramVisitor &visitor) override;

  bool IsNoOp(void) const noexcept override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq,
              Node<ProgramRegion> *that) const noexcept override;

  Node<ProgramExistenceAssertionRegion> *
  AsExistenceAssertion(void) noexcept override;

  // Variables associated with these existence checks.
  UseList<VAR> cond_vars;
};

using ASSERT = Node<ProgramExistenceAssertionRegion>;

// An equi-join between two or more tables.
template <>
class Node<ProgramTableJoinRegion> final : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);
  inline Node(Node<ProgramRegion> *parent_, QueryJoin query_join_)
      : Node<ProgramOperationRegion>(parent_, ProgramOperation::kJoinTables),
        query_join(query_join_),
        tables(this),
        indices(this),
        pivot_vars(this),
        pivot_cols() {}

  void Accept(ProgramVisitor &visitor) override;

  bool IsNoOp(void) const noexcept override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq,
              Node<ProgramRegion> *that) const noexcept override;

  Node<ProgramTableJoinRegion> *AsTableJoin(void) noexcept override;

  const QueryJoin query_join;

  UseList<TABLE> tables;
  UseList<TABLEINDEX> indices;
  UseRef<VECTOR> pivot_vec;

  // There is a `1:N` correspondence bween `pivot_vars` and `pivot_cols`.
  DefList<VAR> pivot_vars;
  std::vector<UseList<TABLECOLUMN>> pivot_cols;

  // There is a 1:1 correspondence between `output_vars` and `output_cols`.
  std::vector<DefList<VAR>> output_vars;
  std::vector<UseList<TABLECOLUMN>> output_cols;
};

using TABLEJOIN = Node<ProgramTableJoinRegion>;

// A cross product between two or more tables.
template <>
class Node<ProgramTableProductRegion> final
    : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);
  inline Node(Node<ProgramRegion> *parent_, QueryJoin query_join_)
      : Node<ProgramOperationRegion>(parent_, ProgramOperation::kCrossProduct),
        query_join(query_join_),
        tables(this),
        input_vectors(this) {}

  void Accept(ProgramVisitor &visitor) override;

  bool IsNoOp(void) const noexcept override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq,
              Node<ProgramRegion> *that) const noexcept override;

  Node<ProgramTableProductRegion> *AsTableProduct(void) noexcept override;

  const QueryJoin query_join;

  UseList<TABLE> tables;
  UseList<VECTOR> input_vectors;
  std::vector<DefList<VAR>> output_vars;
};

using TABLEPRODUCT = Node<ProgramTableProductRegion>;

// Perform a scan over a table, possibly using an index. If an index is being
// used the input variables are provided to perform equality matching against
// column values. The results of the scan fill a vector.
template <>
class Node<ProgramTableScanRegion> final
    : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);

  inline Node(Node<ProgramRegion> *parent_)
      : Node<ProgramOperationRegion>(parent_, ProgramOperation::kScanTable),
        out_cols(this),
        in_cols(this),
        in_vars(this) {}
  
  void Accept(ProgramVisitor &visitor) override;

  bool IsNoOp(void) const noexcept override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq,
              Node<ProgramRegion> *that) const noexcept override;

  Node<ProgramTableScanRegion> *AsTableScan(void) noexcept override;

  UseRef<TABLE> table;
  UseList<TABLECOLUMN> out_cols;

  UseRef<TABLEINDEX> index;
  UseList<TABLECOLUMN> in_cols;
  UseList<VAR> in_vars;

  UseRef<VECTOR> output_vector;
};

using TABLESCAN = Node<ProgramTableScanRegion>;

// Comparison between two tuples.
template <>
class Node<ProgramTupleCompareRegion> final
    : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);
  inline Node(Node<ProgramRegion> *parent_, ComparisonOperator op_)
      : Node<ProgramOperationRegion>(parent_, ProgramOperation::kCompareTuples),
        cmp_op(op_),
        lhs_vars(this),
        rhs_vars(this) {}

  void Accept(ProgramVisitor &visitor) override;

  bool IsNoOp(void) const noexcept override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq,
              Node<ProgramRegion> *that) const noexcept override;

  Node<ProgramTupleCompareRegion> *AsTupleCompare(void) noexcept override;

  const ComparisonOperator cmp_op;
  UseList<VAR> lhs_vars;
  UseList<VAR> rhs_vars;
};

using TUPLECMP = Node<ProgramTupleCompareRegion>;

// Calling a functor.
template <>
class Node<ProgramGenerateRegion> final : public Node<ProgramOperationRegion> {
 public:
  virtual ~Node(void);
  inline Node(Node<ProgramRegion> *parent_, ParsedFunctor functor_)
      : Node<ProgramOperationRegion>(
            parent_, functor_.IsFilter() ? ProgramOperation::kCallFilterFunctor
                                         : ProgramOperation::kCallFunctor),
        functor(functor_),
        defined_vars(this),
        used_vars(this) {}

  void Accept(ProgramVisitor &visitor) override;

  bool IsNoOp(void) const noexcept override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq,
              Node<ProgramRegion> *that) const noexcept override;

  Node<ProgramGenerateRegion> *AsGenerate(void) noexcept override;

  const ParsedFunctor functor;

  // Free variables that are generated from the application of the functor.
  DefList<VAR> defined_vars;

  // Bound variables passed in as arguments to the functor.
  UseList<VAR> used_vars;
};

using GENERATOR = Node<ProgramGenerateRegion>;

// A procedure region. This represents some entrypoint of data into the program.
template <>
class Node<ProgramProcedure> : public Node<ProgramRegion> {
 public:
  virtual ~Node(void);

  inline Node(unsigned id_, ProcedureKind kind_)
      : Node<ProgramRegion>(this),
        id(id_),
        kind(kind_),
        tables(this) {}

  void Accept(ProgramVisitor &visitor) override;

  bool IsNoOp(void) const noexcept override;

  Node<ProgramProcedure> *AsProcedure(void) noexcept override;

  // Create a new vector in this procedure for a list of columns.
  VECTOR *VectorFor(ProgramImpl *impl, VectorKind kind,
                    DefinedNodeRange<QueryColumn> cols);

  const unsigned id;
  const ProcedureKind kind;

  std::optional<QueryIO> io;

  // Temporary tables within this procedure.
  DefList<TABLE> tables;

  // Body of this procedure. Initially starts with a loop over an implicit
  // input vector.
  UseRef<REGION> body;

  // Input vectors and variables.
  DefList<VECTOR> input_vectors;
  DefList<VAR> input_vars;

  // Vectors defined in this procedure. If this is a vector procedure then
  // the first vector is the input vector.
  DefList<VECTOR> vectors;

  // Are we currently checking if this procedure is being used? This is to
  // avoid infinite recursion when doing a procedure call NoOp checks.
  mutable bool checking_if_nop{false};
};

using PROC = Node<ProgramProcedure>;

// A series region is where the `regions[N]` must finish before `regions[N+1]`
// begins.
template <>
class Node<ProgramSeriesRegion> final : public Node<ProgramRegion> {
 public:
  inline Node(REGION *parent_) : Node<ProgramRegion>(parent_), regions(this) {}

  virtual ~Node(void);

  void Accept(ProgramVisitor &visitor) override;
  bool IsNoOp(void) const noexcept override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq,
              Node<ProgramRegion> *that) const noexcept override;

  Node<ProgramSeriesRegion> *AsSeries(void) noexcept override;

  UseList<Node<ProgramRegion>> regions;
};

using SERIES = Node<ProgramSeriesRegion>;

// A region where multiple things can happen in parallel.
template <>
class Node<ProgramParallelRegion> final : public Node<ProgramRegion> {
 public:
  inline Node(REGION *parent_) : Node<ProgramRegion>(parent_), regions(this) {}

  virtual ~Node(void);

  void Accept(ProgramVisitor &visitor) override;

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq,
              Node<ProgramRegion> *that) const noexcept override;

  bool IsNoOp(void) const noexcept override;

  Node<ProgramParallelRegion> *AsParallel(void) noexcept override;

  UseList<Node<ProgramRegion>> regions;
};

using PARALLEL = Node<ProgramParallelRegion>;

// An induction is a loop centred on a `QueryMerge` node. Some of the views
// incoming to that `QueryMerge` are treated as "inputs", as they bring initial
// data into the `QueryMerge`. Other nodes are treated as "inductions" as they
// cycle back to the `QueryMerge`.
template <>
class Node<ProgramInductionRegion> final : public Node<ProgramRegion> {
 public:
  virtual ~Node(void);

  void Accept(ProgramVisitor &visitor) override;

  explicit Node(ProgramImpl *impl, REGION *parent_);

  // Returns `true` if `this` and `that` are structurally equivalent (after
  // variable renaming).
  bool Equals(EqualitySet &eq,
              Node<ProgramRegion> *that) const noexcept override;

  Node<ProgramInductionRegion> *AsInduction(void) noexcept override;

  // Initial regions that fill up one or more of the inductive vectors.
  UseRef<REGION> init_region;

  // The cyclic regions of this induction. This is a PARALLEL region.
  UseRef<REGION> cyclic_region;

  // The output regions of this induction. This is a PARALLEL region.
  UseRef<REGION> output_region;

  // Vectors built up by this induction.
  UseList<VECTOR> vectors;

  // It could be the case that a when going through the induction we end up
  // going into a co-mingled induction, as is the case in
  // `transitive_closure2.dr` and `transitive_closure3.dr`.
  std::unordered_map<QueryView, UseRef<VECTOR>> view_to_vec;

  // List of append to vector regions inside this induction.
  std::unordered_map<QueryView, UseList<REGION>> view_to_init_appends;

  // List of append to vector regions inside this induction.
  std::unordered_map<QueryView, UseList<REGION>> view_to_cycle_appends;

  // Maps views to their cycles inside of the `cyclic_region`.
  std::unordered_map<QueryView, UseRef<REGION>> view_to_cycle_loop;

  // Maps views to their cycles inside of the `output_region`.
  std::unordered_map<QueryView, UseRef<REGION>> view_to_output_loop;

  enum State {
    kAccumulatingInputRegions,
    kAccumulatingCycleRegions,
    kBuildingOutputRegions
  } state = kAccumulatingInputRegions;
};

using INDUCTION = Node<ProgramInductionRegion>;

class ProgramImpl : public User {
 public:
  ~ProgramImpl(void);

  inline explicit ProgramImpl(Query query_)
      : User(this),
        query(query_),
        procedure_regions(this),
        series_regions(this),
        parallel_regions(this),
        induction_regions(this),
        operation_regions(this),
        tables(this) {}

  void Optimize(void);

  // The data flow representation from which this was created.
  const Query query;

  DefList<PROC> procedure_regions;
  DefList<SERIES> series_regions;
  DefList<PARALLEL> parallel_regions;
  DefList<INDUCTION> induction_regions;
  DefList<OP> operation_regions;
  DefList<TABLE> tables;
  DefList<VAR> global_vars;
  unsigned next_id{0u};

  DefList<VAR> const_vars;

  // Maps constants to their global variables.
  std::unordered_map<QueryConstant, VAR *> const_to_var;

  // Maps query conditions to the reference counts associated with them.
  std::unordered_map<QueryCondition, VAR *> cond_ref_counts;

  // Maps views whose outputs are all constants or constant references to
  // condition variables that tracker whether or not we need to actually re-
  // execute the successors.
  std::unordered_map<QueryView, VAR *> const_view_to_var;

  // We build up "data models" of views that can share the same backing storage.
  std::vector<std::unique_ptr<DataModel>> models;
  std::unordered_map<QueryView, DataModel *> view_to_model;

  // Maps views to procedures for bottom-up proving that goes and removes
  // tuples. Removal of tuples changes their state from PRESENT to UNKNOWN.
  std::unordered_map<QueryView, PROC *> view_to_bottom_up_remover;
};

}  // namespace hyde
