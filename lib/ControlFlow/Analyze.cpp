// Copyright 2020, Trail of Bits. All rights reserved.

#include <algorithm>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>

#include "Program.h"

namespace hyde {
namespace {

struct ColumnProvenance {
  // The variable from which the rest of this information was derived.
  // Helpful if we want to re-derive it.
  VAR *input_var{nullptr};
  REGION *input_var_use{nullptr};

  TABLECOLUMN *col{nullptr};

  TABLE *src_table{nullptr};
  TABLECOLUMN *src_col{nullptr};

  VAR *src_global{nullptr};

  VECTOR *src_vec{nullptr};
  GENERATOR *src_generator{nullptr};

  // From either `src_vec` or `src_generator`
  VAR *src_var{nullptr};
  unsigned index_of_src_var{0};

  // For `join` and `product`.
  unsigned index_of_src_table{0};

  TABLEJOIN *join{nullptr};
  TABLEPRODUCT *product{nullptr};
  CHANGERECORD *change{nullptr};
  CHECKRECORD *check{nullptr};
  TABLESCAN *scan{nullptr};
  VECTORLOOP *loop{nullptr};
  GENERATOR *generator{nullptr};

  unsigned EstimateSizeInBits(void) const {
    TypeLoc type = input_var->Type();

    switch (type.UnderlyingKind()) {
      case TypeKind::kInvalid:
        assert(false);
        return 64u;  // Hrmm.
      case TypeKind::kBoolean: return 1u;
      case TypeKind::kSigned8: return 8u;
      case TypeKind::kSigned16: return 16u;
      case TypeKind::kSigned32: return 32u;
      case TypeKind::kSigned64: return 64u;
      case TypeKind::kUnsigned8: return 8u;
      case TypeKind::kUnsigned16: return 16u;
      case TypeKind::kUnsigned32: return 32u;
      case TypeKind::kUnsigned64: return 64u;
      case TypeKind::kFloat: return 32u;
      case TypeKind::kDouble: return 64u;
      case TypeKind::kBytes: return 64u;  // It's not transparent.

      // TODO(pag): Maybe add a way to communicate expected size into the
      //            language?
      case TypeKind::kForeignType:
        return 64u;  // Pointer-sized for a `Ref<T>`.
    }

    return 64u;
  }
};

struct RowProvenance {
  RowProvenance(void)
      : columns(new std::vector<ColumnProvenance>) {}

  TABLE *table{nullptr};

  // If the generator has a range of `zero-or-more`, or `one-or-more` then
  // it is "expanding", i.e. it might take a given input and then convert it
  // to many outputs. A generator dependency of this kind cannot be folded
  // into some prior tuple.
  bool generator_is_expanding{false};

  std::unique_ptr<std::vector<ColumnProvenance>> columns;

  // Converts this row provenance into a string, which can be used for
  // deduplicating two row provenances.
  std::string Key(void) const;
};

class AnalysisContext {
 private:
  using UpdateList = std::vector<OP *>;

  // Mappings of vectors to the append operations into those vectors. We use
  // this to drill down through vector appends to find the provenance of those
  // columns.
  std::unordered_map<VECTOR *, std::vector<VECTORAPPEND *>> vector_appends;

  std::unordered_set<std::string> seen_rows;

  std::unordered_map<TABLE *, UpdateList> table_updates;
  std::unordered_map<TABLE *, std::vector<RowProvenance>> table_sources;
  std::vector<RowProvenance> pending_table_sources;

  std::unordered_map<std::string, RowProvenance *> key_to_provenance;
  std::unordered_map<TABLE *, std::vector<RowProvenance *>> unique_table_sources;

  // Go find every transition state, and organize it by table, so that we can
  // analyze a table all at once.
  void CollectMetadata(ProgramImpl *impl);

  // Does `row` still have dependencies on `vector-append`s?
  bool HasAppendingVectors(const RowProvenance &row) const;

  // Analyze a particular variable.
  void AnalyzeVariable(TABLE *table, unsigned table_col_index, VAR *var,
                       REGION *var_use, RowProvenance &row);

  // Analyze `var`, which can be the source of the `table_col_index`th column
  // of `table`, and where `var` is a global or constant.
  void AnalyzeGlobalColumn(TABLE *table, unsigned table_col_index,
                           VAR *var, REGION *var_use, RowProvenance &row);

  // Analyze `var`, which can be the source of the `table_col_index`th column
  // of `table`, and is defined in `src`.
  void AnalyzeColumn(TABLE *table, unsigned table_col_index,
                     VAR *var, REGION *var_use, TABLEJOIN *src,
                     RowProvenance &row);

  // Analyze `var`, which can be the source of the `table_col_index`th column
  // of `table`, and is defined in `src`.
  void AnalyzeColumn(TABLE *table, unsigned table_col_index,
                     VAR *var, REGION *var_use, TABLEPRODUCT *src,
                     RowProvenance &row);

  // Analyze `var`, which can be the source of the `table_col_index`th column
  // of `table`, and is defined in `src`.
  void AnalyzeColumn(TABLE *table, unsigned table_col_index,
                     VAR *var, REGION *var_use, TABLESCAN *src,
                     RowProvenance &row);

  // Analyze `var`, which can be the source of the `table_col_index`th column
  // of `table`, and is defined in `src`.
  void AnalyzeColumn(TABLE *table, unsigned table_col_index,
                     VAR *var, REGION *var_use, VECTORLOOP *src,
                     RowProvenance &row);

  // Analyze `var`, which can be the source of the `table_col_index`th column
  // of `table`, and is defined in `src`.
  void AnalyzeColumn(TABLE *table, unsigned table_col_index,
                     VAR *var, REGION *var_use, GENERATOR *src,
                     RowProvenance &row);

  // Analyze `var`, which can be the source of the `table_col_index`th column
  // of `table`, and is defined in `src`.
  void AnalyzeColumn(TABLE *table, unsigned table_col_index,
                     VAR *var, REGION *var_use, CHANGERECORD *src,
                     RowProvenance &row);

  // Analyze `var`, which can be the source of the `table_col_index`th column
  // of `table`, and is defined in `src`.
  void AnalyzeColumn(TABLE *table, unsigned table_col_index,
                     VAR *var, REGION *var_use, CHECKRECORD *src,
                     RowProvenance &row);

  // Analyze a specific update to a specific table.
  void AnalyzeTable(TABLE *table, const UseList<VAR> &col_values,
                    REGION *var_use);
  void AnalyzeTable(TABLE *table, CHANGETUPLE *update);
  void AnalyzeTable(TABLE *table, CHANGERECORD *update);

  // Analyze a specific table.
  void AnalyzeTable(TABLE *table, const UpdateList &updates);

  // Take the analysis results and work them through the vector appends, so
  // that we can track back to the original source of some row.
  void AnalyzeVectorAppends(void);

  // Unique and group the row provenance information.
  void UniqueAndGroupRowProvenance(void);

  // Convert a `CHECKTUPLE` into a `CHECKRECORD`.
  void ConvertToCheckRecord(ProgramImpl *impl, CHECKTUPLE *check);

  // Convert a `CHANGETUPLE` into a `CHANGERECORD`.
  void ConvertToChangeRecord(ProgramImpl *impl, CHANGETUPLE *change);

  // Convert uses of tuples from tables in the set to uses of records.
  bool ConvertTablesToRecords(ProgramImpl *impl,
                              const std::unordered_set<TABLE *> &tables);

 public:

  // Convert state transitions and state checks on induction tables into
  // state emplacements (for records) and record getters.
  void ConvertInductionsToRecords(ProgramImpl *impl);

  // Analyze all tables.
  void AnalyzeTables(ProgramImpl *impl);

  // Convert uses of rows that would rely only on a single pointer and some
  // constants into records.
  bool ConvertSinglePointerTuplesToRecords(ProgramImpl *impl);

  // Build up the record and record case data structures.
  void Build(ProgramImpl *impl);

  void Dump(ProgramImpl *impl);
};

// Go find every transition state, and organize it by table, so that we can
// analyze a table all at once. Also collect mappings of vectors to vector
// appends.
void AnalysisContext::CollectMetadata(ProgramImpl *impl) {
  for (OP *op : impl->operation_regions) {
    if (CHANGETUPLE *change_state = op->AsChangeTuple()) {
      table_updates[change_state->table.get()].push_back(change_state);

    } else if (CHANGERECORD *change_record = op->AsChangeRecord()) {
      table_updates[change_record->table.get()].push_back(change_record);

    } else if (VECTORAPPEND *append = op->AsVectorAppend()) {
      auto found_switch = append->ContainingModeSwitch();
      if (!found_switch) {
        vector_appends[append->vector.get()].push_back(append);
      }
    }
  }

  // Vectors get swapped/cleared for the sake of inductions. Thus, we need to
  // track provenance across swaps.
  for (OP *op : impl->operation_regions) {
    if (VECTORSWAP *swap = op->AsVectorSwap()) {
      std::vector<VECTORAPPEND *> new_appends;
      auto &lhs_appends = vector_appends[swap->lhs.get()];
      auto &rhs_appends = vector_appends[swap->rhs.get()];

      for (auto append : lhs_appends) {
        new_appends.push_back(append);
      }
      for (auto append : rhs_appends) {
        new_appends.push_back(append);
      }

      std::sort(new_appends.begin(), new_appends.end());
      auto it = std::unique(new_appends.begin(), new_appends.end());
      new_appends.erase(it, new_appends.end());

      lhs_appends = new_appends;
      rhs_appends.swap(new_appends);
    }
  }
}

// Analyze `var`, which can be the source of the `table_col_index`th column
// of `table`, and where `var` is a global or constant.
void AnalysisContext::AnalyzeGlobalColumn(
    TABLE *table, unsigned table_col_index, VAR *var, REGION *var_use,
    RowProvenance &row) {

  ColumnProvenance provenance;
  provenance.input_var = var;
  provenance.input_var_use = var_use;
  provenance.col = table->columns[table_col_index];
  provenance.src_global = var;
  provenance.src_var = nullptr;

  row.columns->emplace_back(std::move(provenance));
}

// Analyze `var`, which can be the source of the `table_col_index`th column
// of `table`, and is defined in `src`.
void AnalysisContext::AnalyzeColumn(TABLE *table, unsigned table_col_index,
                                    VAR *var, REGION *var_use, TABLEJOIN *src,
                                    RowProvenance &row) {

  // We implement the table join region so that it should always come from
  // the most represented table in terms of non-pivot variables.
  for (auto pivot_var : src->pivot_vars) {
    if (var == pivot_var) {
      assert(false);
    }
  }

  auto src_table_index = 0u;
  auto src_column_index = 0u;
  for (auto &src_table_vars : src->output_vars) {
    src_column_index = 0u;
    for (auto src_var : src_table_vars) {
      if (src_var == var) {
        goto found;
      }
      ++src_column_index;
    }
    ++src_table_index;
  }
  assert(false);

found:

  ColumnProvenance provenance;
  provenance.join = src;
  provenance.input_var = var;
  provenance.input_var_use = var_use;
  provenance.col = table->columns[table_col_index];
  provenance.src_table = src->tables[src_table_index];
  provenance.src_col = provenance.src_table->columns[src_column_index];
  provenance.index_of_src_var = src_column_index;
  provenance.index_of_src_table = src_table_index;
  row.columns->emplace_back(std::move(provenance));
}

// Analyze `var`, which can be the source of the `table_col_index`th column
// of `table`, and is defined in `src`.
void AnalysisContext::AnalyzeColumn(TABLE *table, unsigned table_col_index,
                                    VAR *var, REGION *var_use,
                                    TABLEPRODUCT *src, RowProvenance &row) {
  auto src_table_index = 0u;
  auto src_column_index = 0u;
  for (auto &src_table_vars : src->output_vars) {
    src_column_index = 0u;
    for (auto src_var : src_table_vars) {
      if (src_var == var) {
        goto found;
      }
      ++src_column_index;
    }
    ++src_table_index;
  }
  assert(false);

found:

  ColumnProvenance provenance;
  provenance.product = src;
  provenance.input_var = var;
  provenance.input_var_use = var_use;
  provenance.col = table->columns[table_col_index];
  provenance.src_table = src->tables[src_table_index];
  provenance.src_col = provenance.src_table->columns[src_column_index];
  provenance.index_of_src_var = src_column_index;
  provenance.index_of_src_table = src_table_index;
  row.columns->emplace_back(std::move(provenance));
}

// Analyze `var`, which can be the source of the `table_col_index`th column
// of `table`, and is defined in `src`.
void AnalysisContext::AnalyzeColumn(TABLE *table, unsigned table_col_index,
                                    VAR *var, REGION *var_use, TABLESCAN *src,
                                    RowProvenance &row) {
  auto src_column_index = 0u;
  for (auto src_var : src->out_vars) {
    if (src_var == var) {
      goto found;
    }
    ++src_column_index;
  }
  assert(false);

found:

  ColumnProvenance provenance;
  provenance.scan = src;
  provenance.input_var = var;
  provenance.input_var_use = var_use;
  provenance.col = table->columns[table_col_index];
  provenance.src_table = src->table.get();
  provenance.src_col = provenance.src_table->columns[src_column_index];

  row.columns->emplace_back(std::move(provenance));

  auto i = 0u;
  for (auto input_col : src->in_cols) {
    if (input_col == provenance.src_col) {
      provenance.src_var = src->in_vars[i];
      provenance.index_of_src_var = i;
      break;
    }
    ++i;
  }
}

// Analyze `var`, which can be the source of the `table_col_index`th column
// of `table`, and is defined in `src`.
void AnalysisContext::AnalyzeColumn(
    TABLE *table, unsigned table_col_index,
    VAR *var, REGION *var_use, CHANGERECORD *src, RowProvenance &row) {
  auto src_column_index = 0u;
  for (auto src_var : src->record_vars) {
    if (src_var == var) {
      goto found;
    }
    ++src_column_index;
  }
  assert(false);

found:

  ColumnProvenance provenance;
  provenance.change = src;
  provenance.input_var = var;
  provenance.input_var_use = var_use;
  provenance.col = table->columns[table_col_index];
  provenance.src_table = src->table.get();
  provenance.src_col = provenance.src_table->columns[src_column_index];
  provenance.src_var = src->col_values[src_column_index];
  provenance.index_of_src_var = src_column_index;
  row.columns->emplace_back(std::move(provenance));
}

// Analyze `var`, which can be the source of the `table_col_index`th column
// of `table`, and is defined in `src`.
void AnalysisContext::AnalyzeColumn(
    TABLE *table, unsigned table_col_index,
    VAR *var, REGION *var_use, CHECKRECORD *src, RowProvenance &row) {
  auto src_column_index = 0u;
  for (auto src_var : src->record_vars) {
    if (src_var == var) {
      goto found;
    }
    ++src_column_index;
  }
  assert(false);

found:

  ColumnProvenance provenance;
  provenance.check = src;
  provenance.input_var = var;
  provenance.input_var_use = var_use;
  provenance.col = table->columns[table_col_index];
  provenance.src_table = src->table.get();
  provenance.src_col = provenance.src_table->columns[src_column_index];
  provenance.src_var = src->col_values[src_column_index];
  provenance.index_of_src_var = src_column_index;
  row.columns->emplace_back(std::move(provenance));
}

// Analyze `var`, which can be the source of the `table_col_index`th column
// of `table`, and is defined in `src`.
void AnalysisContext::AnalyzeColumn(TABLE *table, unsigned table_col_index,
                                    VAR *var, REGION *var_use, VECTORLOOP *src,
                                    RowProvenance &row) {
  auto src_column_index = 0u;
  for (auto src_var : src->defined_vars) {
    if (src_var == var) {
      goto found;
    }
    ++src_column_index;
  }
  assert(false);

found:

  ColumnProvenance provenance;
  provenance.loop = src;
  provenance.input_var = var;
  provenance.input_var_use = var_use;
  provenance.col = table->columns[table_col_index];
  provenance.index_of_src_var = src_column_index;
  provenance.src_var = nullptr;

  if (TABLE *src_table = src->induction_table.get()) {
    provenance.src_table = src_table;
    provenance.src_col = src_table->columns[src_column_index];

  } else {
    provenance.src_vec = src->vector.get();
  }

  row.columns->emplace_back(std::move(provenance));
}

bool AnalysisContext::HasAppendingVectors(const RowProvenance &row) const {
  for (const ColumnProvenance &col : *(row.columns)) {
    if (col.src_vec && vector_appends.count(col.src_vec)) {
      return true;
    }
  }
  return false;
}

// Analyze `var`, which can be the source of the `table_col_index`th column
// of `table`, and is defined in `src`.
void AnalysisContext::AnalyzeColumn(TABLE *table, unsigned table_col_index,
                                    VAR *var, REGION *var_use, GENERATOR *src,
                                    RowProvenance &row) {
  auto i = 0u;
  for (auto out_var : src->defined_vars) {
    if (var == out_var) {
      ColumnProvenance provenance;
      provenance.generator = src;
      provenance.input_var = var;
      provenance.input_var_use = var_use;
      provenance.col = table->columns[table_col_index];
      provenance.src_generator = src;
      provenance.src_var = nullptr;
      provenance.index_of_src_var = i;
      row.columns->emplace_back(std::move(provenance));

      ParsedFunctor functor = src->functor;
      switch (functor.Range()) {
        case FunctorRange::kZeroOrOne:
        case FunctorRange::kOneOrMore:
          row.generator_is_expanding = true;
          break;
        default:
          break;
      }

      return;
    }
    ++i;
  }
}

void AnalysisContext::AnalyzeVariable(
    TABLE *table, unsigned table_col_index, VAR *var, REGION *var_use,
    RowProvenance &row) {
  REGION * const var_src = var->defining_region;
  if (!var_src) {
    assert(var->IsGlobal());
    AnalyzeGlobalColumn(table, table_col_index, var, var_use, row);
    return;
  }

  if (OP *var_src_op = var_src->AsOperation()) {

    // Try to figure out if `var` is actually a constant in this context.
    VAR *var_const = nullptr;

    var->ForEachUse<TUPLECMP>([&] (TUPLECMP *cmp, VAR *var) {
      if (cmp->cmp_op != ComparisonOperator::kEqual) {
        return;
      }

      auto num_vars = cmp->lhs_vars.Size();
      for (auto i = 0u; i < num_vars; ++i) {
        VAR *lhs = cmp->lhs_vars[i];
        VAR *rhs = cmp->rhs_vars[i];
        if (lhs == var) {
          if (rhs->IsConstant() &&
              var_use->FindCommonAncestor(cmp) == cmp) {
            var_const = rhs;
            return;
          }
        } else if (rhs == var) {
          if (lhs->IsConstant() &&
              var_use->FindCommonAncestor(cmp) == cmp) {
            var_const = lhs;
            return;
          }
        }
      }
    });

    if (var_const) {
      std::cerr << "Var " << var->id << " is constant "
                << var_const->id << '\n';
      AnalyzeGlobalColumn(table, table_col_index, var_const, var_use, row);
      return;
    }

    if (TABLEJOIN *join = var_src_op->AsTableJoin()) {
      AnalyzeColumn(table, table_col_index, var, var_use, join, row);

    } else if (TABLEPRODUCT *product = var_src_op->AsTableProduct()) {
      AnalyzeColumn(table, table_col_index, var, var_use, product, row);

    } else if (TABLESCAN *scan = var_src_op->AsTableScan()) {
      AnalyzeColumn(table, table_col_index, var, var_use, scan, row);

    } else if (VECTORLOOP *loop = var_src_op->AsVectorLoop()) {
      AnalyzeColumn(table, table_col_index, var, var_use, loop, row);

    } else if (GENERATOR *generator = var_src_op->AsGenerate()) {
      AnalyzeColumn(table, table_col_index, var, var_use, generator, row);

    } else if (CHANGERECORD *change = var_src_op->AsChangeRecord()) {
      AnalyzeColumn(table, table_col_index, var, var_use, change, row);

    } else if (CHECKRECORD *check = var_src_op->AsCheckRecord()) {
      AnalyzeColumn(table, table_col_index, var, var_use, check, row);

    } else {
      assert(false);
    }

  // This variable is a parameter to a procedure.
  } else if (PROC *var_src_proc = var_src->AsProcedure()) {
    assert(var_src_proc->kind == ProcedureKind::kTupleFinder);
    assert(false);  // We should never reach here.

  } else {
    assert(false);
  }
}

// Take the analysis results and work them through the vector appends, so
// that we can track back to the original source of some row.
void AnalysisContext::AnalyzeVectorAppends(void) {
  while (!pending_table_sources.empty()) {
    std::cerr << "Processing pending: " << pending_table_sources.size() << "\n";

    RowProvenance row(std::move(pending_table_sources.back()));
    pending_table_sources.pop_back();


    for (auto &col : *(row.columns)) {
      std::cerr << "  input_var: " << col.input_var->id << "\n";
    }

    const auto c_max = row.columns->size();
    for (auto c = 0u; c < c_max; ++c) {
      const ColumnProvenance &col = row.columns->at(c);
      if (!col.src_vec || !vector_appends.count(col.src_vec)) {
        continue;
      }

      std::cerr << "    focusing on " << col.input_var->id << "\n";

      // Analyze this column in the context of each append into the
      // vector. This will produce a new row provenance for each such
      // vector append.
      for (VECTORAPPEND *append : vector_appends[col.src_vec]) {

        RowProvenance new_row;
        new_row.table = row.table;

        // Re-create the main inputs.
        for (auto i = 0u; i < c; ++i) {
          const ColumnProvenance &old_col = row.columns->at(i);
          new_row.columns->push_back(old_col);
        }

        // Now analyze the source variable of the vector append.
        AnalyzeVariable(
            row.table, c, append->tuple_vars[col.index_of_src_var],
            append, new_row);

        // Maintain the original provenance.
        auto &fixed_col = new_row.columns->back();
        std::cerr << "       deriving from " << fixed_col.input_var->id << "\n";
        fixed_col.input_var = col.input_var;
        fixed_col.input_var_use = col.input_var_use;

        // Add the other columns in.
        for (auto i = c + 1u; i < c_max; ++i) {
          const ColumnProvenance &old_col = row.columns->at(i);
          new_row.columns->push_back(old_col);
        }

        if (HasAppendingVectors(new_row)) {
          pending_table_sources.emplace_back(std::move(new_row));
        } else {
          table_sources[new_row.table].emplace_back(std::move(new_row));
        }
      }

      // Handle any subsequent columns in future work list iterations.
      break;
    }
    std::cerr << "\n";
  }
}

// Unique and group the row provenance information.
void AnalysisContext::UniqueAndGroupRowProvenance(void) {
  for (auto &[table, rows] : table_sources) {

    auto &unique_rows = unique_table_sources[table];

    for (RowProvenance &row : rows) {
      auto key = row.Key();
      auto [it, added] = key_to_provenance.emplace(std::move(key), &row);
      RowProvenance * const row_ptr = it->second;
      if (std::find(unique_rows.begin(), unique_rows.end(), row_ptr) ==
          unique_rows.end()) {
        unique_rows.push_back(row_ptr);
      }
    }
  }
}

void AnalysisContext::AnalyzeTable(TABLE *table, const UseList<VAR> &col_values,
                                   REGION *var_use) {
  auto &rows = table_sources[table];

  RowProvenance row;
  row.table = table;

  auto i = 0u;
  for (VAR *var : col_values) {
    AnalyzeVariable(table, i++, var, var_use, row);
  }

  auto row_key = row.Key();
  if (auto [it, added] = seen_rows.emplace(std::move(row_key)); added) {
    if (HasAppendingVectors(row)) {
      pending_table_sources.emplace_back(std::move(row));
    } else {
      rows.emplace_back(std::move(row));
    }
  }
}

// Analyze a specific update to a specific table.
void AnalysisContext::AnalyzeTable(TABLE *table, CHANGETUPLE *update) {

  // We care only about the sources of added data.
  if (update->to_state != TupleState::kPresent ||
      update->containing_procedure->kind == ProcedureKind::kTupleFinder) {
    return;
  }

  AnalyzeTable(table, update->col_values, update);
}

void AnalysisContext::AnalyzeTable(TABLE *table, CHANGERECORD *update) {

  // We care only about the sources of added data.
  if (update->to_state != TupleState::kPresent ||
      update->containing_procedure->kind == ProcedureKind::kTupleFinder) {
    return;
  }

  AnalyzeTable(table, update->col_values, update);
}

// Analyze a specific table.
void AnalysisContext::AnalyzeTable(TABLE *table, const UpdateList &updates) {
  for (OP *update : updates) {
    if (CHANGETUPLE *tuple = update->AsChangeTuple()) {
      AnalyzeTable(table, tuple);
    } else if (CHANGERECORD *record = update->AsChangeRecord()) {
      AnalyzeTable(table, record);
    } else {
      assert(false);
    }
  }
}

// Convert a `CHECKTUPLE` into a `CHECKRECORD`.
void AnalysisContext::ConvertToCheckRecord(
    ProgramImpl *impl, CHECKTUPLE *check) {
  CHECKRECORD *record = impl->operation_regions.CreateDerived<CHECKRECORD>(
      impl->next_id++, check->parent);
  record->col_values.Swap(check->col_values);
  record->table.Emplace(record, check->table.get());

  REGION *body = check->body.get();
  REGION *absent_body = check->absent_body.get();
  REGION *unknown_body = check->unknown_body.get();

  check->body.Clear();
  check->absent_body.Clear();
  check->unknown_body.Clear();

  if (body) {
    body->parent = record;
    record->body.Emplace(record, body);
  }

  if (absent_body) {
    absent_body->parent = record;
    record->absent_body.Emplace(record, absent_body);
  }

  if (unknown_body) {
    unknown_body->parent = record;
    record->unknown_body.Emplace(record, unknown_body);
  }

  for (VAR *in_var : record->col_values) {
    VAR *out_var = record->record_vars.Create(
        impl->next_id++, VariableRole::kRecordElement);
    out_var->defining_region = record;
    out_var->query_column = in_var->query_column;
    out_var->query_cond = in_var->query_cond;
    out_var->query_const = in_var->query_const;

    // Replace all uses of each variable used as input to the check-state
    // with an output variable of the get-record, so long as that use is
    // dominated by the get-record.
    in_var->ReplaceUsesWithIf<REGION>(
        out_var, [=] (REGION *user, VAR *) {
          if (user->containing_procedure != record->containing_procedure ||
              user == record) {
            return false;
          } else {
            return user->FindCommonAncestor(record) == record;
          }
//          if (user->containing_procedure != record->containing_procedure ||
//              user == record) {
//            return false;
//          } else {
//            for (auto region = user;
//                 region && region != region->containing_procedure;
//                 region = region->parent) {
//              if (region == body || region == unknown_body) {
//                return true;
//              }
//            }
//            return false;
//          }
        });
  }

  check->ReplaceAllUsesWith(record);
  check->parent = nullptr;
}

// Convert a `CHANGETUPLE` into a `CHANGERECORD`.
void AnalysisContext::ConvertToChangeRecord(
    ProgramImpl *impl, CHANGETUPLE *change) {

  TABLE * const table = change->table.get();
  REGION *prev_record = nullptr;

  // Go try to figure out if this record change is taking as inputs stuff
  // from a check record, where everything lines up perfectly.
  auto i = 0u;
  for (VAR *in_var : change->col_values) {
    if (!in_var->defining_region) {
      prev_record = nullptr;
      break;
    }

    auto op_region = in_var->defining_region->AsOperation();
    if (!op_region) {
      prev_record = nullptr;
      break;
    }

    auto op_check = op_region->AsCheckRecord();
    auto op_change = op_region->AsChangeRecord();

    TABLE *prev_table = nullptr;
    DefList<VAR> *record_vars = nullptr;
    if (op_check) {
      prev_table = op_check->table.get();
      record_vars = &(op_check->record_vars);
    } else if (op_change) {
      prev_table = op_change->table.get();
      record_vars = &(op_change->record_vars);
    } else {
      prev_record = nullptr;
      break;
    }

    if (prev_table != table) {
      prev_record = nullptr;
      break;
    }

    assert(i < record_vars->Size());

    if ((*record_vars)[i] != in_var) {
      prev_record = nullptr;
      break;
    }

    if (!prev_record) {
      if (!i) {
        prev_record = op_region;
      } else {
        break;
      }
    } else if (prev_record != op_check) {
      prev_record = nullptr;
      break;
    }

    ++i;
  }

  // Not worth changing.
  if (prev_record) {
    return;
  }

  CHANGERECORD *record = impl->operation_regions.CreateDerived<CHANGERECORD>(
      impl->next_id++, change->parent, change->from_state, change->to_state);
  record->col_values.Swap(change->col_values);
  record->table.Emplace(record, table);

  REGION *body = change->body.get();
  REGION *failed_body = change->failed_body.get();

  change->body.Clear();
  change->failed_body.Clear();

  if (body) {
    body->parent = record;
    record->body.Emplace(record, body);
  }

  if (failed_body) {
    failed_body->parent = record;
    record->failed_body.Emplace(record, failed_body);
  }

  for (VAR *in_var : record->col_values) {
    VAR *out_var = record->record_vars.Create(
        impl->next_id++, VariableRole::kRecordElement);
    out_var->defining_region = record;
    out_var->query_column = in_var->query_column;
    out_var->query_cond = in_var->query_cond;
    out_var->query_const = in_var->query_const;

    // Replace all uses of each variable used as input to the check-state
    // with an output variable of the get-record, so long as that use is
    // dominated by the get-record.
    in_var->ReplaceUsesWithIf<REGION>(
        out_var, [=] (REGION *user, VAR *) {
          if (user->containing_procedure != record->containing_procedure ||
              user == record) {
            return false;
          } else {
            return user->FindCommonAncestor(record) == record;
          }
        });
  }

  change->ReplaceAllUsesWith(record);
  change->parent = nullptr;
}

namespace {

static bool OrderDeepestRegionFirst(REGION *a, REGION *b) {
  return a->CachedDepth() > b->CachedDepth();
}

}  // namespace

// Convert uses of tuples from tables in the set to uses of records.
bool AnalysisContext::ConvertTablesToRecords(
    ProgramImpl *impl, const std::unordered_set<TABLE *> &tables) {

  std::unordered_map<TABLE *, std::vector<CHANGETUPLE *>> change_states;
  std::unordered_map<TABLE *, std::vector<CHECKTUPLE *>> check_states;

  for (OP *op : impl->operation_regions) {
    if (CHANGETUPLE *change = op->AsChangeTuple()) {
      change_states[change->table.get()].push_back(change);

    } else if (CHECKTUPLE *check = op->AsCheckTuple()) {
      check_states[check->table.get()].push_back(check);
    }
  }

  auto changed = false;
  for (TABLE *table : tables) {

//    // Order deepest first.
//    auto &checkers = check_states[table];
//    std::sort(checkers.begin(), checkers.end(), OrderDeepestRegionFirst);
//
//    // Check states often contain change states, so we want change states to
//    // see the record variables of check states, if possible.
//    for (CHECKTUPLE *check : checkers) {
//      changed = true;
//      ConvertToCheckRecord(impl, check);
//    }

    auto &changers = change_states[table];
    std::sort(changers.begin(), changers.end(), OrderDeepestRegionFirst);

    for (CHANGETUPLE *change : changers) {
      changed = true;
      ConvertToChangeRecord(impl, change);
    }
  }

  impl->operation_regions.RemoveUnused();
  return changed;
}

// Convert state transitions and state checks on induction tables into
// state emplacements (for records) and record getters.
void AnalysisContext::ConvertInductionsToRecords(ProgramImpl *impl) {

  std::unordered_set<TABLE *> induction_tables;
  for (OP *op : impl->operation_regions) {
    if (VECTORLOOP *loop = op->AsVectorLoop()) {
      if (TABLE *induction_table = loop->induction_table.get()) {
        induction_tables.insert(induction_table);
      }
    }
  }

  ConvertTablesToRecords(impl, induction_tables);
}

// Analyze all tables.
void AnalysisContext::AnalyzeTables(ProgramImpl *impl) {

  // First, go and change every single `CHANGETUPLE` into a `CHANGERECORD`.
  std::unordered_set<TABLE *> tables;
  for (TABLE *table : impl->tables) {
    tables.insert(table);
  }
  ConvertTablesToRecords(impl, tables);

  table_updates.clear();
  table_sources.clear();
  pending_table_sources.clear();
  key_to_provenance.clear();
  unique_table_sources.clear();
  seen_rows.clear();
  CollectMetadata(impl);
  for (const auto &[table, updates] : table_updates) {
    AnalyzeTable(table, updates);
  }
  AnalyzeVectorAppends();
  UniqueAndGroupRowProvenance();

  // Go and normalize the records, so that if a column is is a variable in
  // any of the provenances for that row, then it is always a variable.
  for (auto changed = true; changed; ) {
    changed = false;
    break;
    std::cerr << "Running...\n";
    for (auto &[table, rows] : unique_table_sources) {

      std::vector<bool> any_derived(table->columns.Size(), false);
      std::vector<bool> all_derived(table->columns.Size(), true);

      for (RowProvenance *row : rows) {
        auto i = 0u;
        for (const ColumnProvenance &col : *(row->columns)) {
          if (col.src_table) {
            any_derived[i] = true;
          } else {
            all_derived[i] = false;
          }
          ++i;
        }
      }

      for (RowProvenance *row : rows) {
        auto i = 0u;
        for (ColumnProvenance &col : *(row->columns)) {
          if (any_derived[i] && !all_derived[i]) {
            std::cerr << "  Killing var " << col.input_var->id << '\n';
            if (col.input_var->id == 8528) {
              assert(false);
            }
            col.src_table = nullptr;
            col.src_col = nullptr;
            changed = true;
          }
          ++i;
        }
      }
    }
  }
}

// Converts this row provenance into a string, which can be used for
// deduplicating two row provenances.
std::string RowProvenance::Key(void) const {
  std::stringstream ss;

  auto sep = "";
  for (auto &col : *columns) {
    if (col.src_col) {
      ss << sep << "col" << col.src_col->id;

    } else if (col.src_var) {
      if (col.src_var->IsConstant()) {
        ss << sep << "const" << col.src_var->id;
      } else if (col.src_var->IsGlobal()) {
        ss << sep << "global" << col.src_var->id;
      } else {
        ss << sep << "var" << col.src_var->id;
      }
    } else if (col.input_var) {
      ss << sep << "var" << col.input_var->id;
    }
    sep = "_";
  }

  return ss.str();
}

// Convert uses of rows that would rely only on a single pointer and some
// constants into records.
bool AnalysisContext::ConvertSinglePointerTuplesToRecords(
    ProgramImpl *impl) {

//  std::unordered_set<REGION *> regions;
//  std::unordered_set<TABLEJOIN *> joins;
//  std::unordered_set<TABLEPRODUCT *> products;
//  std::unordered_set<TABLESCAN *> scans;
//  std::unordered_set<CHANGERECORD *> changes;
//  std::unordered_set<CHECKRECORD *> checks;
//  std::unordered_set<VECTORLOOP *> loops;
//  std::unordered_set<GENERATOR *> generators;
//
//  std::unordered_map<TABLE *, std::unordered_set<size_t>> needed_pointers;
//  std::unordered_map<TABLE *, unsigned> max_tuple_size;
//  std::unordered_map<TABLE *, unsigned> max_record_size;
//
//  std::unordered_map<RowProvenance *, std::vector<TABLE *>> row_to_tables;
//
//
//  for (auto &[key, row_ptr] : key_to_provenance) {
//
//
//    for (const ColumnProvenance &col : *row->columns) {
//      if (col.src_table) {
//        needed_pointers[row->table].insert(num_needed_pointers);
//      }
//
//
//      inductions.insert(col.induction_table);
//      joins.insert(col.join);
//      products.insert(col.product);
//      scans.insert(col.scan);
//      changes.insert(col.change);
//      checks.insert(col.check);
//      loops.insert(col.loop);
//      generators.insert(col.generator);
//
//      const auto col_size = col.EstimateSizeInBits();
//      tuple_size += col_size;
//
//      if (!col.change && !col.check && !col.induction_table && !col.join &&
//          !col.product && !col.scan) {
//        record_size += col_size;
//      }
//    }
//
//    inductions.erase(nullptr);
//    joins.erase(nullptr);
//    products.erase(nullptr);
//    scans.erase(nullptr);
//    changes.erase(nullptr);
//    checks.erase(nullptr);
//    loops.erase(nullptr);
//    generators.erase(nullptr);
//
//    size_t num_needed_pointers = 0u;
//    num_needed_pointers += scans.size();
//    num_needed_pointers += inductions.size();
//    num_needed_pointers += changes.size();
//    num_needed_pointers += checks.size();
//
//    for (TABLEJOIN *join : joins) {
//      for (auto &out_var_list : join->output_vars) {
//
//        // If one table only contributes pivots, then we don't need to
//        // store a pointer to its data.
//        if (out_var_list.Size() > join->pivot_vars.Size()) {
//          ++num_needed_pointers;
//        }
//      }
//    }
//
//    for (TABLEPRODUCT *product : products) {
//      num_needed_pointers += product->tables.Size();
//    }
//
//    record_size += num_needed_pointers * 64;
//
//
//    auto &curr_max_tuple_size = max_tuple_size[row->table];
//    auto &curr_max_record_size = max_record_size[row->table];
//
//    curr_max_tuple_size = std::max(curr_max_tuple_size, tuple_size);
//    curr_max_record_size = std::max(curr_max_record_size, record_size);
//  }
//
//  std::unordered_set<TABLE *> single_pointer_tables;
//  for (const auto &[table, pointer_counts] : needed_pointers) {
//
//    // NOTE(pag): We consider the case of zero pointers to be for data that
//    //            depends on input messages, and the case of 1 pointers to be
//    //            the case where all ways of inserting into a given table only
//    //            ever depends on one other table.
//    //
//    // NOTE(pag): The zero-or-one case (where there are two entries in
//    //            `pointer_counts` should get handled by repeated applications.
//    if (pointer_counts.size() == 1u && *(pointer_counts.begin()) <= 1u) {
//      single_pointer_tables.insert(table);
//    }
//  }
//
//  if (!single_pointer_tables.empty() &&
//      ConvertTablesToRecords(impl, single_pointer_tables)) {
//    return true;
//  }
//
//  single_pointer_tables.clear();
//  for (auto [table, tuple_size] : max_tuple_size) {
//    if (tuple_size >= max_record_size[table]) {
//      single_pointer_tables.insert(table);
//    }
//  }
//
//  if (single_pointer_tables.empty()) {
//    return false;
//  }
//
//  return ConvertTablesToRecords(impl, single_pointer_tables);
  return false;
}

// Build up the record and record case data structures.
void AnalysisContext::Build(ProgramImpl *impl) {
  std::unordered_map<TABLE *, DATARECORD *> table_records;
  std::unordered_map<RowProvenance *, DATARECORDCASE *> cases;
  std::map<std::pair<void *, unsigned>, std::pair<DATARECORD *, unsigned>>
      record_uses;

  for (const auto &[table, rows] : unique_table_sources) {
    DATARECORD * record = table->records.Create(impl->next_id++, table);
    table_records.emplace(table, record);
  }

  auto find_record_use =
      [&] (DATARECORDCASE *rc, REGION *region, TABLE *table_used,
           unsigned index_of_table) -> unsigned {
        std::pair<REGION *, unsigned> key(region, index_of_table);
        std::pair<DATARECORD *, unsigned> &use = record_uses[key];
        if (!use.first) {
          use.second = rc->derived_from.Size();
          use.first = table_records[table_used];
          rc->derived_from.AddUse(use.first);
        }
        return use.second;
      };

  for (const auto &[key, row] : key_to_provenance) {
    DATARECORDCASE *rc = impl->record_cases.Create(impl->next_id++);
    cases.emplace(row, rc);

    rc->columns.reserve(row->columns->size());

    record_uses.clear();

    for (const ColumnProvenance &col : *row->columns) {
      rc->columns.emplace_back();
      RecordColumn &rc_col = rc->columns.back();

      if (col.src_table) {
        REGION *regions[] = {col.loop, col.scan, col.change, col.check,
                             col.join, col.product};
        for (auto region : regions) {
          if (region) {
            auto r_index = find_record_use(rc, region, col.src_table,
                                           col.index_of_src_table);

            rc_col.column.Emplace(rc, col.src_col);
            rc_col.derived_index = r_index;
            rc_col.derived_offset = col.src_col->index;
            goto import_next_column;
          }
        }

        assert(false);

      import_next_column:
        continue;

      } else if (col.loop) {
        assert(col.input_var->defining_region == col.loop);
        rc_col.var.Emplace(rc, col.input_var);

      } else if (col.generator) {
        assert(col.input_var->defining_region == col.generator);
        rc_col.var.Emplace(rc, col.input_var);

      } else if (col.input_var) {
        rc_col.var.Emplace(rc, col.input_var);

      } else {
        assert(false);
      }
    }
  }

  for (const auto &[table, rows] : unique_table_sources) {
    DATARECORD * record = table_records[table];
    for (RowProvenance *row : rows) {
      DATARECORDCASE *rc = cases[row];
      record->cases.AddUse(rc);

      if (record->table->id == 189) {
        if (rc->derived_from.Size() == 1u &&
            rc->derived_from[0]->table->id == 223) {
          std::cerr << "189 from 223:\n";
          for (auto &col : *(row->columns)) {
            std::cerr << "  var " << col.input_var->id;
            if (col.src_var) {
              std::cerr << "  src " << col.src_var->id;
            }
            std::cerr << "\n";
          }
        }
      }
    }
  }
}

void AnalysisContext::Dump(ProgramImpl *impl) {
  std::ofstream os("/tmp/tables.dot");

  static constexpr auto kTable = "<TABLE cellpadding=\"0\" cellspacing=\"0\" border=\"1\">";
  static constexpr auto kRow = "<TR>";
  static constexpr auto kCell = "<TD>";
  static constexpr auto kBold = "<B>";


  static constexpr auto kEndTable = "</TABLE>";
  static constexpr auto kEndRow = "</TR>";
  static constexpr auto kEndCell = "</TD>";
  static constexpr auto kEndBold = "</B>";

  os << "digraph {\n"
     << "node [shape=none margin=0 nojustify=false labeljust=l font=courier];\n";

  for (TABLE *table : impl->tables) {
    for (DATARECORD *record : table->records) {
      os << "r" << record->id << " [label=<" << kTable << kRow
         << kCell << kBold << "TABLE " << table->id << kEndBold << kEndCell
         << kCell << kBold << "RECORD " << record->id << kEndBold << kEndCell;

      auto i = 0u;
      for (TABLECOLUMN *col : table->columns) {
        os << "<TD port=\"c" << (i++) << "\">COL " << col->id << kEndCell;
      }

      os << kEndRow << kEndTable << ">];\n";

      for (DATARECORDCASE *rc : record->cases) {

        os << "r" << record->id << " -> rc" << rc->id << ";\n"
           << "rc" << rc->id << " [label=<" << kTable << kRow << kCell
           << kBold << "CASE " << rc->id << kEndBold << kEndCell;

        i = 0u;
        for (RecordColumn &rc_col : rc->columns) {
          if (rc_col.column) {
            os << "<TD port=\"c" << i << "\">COL " << rc_col.column->id;
          } else if (rc_col.var->IsConstant()) {
            os << kCell << "CONST " << rc_col.var->id;

          } else {
            os << kCell << "VAR " << rc_col.var->id;
          }
          os << kEndCell;
          ++i;
        }

        os << kEndRow << kEndTable << ">];\n";

        i = 0u;
        for (RecordColumn &rc_col : rc->columns) {
          if (rc_col.column) {
            DATARECORD *dr = rc->derived_from[rc_col.derived_index];
            os << "rc" << rc->id << ":c" << i << " -> r" << dr->id
               << ":c" << rc_col.derived_offset << " [label=\""
               << rc_col.derived_index << "\"];\n";
          }
          ++i;
        }
      }
    }

  }

  os << "}\n";
}

}  // namespace

// Analyze the control-flow IR and table usage, looking for strategies that
// can be used to eliminate redundancies in the data storage model. We do this
// after optimizing the control-flow IR so that we can observe the effects
// of copy propagation, which gives us the ability to "hop backward" to the
// provenance of some data, as opposed to having to jump one `QueryView` at
// a time.
void ProgramImpl::Analyze(void) {

  AnalysisContext context;

  unsigned max_depth = 0u;
  for (OP *region : operation_regions) {
    max_depth = std::max(max_depth, region->CachedDepth());
  }

//  context.ConvertInductionsToRecords(this);
  context.AnalyzeTables(this);
//  for (auto i = 0u; i < max_depth; ++i) {
//    if (context.ConvertSinglePointerTuplesToRecords(this)) {
//      context.AnalyzeTables(this);
//      std::cerr << "Updated!!\n";
//    } else {
//      std::cerr << "Converged at iteration " << i << "\n";
//      break;
//    }
//  }

  context.Build(this);
  context.Dump(this);
}

}  // namespace hyde
