// Copyright 2020, Trail of Bits. All rights reserved.

#include <algorithm>
#include <fstream>
#include <iostream>
#include <sstream>

#include "Program.h"

namespace hyde {
namespace {

struct ColumnProvenance {
  // The variable from which the rest of this information was derived.
  // Helpful if we want to re-derive it.
  VAR *input_var{nullptr};

  TABLECOLUMN *col{nullptr};

  TABLE *src_table{nullptr};
  TABLECOLUMN *src_col{nullptr};

  VAR *src_global{nullptr};

  VECTOR *src_vec{nullptr};
  GENERATOR *src_generator{nullptr};

  // From either `src_vec` or `src_generator`
  VAR *src_var{nullptr};
  unsigned index_of_src_var{0};

  TABLE *induction_table{nullptr};
  TABLEJOIN *join{nullptr};
  TABLEPRODUCT *product{nullptr};
  TABLESCAN *scan{nullptr};
  VECTORLOOP *loop{nullptr};
  GENERATOR *generator{nullptr};

  unsigned EstimateSizeInBits(const ParsedModule &module, Language lang) const {
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
        if (type.IsReferentiallyTransparent(module, lang)) {
          return 8u;  // Be really generous.
        } else {
          return 64u;  // Pointer-sized for a `Ref<T>`.
        }
    }

    return 64u;
  }
};

struct RowProvenance {
  RowProvenance(void)
      : columns(new std::vector<ColumnProvenance>) {}

  TABLE *table{nullptr};

  // Counters of how many columns in this row are derived from various kinds
  // of sources.
  unsigned num_joins{0};
  unsigned num_products{0};
  unsigned num_merges{0};
  unsigned num_scans{0};
  unsigned num_globals{0};
  unsigned num_generators{0};
  unsigned num_appending_vectors{0};
  unsigned num_vectors{0};

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
  using UpdateList = std::vector<CHANGESTATE *>;

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

  // Analyze a particular variable.
  void AnalyzeVariable(TABLE *table, unsigned table_col_index, VAR *var,
                       RowProvenance &row);

  // Analyze `var`, which can be the source of the `table_col_index`th column
  // of `table`, and where `var` is a global or constant.
  void AnalyzeGlobalColumn(TABLE *table, unsigned table_col_index,
                           VAR *var, RowProvenance &row);

  // Analyze `var`, which can be the source of the `table_col_index`th column
  // of `table`, and is defined in `src`.
  void AnalyzeColumn(TABLE *table, unsigned table_col_index,
                     VAR *var, TABLEJOIN *src, RowProvenance &row);

  // Analyze `var`, which can be the source of the `table_col_index`th column
  // of `table`, and is defined in `src`.
  void AnalyzeColumn(TABLE *table, unsigned table_col_index,
                     VAR *var, TABLEPRODUCT *src, RowProvenance &row);

  // Analyze `var`, which can be the source of the `table_col_index`th column
  // of `table`, and is defined in `src`.
  void AnalyzeColumn(TABLE *table, unsigned table_col_index,
                     VAR *var, TABLESCAN *src, RowProvenance &row);

  // Analyze `var`, which can be the source of the `table_col_index`th column
  // of `table`, and is defined in `src`.
  void AnalyzeColumn(TABLE *table, unsigned table_col_index,
                     VAR *var, VECTORLOOP *src, RowProvenance &row);

  // Analyze `var`, which can be the source of the `table_col_index`th column
  // of `table`, and is defined in `src`.
  void AnalyzeColumn(TABLE *table, unsigned table_col_index,
                     VAR *var, GENERATOR *src, RowProvenance &row);

  // Analyze a specific update to a specific table.
  void AnalyzeTable(TABLE *table, CHANGESTATE *update);

  // Analyze a specific table.
  void AnalyzeTable(TABLE *table, const UpdateList &updates);

  // Take the analysis results and work them through the vector appends, so
  // that we can track back to the original source of some row.
  void AnalyzeVectorAppends(void);

  // Unique and group the row provenance information.
  void UniqueAndGroupRowProvenance(void);

 public:

  // Convert state transitions and state checks on induction tables into
  // state emplacements (for records) and record getters.
  void ConvertInductionsToRecords(ProgramImpl *impl);

  // Analyze all tables.
  void AnalyzeTables(ProgramImpl *impl);

  void Dump(const ParsedModule &module, Language lang);
};

// Go find every transition state, and organize it by table, so that we can
// analyze a table all at once. Also collect mappings of vectors to vector
// appends.
void AnalysisContext::CollectMetadata(ProgramImpl *impl) {
  for (OP *op : impl->operation_regions) {
    if (CHANGESTATE *transition = op->AsTransitionState()) {
      table_updates[transition->table.get()].push_back(transition);

    } else if (CHANGERECORD *emplace = op->AsChangeRecord()) {
      assert(false);  // TODO!

    } else if (VECTORAPPEND *append = op->AsVectorAppend()) {
      vector_appends[append->vector.get()].push_back(append);
    }
  }
}

// Analyze `var`, which can be the source of the `table_col_index`th column
// of `table`, and where `var` is a global or constant.
void AnalysisContext::AnalyzeGlobalColumn(
    TABLE *table, unsigned table_col_index, VAR *var, RowProvenance &row) {

  ColumnProvenance provenance;
  provenance.input_var = var;
  provenance.col = table->columns[table_col_index];
  provenance.src_global = var;
  provenance.src_var = var;

  row.columns->emplace_back(std::move(provenance));
  row.num_globals++;
}

// Analyze `var`, which can be the source of the `table_col_index`th column
// of `table`, and is defined in `src`.
void AnalysisContext::AnalyzeColumn(TABLE *table, unsigned table_col_index,
                                    VAR *var, TABLEJOIN *src,
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
  provenance.col = table->columns[table_col_index];
  provenance.src_table = src->tables[src_table_index];
  provenance.src_col = src->output_cols[src_table_index][src_column_index];
  provenance.index_of_src_var = src_column_index;
  row.columns->emplace_back(std::move(provenance));
  row.num_joins++;
}

// Analyze `var`, which can be the source of the `table_col_index`th column
// of `table`, and is defined in `src`.
void AnalysisContext::AnalyzeColumn(TABLE *table, unsigned table_col_index,
                                    VAR *var, TABLEPRODUCT *src,
                                    RowProvenance &row) {
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
  provenance.col = table->columns[table_col_index];
  provenance.src_table = src->tables[src_table_index];
  provenance.src_col = provenance.src_table->columns[src_column_index];
  provenance.index_of_src_var = src_column_index;
  row.columns->emplace_back(std::move(provenance));
  row.num_products++;
}

// Analyze `var`, which can be the source of the `table_col_index`th column
// of `table`, and is defined in `src`.
void AnalysisContext::AnalyzeColumn(TABLE *table, unsigned table_col_index,
                                    VAR *var, TABLESCAN *src,
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
  provenance.col = table->columns[table_col_index];
  provenance.src_table = src->table.get();
  provenance.src_col = provenance.src_table->columns[src_column_index];
  provenance.index_of_src_var = src_column_index;
  row.columns->emplace_back(std::move(provenance));
  row.num_scans++;
}

// Analyze `var`, which can be the source of the `table_col_index`th column
// of `table`, and is defined in `src`.
void AnalysisContext::AnalyzeColumn(TABLE *table, unsigned table_col_index,
                                    VAR *var, VECTORLOOP *src,
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
  provenance.col = table->columns[table_col_index];
  provenance.index_of_src_var = src_column_index;

  if (auto src_table = src->induction_table.get()) {
    provenance.induction_table = src_table;
    provenance.src_table = src_table;
    provenance.src_col = src_table->columns[src_column_index];
    row.num_merges++;

  } else {
    provenance.src_vec = src->vector.get();
    provenance.src_var = var;

    if (vector_appends.count(provenance.src_vec)) {
      row.num_appending_vectors++;
    } else {
      row.num_vectors++;
    }
  }

  row.columns->emplace_back(std::move(provenance));
}

// Analyze `var`, which can be the source of the `table_col_index`th column
// of `table`, and is defined in `src`.
void AnalysisContext::AnalyzeColumn(TABLE *table, unsigned table_col_index,
                                    VAR *var, GENERATOR *src,
                                    RowProvenance &row) {
  auto i = 0u;
  for (auto out_var : src->defined_vars) {
    if (var == out_var) {
      ColumnProvenance provenance;
      provenance.generator = src;
      provenance.input_var = var;
      provenance.col = table->columns[table_col_index];
      provenance.src_generator = src;
      provenance.src_var = var;
      provenance.index_of_src_var = i;
      row.columns->emplace_back(std::move(provenance));
      row.num_generators++;

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
    TABLE *table, unsigned table_col_index, VAR *var, RowProvenance &row) {
  REGION * const var_src = var->defining_region;
  if (!var_src) {
    assert(var->IsGlobal());
    AnalyzeGlobalColumn(table, table_col_index, var, row);
    return;
  }

  if (OP *var_src_op = var_src->AsOperation()) {

    if (TABLEJOIN *join = var_src_op->AsTableJoin()) {
      AnalyzeColumn(table, table_col_index, var, join, row);

    } else if (TABLEPRODUCT *product = var_src_op->AsTableProduct()) {
      AnalyzeColumn(table, table_col_index, var, product, row);

    } else if (TABLESCAN *scan = var_src_op->AsTableScan()) {
      AnalyzeColumn(table, table_col_index, var, scan, row);

    } else if (VECTORLOOP *loop = var_src_op->AsVectorLoop()) {
      AnalyzeColumn(table, table_col_index, var, loop, row);

    } else if (GENERATOR *generator = var_src_op->AsGenerate()) {
      AnalyzeColumn(table, table_col_index, var, generator, row);

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

    RowProvenance row(std::move(pending_table_sources.back()));
    pending_table_sources.pop_back();

    const auto c_max = row.columns->size();
    for (auto c = 0u; c < c_max; ++c) {
      const ColumnProvenance &col = row.columns->at(c);

      if (!col.src_vec || !vector_appends.count(col.src_vec)) {
        continue;
      }

      // Analyze this column in the context of each append into the
      // vector. This will produce a new row provenance for each such
      // vector append.
      for (VECTORAPPEND *append : vector_appends[col.src_vec]) {

        RowProvenance new_row;
        new_row.table = row.table;

        // Re-create the main inputs.
        for (auto i = 0u; i < c; ++i) {
          const ColumnProvenance &old_col = row.columns->at(c);
          AnalyzeVariable(row.table, i, old_col.input_var, new_row);
        }

        // Now analyze the source variable of the vector append.
        AnalyzeVariable(
            row.table, c, append->tuple_vars[col.index_of_src_var], new_row);

        for (auto i = c + 1u; i < c_max; ++i) {
          const ColumnProvenance &old_col = row.columns->at(c);
          AnalyzeVariable(row.table, i, old_col.input_var, new_row);
        }

        auto row_key = row.Key();
        if (auto [it, added] = seen_rows.emplace(std::move(row_key)); added) {
          if (new_row.num_appending_vectors) {
            pending_table_sources.emplace_back(std::move(new_row));
          } else {
            table_sources[new_row.table].emplace_back(std::move(new_row));
          }
        }
      }

      // Handle any subsequent columns in future work list iterations.
      break;
    }
  }
}

// Unique and group the row provenance information.
void AnalysisContext::UniqueAndGroupRowProvenance(void) {
  for (auto &[table, rows] : table_sources) {

    auto &unique_rows = unique_table_sources[table];

    for (RowProvenance &row : rows) {
      auto key = row.Key();
      auto [it, added] = key_to_provenance.emplace(std::move(key), &row);
      auto row_ptr = it->second;
      if (std::find(unique_rows.begin(), unique_rows.end(), row_ptr) ==
          unique_rows.end()) {
        unique_rows.push_back(row_ptr);
      }
    }
  }
}

// Analyze a specific update to a specific table.
void AnalysisContext::AnalyzeTable(TABLE *table, CHANGESTATE *update) {

  // We ignore tuple finders because the work top-down, and our goal here
  // is to find ways to share data bottom-up.
  if (update->containing_procedure->kind == ProcedureKind::kTupleFinder) {
    return;
  }

  auto &rows = table_sources[table];

  RowProvenance row;
  row.table = table;

  auto i = 0u;
  for (VAR *var : update->col_values) {
    AnalyzeVariable(table, i++, var, row);
  }

  auto row_key = row.Key();
  if (auto [it, added] = seen_rows.emplace(std::move(row_key)); added) {
    if (row.num_appending_vectors) {
      pending_table_sources.emplace_back(std::move(row));
    } else {
      rows.emplace_back(std::move(row));
    }
  }
}

// Analyze a specific table.
void AnalysisContext::AnalyzeTable(TABLE *table, const UpdateList &updates) {
  for (CHANGESTATE *update : updates) {
    AnalyzeTable(table, update);
  }
}

// Convert state transitions and state checks on induction tables into
// state emplacements (for records) and record getters.
void AnalysisContext::ConvertInductionsToRecords(ProgramImpl *impl) {

}

// Analyze all tables.
void AnalysisContext::AnalyzeTables(ProgramImpl *impl) {
  table_updates.clear();
  seen_rows.clear();
  CollectMetadata(impl);
  for (const auto &[table, updates] : table_updates) {
    AnalyzeTable(table, updates);
  }
  AnalyzeVectorAppends();
  UniqueAndGroupRowProvenance();
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
    }
    sep = "_";
  }

  return ss.str();
}

void AnalysisContext::Dump(const ParsedModule &module, Language lang) {
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
  for (const auto &[table, rows] : table_sources) {
    os << "t" << table->id << " [label=<" << kTable << kRow
       << kCell << kBold << "TABLE " << table->id << kEndBold << kEndCell;

    for (TABLECOLUMN *col : table->columns) {
      os << "<TD port=\"c" << col->id << "\">" << col->id << kEndCell;
    }

    os << kEndRow << kEndTable << ">];\n";

    auto r = 0;
    for (const RowProvenance &row : rows) {
      os << "r" << table->id << "_" << r << " [label=<"
         << kTable << kRow;

      if (row.num_joins) {
        os << kCell << "JOINS=" << row.num_joins << kEndCell;
      }
      if (row.num_products) {
        os << kCell << "PRODUCTS=" << row.num_products << kEndCell;
      }
      if (row.num_merges) {
        os << kCell << "MERGES=" << row.num_merges << kEndCell;
      }
      if (row.num_scans) {
        os << kCell << "SCANS=" << row.num_scans << kEndCell;
      }
      if (row.num_globals) {
        os << kCell << "GLOBALS=" << row.num_globals << kEndCell;
      }
      if (row.num_vectors) {
        os << kCell << "VECTORS=" << row.num_vectors << kEndCell;
      }
      if (row.num_generators) {
        os << kCell << "GENERATORS=" << row.num_generators << kEndCell;
      }

      auto i = 0u;
      for (auto &col : *(row.columns)) {
        os << "<TD port=\"c" << i << "\">";
        if (col.src_col) {
          os << "COL " << col.src_col->id << kEndCell;

        } else if (col.src_var) {
          if (col.src_var->IsConstant()) {
            os << "CONST " << col.src_var->id << kEndCell;
          } else if (col.src_var->IsGlobal()) {
            os << "GLOBL " << col.src_var->id << kEndCell;
          } else {
            os << "VAR " << col.src_var->id << kEndCell;
          }
        }
        ++i;
      }
      os << kEndRow << kEndTable << ">];\n";

      // Linke the record columns to the tables that feed the record.
      i = 0;
      for (auto &col : *(row.columns)) {
        if (col.src_col) {
          os << "r" << table->id << "_" << r << ":c" << i
             << " -> " << "t" << col.src_table->id << ":c" << col.src_col->id
             << ";\n";
        }
        ++i;
      }

      i = 0;

      // Link table columns to the record columns that feed the tables.
      for (auto col : table->columns) {
        assert(col->index == i);
        os << "t" << table->id << ":c" << col->id << " -> r"
           << table->id << "_" << r << ":c" << i << ";\n";
        ++i;
      }

      ++r;
    }
  }
  os << "}\n";

  for (auto &[table_, rows] : unique_table_sources) {
    TABLE *table = table_;
    std::cerr << "struct table_" << table->id << ";\n";
  }

  for (auto &[key, row_ptr] : key_to_provenance) {
    std::cerr << "struct record_" << key << ";\n";
  }

  for (auto &[table_, rows] : unique_table_sources) {
    TABLE *table = table_;
    std::cerr
        << "struct table_" << table->id << " {\n"
        << "  union {\n";

    auto r = 0u;
    for (RowProvenance *row : rows) {
      auto key = row->Key();
      std::cerr << "    struct record_" << key << " r" << r << ";\n";
      ++r;
    }

    std::cerr
        << "  } u;\n";
    std::cerr << "};\n\n";
  }


  std::unordered_set<TABLE *> inductions;
  std::unordered_set<TABLEJOIN *> joins;
  std::unordered_set<TABLEPRODUCT *> products;
  std::unordered_set<TABLESCAN *> scans;
  std::unordered_set<VECTORLOOP *> loops;
  std::unordered_set<GENERATOR *> generators;

  for (auto &[key, row_ptr] : key_to_provenance) {
    std::cerr << "struct record_" << key << " {\n";

    inductions.clear();
    joins.clear();
    products.clear();
    scans.clear();
    loops.clear();
    generators.clear();

    auto estimated_tuple_size = 0u;

    const RowProvenance *row = row_ptr;
    for (const ColumnProvenance &col : *row->columns) {
      inductions.insert(col.induction_table);
      joins.insert(col.join);
      products.insert(col.product);
      scans.insert(col.scan);
      loops.insert(col.loop);
      generators.insert(col.generator);

      estimated_tuple_size += col.EstimateSizeInBits(module, lang);
    }

    inductions.erase(nullptr);
    joins.erase(nullptr);
    products.erase(nullptr);
    scans.erase(nullptr);
    loops.erase(nullptr);
    generators.erase(nullptr);

    std::cerr
        << "  // Num inductions: " << inductions.size() << "\n"
        << "  // Num joins: " << joins.size() << "\n"
        << "  // Num products: " << products.size() << "\n"
        << "  // Num scans: " << scans.size() << "\n"
        << "  // Num vector loops: " << loops.size() << "\n"
        << "  // Num generators: " << generators.size() << "\n"
        << "  // Estimated tuple size in bits: " << estimated_tuple_size << "\n"
        << "  // Estimated tuple size in bytes: "
        << ((estimated_tuple_size + 7) / 8) << "\n";

    size_t num_needed_pointers = 0u;
    num_needed_pointers += scans.size();
    num_needed_pointers += inductions.size();

    for (TABLEJOIN *join : joins) {
      for (auto &out_var_list : join->output_vars) {

        // If one table only contributes pivots, then we don't need to
        // store a pointer to its data.
        if (out_var_list.Size() > join->pivot_vars.Size()) {
          ++num_needed_pointers;
        }
      }
    }

    for (TABLEPRODUCT *product : products) {
      num_needed_pointers += product->tables.Size();
    }

    auto estimated_record_size = num_needed_pointers * 64;
    for (const ColumnProvenance &col : *row->columns) {
      if (col.join || col.product || col.induction_table || col.scan) {
        continue;
      } else {
        estimated_record_size += col.EstimateSizeInBits(module, lang);
      }
    }


    std::cerr
        << "  // Estimated record size in bits: "
        << estimated_record_size << "\n"
        << "  // Estimated record size in bytes: "
        << ((estimated_record_size + 7) / 8) << "\n";

    std::cerr << "};\n\n";
  }

}

}  // namespace

// Analyze the control-flow IR and table usage, looking for strategies that
// can be used to eliminate redundancies in the data storage model. We do this
// after optimizing the control-flow IR so that we can observe the effects
// of copy propagation, which gives us the ability to "hop backward" to the
// provenance of some data, as opposed to having to jump one `QueryView` at
// a time.
void ProgramImpl::Analyze(Language lang) {

  AnalysisContext context;

  context.ConvertInductionsToRecords(this);
  context.AnalyzeTables(this);
  context.Dump(query.ParsedModule(), lang);

}

}  // namespace hyde
