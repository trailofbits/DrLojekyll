// Copyright 2019, Trail of Bits, Inc. All rights reserved.

#include <cstdlib>
#include <cstring>
#include <cassert>
#include <iostream>
#include <sstream>
#include <string>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <vector>
#include <cassert>

#include <drlojekyll/Display/DisplayConfiguration.h>
#include <drlojekyll/Display/DisplayManager.h>
#include <drlojekyll/Parse/ErrorLog.h>
#include <drlojekyll/Parse/Parser.h>
#include <drlojekyll/Parse/Format.h>
#include <drlojekyll/Sema/BottomUpAnalysis.h>
#include <drlojekyll/Sema/SIPSAnalysis.h>
#include <drlojekyll/Transforms/CombineModules.h>

namespace hyde {

#if 0
class PseudoCodePrinter final : public SIPSVisitor {
 public:
  PseudoCodePrinter(DisplayManager display_manager_)
      : display_manager(display_manager_),
        os(display_manager_, ss) {}

  virtual ~PseudoCodePrinter(void) = default;

  void Begin(ParsedPredicate assumption) override {
    std::string_view clause_data;
    auto has_new_line = false;
    auto clause = ParsedClause::Containing(assumption);

    // Print out the clause.
    if (display_manager.TryReadData(clause.SpellingRange(), &clause_data)) {
      os << "// ";
      for (auto ch : clause_data) {
        if (has_new_line) {
          os << "\n// ";
          has_new_line = false;
        }
        if (ch == '\n') {
          has_new_line = true;
        } else {
          os << ch;
        }
      }
    }

    os << "\nfor each " << assumption << ":\n";
    indent += "  ";
  }

  void DeclareParameter(const Column &col) override {
    os << indent << "v" << col.id << " = " << col.var.Name()
       << "  // " << col.param.Type() << '\n';
  }

  void DeclareVariable(ParsedVariable var, unsigned id) override {
    os << indent << "v" << id << "  // " << var.Name() << '\n';
  }

  void DeclareConstant(ParsedLiteral val, unsigned id) override {
    os << indent << "v" << id << " = " << val.SpellingRange() << '\n';
  }

  void AssertEqual(unsigned lhs_id, unsigned rhs_id) override {
    os << indent << "if v" << lhs_id << " != " << rhs_id << ":\n"
       << indent << "  continue\n";
  }

  void AssertNotEqual(unsigned lhs_id, unsigned rhs_id) override {
    os << indent << "if v" << lhs_id << " == " << rhs_id << ":\n"
       << indent << "  continue\n";
  }

  void AssertLessThan(unsigned lhs_id, unsigned rhs_id) override {
    os << indent << "if v" << lhs_id << " >= " << rhs_id << ":\n"
       << indent << "  continue\n";
  }

  void AssertGreaterThan(unsigned lhs_id, unsigned rhs_id) override {
    os << indent << "if v" << lhs_id << " <= " << rhs_id << ":\n"
       << indent << "  continue\n";
  }

  void AssertPresent(ParsedPredicate pred, const Column *begin,
                     const Column *end) override {
    os << indent << "if not exists " << ParsedDeclaration::Of(pred).Name();
    auto comma = "(";
    for (auto col = begin; col < end; ++col) {
      os << comma << 'v' << col->id;
      comma = ", ";
    }
    os << "):\n"
       << indent << "  continue\n";
  }

  void AssertAbsent(ParsedPredicate pred, const Column *begin,
                    const Column *end) override {
    os << indent << "if exists " << ParsedDeclaration::Of(pred).Name();
    auto comma = "(";
    for (auto col = begin; col < end; ++col) {
      os << comma << 'v' << col->id;
      comma = ", ";
    }
    os << "):\n"
       << indent << "  continue\n";
  }

  void Insert(ParsedDeclaration decl, const Column *begin,
              const Column *end) override {
    os << indent << "publish " << decl.Name();
    auto comma = "(";
    for (auto col = begin; col < end; ++col) {
      os << comma << col->param.Name() << "=v" << col->id;
      comma = ", ";
    }
    os << ")\n";
  }

  void EnterFromWhereSelect(ParsedPredicate pred,
                            ParsedDeclaration from,
                            const Column *where_begin,
                            const Column *where_end,
                            const Column *select_begin,
                            const Column *select_end) override {
    os << indent << "for each ";
    auto comma = "(";
    for (auto col = select_begin; col < select_end; ++col) {
      os << comma << 'v' << col->id << "=" << col->param.Name();
      comma = ", ";
    }
    os << ") in " << from.Name();
    comma = "(";
    for (auto col = where_begin; col < where_end; ++col) {
      os << comma << col->param.Name() << "=v" << col->id;
      comma = ", ";
    }
    os << "):\n";
    indent += "  ";
  }

  void EnterFromSelect(ParsedPredicate pred,
                       ParsedDeclaration from,
                       const Column *select_begin,
                       const Column *select_end) override {
    os << indent << "for each ";
    auto comma = "(";
    for (auto col = select_begin; col < select_end; ++col) {
      os << comma << 'v' << col->id << "=" << col->param.Name();
      comma = ", ";
    }
    os << ") in ";
    if (from.IsFunctor() && ParsedFunctor::From(from).IsAggregate()) {
      os << "A" << pred.UniqueId();
    } else {
      os << from.Name();
    }
    os << ":\n";
    indent += "  ";
  }

  void ExitSelect(ParsedPredicate pred, ParsedDeclaration from) override {
    indent.pop_back();
    indent.pop_back();
  }

  void EnterAggregation(ParsedPredicate functor,
                        ParsedDeclaration decl,
                        const Column *bound_begin,
                        const Column *bound_end) override {
    os << indent << "A" << functor.UniqueId()
       << " = aggregator " << decl.Name() << "(";
    auto comma = "(";
    for (auto col = bound_begin; col < bound_end; ++col) {
      os << comma << col->param.Name() << " = v" << col->id;
      comma = ", ";
    }
    os << ")\n";
    indent += "  ";
  }
  void Collect(ParsedPredicate agg_pred, ParsedDeclaration agg_decl,
               const Column *begin, const Column *end) override {
    os << indent << "add " << agg_decl.Name();
    auto comma = "(";
    for (auto col = begin; col < end; ++col) {
      os << comma << col->param.Name() << "=v" << col->id;
      comma = ", ";
    }
    os << ") into A" << agg_pred.UniqueId() << "\n";
  }

  void Summarize(ParsedPredicate functor, ParsedDeclaration decl) override {
    indent.pop_back();
    indent.pop_back();
    os << indent << "summarize A" << functor.UniqueId() << '\n';
  }

  void Assign(unsigned dest_id, unsigned rhs_id) override {
    os << indent << "v" << dest_id << " = v" << rhs_id << '\n';
  }

  void Commit(void) override {
    accepted = true;
  }

  bool Advance(void) override {
    return !accepted;
  }

  const DisplayManager display_manager;
  std::stringstream ss;
  OutputStream os;
  std::string indent;
  bool accepted{false};
};
#endif

#if 0
class GraphPrinter : public BottomUpVisitor {
 public:
  GraphPrinter(DisplayManager &display_manager_)
      : display_manager(display_manager_),
        os(display_manager, std::cerr) {}

  virtual ~GraphPrinter(void) = default;

  bool VisitState(const State *state) {
    auto to_pred = state->assumption;
    if (!seen.count(to_pred)) {
      os << "p" << to_pred.UniqueId() << " [label=\""
         << to_pred << "\"];\n";
    }

    if (state->parent) {
      auto from_pred = state->parent->assumption;
      if (!seen.count(from_pred)) {
        os << "p" << from_pred.UniqueId() << " [label=\""
           << from_pred << "\"];\n";
      }

      auto &edges = seen[from_pred];
      if (edges.count(to_pred)) {
        return false;
      }

      os << "p" << from_pred.UniqueId() << " -> p" << to_pred.UniqueId() << ";\n";

      edges.insert(to_pred);
    }

    return true;
  }

  DisplayManager display_manager;
  OutputStream os;
  std::unordered_map<ParsedPredicate, std::unordered_set<ParsedPredicate>> seen;
};
#endif

// Represents a query.
class Query {
 public:
  explicit Query(ParsedDeclaration output_table_) {
    output_table = TableFor(output_table_);
  }

  class Table {
   public:
    Table(ParsedDeclaration decl_)
        : decl(decl_) {
      for (auto param : decl.Parameters()) {
        columns.push_back(param);
      }
    }
    ParsedDeclaration decl;
    std::vector<ParsedParameter> columns;
  };

  class View;

  class Column {
   public:
    Column(const View *view_, ParsedVariable var_, unsigned index_)
        : parent(this),
          view(view_),
          var(var_),
          index(index_) {}

    Column *Self(void) {
      if (parent != this) {
        parent = parent->Self();
      }
      return parent;
    }

    static Column *Union(Column *lhs, Column *rhs) {
      lhs = lhs->Self();
      rhs = rhs->Self();
      if (lhs == rhs) {
        return lhs;
      }

      if (lhs->var.Order() < rhs->var.Order()) {
        rhs->parent = lhs;
        return lhs;

      } else {
        lhs->parent = rhs;
        return rhs;
      }
    }

    Column *parent;
    const View * const view;
    const ParsedVariable var;
    const unsigned index;
  };

  class View {
   public:
    std::vector<std::unique_ptr<Column>> columns;
  };

  class Select : public View {
   public:
    explicit Select(const Table *table_)
        : table(table_) {}

    const Table * const table;
  };

  class Join : public View {
   public:
    std::vector<std::pair<Column *, Column *>> equalities;
  };

  struct Condition {
   public:
    ComparisonOperator conditional_operator;
    Column *lhs;
    Column *rhs;
  };

  const Table *TableFor(ParsedDeclaration decl) {
    auto &table = tables[decl];
    if (!table) {
      table.reset(new Table(decl));
    }
    return table.get();
  }

  const Table *TableFor(ParsedPredicate pred) {
    return TableFor(ParsedDeclaration::Of(pred));
  }

  const Select *SelectFor(ParsedPredicate pred) {
    auto table = TableFor(pred);
    auto select = new Select(table);
    selects.emplace_back(select);
    unsigned i = 0;
    for (auto arg : pred.Arguments()) {
      auto col = new Query::Column(select, arg, i++);
      select->columns.emplace_back(col);
    }
    return select;
  }

  void Dump(OutputStream &os) const {
    os << "digraph {\n"
       << "node [shape=plaintext];\n";

    unsigned i = 0;
    for (auto &out_col : output_columns) {
      auto out_id = i++;
      os << "o" << out_id << " [label=\"" << out_col.second.Name() << "\"];\n"
         << "o" << out_id << " -> v"
         << reinterpret_cast<uintptr_t>(out_col.first->view) << ":c"
         << reinterpret_cast<uintptr_t>(out_col.first) << ";\n";
    }

    auto table_style = "cellpadding=\"0\" cellspacing=\"0\" border=\"1\"";

    for (const auto &view : joins) {
      os << "v" << reinterpret_cast<uintptr_t>(view.get())
         << " [label=<<TABLE " << table_style << "><TR><TD colspan=\""
         << (view->columns.size() * 2) << "\">JOIN</TD></TR><TR>";

      // One cell per column.
      for (const auto &col : view->columns) {
        os << "<TD port=\"c" << reinterpret_cast<uintptr_t>(col.get())
           << "\" colspan=\"2\">" << col->var.Name() << "</TD>";
      }

      os << "</TR><TR>";

      i = 0;
      for (auto &eq : view->equalities) {
        const auto &col = view->columns[i++];
        auto lhs_col = eq.first;
        auto rhs_col = eq.second;
        os << "<TD port=\"j0_" << reinterpret_cast<uintptr_t>(col.get())
           << "\">" << lhs_col->var.Name() << "</TD>";
        os << "<TD port=\"j1_" << reinterpret_cast<uintptr_t>(col.get())
           << "\">" << rhs_col->var.Name() << "</TD>";
      }

      os << "</TR></TABLE>>];\n";

      i = 0;
      for (auto &eq : view->equalities) {
        const auto &col = view->columns[i++];
        auto lhs_col = eq.first;
        auto rhs_col = eq.second;

        os << "v" << reinterpret_cast<uintptr_t>(view.get()) << ":j0_"
           << reinterpret_cast<uintptr_t>(col.get()) << " -> v"
           << reinterpret_cast<uintptr_t>(lhs_col->view) << ":c"
           << reinterpret_cast<uintptr_t>(lhs_col->view->columns[lhs_col->index].get())
           << ";\n";

        os << "v" << reinterpret_cast<uintptr_t>(view.get()) << ":j1_"
           << reinterpret_cast<uintptr_t>(col.get()) << " -> v"
           << reinterpret_cast<uintptr_t>(rhs_col->view) << ":c"
           << reinterpret_cast<uintptr_t>(rhs_col->view->columns[rhs_col->index].get())
           << ";\n";
      }
    }

    for (const auto &view : selects) {
      os << "v" << reinterpret_cast<uintptr_t>(view.get())
         << " [label=<<TABLE " << table_style << "><TR><TD colspan=\""
         << view->columns.size() << "\">SELECT</TD></TR><TR>";

      // One cell per column.
      for (const auto &col : view->columns) {
        os << "<TD port=\"c" << reinterpret_cast<uintptr_t>(col.get())
           << "\">" << col->var.Name() << "</TD>";
      }

      os << "</TR></TABLE>>];\n";

      // Arrows between selects and tables.
      os << "v" << reinterpret_cast<uintptr_t>(view.get())
         << " -> t" << reinterpret_cast<uintptr_t>(view->table) << ";\n";
    }

    // Tables.
    for (const auto &table : tables) {
      os << "t" << reinterpret_cast<uintptr_t>(table.second.get())
         << " [label=<<TABLE " << table_style << "><TR><TD colspan=\""
         << table.second->columns.size() << "\">" << table.first.Name()
         << "</TD></TR><TR>";
      for (auto col : table.second->columns) {
        os << "<TD>" << col.Name() << "</TD>";
      }
      os << "</TR></TABLE>>];\n";
    }

    os << "}\n";
  }

  using Relation = std::tuple<Query::Column *, Query::Column *, ParsedVariable>;

  const Join *JoinFor(const std::vector<Relation> &eqs) {
    auto join = new Join;
    joins.emplace_back(join);
    unsigned i = 0;
    for (const auto &eq : eqs) {
      auto col = new Query::Column(join, std::get<2>(eq), i++);
      join->columns.emplace_back(col);
      auto lhs = std::get<0>(eq);
      auto rhs = std::get<1>(eq);
      join->equalities.emplace_back(lhs, rhs);
    }
    return join;
  }

  const Table *output_table;
  std::unordered_map<ParsedDeclaration, std::unique_ptr<Table>> tables;
  std::vector<std::unique_ptr<Select>> selects;
  std::vector<std::unique_ptr<Join>> joins;
  std::vector<Condition> conditions;
  std::vector<std::pair<Column *, ParsedParameter>> output_columns;
};

class QueryBuilder final : public SIPSVisitor {
 public:
  virtual ~QueryBuilder(void) = default;

  QueryBuilder(DisplayManager display_manager_)
      : display_manager(display_manager_) {}

  void Begin(ParsedPredicate pred) override {
    id_to_col.clear();
    query.reset(new Query(
        ParsedDeclaration::Of(ParsedClause::Containing(pred))));
    initial_view = query->SelectFor(pred);
  }

  void DeclareParameter(const Column &col) override {
    id_to_col[col.id] = initial_view->columns[col.n].get();
  }

  void AssertEqual(unsigned lhs_id, unsigned rhs_id) override {
    auto &lhs_col = id_to_col[lhs_id];
    auto &rhs_col = id_to_col[rhs_id];

    if (lhs_col && rhs_col) {
      query->conditions.push_back(
          {ComparisonOperator::kEqual,
           lhs_col->Self(), rhs_col->Self()});

      auto ret = Query::Column::Union(lhs_col, rhs_col);
      lhs_col = ret;
      rhs_col = ret;

    } else if (lhs_col) {
      rhs_col = lhs_col;

    } else if (rhs_col) {
      lhs_col = rhs_col;
    }
  }

  void AssertInequality(ComparisonOperator op,
                        unsigned lhs_id, unsigned rhs_id) {
    auto lhs_col = id_to_col[lhs_id];
    auto rhs_col = id_to_col[rhs_id];
    if (lhs_col && rhs_col) {
      query->conditions.push_back({op, lhs_col->Self(), rhs_col->Self()});
    }
  }

  void AssertNotEqual(unsigned lhs_id, unsigned rhs_id) override {
    AssertInequality(ComparisonOperator::kNotEqual, lhs_id, rhs_id);
  }

  void AssertLessThan(unsigned lhs_id, unsigned rhs_id) override {
    AssertInequality(ComparisonOperator::kLessThan, lhs_id, rhs_id);
  }

  void AssertGreaterThan(unsigned lhs_id, unsigned rhs_id) override {
    AssertInequality(ComparisonOperator::kGreaterThan, lhs_id, rhs_id);
  }

  virtual void EnterFromSelect(
      ParsedPredicate pred, ParsedDeclaration from,
      const Column *select_begin, const Column *select_end) override {
    auto select = query->SelectFor(pred);
    for (auto col = select_begin; col < select_end; ++col) {
      auto &prev_col = id_to_col[col->id];
      assert(!prev_col);
      prev_col = select->columns[col->n].get();
    }
  }

  void AssertPresent(
      ParsedPredicate pred, const Column *begin,
      const Column *end) override {
    EnterFromWhereSelect(pred, ParsedDeclaration::Of(pred),
                         begin, end, nullptr, nullptr);
  }

  void EnterFromWhereSelect(
      ParsedPredicate pred, ParsedDeclaration,
      const Column *where_begin, const Column *where_end,
      const Column *select_begin, const Column *select_end) override {
    auto select = query->SelectFor(pred);

    std::unordered_map<const Query::View *, std::vector<Query::Relation>>
        grouped_relations;

    for (auto col = where_begin; col < where_end; ++col) {
      if (auto prev_col = id_to_col[col->id]) {
        prev_col = prev_col->Self();
        auto new_col = select->columns[col->n].get();
        auto view_col = Query::Column::Union(prev_col, new_col);
        grouped_relations[view_col->view].emplace_back(
            prev_col, new_col, col->var);
      }
    }

    std::unordered_map<const Query::View *, std::vector<Query::Column *>>
        joined_cols;

    for (auto &rel_group : grouped_relations) {
      auto join = query->JoinFor(rel_group.second);
      auto &join_cols = joined_cols[rel_group.first];
      for (const auto &col : join->columns) {
        join_cols.push_back(col.get());
      }
      std::reverse(join_cols.begin(), join_cols.end());
    }

    for (auto col = where_begin; col < where_end; ++col) {
      auto next_col = select->columns[col->n]->Self();
      if (auto prev_col = id_to_col[col->id]) {
        prev_col = prev_col->Self();
        auto &join_cols = joined_cols[prev_col->view];
        next_col = join_cols.back();
        join_cols.pop_back();

        next_col = Query::Column::Union(prev_col, next_col);
      }
      id_to_col[col->id] = next_col;
    }

    for (auto col = select_begin; col < select_end; ++col) {
      auto &prev_col = id_to_col[col->id];
      assert(!prev_col);
      prev_col = select->columns[col->n].get();
    }
  }

  void Insert(
      ParsedDeclaration, const Column *begin,
      const Column *end) override {
    for (auto col = begin; col < end; ++col) {
      if (auto output_col = id_to_col[col->id]) {
        query->output_columns.emplace_back(output_col, col->param);
      }
    }
  }

  void Commit(ParsedPredicate assumption) override {
    OutputStream os(display_manager, std::cerr);
    os << "// " << assumption << "\n// "
       << ParsedClause::Containing(assumption) << "\n";
    query->Dump(os);
    os << "\n\n";
  }

 private:
  DisplayManager display_manager;
  std::unique_ptr<Query> query;
  const Query::View *initial_view{nullptr};
  std::unordered_map<unsigned, Query::Column *> id_to_col;
};


class BottomUpSIPSVisitor : public BottomUpVisitor {
 public:
  explicit BottomUpSIPSVisitor(SIPSVisitor &sips_visitor_)
      : sips_visitor(sips_visitor_) {}

  bool VisitState(const State *state) {
    if (seen.count(state->assumption)) {
      return false;
    }

    seen.insert(state->assumption);

    SIPSGenerator gen(state->assumption);
    do {
      (void) gen.Visit(sips_visitor);
    } while (gen.Advance());

    return true;
  }

 private:
  std::unordered_set<ParsedPredicate> seen;
  SIPSVisitor &sips_visitor;
};

static void CodeDumper(DisplayManager display_manager,
                       ParsedModule module) {

  BottomUpAnalysis analysis;
  QueryBuilder join_tracker(display_manager);
  BottomUpSIPSVisitor visitor(join_tracker);
  for (analysis.Start(module); analysis.Step(visitor); ) {}

//  GraphPrinter printer(display_manager);
//  std::cerr << "digraph {\n";
//  for (analysis.Start(module); analysis.Step(printer); ) {
//    // ...
//  }
//  std::cerr << "}\n";
}


}  // namespace hyde



static int ProcessModule(hyde::DisplayManager display_manager,
                         hyde::ErrorLog error_log,
                         hyde::ParsedModule module) {

  if (!error_log.IsEmpty()) {
    error_log.Render(std::cerr);
    return EXIT_FAILURE;
  } else {
    module = CombineModules(display_manager, module);

    std::stringstream ss;
    do {
      hyde::OutputStream os(display_manager, ss);
      os << module;
    } while (false);

    hyde::Parser parser(display_manager, error_log);
    auto module2 = parser.ParseStream(ss, hyde::DisplayConfiguration());
    if (!error_log.IsEmpty()) {
      error_log.Render(std::cerr);
      assert(error_log.IsEmpty());
      return EXIT_FAILURE;
    }

    std::stringstream ss2;
    do {
      hyde::OutputStream os(display_manager, ss2);
      os << module2;
    } while (false);

    std::cerr << ss.str() << "\n\n" << ss2.str() << "\n\n";
    assert(ss.str() == ss2.str());

    CodeDumper(display_manager, module);
    //Simulate(os, module);
    return EXIT_SUCCESS;
  }
}

int main(int argc, char *argv[]) {
  hyde::DisplayManager display_manager;
  hyde::ErrorLog error_log;
  hyde::Parser parser(display_manager, error_log);

  std::string input_path;
  std::string output_path;
  auto num_input_paths = 0;

  std::stringstream linked_module;

  // Parse the command-line arguments.
  for (auto i = 1; i < argc; ++i) {

    // Output file of compiled datalog.
    if (!strcmp(argv[i], "-o")) {
      ++i;
      if (i >= argc) {
        hyde::Error err;
        err << "Command-line argument '-o' must be followed by a file path";
        error_log.Append(std::move(err));
      } else {
        output_path = argv[i];
      }

    // Input file search path.
    } else if (!strcmp(argv[i], "-I")) {
      ++i;
      if (i >= argc) {
        hyde::Error err;
        err << "Command-line argument '-I' must be followed by a directory path";
        error_log.Append(std::move(err));

      } else {
        parser.AddSearchPath(argv[i]);
      }

    // Input datalog file, add it to the list of paths to parse.
    } else {
      linked_module << "#import \"" << argv[i] << "\"\n";
      input_path = argv[i];
      ++num_input_paths;
    }
  }

  if (!num_input_paths) {
    hyde::Error err;
    err << "No input files to parse";
    error_log.Append(std::move(err));

  // Parse a single module.
  } else if (1 == num_input_paths) {
    hyde::DisplayConfiguration config = {
        input_path,  // `name`.
        2,  // `num_spaces_in_tab`.
        true  // `use_tab_stops`.
    };
    auto module = parser.ParsePath(input_path, config);
    return ProcessModule(display_manager, error_log, module);

  // Parse multiple modules as a single module including each module to
  // be parsed.
  } else {
    hyde::DisplayConfiguration config = {
        "<amalgamation>",  // `name`.
        2,  // `num_spaces_in_tab`.
        true  // `use_tab_stops`.
    };

    auto module = parser.ParseStream(linked_module, config);
    return ProcessModule(display_manager, error_log, module);
  }
}
