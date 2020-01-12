// Copyright 2019, Trail of Bits. All rights reserved.

#include <drlojekyll/Rel/Format.h>

#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Lex/Format.h>
#include <drlojekyll/Parse/Format.h>

namespace hyde {
namespace {

static const char *kBeginTable = "<TABLE cellpadding=\"0\" cellspacing=\"0\" border=\"1\"><TR>";
static const char *kEndTable = "</TR></TABLE>";

}  // namespace

OutputStream &operator<<(OutputStream &os, Query query) {
  os << "digraph {\n"
     << "node [shape=none margin=0 nojustify=false labeljust=l];\n";

  for (auto relation : query.Relations()) {
    const auto decl = relation.Declaration();
    const auto arity = decl.Arity();
    os << "t" << relation.UniqueId() << " [ label=<" << kBeginTable
       << "<TD>RELATION</TD><TD>";

    if (relation.IsNegative()) {
      os << "!";
    }

    os << ParsedDeclarationName(decl) << "</TD>";
    for (auto i = 0u; i < arity; ++i) {
      auto param = decl.NthParameter(i);
      os << "<TD port=\"p" << i << "\">" << param.Name() << "</TD>";
    }

    os << kEndTable << ">];\n";
  }

  for (auto input : query.Inputs()) {
    const auto decl = input.Declaration();
    const auto arity = decl.Arity();
    os << "t" << input.UniqueId() << " [ label=<" << kBeginTable
       << "<TD>INPUT</TD><TD>" << ParsedDeclarationName(decl) << "</TD>";
    for (auto i = 0u; i < arity; ++i) {
      auto param = decl.NthParameter(i);
      os << "<TD port=\"p" << i << "\">" << param.Name() << "</TD>";
    }
    os << kEndTable << ">];\n";
  }

  for (auto generator : query.Generators()) {
    const auto decl = generator.Declaration();
    const auto arity = decl.Arity();
    os << "t" << generator.UniqueId() << " [ label=<" << kBeginTable
       << "<TD>GENERATOR</TD><TD>" << ParsedDeclarationName(decl) << "</TD>";
    for (auto i = 0u; i < arity; ++i) {
      auto param = decl.NthParameter(i);
      os << "<TD port=\"p" << i << "\">" << param.Name() << "</TD>";
    }
    os << kEndTable << ">];\n";
  }

  for (auto constant : query.Constants()) {
    os << "t" << constant.UniqueId() << " [ label=<" << kBeginTable
       << "<TD port=\"p0\">" << constant.Literal() << "</TD>" << kEndTable
       << ">];\n";
  }

  for (auto select : query.Selects()) {
    os << "v" << select.UniqueId() << " [ label=<" << kBeginTable;
    if (select.IsRelation()) {
      os << "<TD>SELECT</TD>";
    } else {
      os << "<TD>PULL</TD>";  // Pull from a stream.
    }
    auto i = 0u;
    for (auto col : select.Columns()) {
      os << "<TD port=\"c" << col.UniqueId() << "\">"
         << col.Variable() << "</TD>";
      ++i;
    }

    os << kEndTable << ">];\n";

    // Link the joined columns to their sources.
    i = 0u;
    uint64_t target_id = 0;

    if (select.IsRelation()) {
      target_id = select.Relation().UniqueId();

    } else if (select.IsStream()){
      target_id = select.Stream().UniqueId();
    } else {
      assert(false);
    }

    for (auto col : select.Columns()) {
      os << "v" << select.UniqueId() << ":c" << col.UniqueId() << " -> t"
         << target_id << ":p" << i << ";\n";
      ++i;
    }
  }

  for (auto constraint : query.Constraints()) {
    os << "v" << constraint.UniqueId() << " [ label=<" << kBeginTable
       << "<TD rowspan=\"2\">FILTER ";
    switch (constraint.Operator()) {
      case ComparisonOperator::kEqual:
        os << "eq";
        break;
      case ComparisonOperator::kGreaterThan:
        os << "gt";
        break;
      case ComparisonOperator::kLessThan:
        os << "lt";
        break;
      case ComparisonOperator::kNotEqual:
        os << "neq";
        break;
    }
    const auto lhs = constraint.LHS();
    const auto rhs = constraint.RHS();
    const auto input_lhs = constraint.InputLHS();
    const auto input_rhs = constraint.InputRHS();
    const auto input_lhs_view = QueryView::Containing(input_lhs);
    const auto input_rhs_view = QueryView::Containing(input_rhs);
    os << "</TD><TD port=\"c" << lhs.UniqueId() << "\">" << lhs.Variable()
       << "</TD><TD port=\"c" << rhs.UniqueId() << "\">" << rhs.Variable()
       << "</TD></TR><TR><TD port=\"p0\"> </TD><TD port=\"p1\"> </TD>"
       << kEndTable << ">];\n"
       << "v" << constraint.UniqueId() << ":p0 -> v"
       << input_lhs_view.UniqueId() << ":c" << input_lhs.UniqueId() << ";\n"
       << "v" << constraint.UniqueId() << ":p1 -> v"
       << input_rhs_view.UniqueId() << ":c" << input_rhs.UniqueId() << ";\n";
  }

  const char *kColors[] = {
      "antiquewhite",
      "aquamarine",
      "cadetblue1",
      "chartreuse1",
      "chocolate1",
      "darkslategrey",
      "deepskyblue2",
      "goldenrod1",
  };

  for (auto join : query.Joins()) {
    os << "v" << join.UniqueId() << " [ label=<" << kBeginTable;

    auto i = 0u;
    for (auto pivot_col : join.PivotColumns()) {
      auto color = kColors[i];
      os << "<TD rowspan=\"3\" bgcolor=\"" << color << "\">"
         << pivot_col.Variable() << "</TD>";
      ++i;
    }

    os << "<TD rowspan=\"2\">JOIN</TD>";

    auto found = false;
    for (auto col : join.Columns()) {
      for (auto j = 0u; j < join.NumPivotColumns(); ++j) {
        auto pivot_col = join.NthPivotColumn(j);
        if (pivot_col.EquivalenceClass() == col.EquivalenceClass()) {
          os << "<TD bgcolor=\"" << kColors[j]
             << "\" port=\"c" << col.UniqueId() << "\">"
             << col.Variable() << "</TD>";
          found = true;
          goto handle_next;
        }
      }
      os << "<TD port=\"c" << col.UniqueId() << "\">"
         << col.Variable() << "</TD>";
    handle_next:
      continue;
    }

    assert(found);

    os << "</TR><TR>";
    for (i = 0u; i < join.NumInputColumns(); ++i) {
      os << "<TD port=\"p" << i << "\"> &nbsp; </TD>";
    }

    os << kEndTable << ">];\n";

    // Link the joined columns to their sources.
    for (i = 0u; i < join.NumInputColumns(); ++i) {
      auto col = join.NthInputColumn(i);
      auto view = QueryView::Containing(col);
      os << "v" << join.UniqueId() << ":p" << i << " -> v"
         << view.UniqueId() << ":c" << col.UniqueId() << ";\n";
    }
  }

  for (auto map : query.Maps()) {
    os << "v" << map.UniqueId() << " [ label=<" << kBeginTable;
    os << "<TD rowspan=\"2\">MAP " << ParsedDeclarationName(map.Functor()) << "</TD>";
    for (auto col : map.Columns()) {
      os << "<TD port=\"c" << col.UniqueId() << "\">"
         << col.Variable() << "</TD>";
    }

    os << "</TR><TR>";
    for (auto i = 0u; i < map.NumInputColumns(); ++i) {
      os << "<TD port=\"p" << i << "\"> &nbsp; </TD>";
    }

    // Empty space.
    if (auto diff = (map.Arity() - map.NumInputColumns())) {
      os << "<TD colspan=\"" << diff << "\"></TD>";
    }

    os << kEndTable << ">];\n";

    // Link the input columns to their sources.
    for (auto i = 0u; i < map.NumInputColumns(); ++i) {
      auto col = map.NthInputColumn(i);
      auto view = QueryView::Containing(col);
      os << "v" << map.UniqueId() << ":p" << i << " -> v"
         << view.UniqueId() << ":c" << col.UniqueId() << ";\n";
    }
  }

  for (auto agg : query.Aggregates()) {
    os << "v" << agg.UniqueId() << " [ label=<" << kBeginTable;
    os << "<TD rowspan=\"3\">AGGREGATE "
       << ParsedDeclarationName(agg.Functor()) << "</TD>";
    for (auto col : agg.Columns()) {
      os << "<TD port=\"c" << col.UniqueId() << "\">"
         << col.Variable() << "</TD>";
    }
    os << "</TR><TR>";
    auto num_group = agg.NumGroupColumns();
    if (num_group) {
      os << "<TD colspan=\"" << num_group << "\">GROUP</TD>";
    }
    auto num_config = agg.NumConfigColumns();
    if (num_config) {
      os << "<TD colspan=\"" << num_config << "\">CONFIG</TD>";
    }
    auto num_summ = agg.NumSummarizedColumns();
    if (num_summ) {

      os << "<TD colspan=\"" << num_summ << "\">SUMMARIZE</TD>";
    }
    os << "</TR><TR>";
    for (auto i = 0u; i < num_group; ++i) {
      auto col = agg.NthGroupColumn(i);
      os << "<TD port=\"g1_" << i << "\">" << col.Variable() << "</TD>";
    }
    for (auto i = 0u; i < num_config; ++i) {
      auto col = agg.NthConfigColumn(i);
      os << "<TD port=\"g2_" << i << "\">" << col.Variable() << "</TD>";
    }
    for (auto i = 0u; i < num_summ; ++i) {
      auto col = agg.NthSummarizedColumn(i);
      os << "<TD port=\"s" << i << "\">" << col.Variable() << "</TD>";
    }
    os << kEndTable << ">];\n";
    for (auto i = 0u; i < num_group; ++i) {
      auto col = agg.NthGroupColumn(i);
      auto view = QueryView::Containing(col);
      os << "v" << agg.UniqueId() << ":g1_" << i << " -> v"
         << view.UniqueId() << ":c" << col.UniqueId() << ";\n";
    }
    for (auto i = 0u; i < num_config; ++i) {
      auto col = agg.NthConfigColumn(i);
      auto view = QueryView::Containing(col);
      os << "v" << agg.UniqueId() << ":g2_" << i << " -> v"
         << view.UniqueId() << ":c" << col.UniqueId() << ";\n";
    }
    for (auto i = 0u; i < num_summ; ++i) {
      auto col = agg.NthSummarizedColumn(i);
      auto view = QueryView::Containing(col);
      os << "v" << agg.UniqueId() << ":s" << i << " -> v"
         << view.UniqueId() << ":c" << col.UniqueId() << ";\n";
    }
  }

  for (auto insert : query.Inserts()) {
    os << "i" << insert.UniqueId() << " [ label=<" << kBeginTable
       << "<TD>INSERT "
       << ParsedDeclarationName(insert.Relation().Declaration()) << "</TD>";

    for (auto i = 0u, max_i = insert.Arity(); i < max_i; ++i) {
      os << "<TD port=\"c" << i << "\"> &nbsp; </TD>";
    }

    os << kEndTable << ">];\n";

    for (auto i = 0u, max_i = insert.Arity(); i < max_i; ++i) {
      const auto col = insert.NthColumn(i);
      const auto view = QueryView::Containing(col);
      os << "i" << insert.UniqueId() << ":c" << i << " -> "
         << "v" << view.UniqueId() << ":c" << col.UniqueId() << ";\n";
    }
  }

  os << "}\n";
  return os;
}

}  // namespace hyde
