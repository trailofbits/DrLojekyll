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
       << "<TD>RELATION ";

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
    os << "t" << input.UniqueId() << " [ label=<" << kBeginTable << "<TD>";
    if (decl.IsMessage()) {
      os << "RECV ";
    } else if (decl.IsQuery()) {
      os << "QUERY ";
    }
    os << ParsedDeclarationName(decl) << "</TD>";
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
       << "<TD>GENERATOR " << ParsedDeclarationName(decl) << "</TD>";
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
      os << "<TD>PUSH</TD>";
    } else if (select.IsStream()) {
      auto stream = select.Stream();
      if (stream.IsConstant()) {
        os << "<TD>PULL</TD>";  // Pull from a stream.
      } else if (stream.IsGenerator()) {
        os << "<TD>PULL</TD>";  // Pull from a stream.
      } else {
        os << "<TD>PUSH</TD>";  // Pull from a stream.
      }
    } else {
      assert(false);
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
    os << "v" << constraint.UniqueId() << " [ label=<" << kBeginTable;
    const auto out_copied_cols = constraint.CopiedColumns();
    const auto in_copied_cols = constraint.InputCopiedColumns();
    if (!out_copied_cols.empty()) {
      os << "<TD rowspan=\"2\">COPY</TD>";
      for (auto col : out_copied_cols) {
        os << "<TD port=\"c" << col.UniqueId() << "\">" << col.Variable()
           << "</TD>";
      }
    }
    os << "<TD rowspan=\"2\">FILTER ";
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

    if (lhs == rhs) {
      os << "</TD><TD port=\"c" << lhs.UniqueId() << "\" colspan=\"2\">"
         << lhs.Variable();
    } else {
      os << "</TD><TD port=\"c" << lhs.UniqueId() << "\">" << lhs.Variable()
         << "</TD><TD port=\"c" << rhs.UniqueId() << "\">" << rhs.Variable();
    }

    os << "</TD></TR><TR>";

    if (!in_copied_cols.empty()) {
      auto i = 0u;
      for (auto col : in_copied_cols) {
        os << "<TD port=\"g" << i << "\">" << col.Variable() << "</TD>";
        ++i;
      }
    }

    os << "<TD port=\"p0\"> </TD><TD port=\"p1\"> </TD>";


    os << kEndTable << ">];\n"
       << "v" << constraint.UniqueId() << ":p0 -> v"
       << input_lhs_view.UniqueId() << ":c" << input_lhs.UniqueId() << ";\n"
       << "v" << constraint.UniqueId() << ":p1 -> v"
       << input_rhs_view.UniqueId() << ":c" << input_rhs.UniqueId() << ";\n";

    for (auto i = 0u; i < in_copied_cols.size(); ++i) {
      const auto col = in_copied_cols[i];
      const auto view = QueryView::Containing(col);
      os << "v" << constraint.UniqueId() << ":g" << i << " -> v"
         << view.UniqueId() << ":c" << col.UniqueId() << ";\n";
    }
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

//    auto num_pivot_inputs = 0u;
//    auto num_pivots = join.NumPivotSets();
//    for (auto i = 0u; i < join.NumPivotSets(); ++i) {
//      num_pivot_inputs += join.NthPivotSet(i).size();
//    }
//
//    for (auto i = 0u; i < join.NumPivotSets(); ++i) {
//      auto color = kColors[i];
//      os << "<TD colspan=\"" << join.NthPivotSet(i).size() << "\" bgcolor=\""
//         << color << "\">" << join.NthPivotColumn(i).Variable() << "</TD>";
//    }

    const auto num_pivots = join.NumPivots();
    const auto num_outputs = join.NumOutputColumns();
    auto i = 0u;
    for (; i < num_pivots; ++i) {
      const auto pivot_set_size = join.NthPivotSet(i).size();
      const auto col = join.NthPivotColumn(i);
      const auto color = kColors[i];
      os << "<TD port=\"c" << col.UniqueId() << "\" colspan=\""
         << pivot_set_size << "\" bgcolor=\""
         << color << "\">" << col.Variable() << "</TD>";
    }

    if (num_pivots) {
      os << "<TD rowspan=\"2\">JOIN</TD>";
    } else {
      os << "<TD rowspan=\"2\">PRODUCT</TD>";
    }

    for (i = 0u; i < num_outputs; ++i) {
      const auto col = join.NthOutputColumn(i);
      os << "<TD port=\"c" << col.UniqueId() << "\">"
         << col.Variable() << "</TD>";
    }

    os << "</TR><TR>";

    auto j = 0u;
    for (i = 0u; i < num_pivots; ++i) {
      auto color = kColors[i];
      for (auto col : join.NthPivotSet(i)) {
        os << "<TD bgcolor=\"" << color << "\" port=\"p" << j << "\">"
           << col.Variable() << "</TD>";
        j++;
      }
    }

    for (i = 0u; i < num_outputs; ++i) {
      const auto col = join.NthInputColumn(i);
      os << "<TD port=\"p" << j << "\">" << col.Variable() << "</TD>";
      j++;
    }

    os << kEndTable << ">];\n";

    // Link the joined columns to their sources.

    j = 0u;
    for (i = 0u; i < num_pivots; ++i) {
      for (auto col : join.NthPivotSet(i)) {
        const auto view = QueryView::Containing(col);
        os << "v" << join.UniqueId() << ":p" << j << " -> v"
           << view.UniqueId() << ":c" << col.UniqueId() << ";\n";
        j++;
      }
    }

    for (i = 0u; i < num_outputs; ++i) {
      const auto col = join.NthInputColumn(i);
      const auto view = QueryView::Containing(col);
      os << "v" << join.UniqueId() << ":p" << j << " -> v"
         << view.UniqueId() << ":c" << col.UniqueId() << ";\n";
      j++;
    }
  }

  for (auto map : query.Maps()) {
    os << "v" << map.UniqueId() << " [ label=<" << kBeginTable;

    auto num_group = map.NumCopiedColumns();
    if (num_group) {
      os << "<TD rowspan=\"2\">COPY</TD>";
      for (auto col : map.CopiedColumns()) {
        os << "<TD port=\"c" << col.UniqueId() << "\">"
           << col.Variable() << "</TD>";
      }
    }

    os << "<TD rowspan=\"2\">MAP "
       << ParsedDeclarationName(map.Functor()) << "</TD>";
    for (auto col : map.Columns()) {
      os << "<TD port=\"c" << col.UniqueId() << "\">"
         << col.Variable() << "</TD>";
    }

    const auto num_inputs = map.NumInputColumns();
    if (num_group + num_inputs) {
      os << "</TR><TR>";

      for (auto i = 0u; i < num_group; ++i) {
        const auto col = map.NthInputCopiedColumn(i);
        os << "<TD port=\"g" << i << "\">" << col.Variable() << "</TD>";
      }

      for (auto i = 0u; i < num_inputs; ++i) {
        const auto col = map.NthInputColumn(i);
        os << "<TD port=\"p" << i << "\">" << col.Variable() << "</TD>";
      }
    }

//    // Empty space.
//    if (auto diff = (map.Arity() - map.NumInputColumns())) {
//      os << "<TD colspan=\"" << diff << "\"></TD>";
//    }

    os << kEndTable << ">];\n";

    for (auto i = 0u; i < num_group; ++i) {
      auto col = map.NthInputCopiedColumn(i);
      auto view = QueryView::Containing(col);
      os << "v" << map.UniqueId() << ":g" << i << " -> v"
         << view.UniqueId() << ":c" << col.UniqueId() << ";\n";
    }

    // Link the input columns to their sources.
    for (auto i = 0u; i < num_inputs; ++i) {
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
      auto col = agg.NthInputGroupColumn(i);
      os << "<TD port=\"g1_" << i << "\">" << col.Variable() << "</TD>";
    }
    for (auto i = 0u; i < num_config; ++i) {
      auto col = agg.NthInputConfigColumn(i);
      os << "<TD port=\"g2_" << i << "\">" << col.Variable() << "</TD>";
    }
    for (auto i = 0u; i < num_summ; ++i) {
      auto col = agg.NthInputSummarizedColumn(i);
      os << "<TD port=\"s" << i << "\">" << col.Variable() << "</TD>";
    }
    os << kEndTable << ">];\n";
    for (auto i = 0u; i < num_group; ++i) {
      auto col = agg.NthInputGroupColumn(i);
      auto view = QueryView::Containing(col);
      os << "v" << agg.UniqueId() << ":g1_" << i << " -> v"
         << view.UniqueId() << ":c" << col.UniqueId() << ";\n";
    }
    for (auto i = 0u; i < num_config; ++i) {
      auto col = agg.NthInputConfigColumn(i);
      auto view = QueryView::Containing(col);
      os << "v" << agg.UniqueId() << ":g2_" << i << " -> v"
         << view.UniqueId() << ":c" << col.UniqueId() << ";\n";
    }
    for (auto i = 0u; i < num_summ; ++i) {
      auto col = agg.NthInputSummarizedColumn(i);
      auto view = QueryView::Containing(col);
      os << "v" << agg.UniqueId() << ":s" << i << " -> v"
         << view.UniqueId() << ":c" << col.UniqueId() << ";\n";
    }
  }

  for (auto insert : query.Inserts()) {
    const auto decl = insert.Declaration();
    os << "v" << insert.UniqueId() << " [ label=<" << kBeginTable << "<TD>";
    if (decl.IsQuery()) {
      os << "RESPOND ";

    } else if (decl.IsMessage()) {
      os << "SEND ";

    } else {
      os << "INSERT ";
    }
    os << ParsedDeclarationName(decl) << "</TD>";

    for (auto i = 0u, max_i = insert.Arity(); i < max_i; ++i) {
      os << "<TD port=\"c" << i << "\">"
         << insert.NthColumn(i).Variable() << "</TD>";
    }

    os << kEndTable << ">];\n";

    for (auto i = 0u, max_i = insert.Arity(); i < max_i; ++i) {
      const auto col = insert.NthColumn(i);
      const auto view = QueryView::Containing(col);
      os << "v" << insert.UniqueId() << ":c" << i << " -> "
         << "v" << view.UniqueId() << ":c" << col.UniqueId() << ";\n";
    }
  }

  for (auto tuple : query.Tuples()) {
    os << "v" << tuple.UniqueId() << " [ label=<" << kBeginTable
       << "<TD rowspan=\"2\">TUPLE</TD>";
    for (auto col : tuple.Columns()) {
      os << "<TD port=\"c" << col.UniqueId() << "\">"
         << col.Variable() << "</TD>";
    }
    os << "</TR><TR>";
    for (auto i = 0u; i < tuple.NumInputColumns(); ++i) {
      os << "<TD port=\"p" << i << "\"> </TD>";
    }
    os << kEndTable << ">];\n";

    // Link the input columns to their sources.
    for (auto i = 0u; i < tuple.NumInputColumns(); ++i) {
      auto col = tuple.NthInputColumn(i);
      auto view = QueryView::Containing(col);
      os << "v" << tuple.UniqueId() << ":p" << i << " -> v"
         << view.UniqueId() << ":c" << col.UniqueId() << ";\n";
    }
  }

  for (auto merge : query.Merges()) {
    os << "v" << merge.UniqueId() << " [ label=<" << kBeginTable
       << "<TD rowspan=\"2\">UNION</TD>";
    for (auto col : merge.Columns()) {
      os << "<TD port=\"c" << col.UniqueId() << "\">"
         << col.Variable() << "</TD>";
    }
    os << kEndTable << ">];\n";

    for (auto view : merge.MergedViews()) {
      os << "v" << merge.UniqueId() << " -> v" << view.UniqueId() << ";\n";
    }
  }

  os << "}\n";
  return os;
}

}  // namespace hyde
