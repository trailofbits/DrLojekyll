// Copyright 2019, Trail of Bits. All rights reserved.

#include <drlojekyll/Rel/Format.h>

#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Lex/Format.h>
#include <drlojekyll/Parse/Format.h>

#define DEBUG(...)

namespace hyde {
namespace {

static const char *kBeginTable = "<TABLE cellpadding=\"0\" cellspacing=\"0\" border=\"1\"><TR>";
static const char *kEndTable = "</TR></TABLE>";

}  // namespace

OutputStream &operator<<(OutputStream &os, Query query) {
  os << "digraph {\n"
     << "node [shape=none margin=0 nojustify=false labeljust=l font=courier];\n";

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
        os << "<TD>PULL</TD>";  // Pull from a constant.
      } else if (stream.IsGenerator()) {
        os << "<TD>PULL</TD>";  // Pull from a generator.
      } else {
        os << "<TD>PUSH</TD>";  // Input stream pushes.
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

    DEBUG(os << "</TR><TR><TD colspan=\"10\">" << select.DebugString() << "</TD>";)

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

    DEBUG(os << "</TR><TR><TD colspan=\"10\">" << constraint.DebugString() << "</TD>";)

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

  for (auto kv : query.KVIndices()) {
    os << "v" << kv.UniqueId() << " [ label=<" << kBeginTable;
    os << "<TD rowspan=\"2\">KEYS</TD>";
    for (auto col : kv.KeyColumns()) {
      os << "<TD port=\"c" << col.UniqueId() << "\">" << col.Variable() << "</TD>";
    }
    os << "<TD rowspan=\"2\">VALS</TD>";
    auto i = 0u;
    for (auto col : kv.ValueColumns()) {
      os << "<TD port=\"c" << col.UniqueId() << "\">"
         << kv.NthValueMergeFunctor(i).Name() << "(" << col.Variable()
         << ")</TD>";
    }
    os << "</TR><TR>";
    i = 0u;
    for (auto col : kv.InputKeyColumns()) {
      os << "<TD port=\"g" << (i++) << "\">" << col.Variable() << "</TD>";
    }
    for (auto col : kv.InputValueColumns()) {
      os << "<TD port=\"g" << (i++) << "\">" << col.Variable() << "</TD>";
    }
    DEBUG(os << "</TR><TR><TD colspan=\"10\">" << join.DebugString() << "</TD>";)

    os << kEndTable << ">];\n";

    i = 0u;
    for (auto col : kv.InputKeyColumns()) {
      const auto view = QueryView::Containing(col);
      os << "v" << kv.UniqueId() << ":g" << (i++) << " -> v"
         << view.UniqueId() << ":c" << col.UniqueId() << ";\n";
    }
    for (auto col : kv.InputValueColumns()) {
      const auto view = QueryView::Containing(col);
      os << "v" << kv.UniqueId() << ":g" << (i++) << " -> v"
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

    const auto num_pivots = join.NumPivotColumns();
    const auto num_outputs = join.NumMergedColumns();
    auto i = 0u;
    for (; i < num_pivots; ++i) {
      const auto pivot_set_size = join.NthInputPivotSet(i).size();
      const auto col = join.NthOutputPivotColumn(i);
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
      const auto col = join.NthOutputMergedColumn(i);
      os << "<TD port=\"c" << col.UniqueId() << "\">"
         << col.Variable() << "</TD>";
    }

    os << "</TR><TR>";

    auto j = 0u;
    for (i = 0u; i < num_pivots; ++i) {
      auto color = kColors[i];
      for (auto col : join.NthInputPivotSet(i)) {
        os << "<TD bgcolor=\"" << color << "\" port=\"p" << j << "\">"
           << col.Variable() << "</TD>";
        j++;
      }
    }

    for (i = 0u; i < num_outputs; ++i) {
      const auto col = join.NthInputMergedColumn(i);
      os << "<TD port=\"p" << j << "\">" << col.Variable() << "</TD>";
      j++;
    }

    DEBUG(os << "</TR><TR><TD colspan=\"10\">" << join.DebugString() << "</TD>";)

    os << kEndTable << ">];\n";

    // Link the joined columns to their sources.

    j = 0u;
    for (i = 0u; i < num_pivots; ++i) {
      for (auto col : join.NthInputPivotSet(i)) {
        const auto view = QueryView::Containing(col);
        os << "v" << join.UniqueId() << ":p" << j << " -> v"
           << view.UniqueId() << ":c" << col.UniqueId() << ";\n";
        j++;
      }
    }

    for (i = 0u; i < num_outputs; ++i) {
      const auto col = join.NthInputMergedColumn(i);
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

    DEBUG(os << "</TR><TR><TD colspan=\"10\">" << map.DebugString() << "</TD>";)

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

    DEBUG(os << "</TR><TR><TD colspan=\"10\">" << agg.DebugString() << "</TD>";)

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

    DEBUG(os << "</TR><TR><TD colspan=\"10\">" << insert.DebugString() << "</TD>";)

    os << kEndTable << ">];\n";

    for (auto i = 0u, max_i = insert.Arity(); i < max_i; ++i) {
      const auto col = insert.NthColumn(i);
      const auto view = QueryView::Containing(col);
      os << "v" << insert.UniqueId() << ":c" << i << " -> "
         << "v" << view.UniqueId() << ":c" << col.UniqueId() << ";\n";
    }

    for (auto select : query.Selects()) {
      if (select.IsRelation() && insert.IsRelation()) {
        if (select.Relation() == insert.Relation()) {
          os << "t" << insert.Relation().UniqueId()
             << " -> v" << insert.UniqueId()
             << " [color=grey];\n";
        }
      } else if (select.IsStream() && insert.IsStream()) {
        if (select.Stream() == insert.Stream()) {
          os << "t" << insert.Stream().UniqueId()
             << " -> v" << insert.UniqueId()
             << " [color=grey];\n";
        }
      }
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

    DEBUG(os << "</TR><TR><TD colspan=\"10\">" << tuple.DebugString() << "</TD>";)

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
