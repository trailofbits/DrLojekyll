// Copyright 2019, Trail of Bits. All rights reserved.

#include <drlojekyll/DataFlow/Format.h>
#include <drlojekyll/Display/Format.h>
#include <drlojekyll/Lex/Format.h>
#include <drlojekyll/Parse/Format.h>

#define DEBUG(...) __VA_ARGS__

namespace hyde {
namespace {

static const char *kBeginTable =
    "<TABLE cellpadding=\"0\" cellspacing=\"0\" border=\"1\"><TR>";
static const char *kEndTable = "</TR></TABLE>";

static const char *kColors[] = {
    "antiquewhite", "aquamarine",   "cadetblue1", "chartreuse1",
    "chocolate1",   "deepskyblue2", "goldenrod1", "beige",
    "cadetblue3",   "floralwhite",  "gainsboro",  "darkseagreen1",
};

}  // namespace

OutputStream &operator<<(OutputStream &os, Query query) {
  os << "digraph {\n"
     << "node [shape=none margin=0 nojustify=false labeljust=l font=courier];\n";

  auto link_conds = [&](auto view_) {
    auto view = QueryView::From(view_);
    auto do_cond = [&](const char *port, QueryCondition cond) {
      os << "v" << view.UniqueId() << ":cc" << port << cond.UniqueId()
         << " -> c" << cond.UniqueId() << "  [color=purple];\n";
    };
    for (auto cond : view.PositiveConditions()) {
      assert(view.CanProduceDeletions());
      do_cond("p", cond);
    }
    for (auto cond : view.NegativeConditions()) {
      assert(view.CanProduceDeletions());
      do_cond("n", cond);
    }
  };

  auto do_conds = [&](int row_span, auto view_) {
    auto view = QueryView::From(view_);
    const auto pos_conds = view.PositiveConditions();
    const auto neg_conds = view.NegativeConditions();
    if (pos_conds.empty() && neg_conds.empty()) {
      return;
    }

    auto do_cond = [&](const char *prefix, const char *port,
                       QueryCondition cond) {
      os << "<TD rowspan=\"" << row_span << "\" port=\"cc" << port
         << cond.UniqueId() << "\">" << prefix;
      if (auto maybe_pred = cond.Predicate(); maybe_pred) {
        os << maybe_pred->Name();
      } else {
        os << cond.UniqueId();
      }
      os << "</TD>";
    };

    os << "<TD rowspan=\"" << row_span << "\">COND</TD>";
    for (auto cond : pos_conds) {
      do_cond("", "p", cond);
    }
    for (auto cond : neg_conds) {
      do_cond("!", "n", cond);
    }
  };

  auto do_col = [&](QueryColumn col) -> OutputStream & {
    if (col.IsConstant()) {
      os << "<B>" << QueryConstant::From(col).Literal() << "</B>";
    } else if (col.IsConstantRef()) {
      os << QueryConstant::From(col).Literal();
    } else {
      os << col.Variable();
    }
    return os;
  };

  for (auto relation : query.Relations()) {
    const auto decl = relation.Declaration();
    const auto arity = decl.Arity();
    if (!arity) {
      continue;  // This relation will be modeled by a CONDition.
    }
    os << "t" << relation.UniqueId() << " [ label=<" << kBeginTable
       << "<TD>RELATION ";

    os << ParsedDeclarationName(decl) << "</TD>";
    for (auto i = 0u; i < arity; ++i) {
      auto param = decl.NthParameter(i);
      os << "<TD port=\"p" << i << "\">" << param.Name() << "</TD>";
    }

    os << kEndTable << ">];\n";

    for (auto select : relation.Selects()) {
      auto color = QueryView::From(select).CanReceiveDeletions()
                       ? " [color=purple]"
                       : "";
      os << "v" << select.UniqueId() << " -> t" << relation.UniqueId() << color
         << ";\n";
    }

    for (auto insert : relation.Inserts()) {
      os << "t" << relation.UniqueId() << " -> v" << insert.UniqueId();

      if (insert.CanProduceDeletions()) {
        os << " [color=purple]";
      }

      os << ";\n";
    }
  }

  for (auto io : query.IOs()) {
    const auto decl = io.Declaration();
    const auto arity = decl.Arity();
    os << "t" << io.UniqueId() << " [ label=<" << kBeginTable << "<TD>"
       << QueryStream(io).KindName() << ' ' << ParsedDeclarationName(decl)
       << "</TD>";
    for (auto i = 0u; i < arity; ++i) {
      auto param = decl.NthParameter(i);
      os << "<TD port=\"p" << i << "\">" << param.Name() << "</TD>";
    }
    os << kEndTable << ">];\n";

    for (auto select : io.Receives()) {
      auto color = QueryView::From(select).CanReceiveDeletions()
                       ? " [color=purple]"
                       : "";
      os << "v" << select.UniqueId() << " -> t" << io.UniqueId() << color
         << ";\n";
    }

    for (auto insert : io.Transmits()) {
      auto color = QueryView::From(insert).CanReceiveDeletions()
                       ? " [color=purple]"
                       : "";
      os << "t" << io.UniqueId() << " -> v" << insert.UniqueId() << color
         << ";\n";
    }
  }

  for (auto constant : query.Constants()) {
    os << "t" << constant.UniqueId() << " [ label=<" << kBeginTable
       << "<TD port=\"p0\">" << constant.Literal() << "</TD>" << kEndTable
       << ">];\n";
  }

  for (auto cond : query.Conditions()) {

    os << "c" << cond.UniqueId() << " [ label=<" << kBeginTable
       << "<TD port=\"p0\">";
    if (auto maybe_pred = cond.Predicate(); maybe_pred) {
      os << maybe_pred->Name();
    } else {
      os << cond.UniqueId();
    }
    os << "</TD>";
    if (DEBUG(1 +) 0) {
      os << "</TR><TR><TD>depth=" << cond.Depth() << "</TD>";
    }
    os << kEndTable << ">];\n";

    for (auto view : cond.Setters()) {
      os << "c" << cond.UniqueId() << " -> v" << view.UniqueId()
         << " [color=purple];\n";
    }
  }

  for (auto select : query.Selects()) {
    os << "v" << select.UniqueId() << " [ label=<" << kBeginTable;
    do_conds(2, select);
    os << "<TD>" << QueryView(select).KindName() << "</TD>";

    auto i = 0u;
    for (auto col : select.Columns()) {
      os << "<TD port=\"c" << col.UniqueId() << "\">" << do_col(col) << "</TD>";
      ++i;
    }

    DEBUG(os << "</TR><TR><TD colspan=\"10\">" << select.DebugString(os)
             << "</TD>";)

    os << kEndTable << ">];\n";

    link_conds(select);
  }

  for (auto constraint : query.Compares()) {
    os << "v" << constraint.UniqueId() << " [ label=<" << kBeginTable;
    do_conds(2, constraint);

    const auto out_copied_cols = constraint.CopiedColumns();
    const auto in_copied_cols = constraint.InputCopiedColumns();
    if (!out_copied_cols.empty()) {
      os << "<TD rowspan=\"2\">COPY</TD>";
      for (auto col : out_copied_cols) {
        os << "<TD port=\"c" << col.UniqueId() << "\">" << do_col(col)
           << "</TD>";
      }
    }

    os << "<TD rowspan=\"2\">" << QueryView(constraint).KindName() << ' ';
    switch (constraint.Operator()) {
      case ComparisonOperator::kEqual: os << "eq"; break;
      case ComparisonOperator::kGreaterThan: os << "gt"; break;
      case ComparisonOperator::kLessThan: os << "lt"; break;
      case ComparisonOperator::kNotEqual: os << "neq"; break;
    }
    const auto lhs = constraint.LHS();
    const auto rhs = constraint.RHS();
    const auto input_lhs = constraint.InputLHS();
    const auto input_rhs = constraint.InputRHS();
    const auto input_lhs_view = QueryView::Containing(input_lhs);
    const auto input_rhs_view = QueryView::Containing(input_rhs);

    if (lhs == rhs) {
      os << "</TD><TD port=\"c" << lhs.UniqueId() << "\" colspan=\"2\">"
         << do_col(lhs);
    } else {
      os << "</TD><TD port=\"c" << lhs.UniqueId() << "\">" << do_col(lhs)
         << "</TD><TD port=\"c" << rhs.UniqueId() << "\">" << do_col(rhs);
    }

    os << "</TD></TR><TR>";

    if (!in_copied_cols.empty()) {
      auto i = 0u;
      for (auto col : in_copied_cols) {
        os << "<TD port=\"g" << i << "\">" << do_col(col) << "</TD>";
        ++i;
      }
    }

    os << "<TD port=\"p0\">" << do_col(input_lhs) << "</TD><TD port=\"p1\">"
       << do_col(input_rhs) << "</TD>";

    DEBUG(os << "</TR><TR><TD colspan=\"10\">" << constraint.DebugString(os)
             << "</TD>";)

    auto color = QueryView::From(constraint).CanReceiveDeletions()
                     ? " [color=purple]"
                     : "";

    os << kEndTable << ">];\n"
       << "v" << constraint.UniqueId() << ":p0 -> v"
       << input_lhs_view.UniqueId() << ":c" << input_lhs.UniqueId() << color
       << ";\n"
       << "v" << constraint.UniqueId() << ":p1 -> v"
       << input_rhs_view.UniqueId() << ":c" << input_rhs.UniqueId() << color
       << ";\n";

    for (auto i = 0u; i < in_copied_cols.size(); ++i) {
      const auto col = in_copied_cols[i];
      const auto view = QueryView::Containing(col);
      os << "v" << constraint.UniqueId() << ":g" << i << " -> v"
         << view.UniqueId() << ":c" << col.UniqueId() << color << ";\n";
    }

    link_conds(constraint);
  }

  for (auto kv : query.KVIndices()) {
    os << "v" << kv.UniqueId() << " [ label=<" << kBeginTable;
    do_conds(2, kv);
    os << "<TD rowspan=\"2\">KEYS</TD>";
    for (auto col : kv.KeyColumns()) {
      os << "<TD port=\"c" << col.UniqueId() << "\">" << do_col(col) << "</TD>";
    }
    os << "<TD rowspan=\"2\">VALS</TD>";
    auto i = 0u;
    for (auto col : kv.ValueColumns()) {
      os << "<TD port=\"c" << col.UniqueId() << "\">"
         << kv.NthValueMergeFunctor(i).Name() << "(" << do_col(col) << ")</TD>";
    }
    os << "</TR><TR>";
    i = 0u;
    for (auto col : kv.InputKeyColumns()) {
      os << "<TD port=\"g" << (i++) << "\">" << do_col(col) << "</TD>";
    }
    for (auto col : kv.InputValueColumns()) {
      os << "<TD port=\"g" << (i++) << "\">" << do_col(col) << "</TD>";
    }
    DEBUG(os << "</TR><TR><TD colspan=\"10\">" << kv.DebugString(os)
             << "</TD>";)

    os << kEndTable << ">];\n";

    auto color =
        QueryView::From(kv).CanReceiveDeletions() ? " [color=purple]" : "";

    i = 0u;
    for (auto col : kv.InputKeyColumns()) {
      const auto view = QueryView::Containing(col);
      os << "v" << kv.UniqueId() << ":g" << (i++) << " -> v" << view.UniqueId()
         << ":c" << col.UniqueId() << color << ";\n";
    }
    for (auto col : kv.InputValueColumns()) {
      const auto view = QueryView::Containing(col);
      os << "v" << kv.UniqueId() << ":g" << (i++) << " -> v" << view.UniqueId()
         << ":c" << col.UniqueId() << color << ";\n";
    }
    link_conds(kv);
  }

  for (auto join : query.Joins()) {
    os << "v" << join.UniqueId() << " [ label=<" << kBeginTable;
    do_conds(2, join);

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
         << pivot_set_size << "\" bgcolor=\"" << color << "\">" << do_col(col)
         << "</TD>";
    }

    os << "<TD rowspan=\"2\">" << QueryView(join).KindName() << "</TD>";

    for (i = 0u; i < num_outputs; ++i) {
      const auto col = join.NthOutputMergedColumn(i);
      os << "<TD port=\"c" << col.UniqueId() << "\">" << do_col(col) << "</TD>";
    }

    os << "</TR><TR>";

    auto j = 0u;
    for (i = 0u; i < num_pivots; ++i) {
      auto color = kColors[i];
      for (auto col : join.NthInputPivotSet(i)) {
        os << "<TD bgcolor=\"" << color << "\" port=\"p" << j << "\">"
           << do_col(col) << "</TD>";
        j++;
      }
    }

    for (i = 0u; i < num_outputs; ++i) {
      const auto col = join.NthInputMergedColumn(i);
      os << "<TD port=\"p" << j << "\">" << do_col(col) << "</TD>";
      j++;
    }

    DEBUG(os << "</TR><TR><TD colspan=\"10\">" << join.DebugString(os)
             << "</TD>";)

    os << kEndTable << ">];\n";


    // Link the joined columns to their sources.

    j = 0u;
    for (i = 0u; i < num_pivots; ++i) {
      for (auto col : join.NthInputPivotSet(i)) {
        const auto view = QueryView::Containing(col);
        auto color = view.CanProduceDeletions() ? " [color=purple]" : "";
        os << "v" << join.UniqueId() << ":p" << j << " -> v" << view.UniqueId()
           << ":c" << col.UniqueId() << color << ";\n";
        j++;
      }
    }

    for (i = 0u; i < num_outputs; ++i) {
      const auto col = join.NthInputMergedColumn(i);
      const auto view = QueryView::Containing(col);
      auto color = view.CanProduceDeletions() ? " [color=purple]" : "";
      os << "v" << join.UniqueId() << ":p" << j << " -> v" << view.UniqueId()
         << ":c" << col.UniqueId() << color << ";\n";
      j++;
    }

    link_conds(join);
  }

  for (auto map : query.Maps()) {
    os << "v" << map.UniqueId() << " [ label=<" << kBeginTable;
    do_conds(2, map);

    auto num_group = map.NumCopiedColumns();
    if (num_group) {
      os << "<TD rowspan=\"2\">COPY</TD>";
      for (auto col : map.CopiedColumns()) {
        os << "<TD port=\"c" << col.UniqueId() << "\">" << do_col(col)
           << "</TD>";
      }
    }

    os << "<TD rowspan=\"2\">" << QueryView(map).KindName() << ' ';
    if (!map.IsPositive()) {
      os << '!';
    }
    os << ParsedDeclarationName(map.Functor()) << "</TD>";
    for (auto col : map.MappedColumns()) {
      os << "<TD port=\"c" << col.UniqueId() << "\">" << do_col(col) << "</TD>";
    }

    const auto num_inputs = map.NumInputColumns();
    if (num_group + num_inputs) {
      os << "</TR><TR>";

      for (auto i = 0u; i < num_group; ++i) {
        const auto col = map.NthInputCopiedColumn(i);
        os << "<TD port=\"g" << i << "\">" << do_col(col) << "</TD>";
      }

      for (auto i = 0u; i < num_inputs; ++i) {
        const auto col = map.NthInputColumn(i);
        os << "<TD port=\"p" << i << "\">" << do_col(col) << "</TD>";
      }
    }

    //    // Empty space.
    //    if (auto diff = (map.Arity() - map.NumInputColumns())) {
    //      os << "<TD colspan=\"" << diff << "\"></TD>";
    //    }

    DEBUG(os << "</TR><TR><TD colspan=\"10\">" << map.DebugString(os)
             << "</TD>";)

    os << kEndTable << ">];\n";

    auto color = QueryView(map).CanReceiveDeletions() ? " [color=purple]" : "";

    for (auto i = 0u; i < num_group; ++i) {
      auto col = map.NthInputCopiedColumn(i);
      auto view = QueryView::Containing(col);
      os << "v" << map.UniqueId() << ":g" << i << " -> v" << view.UniqueId()
         << ":c" << col.UniqueId() << color << ";\n";
    }

    // Link the input columns to their sources.
    for (auto i = 0u; i < num_inputs; ++i) {
      auto col = map.NthInputColumn(i);
      auto view = QueryView::Containing(col);
      os << "v" << map.UniqueId() << ":p" << i << " -> v" << view.UniqueId()
         << ":c" << col.UniqueId() << color << ";\n";
    }

    link_conds(map);
  }

  for (auto agg : query.Aggregates()) {
    os << "v" << agg.UniqueId() << " [ label=<" << kBeginTable;
    do_conds(3, agg);
    os << "<TD rowspan=\"3\">" << QueryView(agg).KindName() << ' '
       << ParsedDeclarationName(agg.Functor()) << "</TD>";
    for (auto col : agg.Columns()) {
      os << "<TD port=\"c" << col.UniqueId() << "\">" << do_col(col) << "</TD>";
    }
    os << "</TR><TR>";
    auto num_group = agg.NumGroupColumns();
    if (num_group) {
      os << "<TD colspan=\"" << num_group << "\">GROUP</TD>";
    }
    auto num_config = agg.NumConfigurationColumns();
    if (num_config) {
      os << "<TD colspan=\"" << num_config << "\">CONFIG</TD>";
    }
    auto num_summ = agg.NumAggregateColumns();
    if (num_summ) {
      os << "<TD colspan=\"" << num_summ << "\">SUMMARIZE</TD>";
    }
    os << "</TR><TR>";
    for (auto i = 0u; i < num_group; ++i) {
      auto col = agg.NthInputGroupColumn(i);
      os << "<TD port=\"g1_" << i << "\">" << do_col(col) << "</TD>";
    }
    for (auto i = 0u; i < num_config; ++i) {
      auto col = agg.NthInputConfigurationColumn(i);
      os << "<TD port=\"g2_" << i << "\">" << do_col(col) << "</TD>";
    }
    for (auto i = 0u; i < num_summ; ++i) {
      auto col = agg.NthInputAggregateColumn(i);
      os << "<TD port=\"s" << i << "\">" << do_col(col) << "</TD>";
    }

    DEBUG(os << "</TR><TR><TD colspan=\"10\">" << agg.DebugString(os)
             << "</TD>";)

    os << kEndTable << ">];\n";

    auto color =
        QueryView::From(agg).CanReceiveDeletions() ? " [color=purple]" : "";

    for (auto i = 0u; i < num_group; ++i) {
      auto col = agg.NthInputGroupColumn(i);
      auto view = QueryView::Containing(col);
      os << "v" << agg.UniqueId() << ":g1_" << i << " -> v" << view.UniqueId()
         << ":c" << col.UniqueId() << color << ";\n";
    }
    for (auto i = 0u; i < num_config; ++i) {
      auto col = agg.NthInputConfigurationColumn(i);
      auto view = QueryView::Containing(col);
      os << "v" << agg.UniqueId() << ":g2_" << i << " -> v" << view.UniqueId()
         << ":c" << col.UniqueId() << color << ";\n";
    }
    for (auto i = 0u; i < num_summ; ++i) {
      auto col = agg.NthInputAggregateColumn(i);
      auto view = QueryView::Containing(col);
      os << "v" << agg.UniqueId() << ":s" << i << " -> v" << view.UniqueId()
         << ":c" << col.UniqueId() << color << ";\n";
    }

    link_conds(agg);
  }

  for (auto neg : query.Negations()) {
    os << "v" << neg.UniqueId() << " [ label=<" << kBeginTable;
    do_conds(2, neg);

    auto num_group = neg.NumCopiedColumns();
    if (num_group) {
      os << "<TD rowspan=\"2\">COPY</TD>";
      for (auto col : neg.CopiedColumns()) {
        os << "<TD port=\"c" << col.UniqueId() << "\">" << do_col(col)
           << "</TD>";
      }
    }

    os << "<TD rowspan=\"2\">" << QueryView(neg).KindName() << "</TD>";
    for (auto col : neg.NegatedColumns()) {
      os << "<TD port=\"c" << col.UniqueId() << "\">" << do_col(col) << "</TD>";
    }

    const auto num_inputs = neg.NumInputColumns();
    os << "</TR><TR>";

    for (auto i = 0u; i < num_group; ++i) {
      const auto col = neg.NthInputCopiedColumn(i);
      os << "<TD port=\"g" << i << "\">" << do_col(col) << "</TD>";
    }

    for (auto i = 0u; i < num_inputs; ++i) {
      const auto col = neg.NthInputColumn(i);
      os << "<TD port=\"p" << i << "\">" << do_col(col) << "</TD>";
    }

    DEBUG(os << "</TR><TR><TD colspan=\"10\">" << neg.DebugString(os)
             << "</TD>";)

    os << kEndTable << ">];\n";

    auto color = " [color=purple]";

    for (auto i = 0u; i < num_group; ++i) {
      auto col = neg.NthInputCopiedColumn(i);
      auto view = QueryView::Containing(col);
      os << "v" << neg.UniqueId() << ":g" << i << " -> v" << view.UniqueId()
         << ":c" << col.UniqueId() << color << ";\n";
    }

    // Link the input columns to their sources.
    for (auto i = 0u; i < num_inputs; ++i) {
      auto col = neg.NthInputColumn(i);
      auto view = QueryView::Containing(col);
      os << "v" << neg.UniqueId() << ":p" << i << " -> v" << view.UniqueId()
         << ":c" << col.UniqueId() << color << ";\n";
    }

    os << "v" << neg.UniqueId() << " -> v"
       << neg.NegatedView().UniqueId() << color << ";\n";

    link_conds(neg);
  }

  for (auto insert : query.Inserts()) {
    const auto decl = insert.Declaration();
    os << "v" << insert.UniqueId() << " [ label=<" << kBeginTable;
    do_conds(2, insert);
    os << "<TD>" << QueryView(insert).KindName() << ' '
       << ParsedDeclarationName(decl) << "</TD>";

    for (auto i = 0u, max_i = insert.NumInputColumns(); i < max_i; ++i) {
      os << "<TD port=\"c" << i << "\">" << do_col(insert.NthInputColumn(i))
         << "</TD>";
    }

    DEBUG(os << "</TR><TR><TD colspan=\"10\">" << insert.DebugString(os)
             << "</TD>";)

    os << kEndTable << ">];\n";

    auto color =
        QueryView::From(insert).CanReceiveDeletions() ? " [color=purple]" : "";

    for (auto i = 0u, max_i = insert.NumInputColumns(); i < max_i; ++i) {
      const auto col = insert.NthInputColumn(i);
      const auto view = QueryView::Containing(col);
      os << "v" << insert.UniqueId() << ":c" << i << " -> "
         << "v" << view.UniqueId() << ":c" << col.UniqueId() << color << ";\n";
    }

    link_conds(insert);
  }

  for (auto view : query.Tuples()) {
    os << "v" << view.UniqueId() << " [ label=<" << kBeginTable;
    do_conds(2, view);
    os << "<TD rowspan=\"2\">" << QueryView(view).KindName() << "</TD>";
    for (auto col : view.Columns()) {
      os << "<TD port=\"c" << col.UniqueId() << "\">" << do_col(col) << "</TD>";
    }
    os << "</TR><TR>";

    for (auto i = 0u; i < view.NumInputColumns(); ++i) {
      os << "<TD port=\"p" << i << "\">" << do_col(view.NthInputColumn(i))
         << "</TD>";
    }

    DEBUG(os << "</TR><TR><TD colspan=\"10\">" << view.DebugString(os)
             << "</TD>";)

    os << kEndTable << ">];\n";

    auto color =
        QueryView::From(view).CanReceiveDeletions() ? " [color=purple]" : "";

    // Link the input columns to their sources.
    for (auto i = 0u; i < view.NumInputColumns(); ++i) {
      auto col = view.NthInputColumn(i);
      auto input_view = QueryView::Containing(col);
      os << "v" << view.UniqueId() << ":p" << i << " -> v"
         << input_view.UniqueId() << ":c" << col.UniqueId() << color << ";\n";
    }

    link_conds(view);
  }

  for (auto view : query.Deletes()) {
    os << "v" << view.UniqueId() << " [ label=<" << kBeginTable;
    do_conds(2, view);
    os << "<TD rowspan=\"2\">" << QueryView(view).KindName() << "</TD>";
    for (auto col : view.Columns()) {
      os << "<TD port=\"c" << col.UniqueId() << "\">" << do_col(col) << "</TD>";
    }
    os << "</TR><TR>";

    for (auto i = 0u; i < view.NumInputColumns(); ++i) {
      os << "<TD port=\"p" << i << "\">" << do_col(view.NthInputColumn(i))
         << "</TD>";
    }

    DEBUG(os << "</TR><TR><TD colspan=\"10\">" << view.DebugString(os)
             << "</TD>";)

    os << kEndTable << ">];\n";

    auto color =
        QueryView::From(view).CanReceiveDeletions() ? " [color=purple]" : "";

    // Link the input columns to their sources.
    for (auto i = 0u; i < view.NumInputColumns(); ++i) {
      auto col = view.NthInputColumn(i);
      auto input_view = QueryView::Containing(col);
      os << "v" << view.UniqueId() << ":p" << i << " -> v"
         << input_view.UniqueId() << ":c" << col.UniqueId() << color << ";\n";
    }

    link_conds(view);
  }

  for (auto merge : query.Merges()) {
    os << "v" << merge.UniqueId() << " [ label=<" << kBeginTable;
    do_conds(2, merge);
    os << "<TD rowspan=\"2\">" << QueryView(merge).KindName() << "</TD>";
    for (auto col : merge.Columns()) {
      os << "<TD port=\"c" << col.UniqueId() << "\">" << do_col(col) << "</TD>";
    }
    DEBUG(os << "</TR><TR><TD colspan=\"10\">" << merge.DebugString(os)
             << "</TD>";)
    os << kEndTable << ">];\n";

    for (auto view : merge.MergedViews()) {
      auto color = view.CanProduceDeletions() ? " [color=purple]" : "";
      os << "v" << merge.UniqueId() << " -> v" << view.UniqueId() << color
         << ";\n";
    }

    link_conds(merge);
  }

  os << "}\n";
  return os;
}

}  // namespace hyde
