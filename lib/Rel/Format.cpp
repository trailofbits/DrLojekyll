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

    os << decl.Name() << "</TD>";
    for (auto i = 0u; i < arity; ++i) {
      auto param = decl.NthParameter(i);
      os << "<TD port=\"p" << i << "\">" << param.Name() << "</TD>";
    }

    os << kEndTable << ">];\n";
  }

  for (auto message : query.Messages()) {
    const auto decl = message.Declaration();
    const auto arity = decl.Arity();
    os << "t" << message.UniqueId() << " [ label=<" << kBeginTable
       << "<TD>MESSAGE</TD><TD>" << decl.Name() << "</TD>";
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
       << "<TD>GENERATOR</TD><TD>" << decl.Name() << "</TD>";
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
      os << "<TD>PULL</TD>";
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

    auto i = 0;
    for (auto pivot_col : join.PivotColumns()) {
      auto color = kColors[i];
      os << "<TD rowspan=\"3\" bgcolor=\"" << color << "\">"
         << pivot_col.Variable() << "</TD>";
      ++i;
    }

    os << "<TD rowspan=\"3\">JOIN</TD>";

    auto found = false;
    for (auto col : join.Columns()) {
      for (auto j = 0u; j < join.NumPivotColumns(); ++j) {
        auto pivot_col = join.NthPivotColumn(j);
        if (pivot_col == col) {
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

    if (false) assert(found);

    auto max_cols = std::max(join.NumInputColumns(), join.Arity());

    os << "</TR><TR><TD colspan=\"" << max_cols << "\">";
    auto sep = "";
    for (auto cond : join.Constraints()) {
      os << sep << cond.LHS().Variable() << " = " << cond.RHS().Variable();
      sep = "<BR />";
    }

    os << "</TD></TR><TR>";
    for (auto i = 0u; i < join.NumInputColumns(); ++i) {
      os << "<TD port=\"p" << i << "\"> &nbsp; </TD>";
    }

    os << kEndTable << ">];\n";

    // Link the joined columns to their sources.
    for (auto i = 0u; i < join.NumInputColumns(); ++i) {
      auto col = join.NthInputColumn(i);
      auto view = QueryView::Containing(col);
      os << "v" << join.UniqueId() << ":p" << i << " -> v"
         << view.UniqueId() << ":c" << col.UniqueId() << ";\n";
    }
  }

  for (auto map : query.Maps()) {
    os << "v" << map.UniqueId() << " [ label=<" << kBeginTable;
    os << "<TD rowspan=\"2\">MAP " << map.Functor().Name() << "</TD>";
    for (auto i = 0u; i < map.Arity(); ++i) {
      auto out_col = map.NthOutputColumn(i);
      os << "<TD port=\"c" << out_col.UniqueId() << "\">"
         << out_col.Variable() << "</TD>";
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

  for (auto insert : query.Inserts()) {
    os << "i" << insert.UniqueId() << " [ label=<" << kBeginTable
       << "<TD>INSERT " << insert.Relation().Declaration().Name() << "</TD>";

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
